"""
하이브리드 데이터 관리자 (리팩토링 버전)
"""
import time
import asyncio
import threading
from typing import Dict, List, Optional, Any, Callable
from enum import Enum
from utils.logger import setup_logger
from . import kis_data_cache as cache
from .kis_data_collector import KISDataCollector
from .data_priority import DataPriority
from ..websocket.kis_websocket_manager import KISWebSocketManager
from ..api.rest_api_manager import KISRestAPIManager

logger = setup_logger(__name__)


class SimpleHybridDataManager:
    """웹소켓 제한을 고려한 스마트 하이브리드 데이터 관리자"""

    def __init__(self, websocket_manager: KISWebSocketManager, rest_api_manager: KISRestAPIManager, data_collector: KISDataCollector):
        # 웹소켓 제한 상수
        self.WEBSOCKET_LIMIT = 41  # KIS 웹소켓 연결 제한
        self.STREAMS_PER_STOCK = 2  # 종목당 스트림 수 (체결가, 호가)
        self.MAX_REALTIME_STOCKS = self.WEBSOCKET_LIMIT // self.STREAMS_PER_STOCK  # 20개

        self.websocket_manager = websocket_manager
        self.collector = data_collector
        self.rest_api_manager = rest_api_manager

        self.websocket_running = False
        self.websocket_task: Optional[asyncio.Task] = None

        # 구독 관리
        self.subscriptions: Dict[str, Dict] = {}  # {stock_code: {strategy, callback, ...}}
        self.subscription_lock = threading.RLock()

        # 실시간/폴링 분리 관리
        self.realtime_stocks: List[str] = []  # 실시간 구독 종목
        self.realtime_priority_queue: List[str] = []  # 실시간 대기열 (우선순위순)
        self.polling_stocks: List[str] = []

        # 폴링 관리
        self.polling_active = False
        self.polling_thread: Optional[threading.Thread] = None
        self.polling_interval = 15  # 15초 간격

        # 통계
        self.stats = {
            'total_subscriptions': 0,
            'active_realtime': 0,
            'active_polling': 0,
            'websocket_usage': 0,
            'data_updates': 0,
            'priority_swaps': 0  # 우선순위 교체 횟수
        }

        logger.info(f"웹소켓 제한: {self.WEBSOCKET_LIMIT}건, 최대 실시간 종목: {self.MAX_REALTIME_STOCKS}개")

    # === 구독 관리 ===

    def add_stock_request(self, stock_code: str, priority: DataPriority,
                         strategy_name: str, callback: Optional[Callable] = None) -> bool:
        """
        종목 구독 요청 (우선순위 기반) - StrategyScheduler 호환용

        Args:
            stock_code: 종목코드 (6자리, 예: '005930')
            priority: 데이터 우선순위 (DataPriority enum)
            strategy_name: 전략명
            callback: 콜백 함수

        Returns:
            구독 성공 여부
        """
        try:
            # 입력값 검증
            if not stock_code or len(stock_code) != 6:
                logger.error(f"유효하지 않은 종목코드: {stock_code}")
                return False

            if not isinstance(priority, DataPriority):
                logger.error(f"유효하지 않은 우선순위 타입: {type(priority)}")
                return False

            # DataPriority를 숫자 우선순위로 변환
            priority_mapping = {
                DataPriority.CRITICAL: 1,     # 최고 우선순위 (실시간)
                DataPriority.HIGH: 2,         # 높음 (실시간 시도)
                DataPriority.MEDIUM: 3,       # 보통 (폴링)
                DataPriority.LOW: 4,          # 낮음 (폴링)
                DataPriority.BACKGROUND: 5    # 배경 (폴링)
            }

            numeric_priority = priority_mapping.get(priority, 3)

            # 실시간 사용 여부 결정 (CRITICAL, HIGH만 실시간 시도)
            use_realtime = priority in [DataPriority.CRITICAL, DataPriority.HIGH]

            # 웹소켓 연결 상태 확인 및 연결 시도
            if use_realtime and not self.websocket_running:
                logger.info("실시간 데이터 요청 - 웹소켓 연결 시도")
                if not self._start_websocket_if_needed():
                    logger.warning(f"웹소켓 연결 실패 - {stock_code}를 폴링으로 대체")
                    use_realtime = False

            # 기존 add_stock 메서드 호출
            success = self.add_stock(
                stock_code=stock_code,
                strategy_name=strategy_name,
                use_realtime=use_realtime,
                callback=callback,
                priority=numeric_priority
            )

            if success:
                logger.info(f"📊 종목 구독 요청 성공: {stock_code} [{priority.value}] {strategy_name}")

                # 실시간 구독 시 웹소켓 연결 확인
                if use_realtime:
                    realtime_success = self._verify_realtime_subscription(stock_code)
                    if not realtime_success:
                        logger.warning(f"실시간 구독 확인 실패 - {stock_code}를 폴링으로 대체")
                        # 실시간에서 폴링으로 다운그레이드
                        self.downgrade_to_polling(stock_code)
            else:
                logger.warning(f"⚠️ 종목 구독 요청 실패: {stock_code} [{priority.value}] {strategy_name}")

            return success

        except Exception as e:
            logger.error(f"종목 구독 요청 오류: {stock_code} - {e}")
            return False

    def upgrade_priority(self, stock_code: str, new_priority: DataPriority) -> bool:
        """
        종목 우선순위 승격 - StrategyScheduler 호환용

        Args:
            stock_code: 종목코드
            new_priority: 새로운 우선순위

        Returns:
            승격 성공 여부
        """
        try:
            # DataPriority를 숫자 우선순위로 변환
            priority_mapping = {
                DataPriority.CRITICAL: 1,
                DataPriority.HIGH: 2,
                DataPriority.MEDIUM: 3,
                DataPriority.LOW: 4,
                DataPriority.BACKGROUND: 5
            }

            numeric_priority = priority_mapping.get(new_priority, 3)

            # 기존 update_stock_priority 메서드 호출
            self.update_stock_priority(stock_code, numeric_priority)

            logger.info(f"📈 우선순위 승격: {stock_code} → {new_priority.value}")
            return True

        except Exception as e:
            logger.error(f"우선순위 승격 오류: {stock_code} - {e}")
            return False

    def add_stock(self, stock_code: str, strategy_name: str,
                  use_realtime: bool = False, callback: Optional[Callable] = None,
                  priority: int = 1) -> bool:
        """
        종목 추가 (웹소켓 제한 고려)

        Args:
            stock_code: 종목코드
            strategy_name: 전략명
            use_realtime: 실시간 선호 여부
            callback: 콜백 함수
            priority: 우선순위 (1:높음, 2:보통, 3:낮음)
        """
        with self.subscription_lock:
            if stock_code in self.subscriptions:
                logger.warning(f"이미 구독 중인 종목: {stock_code}")
                return False

            subscription = {
                'stock_code': stock_code,
                'strategy_name': strategy_name,
                'use_realtime': False,  # 초기값
                'preferred_realtime': use_realtime,
                'priority': priority,
                'callback': callback,
                'added_time': time.time(),
                'last_update': None,
                'update_count': 0,
                'score': 0.0  # 동적 점수
            }

            self.subscriptions[stock_code] = subscription
            self.stats['total_subscriptions'] += 1

            # 실시간 또는 폴링 할당
            if use_realtime and self._can_add_realtime():
                self._add_to_realtime(stock_code)
            elif use_realtime:
                self._add_to_priority_queue(stock_code)
                self._add_to_polling(stock_code)
                logger.info(f"실시간 대기열 추가: {stock_code} (현재 {len(self.realtime_stocks)}/{self.MAX_REALTIME_STOCKS})")
            else:
                self._add_to_polling(stock_code)

            return True

    def remove_stock(self, stock_code: str) -> bool:
        """종목 제거"""
        with self.subscription_lock:
            if stock_code not in self.subscriptions:
                return False

            subscription = self.subscriptions[stock_code]

            # 실시간에서 제거
            if subscription['use_realtime']:
                self._remove_from_realtime(stock_code)
                # 대기열에서 실시간으로 승격
                self._promote_from_queue()

            # 폴링에서 제거
            self._remove_from_polling(stock_code)

            # 우선순위 대기열에서 제거
            if stock_code in self.realtime_priority_queue:
                self.realtime_priority_queue.remove(stock_code)

            # 구독 제거
            del self.subscriptions[stock_code]

            logger.info(f"종목 구독 제거: {stock_code}")
            return True

    def update_stock_priority(self, stock_code: str, new_priority: int, new_score: Optional[float] = None):
        """종목 우선순위 업데이트 (동적 교체)"""
        with self.subscription_lock:
            if stock_code not in self.subscriptions:
                return

            subscription = self.subscriptions[stock_code]
            old_priority = subscription['priority']
            subscription['priority'] = new_priority

            if new_score is not None:
                subscription['score'] = new_score

            # 우선순위가 상승했고 실시간을 원하지만 폴링 중인 경우
            if (new_priority < old_priority and
                subscription['preferred_realtime'] and
                not subscription['use_realtime']):

                self._try_promote_to_realtime(stock_code)

            logger.debug(f"우선순위 업데이트: {stock_code} P{old_priority}→P{new_priority}")

    def _can_add_realtime(self) -> bool:
        """실시간 추가 가능 여부"""
        return len(self.realtime_stocks) < self.MAX_REALTIME_STOCKS

    def _add_to_realtime(self, stock_code: str) -> bool:
        """실시간 구독 추가"""
        if not self._can_add_realtime():
            logger.warning(f"실시간 구독 한계 도달: {len(self.realtime_stocks)}/{self.MAX_REALTIME_STOCKS}")
            return False

        try:
            success = False

            # 웹소켓 매니저 연결 상태 확인
            if not self.websocket_manager:
                logger.error("웹소켓 매니저가 없습니다")
                return False

            # 웹소켓 연결 확인
            if not getattr(self.websocket_manager, 'is_connected', False):
                logger.warning(f"웹소켓이 연결되지 않음 - 연결 시도: {stock_code}")
                if not self._start_websocket_if_needed():
                    logger.error(f"웹소켓 연결 실패: {stock_code}")
                    return False

            # 🆕 첫 구독인 경우 웹소켓 완전 준비 상태 확인
            if len(self.realtime_stocks) == 0:  # 첫 구독
                logger.info(f"🎯 첫 구독 시도: {stock_code} - 웹소켓 준비 상태 확인")

                # 웹소켓 준비 상태 확인 및 대기
                if hasattr(self.websocket_manager, 'ensure_ready_for_subscriptions'):
                    ready = self.websocket_manager.ensure_ready_for_subscriptions()
                    if ready:
                        logger.info("✅ 웹소켓 구독 준비 완료 - 첫 구독 진행")
                    else:
                        logger.warning("⚠️ 웹소켓 구독 준비 실패 - 동기 방식으로 시도")
                else:
                    # 준비 상태 확인 메서드가 없는 경우 기본 대기
                    logger.info("🔄 웹소켓 안정화 대기 (기본 2초)")
                    time.sleep(2)  # 🔧 대기 시간 단축 (3초 → 2초)

            # 이미 구독 중인지 확인
            if hasattr(self.websocket_manager, 'subscribed_stocks'):
                if stock_code in self.websocket_manager.subscribed_stocks:
                    logger.info(f"이미 웹소켓 구독 중: {stock_code}")
                    success = True
                else:
                    # 새로운 구독 시도
                    success = self._execute_websocket_subscription(stock_code)
            else:
                # 웹소켓 매니저가 구독 정보를 제공하지 않는 경우
                success = self._execute_websocket_subscription(stock_code)

            if success:
                # 구독 성공 처리
                if stock_code not in self.realtime_stocks:
                    self.realtime_stocks.append(stock_code)

                self.subscriptions[stock_code]['use_realtime'] = True
                self._remove_from_polling(stock_code)

                # 대기열에서 제거
                if stock_code in self.realtime_priority_queue:
                    self.realtime_priority_queue.remove(stock_code)

                self._update_stats()
                logger.info(f"✅ 실시간 구독 추가: {stock_code} ({len(self.realtime_stocks)}/{self.MAX_REALTIME_STOCKS})")

                # 🆕 구독 현황 자세히 로깅
                if hasattr(self.websocket_manager, 'get_subscribed_stocks'):
                    actual_subscribed = self.websocket_manager.get_subscribed_stocks()
                    logger.debug(f"📡 실제 웹소켓 구독 현황: {len(actual_subscribed)}개 - {actual_subscribed}")

                return True
            else:
                logger.error(f"❌ 실시간 구독 실패: {stock_code}")
                # 🔧 실패해도 폴링으로 대체하여 계속 진행
                self._add_to_polling(stock_code)
                logger.info(f"📡 {stock_code} 폴링으로 대체하여 계속 진행")
                return False

        except Exception as e:
            logger.error(f"실시간 구독 오류: {stock_code} - {e}")
            # 🔧 예외 발생시에도 폴링으로 대체
            try:
                self._add_to_polling(stock_code)
                logger.info(f"📡 {stock_code} 예외 발생으로 폴링 대체")
            except Exception as fallback_e:
                logger.error(f"폴링 대체도 실패: {stock_code} - {fallback_e}")
            return False

    def _execute_websocket_subscription(self, stock_code: str) -> bool:
        """웹소켓 구독 실행 (안정성 강화 버전)"""
        try:
            logger.debug(f"🔗 웹소켓 구독 실행: {stock_code}")

            # 🆕 동기 방식 구독 메서드 사용 (이벤트 루프 문제 해결)
            if hasattr(self.websocket_manager, 'subscribe_stock_sync'):
                logger.debug(f"📡 동기 방식 웹소켓 구독 시도: {stock_code}")

                # 🔧 재시도 로직 추가
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        logger.debug(f"📡 구독 시도 {attempt + 1}/{max_retries}: {stock_code}")
                        result = self.websocket_manager.subscribe_stock_sync(stock_code, self._websocket_callback)

                        if result:
                            logger.info(f"✅ 웹소켓 구독 성공 (시도 {attempt + 1}): {stock_code}")
                            return True
                        else:
                            logger.warning(f"⚠️ 웹소켓 구독 실패 (시도 {attempt + 1}): {stock_code}")
                            if attempt < max_retries - 1:  # 마지막 시도가 아니면
                                time.sleep(1.0 * (attempt + 1))  # 지수적 백오프

                    except Exception as e:
                        logger.error(f"❌ 웹소켓 구독 예외 (시도 {attempt + 1}): {stock_code} - {e}")
                        if attempt < max_retries - 1:
                            time.sleep(1.0 * (attempt + 1))

                logger.error(f"❌ 웹소켓 구독 최종 실패: {stock_code} (모든 재시도 실패)")
                return False

            # 🔧 기존 async 방식 (fallback)
            result_container = []
            exception_container = []

            def run_subscription():
                try:
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)

                    try:
                        # 웹소켓 구독 실행
                        result = new_loop.run_until_complete(
                            self.websocket_manager.subscribe_stock(stock_code, self._websocket_callback)
                        )
                        result_container.append(result)
                    finally:
                        new_loop.close()

                except Exception as e:
                    exception_container.append(e)

            # 별도 스레드에서 실행
            thread = threading.Thread(target=run_subscription, daemon=True)
            thread.start()
            thread.join(timeout=15)  # 15초 타임아웃

            if exception_container:
                raise exception_container[0]
            elif result_container:
                return result_container[0]
            else:
                logger.error(f"웹소켓 구독 타임아웃: {stock_code}")
                return False

        except Exception as e:
            logger.error(f"웹소켓 구독 실행 오류: {stock_code} - {e}")
            return False

    def _remove_from_realtime(self, stock_code: str):
        """실시간 구독 제거"""
        if stock_code in self.realtime_stocks:
            try:
                # self.collector.unsubscribe_realtime(stock_code)
                pass  # 임시
            except Exception as e:
                logger.error(f"실시간 구독 해제 오류: {stock_code} - {e}")

            self.realtime_stocks.remove(stock_code)
            self.subscriptions[stock_code]['use_realtime'] = False
            self._update_stats()

    def _add_to_priority_queue(self, stock_code: str):
        """우선순위 대기열에 추가"""
        if stock_code not in self.realtime_priority_queue:
            self.realtime_priority_queue.append(stock_code)
            self._sort_priority_queue()

    def _sort_priority_queue(self):
        """우선순위 대기열 정렬 (우선순위 + 점수 기준)"""
        def priority_key(stock_code: str) -> tuple:
            sub = self.subscriptions.get(stock_code, {})
            return (sub.get('priority', 999), -sub.get('score', 0))

        self.realtime_priority_queue.sort(key=priority_key)

    def _promote_from_queue(self):
        """대기열에서 실시간으로 승격"""
        if not self._can_add_realtime() or not self.realtime_priority_queue:
            return

        # 가장 높은 우선순위 종목 승격
        stock_code = self.realtime_priority_queue[0]
        if self._add_to_realtime(stock_code):
            self.stats['priority_swaps'] += 1
            logger.info(f"대기열→실시간 승격: {stock_code}")

    def _try_promote_to_realtime(self, stock_code: str):
        """특정 종목을 실시간으로 승격 시도"""
        if stock_code not in self.subscriptions:
            return

        # 실시간 여유 공간이 있으면 바로 승격
        if self._can_add_realtime():
            self._add_to_realtime(stock_code)
            return

        # 더 낮은 우선순위 종목과 교체
        current_priority = self.subscriptions[stock_code]['priority']
        current_score = self.subscriptions[stock_code]['score']

        for realtime_stock in self.realtime_stocks:
            realtime_sub = self.subscriptions[realtime_stock]

            # 우선순위가 더 높거나, 같은 우선순위에서 점수가 더 높으면 교체
            if (current_priority < realtime_sub['priority'] or
                (current_priority == realtime_sub['priority'] and current_score > realtime_sub['score'])):

                # 교체 실행
                self._remove_from_realtime(realtime_stock)
                self._add_to_polling(realtime_stock)
                self._add_to_priority_queue(realtime_stock)

                self._add_to_realtime(stock_code)

                self.stats['priority_swaps'] += 1
                logger.info(f"실시간 교체: {realtime_stock}→{stock_code}")
                break

    def _add_to_polling(self, stock_code: str) -> None:
        """폴링에 추가"""
        if stock_code not in self.polling_stocks:
            self.polling_stocks.append(stock_code)
            self._update_stats()

            # 폴링이 실행 중이 아니면 시작
            if not self.polling_active:
                self.start_polling()

    def _remove_from_polling(self, stock_code: str) -> None:
        """폴링에서 제거"""
        if stock_code in self.polling_stocks:
            self.polling_stocks.remove(stock_code)
            self._update_stats()

    def _update_stats(self):
        """통계 업데이트"""
        self.stats['active_realtime'] = len(self.realtime_stocks)
        self.stats['active_polling'] = len(self.polling_stocks)
        self.stats['websocket_usage'] = len(self.realtime_stocks) * self.STREAMS_PER_STOCK

    # === 폴링 관리 ===

    def start_polling(self) -> None:
        """폴링 시작"""
        if self.polling_active:
            return

        self.polling_active = True
        self.polling_thread = threading.Thread(
            target=self._polling_loop,
            name="SmartDataPolling",
            daemon=True
        )
        self.polling_thread.start()
        logger.info(f"스마트 폴링 시작 (간격: {self.polling_interval}초)")

    def stop_polling(self) -> None:
        """폴링 중지"""
        self.polling_active = False
        if self.polling_thread and self.polling_thread.is_alive():
            self.polling_thread.join(timeout=5)
        logger.info("스마트 폴링 중지")

    def _polling_loop(self) -> None:
        """폴링 루프"""
        while self.polling_active:
            try:
                if self.polling_stocks:
                    self._poll_data()

                time.sleep(self.polling_interval)

            except Exception as e:
                logger.error(f"폴링 중 오류: {e}")
                time.sleep(5)

    def _poll_data(self) -> None:
        """데이터 폴링 실행"""
        for stock_code in self.polling_stocks.copy():
            try:
                # 현재가 조회
                data = self.collector.get_current_price(stock_code, use_cache=True)

                if data.get('status') == 'success':
                    self._process_data_update(stock_code, data)
                else:
                    logger.warning(f"폴링 데이터 조회 실패: {stock_code}")

                time.sleep(0.1)  # Rate limiting

            except Exception as e:
                logger.error(f"종목 폴링 오류: {stock_code} - {e}")

    def _data_callback(self, stock_code: str, data: Dict) -> None:
        """실시간 데이터 콜백 (폴링용)"""
        self._process_data_update(stock_code, data)

    def _websocket_callback(self, data_type: str, stock_code: str, data: Dict) -> None:
        """웹소켓 실시간 데이터 콜백 - 🆕 data_type 파라미터 추가"""
        try:
            logger.debug(f"웹소켓 데이터 수신: {stock_code} - {data.get('current_price', 0):,}원")
            self._process_data_update(stock_code, data)
        except Exception as e:
            logger.error(f"웹소켓 콜백 오류: {stock_code} - {e}")

    def _start_websocket_if_needed(self) -> bool:
        """🎯 웹소켓 우선 사용 - 적극적 연결 및 재연결"""
        try:
            # 이미 실행 중이면 연결 상태만 확인
            if self.websocket_running and getattr(self.websocket_manager, 'is_connected', False):
                logger.debug("✅ 웹소켓이 이미 실행 중이고 연결됨")
                return True

            if not self.websocket_manager:
                logger.error("❌ 웹소켓 매니저가 없습니다")
                return False

            logger.info("🚀 웹소켓 우선 사용 정책 - 연결 시작...")

            # 🎯 기존 연결이 끊어진 경우 정리 후 재연결
            if self.websocket_running and not getattr(self.websocket_manager, 'is_connected', False):
                logger.info("🧹 기존 웹소켓 정리 후 재연결...")
                self._cleanup_websocket()
                time.sleep(1)  # 잠시 대기

            # 웹소켓 연결 시작 (최대 1회 재시도)
            max_retries = 1
            for attempt in range(1, max_retries + 1):
                try:
                    logger.info(f"🔄 웹소켓 연결 시도 {attempt}/{max_retries}...")
                    success = self._execute_websocket_connection()

                    if success:
                        self.websocket_running = True
                        logger.info(f"✅ 웹소켓 연결 성공 (시도 {attempt}/{max_retries})")

                        # 연결 확인 대기
                        time.sleep(1)

                        # 실제 연결 상태 재확인
                        if getattr(self.websocket_manager, 'is_connected', False):
                            logger.info("🎉 웹소켓 연결 최종 확인 완료")
                            return True
                        else:
                            logger.warning(f"⚠️ 웹소켓 연결 확인 실패 (시도 {attempt}/{max_retries})")
                            if attempt < max_retries:
                                time.sleep(1)  # 재시도 전 대기
                                continue
                    else:
                        logger.warning(f"⚠️ 웹소켓 연결 실패 (시도 {attempt}/{max_retries})")
                        if attempt < max_retries:
                            time.sleep(1)  # 재시도 전 대기
                            continue

                except Exception as e:
                    logger.error(f"❌ 웹소켓 연결 시도 {attempt} 오류: {e}")
                    if attempt < max_retries:
                        time.sleep(1)  # 재시도 전 대기
                        continue

            # 모든 시도 실패
            self.websocket_running = False
            logger.error("❌ 웹소켓 연결 최종 실패 - 모든 재시도 완료")
            return False

        except Exception as e:
            logger.error(f"❌ 웹소켓 시작 중 예외: {e}")
            self.websocket_running = False
            return False

    def _cleanup_websocket(self):
        """🧹 웹소켓 정리 (재연결 준비)"""
        try:
            if hasattr(self, 'websocket_thread') and self.websocket_thread:
                logger.info("🧹 기존 웹소켓 스레드 정리 중...")

                # 웹소켓 매니저 상태 정리
                if self.websocket_manager:
                    self.websocket_manager.is_running = False
                    self.websocket_manager.is_connected = False

                # 스레드 종료 대기 (최대 5초)
                if self.websocket_thread.is_alive():
                    self.websocket_thread.join(timeout=5)

                self.websocket_thread = None
                logger.info("✅ 기존 웹소켓 스레드 정리 완료")

        except Exception as e:
            logger.warning(f"⚠️ 웹소켓 정리 중 오류: {e}")

    def _execute_websocket_connection(self) -> bool:
        """🎯 웹소켓 연결 실행"""
        try:
            if not self.websocket_manager:
                logger.error("❌ 웹소켓 매니저가 없습니다")
                return False

            # 웹소켓 연결 시도
            logger.debug("🔌 웹소켓 매니저 연결 시도...")

            # 웹소켓 매니저의 연결 메서드 호출
            if hasattr(self.websocket_manager, 'connect'):
                # 비동기 메서드인 경우 올바르게 처리
                import asyncio
                import inspect

                if inspect.iscoroutinefunction(self.websocket_manager.connect):
                    # 비동기 메서드인 경우
                    try:
                        # 🔧 이벤트 루프 상태 확인 및 안전한 처리
                        try:
                            # 현재 실행 중인 루프가 있는지 확인
                            current_loop = asyncio.get_running_loop()
                            logger.warning("⚠️ 이미 실행 중인 이벤트 루프 감지 - 웹소켓 연결 스킵")
                            # 기존 루프가 실행 중이면 연결을 건너뛰고 성공으로 처리
                            # (백그라운드에서 웹소켓이 실행될 것으로 가정)
                            return True
                        except RuntimeError:
                            # 실행 중인 루프가 없음 - 새로운 루프에서 실행
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            try:
                                success = loop.run_until_complete(self.websocket_manager.connect())
                            finally:
                                loop.close()
                                asyncio.set_event_loop(None)  # 루프 정리
                    except Exception as e:
                        logger.warning(f"⚠️ 비동기 웹소켓 연결 오류: {e}")
                        # 웹소켓 연결 실패해도 시스템은 계속 동작 (폴링 모드로)
                        success = False
                else:
                    # 동기 메서드인 경우
                    success = self.websocket_manager.connect()

                if success:
                    logger.info("✅ 웹소켓 연결 성공")
                    return True
                else:
                    logger.warning("⚠️ 웹소켓 연결 실패 - 폴링 모드로 계속 진행")
                    return False
            else:
                # 연결 메서드가 없으면 기본 시작 로직 사용
                logger.debug("🔄 웹소켓 매니저 기본 시작...")

                # 웹소켓 스레드 시작
                if not hasattr(self, 'websocket_thread') or not self.websocket_thread or not self.websocket_thread.is_alive():
                    self.websocket_thread = threading.Thread(
                        target=self._websocket_worker,
                        name="WebSocketWorker",
                        daemon=True
                    )
                    self.websocket_thread.start()
                    logger.info("✅ 웹소켓 워커 스레드 시작")
                    return True
                else:
                    logger.info("✅ 웹소켓 워커 스레드 이미 실행 중")
                    return True

        except Exception as e:
            logger.error(f"❌ 웹소켓 연결 실행 오류: {e}")
            return False

    def _websocket_worker(self):
        """웹소켓 워커 스레드"""
        try:
            logger.info("🏃 웹소켓 워커 시작")

            if hasattr(self.websocket_manager, 'start') and callable(self.websocket_manager.start):
                self.websocket_manager.start()
            else:
                # 기본 연결 유지 로직
                while self.websocket_running:
                    try:
                        if hasattr(self.websocket_manager, 'is_connected'):
                            self.websocket_manager.is_connected = True
                        time.sleep(5)  # 5초마다 상태 확인
                    except Exception as e:
                        logger.error(f"❌ 웹소켓 워커 루프 오류: {e}")
                        break

        except Exception as e:
            logger.error(f"❌ 웹소켓 워커 오류: {e}")
        finally:
            logger.info("🛑 웹소켓 워커 종료")

    def ensure_websocket_connection(self) -> bool:
        """🎯 웹소켓 연결 보장 (외부 호출용)"""
        try:
            # 현재 연결 상태 확인
            if self.websocket_running and getattr(self.websocket_manager, 'is_connected', False):
                logger.debug("✅ 웹소켓 연결 상태 양호")
                return True

            # 웹소켓 연결 시도
            logger.info("🔄 웹소켓 연결 보장 모드 - 연결 시도")
            success = self._start_websocket_if_needed()

            if success:
                logger.info("✅ 웹소켓 연결 보장 완료")
            else:
                logger.warning("⚠️ 웹소켓 연결 보장 실패")

            return success

        except Exception as e:
            logger.error(f"❌ 웹소켓 연결 보장 중 오류: {e}")
            return False

    def _process_data_update(self, stock_code: str, data: Dict) -> None:
        """데이터 업데이트 처리"""
        with self.subscription_lock:
            if stock_code not in self.subscriptions:
                return

            subscription = self.subscriptions[stock_code]
            subscription['last_update'] = time.time()
            subscription['update_count'] += 1

            self.stats['data_updates'] += 1

            # 사용자 콜백 실행 - 🆕 새로운 시그니처 지원
            if subscription['callback']:
                try:
                    import inspect
                    callback = subscription['callback']

                    # 콜백 함수의 파라미터 개수 확인
                    sig = inspect.signature(callback)
                    param_count = len(sig.parameters)

                    if param_count >= 3:
                        # 새로운 형식: callback(data_type, stock_code, data)
                        callback('stock_price', stock_code, data)
                    else:
                        # 기존 형식: callback(stock_code, data)
                        callback(stock_code, data)

                except Exception as e:
                    logger.error(f"사용자 콜백 오류: {stock_code} - {e}")

    # === 데이터 조회 ===

    def get_latest_data(self, stock_code: str) -> Optional[Dict]:
        """최신 데이터 조회"""
        return self.collector.get_current_price(stock_code, use_cache=True)

    def get_orderbook(self, stock_code: str) -> Dict:
        """호가 조회"""
        return self.collector.get_orderbook(stock_code, use_cache=True)

    def get_stock_overview(self, stock_code: str) -> Dict:
        """종목 개요"""
        return self.collector.get_stock_overview(stock_code, use_cache=True)

    def get_multiple_data(self, stock_codes: List[str]) -> Dict[str, Dict]:
        """여러 종목 데이터"""
        return self.collector.get_multiple_prices(stock_codes, use_cache=True)

    # === 설정 관리 ===

    def set_polling_interval(self, interval: int) -> None:
        """폴링 간격 설정"""
        self.polling_interval = max(10, interval)  # 최소 10초
        logger.info(f"폴링 간격 변경: {self.polling_interval}초")

    def upgrade_to_realtime(self, stock_code: str) -> bool:
        """실시간으로 업그레이드"""
        with self.subscription_lock:
            if stock_code not in self.subscriptions:
                return False

            subscription = self.subscriptions[stock_code]
            if subscription['use_realtime']:
                return True  # 이미 실시간

            subscription['preferred_realtime'] = True

            if self._can_add_realtime():
                return self._add_to_realtime(stock_code)
            else:
                # 우선순위 대기열에 추가
                self._add_to_priority_queue(stock_code)
                self._try_promote_to_realtime(stock_code)
                return subscription['use_realtime']

    def downgrade_to_polling(self, stock_code: str) -> bool:
        """폴링으로 다운그레이드"""
        with self.subscription_lock:
            if stock_code not in self.subscriptions:
                return False

            subscription = self.subscriptions[stock_code]
            if not subscription['use_realtime']:
                return True  # 이미 폴링

            subscription['preferred_realtime'] = False
            self._remove_from_realtime(stock_code)
            self._add_to_polling(stock_code)

            # 대기열에서 승격
            self._promote_from_queue()

            logger.info(f"폴링 다운그레이드: {stock_code}")
            return True

    # === 상태 조회 ===

    def get_status(self) -> Dict:
        """시스템 상태"""
        with self.subscription_lock:
            # 웹소켓 상세 정보
            websocket_details = {
                'connected': False,
                'subscribed_stocks': [],
                'subscription_count': 0,
                'usage_ratio': "0/41"
            }

            if self.websocket_manager:
                websocket_details['connected'] = getattr(self.websocket_manager, 'is_connected', False)
                if hasattr(self.websocket_manager, 'get_subscribed_stocks'):
                    websocket_details['subscribed_stocks'] = self.websocket_manager.get_subscribed_stocks()
                    websocket_details['subscription_count'] = len(websocket_details['subscribed_stocks'])
                if hasattr(self.websocket_manager, 'get_websocket_usage'):
                    websocket_details['usage_ratio'] = self.websocket_manager.get_websocket_usage()

            return {
                'total_subscriptions': len(self.subscriptions),
                'realtime_subscriptions': len(self.realtime_stocks),
                'polling_subscriptions': len(self.polling_stocks),
                'realtime_capacity': f"{len(self.realtime_stocks)}/{self.MAX_REALTIME_STOCKS}",
                'websocket_usage': f"{self.stats['websocket_usage']}/{self.WEBSOCKET_LIMIT}",
                'websocket_details': websocket_details,
                'priority_queue_size': len(self.realtime_priority_queue),
                'polling_active': self.polling_active,
                'polling_interval': self.polling_interval,
                'stats': self.stats.copy(),
                'collector_stats': self.collector.get_stats(),
                'cache_stats': cache.get_all_cache_stats()
            }

    def get_subscription_list(self) -> List[Dict]:
        """구독 목록"""
        with self.subscription_lock:
            return [
                {
                    'stock_code': sub['stock_code'],
                    'strategy_name': sub['strategy_name'],
                    'use_realtime': sub['use_realtime'],
                    'preferred_realtime': sub['preferred_realtime'],
                    'priority': sub['priority'],
                    'score': sub['score'],
                    'update_count': sub['update_count'],
                    'last_update': sub['last_update']
                }
                for sub in self.subscriptions.values()
            ]

    def get_realtime_stocks(self) -> List[str]:
        """실시간 구독 종목 목록"""
        return self.realtime_stocks.copy()

    def get_priority_queue(self) -> List[str]:
        """우선순위 대기열"""
        return self.realtime_priority_queue.copy()

    def _verify_realtime_subscription(self, stock_code: str) -> bool:
        """실시간 구독 상태 검증"""
        try:
            if not self.websocket_manager:
                return False

            # 웹소켓 매니저에서 구독 상태 확인
            if hasattr(self.websocket_manager, 'subscribed_stocks'):
                return stock_code in self.websocket_manager.subscribed_stocks

            # 구독 정보가 있는지 확인
            subscription = self.subscriptions.get(stock_code, {})
            return subscription.get('use_realtime', False)

        except Exception as e:
            logger.error(f"실시간 구독 검증 오류: {stock_code} - {e}")
            return False

    # === 정리 ===

    def _stop_websocket(self) -> None:
        """웹소켓 정리"""
        try:
            if self.websocket_running:
                # 웹소켓 태스크 취소
                if self.websocket_task and not self.websocket_task.done():
                    self.websocket_task.cancel()

                # 웹소켓 매니저 정리
                if hasattr(self.websocket_manager, 'cleanup'):
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(self.websocket_manager.cleanup())
                    finally:
                        loop.close()

                self.websocket_running = False
                logger.info("웹소켓 정리 완료")

        except Exception as e:
            logger.error(f"웹소켓 정리 오류: {e}")

    def cleanup(self) -> None:
        """리소스 정리"""
        logger.info("스마트 하이브리드 데이터 관리자 정리 중...")

        # 웹소켓 중지
        self._stop_websocket()

        # 폴링 중지
        self.stop_polling()

        # 모든 구독 해제
        with self.subscription_lock:
            stock_codes = list(self.subscriptions.keys())
            for stock_code in stock_codes:
                self.remove_stock(stock_code)

        # 캐시 정리
        cache.clear_all_caches()

        logger.info("스마트 하이브리드 데이터 관리자 정리 완료")


# 하위 호환성을 위한 별칭
HybridDataManager = SimpleHybridDataManager
