"""
KIS 하이브리드 데이터 관리자 (간소화 버전)
데이터 수집기 + 캐시 + 간단한 스케줄링
"""
import time
import threading
import asyncio
from typing import Dict, List, Optional, Callable
from utils.logger import setup_logger
from . import kis_data_cache as cache
from .kis_data_collector import KISDataCollector
from .kis_websocket_manager import KISWebSocketManager

logger = setup_logger(__name__)


class SimpleHybridDataManager:
    """웹소켓 제한을 고려한 스마트 하이브리드 데이터 관리자"""

    def __init__(self, is_demo: bool = False):
        # 웹소켓 제한 상수
        self.WEBSOCKET_LIMIT = 41  # KIS 웹소켓 제한
        self.STREAMS_PER_STOCK = 3  # 종목당 스트림 수 (체결가 + 호가 + 예상체결)
        self.MAX_REALTIME_STOCKS = self.WEBSOCKET_LIMIT // self.STREAMS_PER_STOCK  # 13개

        self.is_demo = is_demo

        # 데이터 수집기
        self.collector = KISDataCollector(is_demo=is_demo)

        # 웹소켓 매니저
        self.websocket_manager = KISWebSocketManager(is_demo=is_demo)
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

        logger.info(f"스마트 하이브리드 데이터 관리자 초기화 완료 ({'모의투자' if is_demo else '실전투자'})")
        logger.info(f"웹소켓 제한: {self.WEBSOCKET_LIMIT}건, 최대 실시간 종목: {self.MAX_REALTIME_STOCKS}개")

    # === 구독 관리 ===

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
            return False

        try:
            # 웹소켓 구독 시도
            if self.websocket_running:
                # 비동기 구독을 동기적으로 처리
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # 이미 실행 중인 루프에서는 create_task 사용
                    asyncio.create_task(
                        self.websocket_manager.subscribe_stock(stock_code, self._websocket_callback)
                    )
                    success = True
                else:
                    # 새 루프에서 실행
                    success = loop.run_until_complete(
                        self.websocket_manager.subscribe_stock(stock_code, self._websocket_callback)
                    )
            else:
                # 웹소켓이 실행 중이 아니면 시작
                if self._start_websocket_if_needed():
                    # 웹소켓 시작 후 구독 시도
                    time.sleep(0.5)  # 연결 대기
                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        success = loop.run_until_complete(
                            self.websocket_manager.subscribe_stock(stock_code, self._websocket_callback)
                        )
                        loop.close()
                    except Exception as e:
                        logger.error(f"웹소켓 구독 오류: {stock_code} - {e}")
                        success = False
                else:
                    success = False

            if success:
                self.realtime_stocks.append(stock_code)
                self.subscriptions[stock_code]['use_realtime'] = True
                self._remove_from_polling(stock_code)

                # 대기열에서 제거
                if stock_code in self.realtime_priority_queue:
                    self.realtime_priority_queue.remove(stock_code)

                self._update_stats()
                logger.info(f"실시간 구독 추가: {stock_code} ({len(self.realtime_stocks)}/{self.MAX_REALTIME_STOCKS})")
                return True
            else:
                logger.error(f"실시간 구독 실패: {stock_code}")
                return False

        except Exception as e:
            logger.error(f"실시간 구독 오류: {stock_code} - {e}")
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

    def _websocket_callback(self, stock_code: str, data: Dict) -> None:
        """웹소켓 실시간 데이터 콜백"""
        try:
            logger.debug(f"웹소켓 데이터 수신: {stock_code} - {data.get('current_price', 0):,}원")
            self._process_data_update(stock_code, data)
        except Exception as e:
            logger.error(f"웹소켓 콜백 오류: {stock_code} - {e}")

    def _start_websocket_if_needed(self) -> bool:
        """필요시 웹소켓 시작"""
        try:
            if self.websocket_running:
                return True

            # 웹소켓 연결 시작
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def start_websocket():
                try:
                    # 웹소켓 연결
                    if await self.websocket_manager.connect():
                        self.websocket_running = True
                        logger.info("✅ 웹소켓 연결 성공")

                        # 백그라운드에서 메시지 처리 시작
                        self.websocket_task = asyncio.create_task(
                            self.websocket_manager.start_listening()
                        )
                        return True
                    else:
                        logger.error("❌ 웹소켓 연결 실패")
                        return False
                except Exception as e:
                    logger.error(f"웹소켓 시작 오류: {e}")
                    return False

            # 새 스레드에서 웹소켓 실행
            def run_websocket():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    success = loop.run_until_complete(start_websocket())
                    if success:
                        # 연결 후 메시지 처리 루프 시작
                        loop.run_until_complete(self.websocket_manager._message_handler())
                except Exception as e:
                    logger.error(f"웹소켓 루프 오류: {e}")
                finally:
                    loop.close()

            websocket_thread = threading.Thread(
                target=run_websocket,
                name="WebSocketManager",
                daemon=True
            )
            websocket_thread.start()

            # 짧은 대기 후 연결 상태 확인
            import time
            time.sleep(1)
            return self.websocket_running

        except Exception as e:
            logger.error(f"웹소켓 시작 실패: {e}")
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

            # 사용자 콜백 실행
            if subscription['callback']:
                try:
                    subscription['callback'](stock_code, data)
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
            return {
                'total_subscriptions': len(self.subscriptions),
                'realtime_subscriptions': len(self.realtime_stocks),
                'polling_subscriptions': len(self.polling_stocks),
                'realtime_capacity': f"{len(self.realtime_stocks)}/{self.MAX_REALTIME_STOCKS}",
                'websocket_usage': f"{self.stats['websocket_usage']}/{self.WEBSOCKET_LIMIT}",
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
