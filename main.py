"""
StockBot 메인 실행 파일
실시간 체결통보 + 하이브리드 데이터 수집 + 자동 거래 시스템

주요 개선사항:
- execute_strategy_order: 통합된 주문 실행 메서드 (전략별 주문 관리)
- strategy_type 필드 추가: pending_orders에 전략 정보 포함
- 손절/익절 주문은 일시정지 상태에서도 실행
- KIS API 응답 형식에 맞춘 주문 결과 처리
- 안전한 WebSocket 연결 상태 확인
"""
import asyncio
import configparser
import threading
from datetime import datetime
from typing import Dict, List, Optional
from utils.logger import setup_logger
import os
import sys
import signal

# 핵심 컴포넌트
from core.hybrid_data_manager import HybridDataManager, DataPriority
from core.websocket_manager import KISWebSocketManager, SubscriptionType
from core.rest_api_manager import KISRestAPIManager
from core.strategy import GapTradingStrategy, VolumeBreakoutStrategy, TimeBasedEnsembleManager
from core.strategy_scheduler import StrategyScheduler


# 데이터베이스 관리
from database.db_manager import db_manager
from database.db_models import TimeSlot, OrderType, OrderStatus

# 텔레그램 봇
from telegram_bot.bot import TelegramBot

# 로거 설정
logger = setup_logger(__name__)

class StockBotMain:
    """StockBot 메인 컨트롤러 - 실시간 거래 시스템"""

    def __init__(self):
        # 환경변수에서 테스트 모드 확인
        self.testing_mode = os.getenv('TESTING_MODE', 'false').lower() == 'true'
        self.disable_telegram = os.getenv('DISABLE_TELEGRAM', 'false').lower() == 'true'

        # 0. 설정 파일 로드 (가장 먼저!)
        from utils.config_loader import ConfigLoader
        self.config_loader = ConfigLoader()
        self._load_trading_settings()

        # 1. 데이터베이스 초기화
        db_manager.initialize_database()

        # 1. 데이터 수집 (하이브리드)
        self.data_manager = HybridDataManager()

        # 2. 거래 실행 (REST API) - 먼저 초기화하여 토큰 발급
        self.trading_api = KISRestAPIManager()

        # 3. 실시간 WebSocket (체결통보 포함) - REST API 토큰 사용
        self.websocket_manager = KISWebSocketManager()

        # 4. 전략 매니저들 (실제 주문은 strategy_scheduler를 통해 일원화)
        self.gap_strategy = GapTradingStrategy()
        self.volume_strategy = VolumeBreakoutStrategy()
        self.ensemble = TimeBasedEnsembleManager()

        # 5. 시간대별 전략 스케줄러 (핵심! 모든 종목 탐색은 여기서)
        self.strategy_scheduler = StrategyScheduler(self.trading_api, self.data_manager)
        # 스케줄러에 봇 인스턴스 설정 (콜백에서 사용)
        self.strategy_scheduler.set_bot_instance(self)

        # 6. 텔레그램 봇 (원격 제어)
        if not self.disable_telegram:
            try:
                self.telegram_bot = TelegramBot(stock_bot_instance=self)
                logger.info("🤖 텔레그램 봇 초기화 완료")
            except Exception as e:
                logger.warning(f"⚠️ 텔레그램 봇 초기화 실패: {e}")
                self.telegram_bot = None
        else:
            logger.info("🚫 텔레그램 봇이 비활성화되었습니다.")
            self.telegram_bot = None

        # 7. 거래 상태 관리 (메모리 기반, DB와 동기화)
        self.positions: Dict[str, Dict] = {}  # 보유 포지션 (메모리)
        self.pending_orders: Dict[str, Dict] = {}  # 대기 주문 (메모리)
        self.order_history: List[Dict] = []  # 주문 이력 (메모리)

        # 8. 시간대별 선정 종목 관리 (전역)
        self.selected_stocks: Dict[str, List[str]] = {  # 시간대별 선정 종목
            'golden_time': [],
            'morning_leaders': [],
            'lunch_time': [],
            'closing_trend': []
        }
        self.current_time_slot: Optional[str] = None  # 현재 시간대
        self.previous_time_slot: Optional[str] = None  # 이전 시간대 (매수 종목 추적용)

        # 8. 스레드 안전 락
        self.position_lock = threading.RLock()

        # 9. 계좌 잔고 캐시 초기화
        self._cached_balance = {
            'total_assets': 0,
            'available_cash': 0,
            'stock_evaluation': 0,
            'profit_loss': 0,
            'profit_rate': 0,
        }

        # 10. 🚀 고성능 거래 신호 시스템 (기존 trading_signals 대체)
        # self.trading_signals: asyncio.Queue = asyncio.Queue()  # 제거됨 (순차 처리)
        # self.signal_lock = threading.RLock()                   # 제거됨 (불필요)

        # 🚀 신규: 고성능 신호 처리 시스템
        # 우선순위 큐로 업그레이드 (signal_strength 기반)
        from queue import PriorityQueue
        import heapq
        self.priority_signals = asyncio.PriorityQueue()  # 우선순위 기반 신호 큐
        self.signal_batch_processor = None  # 배치 처리 태스크
        self.signal_processing_active = True  # 신호 처리 활성화 플래그

        # 🚀 중복 신호 방지 시스템
        self.recent_signals: Dict[str, Dict] = {}  # {종목코드: {마지막_액션, 타임스탬프, 전략}}
        self.signal_dedup_lock = threading.RLock()  # 중복 체크용 락
        self.signal_cooldown_seconds = 5  # 같은 종목 신호 간 최소 간격 (초)

        # 병렬 처리 설정
        self.max_concurrent_orders = 5  # 동시 주문 처리 수 (API 부하 고려)
        self.signal_processing_semaphore = asyncio.Semaphore(self.max_concurrent_orders)

        # 신호 처리 성능 추적
        self.signal_stats = {
            'total_received': 0,
            'total_processed': 0,
            'total_filtered': 0,  # 중복 제거된 신호 수
            'concurrent_peak': 0,
            'average_processing_time': 0.0,  # float로 명시적 초기화
            'last_batch_size': 0
        }

        # 11. 🆕 전역 실시간 데이터 캐시 (웹소켓 우선, REST API fallback)
        self.realtime_cache: Dict[str, Dict] = {}  # {종목코드: {price, timestamp, source}}
        self.cache_lock = threading.RLock()

        # 🆕 12. 미체결 주문 관리 시스템
        self.pending_order_adjustments: Dict[str, int] = {}  # {order_no: 조정_횟수}
        self.pending_order_lock = threading.RLock()

        # 신호 핸들러 등록
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    async def initialize(self):
        """시스템 초기화 - 장외시간 대응 포함"""
        logger.info("🚀 StockBot 초기화 시작")

        if self.testing_mode:
            logger.warning("🧪 테스트 모드로 실행됩니다. 장외시간 대응이 활성화됩니다.")

        # 장외시간 체크
        from datetime import datetime
        import pytz
        from core.rest_api_manager import KISRestAPIManager
        kst = pytz.timezone('Asia/Seoul')
        now = datetime.now(kst)
        is_market_open = KISRestAPIManager.is_market_open(now)

        if not is_market_open:
            logger.warning(f"🕐 장외시간 ({now.strftime('%Y-%m-%d %H:%M:%S')}): 제한된 모드로 실행됩니다")

        # 1. WebSocket 연결 (장외시간에는 선택적)
        if is_market_open or self.testing_mode:
            if not await self.websocket_manager.connect():
                if is_market_open:
                    raise Exception("WebSocket 연결 실패")
                else:
                    logger.warning("WebSocket 연결 실패 (장외시간이므로 계속 진행)")
        else:
            logger.info("장외시간: WebSocket 연결 생략")

        # 2. 체결통보 구독 (장중에만)
        if is_market_open or self.testing_mode:
            await self._setup_execution_notification()

        # 3. 하이브리드 데이터 매니저 시작
        self.data_manager.start_polling()

        # 4. 계좌 잔고 정보 초기화 (주기적 업데이트용)
        await self.update_cached_balance()

        # 5. 시간대별 전략 스케줄러 준비 (실제 시작은 run()에서)
        # 모든 종목 탐색은 strategy_scheduler를 통해 일원화
        logger.info("📅 전략 스케줄러 준비 완료")

        # 5. 텔레그램 봇 시작 (별도 스레드)
        if self.telegram_bot:
            self.telegram_bot.start_bot()
            logger.info("🤖 텔레그램 봇 시작")

        logger.info("✅ StockBot 초기화 완료")

    async def _setup_execution_notification(self):
        """체결통보 설정 - 핵심! 자동 포지션 관리"""
        logger.info("🔔 체결통보 구독 설정")

        # 체결통보 콜백 등록
        self.websocket_manager.add_callback("stock_execution", self.handle_execution_notification)

        # 체결통보 구독 (모든 계좌의 주문 체결)
        success = await self.websocket_manager.subscribe(
            f"{SubscriptionType.STOCK_EXECUTION.value}|{self.trading_api.account_no}"
        )

        if success:
            logger.info("✅ 체결통보 구독 성공")
        else:
            logger.error("❌ 체결통보 구독 실패")
            raise Exception("체결통보 구독 실패")

    def handle_execution_notification(self, data: Dict):
        """체결통보 처리 - 자동 호출됨 (WebSocket → 포지션 관리)"""
        try:
            stock_code = data.get('stock_code', '')
            order_type = data.get('order_type', '')  # '매수' or '매도'
            execution_price = int(data.get('execution_price', 0))
            execution_qty = int(data.get('execution_qty', 0))
            order_no = data.get('order_no', '')

            logger.info(f"🔔 체결 통보: {stock_code} {order_type} {execution_qty}주 @ {execution_price:,}원")

            with self.position_lock:
                # 거래 기록을 데이터베이스에 저장
                current_slot_name = self.strategy_scheduler.current_slot.name if self.strategy_scheduler.current_slot else "unknown"
                db_manager.record_execution_to_db(data, current_slot_name, self.pending_orders)

                # 포지션 업데이트 (메모리 + DB)
                if order_type == '매수':
                    self._update_buy_position(stock_code, execution_qty, execution_price)
                elif order_type == '매도':
                    self._update_sell_position(stock_code, execution_qty, execution_price)

                # 대기 주문에서 제거
                if order_no in self.pending_orders:
                    del self.pending_orders[order_no]

                # 주문 이력에 추가 (메모리용)
                self.order_history.append({
                    'timestamp': datetime.now(),
                    'stock_code': stock_code,
                    'order_type': order_type,
                    'price': execution_price,
                    'quantity': execution_qty,
                    'order_no': order_no
                })

                # 체결 완료 후 잔고 즉시 갱신 (매수/매도 시마다)
                asyncio.create_task(self.update_cached_balance())

                # 텔레그램 알림 전송 (비동기)
                if self.telegram_bot and self.telegram_bot.is_running():
                    notification_msg = (
                        f"🔔 <b>체결 통보</b>\n\n"
                        f"📊 <b>{stock_code}</b>\n"
                        f"🔄 {order_type} {execution_qty:,}주\n"
                        f"💰 {execution_price:,}원\n"
                        f"⏰ {datetime.now().strftime('%H:%M:%S')}"
                    )
                    # 비동기 방식으로 알림 전송 (효율적)
                    asyncio.create_task(
                        self._send_telegram_notification_async(notification_msg)
                    )

        except Exception as e:
            logger.error(f"체결통보 처리 중 오류: {e}")

    def _update_buy_position(self, stock_code: str, qty: int, price: int):
        """매수 포지션 업데이트 (메모리 + DB 동기화)"""
        # 대기 주문에서 전략 정보 찾기
        strategy_type = "unknown"
        for order_info in self.pending_orders.values():
            if order_info.get('stock_code') == stock_code and order_info.get('order_type') == '매수':
                strategy_type = order_info.get('strategy_type', 'unknown')
                break

        # 메모리 포지션 업데이트
        if stock_code not in self.positions:
            self.positions[stock_code] = {
                'quantity': 0,
                'avg_price': 0,
                'total_amount': 0,
                'strategy_type': strategy_type,  # 전략 정보 추가
                'entry_time': datetime.now(),    # 진입 시간 추가
                'max_profit_rate': 0.0          # 최대 수익률 추적 (trailing stop용)
            }

        position = self.positions[stock_code]
        new_total_amount = position['total_amount'] + (qty * price)
        new_quantity = position['quantity'] + qty
        new_avg_price = new_total_amount // new_quantity if new_quantity > 0 else 0

        self.positions[stock_code] = {
            'quantity': new_quantity,
            'avg_price': new_avg_price,
            'total_amount': new_total_amount,
            'strategy_type': strategy_type,
            'entry_time': position.get('entry_time', datetime.now()),
            'max_profit_rate': position.get('max_profit_rate', 0.0)
        }

        # 첫 매수인 경우 DB에 포지션 생성
        if position['quantity'] == 0:  # 이전에 보유량이 0이었다면 첫 매수
            current_slot = self.strategy_scheduler.current_slot
            position_data = {
                'stock_code': stock_code,
                'stock_name': '',  # 나중에 API로 조회
                'quantity': qty,
                'avg_buy_price': price,
                'total_buy_amount': qty * price,
                'time_slot': current_slot.name if current_slot else "unknown",
                'strategy_type': strategy_type,
            }
            db_manager.create_position(position_data)
        else:
            # 기존 포지션 업데이트
            update_data = {
                'quantity': new_quantity,
                'avg_buy_price': new_avg_price,
                'total_buy_amount': new_total_amount
            }
            db_manager.update_position(stock_code, update_data)

        logger.info(f"📈 매수 완료: {stock_code} 보유량 {new_quantity}주 (평단: {new_avg_price:,}원, 전략: {strategy_type})")

    def _update_sell_position(self, stock_code: str, qty: int, price: int):
        """매도 포지션 업데이트 (메모리 + DB 동기화)"""
        if stock_code in self.positions:
            position = self.positions[stock_code]
            new_quantity = position['quantity'] - qty

            if new_quantity <= 0:
                # 완전 매도 - DB에서 포지션 종료
                profit = (price - position['avg_price']) * position['quantity']
                profit_rate = (profit / position['total_amount']) * 100 if position['total_amount'] > 0 else 0

                sell_data = {
                    'sell_price': price,
                    'sell_amount': qty * price,
                    'realized_pnl': profit,
                    'realized_pnl_rate': profit_rate,
                    'exit_strategy': 'manual'
                }
                db_manager.close_position(stock_code, sell_data)

                logger.info(f"📉 전량 매도: {stock_code} 손익 {profit:+,}원 ({profit_rate:+.2f}%)")
                del self.positions[stock_code]
            else:
                # 부분 매도 - DB 포지션 업데이트
                profit = (price - position['avg_price']) * qty
                profit_rate = (profit / (qty * position['avg_price'])) * 100 if position['avg_price'] > 0 else 0

                self.positions[stock_code]['quantity'] = new_quantity

                update_data = {
                    'quantity': new_quantity
                }
                db_manager.update_position(stock_code, update_data)

                logger.info(f"📉 부분 매도: {stock_code} 잔여 {new_quantity}주, 부분손익 {profit:+,}원 ({profit_rate:+.2f}%)")

    # 전략별 주문 실행은 strategy_scheduler를 통해 일원화됨

    async def execute_strategy_order(self, stock_code: str, order_type: str, quantity: int, price: int = 0, strategy_type: str = "manual") -> Optional[str]:
        """
        전략 기반 주문 실행 (통합 주문 메서드)

        Args:
            stock_code: 종목코드
            order_type: 주문 타입 ('BUY' or 'SELL')
            quantity: 수량
            price: 가격 (0이면 시장가)
            strategy_type: 전략 타입 (gap_trading, volume_breakout, momentum, stop_loss, take_profit, manual)

        Returns:
            주문번호 또는 None

        Note:
            - 손절/익절 주문은 일시정지 상태에서도 실행됨
            - 모든 주문에 strategy_type이 기록되어 추후 분석 가능
        """
        try:
            # 텔레그램 봇 일시정지 체크 (손절/익절은 진행)
            if self.telegram_bot and self.telegram_bot.is_paused() and strategy_type not in ["stop_loss", "take_profit"]:
                logger.info(f"⏸️ 거래 일시정지 중 - {strategy_type} 주문 건너뜀: {stock_code}")
                return None

            # 주문 실행
            if order_type.upper() == 'BUY':
                order_result = self.trading_api.buy_order(
                    stock_code=stock_code,
                    quantity=quantity,
                    price=price
                )
            elif order_type.upper() == 'SELL':
                order_result = self.trading_api.sell_order(
                    stock_code=stock_code,
                    quantity=quantity,
                    price=price
                )
            else:
                logger.error(f"❌ 잘못된 주문 타입: {order_type}")
                return None

            # 주문 결과 확인 (KIS API는 order_no가 있으면 성공)
            order_no = order_result.get('order_no')
            if order_no:
                # 대기 주문에 추가 (strategy_type 포함)
                self.pending_orders[order_no] = {
                    'stock_code': stock_code,
                    'order_type': '매수' if order_type.upper() == 'BUY' else '매도',
                    'price': price,
                    'quantity': quantity,
                    'strategy_type': strategy_type,
                    'timestamp': datetime.now()
                }

                order_type_kr = '매수' if order_type.upper() == 'BUY' else '매도'
                price_str = f"{price:,}원" if price > 0 else "시장가"
                logger.info(f"📋 {strategy_type} {order_type_kr} 주문: {stock_code} {price_str} {quantity}주 (주문번호: {order_no})")

                return order_no
            else:
                logger.error(f"❌ 주문 실패: {stock_code} - {order_result.get('msg1', '알 수 없는 오류')}")
                return None

        except Exception as e:
            logger.error(f"{strategy_type} 주문 실행 오류: {e}")
            return None

    async def monitor_positions(self):
        """포지션 모니터링 - 손절/익절 전담 (신호 처리는 별도 태스크로 분리)"""
        logger.info("🔍 포지션 모니터링 시작 (신호 처리 분리됨)")
        last_check_time = {}  # 종목별 마지막 체크 시간

        while True:
            try:
                # 🚀 신호 처리는 전용 태스크에서 처리하므로 제거됨
                # 포지션 모니터링에만 집중

                if not self.positions:
                    await asyncio.sleep(float(self.no_position_wait_time))  # 설정값 사용
                    continue

                current_time = datetime.now()
                positions_to_check = []

                # 스레드 안전하게 포지션 복사
                with self.position_lock:
                    for stock_code, position in list(self.positions.items()):
                        # 종목별로 최소 5초 간격 체크 (API 부하 감소)
                        last_time = last_check_time.get(stock_code, current_time)
                        if (current_time - last_time).total_seconds() >= 5:
                            positions_to_check.append((stock_code, position.copy()))
                            last_check_time[stock_code] = current_time

                # 락 해제 후 포지션 체크 (블로킹 최소화)
                for stock_code, position in positions_to_check:
                    try:
                        # 🚀 새로운 통합 가격 조회 메서드 사용 (웹소켓 우선, fallback 자동)
                        price_result = self.get_cached_price_with_fallback(stock_code)

                        if not price_result['success']:
                            logger.debug(f"현재가 데이터 없음: {stock_code}")
                            continue

                        current_price = price_result['price']
                        data_source = price_result['source']
                        avg_price = position['avg_price']
                        quantity = position['quantity']

                        # 안전장치: 가격이 0이면 스킵
                        if avg_price <= 0:
                            continue

                        profit_rate = (current_price - avg_price) / avg_price

                        # 최대 수익률 업데이트 (trailing stop용)
                        if profit_rate > position.get('max_profit_rate', 0.0):
                            with self.position_lock:
                                if stock_code in self.positions:
                                    self.positions[stock_code]['max_profit_rate'] = profit_rate

                        # 전략별 매도 조건 적용
                        sell_signal = self._check_strategy_sell_conditions(
                            stock_code, position, current_price, profit_rate
                        )

                        if sell_signal:
                            await self._execute_sell_order(
                                stock_code, quantity, current_price, sell_signal['reason']
                            )

                        # 현재 상태 로깅 (5분마다)
                        elif len(positions_to_check) <= 3:  # 포지션 적을 때만
                            strategy_type = position.get('strategy_type', 'unknown')
                            logger.debug(f"📊 {stock_code}({strategy_type}): {profit_rate*100:+.1f}% ({current_price:,}원) [from {data_source}]")

                    except Exception as e:
                        logger.error(f"포지션 체크 오류 ({stock_code}): {e}")

                # 적응적 대기 시간 (포지션 수에 따라 조절, 설정값 기반)
                position_count = len(self.positions)
                if position_count == 0:
                    sleep_time = float(self.no_position_wait_time)  # 포지션 없음
                elif position_count <= 5:
                    sleep_time = float(self.position_check_interval)   # 적은 포지션
                else:
                    sleep_time = max(2.0, float(self.position_check_interval) - 1.0)   # 많은 포지션 (최소 2초)

                await asyncio.sleep(sleep_time)

            except Exception as e:
                logger.error(f"포지션 모니터링 전체 오류: {e}")
                await asyncio.sleep(10)  # 오류 시 더 긴 대기

    async def _execute_sell_order(self, stock_code: str, quantity: int, price: int, reason: str):
        """매도 주문 실행 (포지션 모니터링용) - 통합 주문 메서드 사용"""
        try:
            # 전략 타입 결정
            strategy_type = "stop_loss" if reason == "손절" else "take_profit" if reason == "익절" else "manual"

            # 통합된 주문 실행 메서드 사용
            order_no = await self.execute_strategy_order(
                stock_code=stock_code,
                order_type="SELL",
                quantity=quantity,
                price=price,
                strategy_type=strategy_type
            )

            if order_no:
                # reason 정보 추가
                if order_no in self.pending_orders:
                    self.pending_orders[order_no]['reason'] = reason

        except Exception as e:
            logger.error(f"매도 주문 실행 오류: {e}")

    async def get_account_balance(self) -> Dict:
        """계좌 잔고 조회"""
        try:
            balance_info = self.trading_api.get_balance()
            if balance_info:
                return {
                    'total_assets': balance_info.get('total_evaluation_amount', 0),  # 총 평가금액
                    'available_cash': balance_info.get('order_possible_cash', 0),    # 주문 가능 현금
                    'stock_evaluation': balance_info.get('total_stock_evaluation', 0),  # 보유 주식 평가금액
                    'profit_loss': balance_info.get('total_profit_loss', 0),        # 총 손익
                    'profit_rate': balance_info.get('total_profit_rate', 0),        # 총 손익률
                }
            return {
                'total_assets': 0,
                'available_cash': 0,
                'stock_evaluation': 0,
                'profit_loss': 0,
                'profit_rate': 0,
            }
        except Exception as e:
            logger.error(f"계좌 잔고 조회 오류: {e}")
            return {
                'total_assets': 0,
                'available_cash': 0,
                'stock_evaluation': 0,
                'profit_loss': 0,
                'profit_rate': 0,
            }

    def calculate_position_size(self, signal: Dict, stock_code: str, current_price: int) -> Dict:
        """포지션 사이징 계산 - 안전한 매수 수량 결정"""
        try:
            # 현재 계좌 정보 (캐시된 값 사용, 실시간 조회는 비용이 큼)
            balance = getattr(self, '_cached_balance', {'available_cash': 1000000, 'total_assets': 5000000})

            # 기본 설정값들
            max_positions = self._safe_int(self.config_loader.get_config_value('trading', 'max_positions', 10), 10)
            position_size_pct = self._safe_float(self.config_loader.get_config_value('trading', 'position_size_pct', 5.0), 5.0)
            daily_risk_limit = self._safe_int(self.config_loader.get_config_value('trading', 'daily_risk_limit', 1000000), 1000000)
            max_cash_usage_pct = self._safe_float(self.config_loader.get_config_value('trading', 'max_cash_usage_pct', 80.0), 80.0)

            # 1. 종목당 최대 투자 금액 계산 (총 자산의 %)
            max_investment_per_stock = int(balance['total_assets'] * (position_size_pct / 100))

            # 2. 신호 강도에 따른 조절 (강한 신호일수록 더 많이 투자)
            signal_strength = signal.get('strength', 0.5)
            strength_multiplier = 0.5 + (signal_strength * 0.5)  # 0.5 ~ 1.0

            # 3. 전략별 가중치 적용
            strategy_multipliers = {
                'gap_trading': 1.2,      # 갭 트레이딩은 더 적극적
                'volume_breakout': 1.0,  # 기본
                'momentum': 0.8,         # 모멘텀은 보수적
                'signal_': 1.0,          # 실시간 신호는 기본
            }

            strategy_type = signal.get('reason', '')
            strategy_multiplier = 1.0
            for strategy, multiplier in strategy_multipliers.items():
                if strategy in strategy_type:
                    strategy_multiplier = multiplier
                    break

            # 4. 현재 보유 종목 수에 따른 분산 투자
            current_positions_count = len(self.positions)
            if current_positions_count >= max_positions * 0.8:  # 80% 이상 차면 보수적
                diversification_multiplier = 0.7
            elif current_positions_count >= max_positions * 0.5:  # 50% 이상 차면 적당히
                diversification_multiplier = 0.85
            else:
                diversification_multiplier = 1.0

            # 5. 최종 투자 금액 계산
            target_investment = int(
                max_investment_per_stock *
                strength_multiplier *
                strategy_multiplier *
                diversification_multiplier
            )

            # 6. 가용 현금 한도 체크
            target_investment = min(target_investment, balance['available_cash'] * (max_cash_usage_pct / 100))

            # 7. 일일 리스크 한도 체크
            target_investment = min(target_investment, daily_risk_limit // max_positions)

            # 8. 최종 수량 계산
            quantity = target_investment // current_price

            # 9. 최소/최대 수량 제한
            min_quantity = 1
            max_quantity = 1000  # 최대 1000주
            quantity = max(min_quantity, min(quantity, max_quantity))

            # 실제 투자 금액
            actual_investment = quantity * current_price

            return {
                'quantity': quantity,
                'investment_amount': actual_investment,
                'target_investment': target_investment,
                'signal_strength': signal_strength,
                'strategy_multiplier': strategy_multiplier,
                'diversification_factor': diversification_multiplier,
                'position_count': current_positions_count,
                'reason': f"강도{signal_strength:.2f} 전략x{strategy_multiplier:.1f} 분산x{diversification_multiplier:.1f}"
            }

        except Exception as e:
            logger.error(f"포지션 사이징 계산 오류: {e}")
            # 안전한 기본값 반환
            safe_quantity = min(10, 500000 // current_price)  # 50만원어치 또는 10주 중 작은 값
            return {
                'quantity': safe_quantity,
                'investment_amount': safe_quantity * current_price,
                'target_investment': 500000,
                'signal_strength': 0.5,
                'strategy_multiplier': 1.0,
                'diversification_factor': 1.0,
                'position_count': len(self.positions),
                'reason': "안전 모드 (계산 오류)"
            }

    async def update_cached_balance(self):
        """캐시된 잔고 업데이트 (주기적 호출)"""
        try:
            balance = await self.get_account_balance()
            self._cached_balance = balance
            logger.debug(f"💰 잔고 업데이트: 가용현금 {balance['available_cash']:,}원, 총자산 {balance['total_assets']:,}원")
        except Exception as e:
            logger.error(f"잔고 캐시 업데이트 오류: {e}")

    async def add_trading_signal(self, signal: Dict):
        """거래 신호를 큐에 추가 (strategy_scheduler에서 호출) - 중복 제거 포함"""
        try:
            stock_code = signal.get('stock_code')
            action = signal.get('action')
            strategy_type = signal.get('strategy_type', 'unknown')

            if not stock_code or not action:
                logger.warning(f"⚠️ 신호 필수 필드 누락: {stock_code}, {action}")
                return

            # 🚀 중복 신호 검사 및 필터링
            current_time = datetime.now().timestamp()
            should_process = True

            with self.signal_dedup_lock:
                if stock_code in self.recent_signals:
                    last_signal = self.recent_signals[stock_code]
                    time_diff = current_time - last_signal['timestamp']

                    # 1. 같은 액션의 중복 체크 (쿨다운 적용)
                    if (last_signal['action'] == action and
                        time_diff < self.signal_cooldown_seconds):
                        logger.debug(f"🚫 중복 신호 필터링: {stock_code} {action} (쿨다운 {time_diff:.1f}s)")
                        should_process = False

                    # 2. 상충 신호 체크 (매수↔매도)
                    elif (last_signal['action'] != action and
                          time_diff < 2.0):  # 2초 이내 상충 신호 방지
                        # 우선순위 비교 (매도 > 매수, 포지션보호 > 전략신호)
                        current_priority = self._calculate_signal_priority(signal)
                        last_priority = last_signal.get('priority', 0.5)

                        if current_priority >= last_priority:  # 우선순위가 낮거나 같으면 무시
                            logger.warning(f"⚠️ 상충 신호 필터링: {stock_code} {action} vs {last_signal['action']} (우선순위 낮음)")
                            should_process = False
                        else:
                            logger.info(f"🔄 상충 신호 우선순위 교체: {stock_code} {action} 우선 처리")

                # 3. 신호 정보 업데이트 (처리 여부와 상관없이)
                if should_process:
                    priority = self._calculate_signal_priority(signal)
                    self.recent_signals[stock_code] = {
                        'action': action,
                        'timestamp': current_time,
                        'strategy_type': strategy_type,
                        'priority': priority
                    }

            if not should_process:
                # 필터링 통계 업데이트
                self.signal_stats['total_filtered'] += 1
                return

            # 🚀 중복이 아닌 경우 우선순위 큐에 추가
            priority = self._calculate_signal_priority(signal)
            timestamp = datetime.now().timestamp()

            # 우선순위 큐에 추가 (priority, timestamp, signal)
            await self.priority_signals.put((priority, timestamp, signal))

            # 통계 업데이트
            self.signal_stats['total_received'] += 1

            logger.debug(f"📡 우선순위 거래 신호 추가: {signal.get('stock_code')} {action} (우선순위: {priority:.2f})")

        except Exception as e:
            logger.error(f"거래 신호 추가 오류: {e}")

    def _calculate_signal_priority(self, signal: Dict) -> float:
        """
        신호 우선순위 계산 (낮을수록 높은 우선순위)

        기준:
        - 신호 강도가 높을수록 우선순위 높음
        - 전략별 가중치 적용
        - 급등/급락 상황에서 더 높은 우선순위
        """
        try:
            signal_strength = signal.get('strength', 0.5)
            strategy_type = signal.get('strategy_type', 'unknown')
            action = signal.get('action', 'BUY')

            # 기본 우선순위 (신호 강도 기반)
            base_priority = 1.0 - signal_strength  # 0.0 ~ 1.0 (낮을수록 높은 우선순위)

            # 전략별 가중치
            strategy_weights = {
                'gap_trading': 0.8,      # 갭 트레이딩 최우선
                'volume_breakout': 0.9,  # 볼륨 브레이크아웃 높은 우선순위
                'momentum': 1.0,         # 모멘텀 기본
                'signal_': 0.95,         # 실시간 신호 높은 우선순위
                'stop_loss': 0.1,        # 손절 최고 우선순위
                'take_profit': 0.2       # 익절 두 번째 우선순위
            }

            strategy_weight = 1.0
            for strategy, weight in strategy_weights.items():
                if strategy in strategy_type:
                    strategy_weight = weight
                    break

            # 매도 신호는 더 높은 우선순위
            action_weight = 0.8 if action == 'SELL' else 1.0

            # 최종 우선순위 계산
            final_priority = base_priority * strategy_weight * action_weight

            return max(0.01, min(1.0, final_priority))  # 0.01 ~ 1.0 범위로 제한

        except Exception as e:
            logger.error(f"우선순위 계산 오류: {e}")
            return 0.5  # 기본값

    async def _process_pending_signals(self):
        """🚀 고성능 병렬 신호 처리 시스템 - 기존 순차 처리를 완전 대체"""
        try:
            batch_signals = []
            start_time = datetime.now()

            # 1. 배치 수집 (최대 20개 또는 100ms 내)
            batch_timeout = 0.1  # 100ms
            max_batch_size = 20

            while len(batch_signals) < max_batch_size:
                try:
                    # 우선순위 큐에서 신호 수집 (타임아웃 포함)
                    priority, timestamp, signal = await asyncio.wait_for(
                        self.priority_signals.get(),
                        timeout=batch_timeout if batch_signals else None
                    )
                    batch_signals.append((priority, timestamp, signal))

                    # 첫 신호 후에는 짧은 타임아웃 적용
                    if len(batch_signals) == 1:
                        batch_timeout = 0.05  # 50ms

                except asyncio.TimeoutError:
                    break  # 타임아웃 시 현재 배치로 처리
                except asyncio.QueueEmpty:
                    break

            if not batch_signals:
                return

            # 2. 배치 크기 통계 업데이트
            self.signal_stats['last_batch_size'] = len(batch_signals)
            logger.debug(f"🔄 신호 배치 처리: {len(batch_signals)}개")

            # 3. 우선순위 정렬 (이미 우선순위 큐에서 나왔지만 안전장치)
            batch_signals.sort(key=lambda x: x[0])  # 우선순위 기준 정렬

            # 4. 🚀 병렬 처리 (동시 실행)
            semaphore_tasks = []
            for priority, timestamp, signal in batch_signals:
                task = asyncio.create_task(
                    self._execute_trading_signal_with_semaphore(signal, priority)
                )
                semaphore_tasks.append(task)

            # 모든 신호 동시 실행 (세마포어로 제한)
            results = await asyncio.gather(*semaphore_tasks, return_exceptions=True)

            # 5. 결과 분석
            success_count = sum(1 for r in results if r is not False and not isinstance(r, Exception))
            error_count = sum(1 for r in results if isinstance(r, Exception))

            # 6. 성능 통계 업데이트
            processing_time = (datetime.now() - start_time).total_seconds()
            self.signal_stats['total_processed'] += success_count
            self.signal_stats['average_processing_time'] = (
                self.signal_stats['average_processing_time'] * 0.8 +
                processing_time * 0.2
            )

            if len(batch_signals) > 0:
                logger.info(
                    f"⚡ 병렬 신호 처리 완료: {success_count}/{len(batch_signals)}개 성공 "
                    f"({processing_time*1000:.0f}ms, 평균 {self.signal_stats['average_processing_time']*1000:.0f}ms)"
                )

            if error_count > 0:
                logger.warning(f"⚠️ 신호 처리 오류: {error_count}개")

        except Exception as e:
            logger.error(f"배치 신호 처리 전체 오류: {e}")

    async def _execute_trading_signal_with_semaphore(self, signal: Dict, priority: float):
        """세마포어를 사용한 제한된 병렬 신호 실행"""
        async with self.signal_processing_semaphore:  # 동시 실행 수 제한
            try:
                start_time = datetime.now()
                result = await self._execute_trading_signal_optimized(signal, priority)

                # 처리 시간 추적
                processing_time = (datetime.now() - start_time).total_seconds()
                if processing_time > 0.5:  # 500ms 이상 소요 시 경고
                    logger.warning(
                        f"⚠️ 느린 신호 처리: {signal.get('stock_code')} {processing_time*1000:.0f}ms"
                    )

                return result

            except Exception as e:
                logger.error(f"세마포어 신호 실행 오류: {e}")
                return False

    async def _execute_trading_signal_optimized(self, signal: Dict, priority: float):
        """🚀 최적화된 개별 거래 신호 실행 - 성능 개선 버전"""
        try:
            stock_code = signal.get('stock_code')
            action = signal.get('action')
            price = signal.get('price', 0)
            reason = signal.get('reason', 'unknown')
            strategy_type = signal.get('strategy_type', 'signal')

            # 필수 필드 검증 (빠른 실패)
            if not stock_code or not action:
                logger.warning(f"⚠️ 신호 필수 필드 누락: stock_code={stock_code}, action={action}")
                return False

            if action == 'BUY':
                # 🚀 성능 최적화: 동시성 체크를 먼저 수행 (빠른 실패)
                if not self.can_open_new_position():
                    logger.debug(f"⚠️ 신규 포지션 오픈 불가: {stock_code}")
                    return False

                # 포지션 사이징 계산
                sizing_result = self.calculate_position_size(signal, stock_code, price)
                quantity = sizing_result['quantity']

                if quantity <= 0:
                    logger.debug(f"⚠️ 매수 수량 부족: {stock_code}")
                    return False

                # 매수 주문 실행
                order_no = await self.execute_strategy_order(
                    stock_code=stock_code,
                    order_type="BUY",
                    quantity=quantity,
                    price=price,
                    strategy_type=strategy_type
                )

                success = order_no is not None
                if success:
                    logger.info(f"✅ 매수 신호 처리 성공: {stock_code} {quantity}주 @ {price:,}원 ({reason})")
                else:
                    logger.warning(f"❌ 매수 신호 처리 실패: {stock_code}")

                return success

            elif action == 'SELL':
                # 🚀 매도 처리 (전략 신호 + 포지션 보호 신호 통합)

                # 포지션 보호 신호의 경우 수량이 이미 지정됨
                if signal.get('source') == 'position_monitoring':
                    quantity = signal.get('quantity', 0)
                    if quantity <= 0:
                        logger.warning(f"⚠️ 포지션 보호 신호 수량 오류: {stock_code}")
                        return False
                else:
                    # 일반 전략 신호의 경우 현재 보유 수량 확인
                    position = self.positions.get(stock_code)
                    if not position:
                        logger.debug(f"⚠️ 매도할 포지션 없음: {stock_code}")
                        return False
                    quantity = position['quantity']

                # 매도 주문 실행
                order_no = await self.execute_strategy_order(
                    stock_code=stock_code,
                    order_type="SELL",
                    quantity=quantity,
                    price=price,
                    strategy_type=strategy_type
                )

                success = order_no is not None
                if success:
                    logger.info(f"✅ 매도 신호 처리 성공: {stock_code} {quantity}주 @ {price:,}원 ({reason})")

                    # 포지션 보호 신호인 경우 특별 로깅
                    if signal.get('source') == 'position_monitoring':
                        logger.info(f"🛡️ 포지션 보호 실행: {reason}")
                else:
                    logger.warning(f"❌ 매도 신호 처리 실패: {stock_code}")

                return success

            else:
                logger.warning(f"⚠️ 알 수 없는 액션: {action}")
                return False

        except Exception as e:
            logger.error(f"최적화된 거래 신호 실행 오류 ({signal.get('stock_code')}): {e}")
            return False

    def can_open_new_position(self) -> bool:
        """새 포지션 오픈 가능 여부 체크"""
        try:
            max_positions = self._safe_int(self.config_loader.get_config_value('trading', 'max_positions', 10), 10)
            current_positions = len(self.positions)

            # 최대 보유 종목 수 체크
            if current_positions >= max_positions:
                logger.info(f"⚠️ 최대 보유 종목 수 도달: {current_positions}/{max_positions}")
                return False

            # 가용 현금 체크 (최소 10만원은 남겨두기)
            balance = getattr(self, '_cached_balance', {'available_cash': 0})
            if balance['available_cash'] < 100000:
                logger.info(f"⚠️ 가용 현금 부족: {balance['available_cash']:,}원")
                return False

            return True

        except Exception as e:
            logger.error(f"포지션 오픈 가능 여부 체크 오류: {e}")
            return False

    async def start_dedicated_signal_processor(self):
        """🚀 전용 신호 처리 루프 - 높은 우선순위로 실행"""
        logger.info("⚡ 전용 신호 처리 태스크 시작")

        while self.signal_processing_active:
            try:
                # 신호가 있을 때만 처리 (블로킹 대기)
                if not self.priority_signals.empty():
                    await self._process_pending_signals()
                else:
                    # 신호가 없으면 짧게 대기 (10ms)
                    await asyncio.sleep(0.01)

            except Exception as e:
                logger.error(f"전용 신호 처리 루프 오류: {e}")
                await asyncio.sleep(0.1)  # 오류 시 100ms 대기

    async def run(self):
        """메인 실행 루프 - 비동기 태스크 관리"""
        try:
            # 초기화
            await self.initialize()

            # 🚀 핵심 태스크들 생성 (통합된 신호 처리 시스템)
            scheduler_task = asyncio.create_task(self.strategy_scheduler.start_scheduler())

            # 🚀 전용 신호 처리 태스크 (모든 거래 신호를 통합 처리)
            signal_processor_task = asyncio.create_task(self.start_dedicated_signal_processor())

            # 🚀 포지션 모니터링을 신호로 변환하는 태스크 (기존 monitor_positions 대체)
            position_monitoring_task = asyncio.create_task(self.start_position_monitoring_signals())

            # 🕐 미체결 주문 모니터링 태스크
            pending_order_monitoring_task = asyncio.create_task(self.start_pending_order_monitoring())

            logger.info("🚀 통합 거래 시스템 시작 - 모든 거래 결정이 하나의 큐로 통합됨")

            # 모든 태스크가 완료될 때까지 대기
            await asyncio.gather(
                scheduler_task,
                signal_processor_task,
                position_monitoring_task,
                pending_order_monitoring_task,
                return_exceptions=True
            )

        except Exception as e:
            logger.error(f"메인 실행 루프 오류: {e}")
            raise
        finally:
            await self.cleanup()

    def get_system_status(self) -> Dict:
        """전체 시스템 상태 조회 - 대시보드용"""
        scheduler_status = self.strategy_scheduler.get_status()
        data_status = self.data_manager.get_system_status()

        # 🆕 active_stocks와 selected_stocks 동기화 상태 확인
        sync_status = self._check_stock_sync_status()

        return {
            'bot_running': True,
            'positions_count': len(self.positions),
            'pending_orders_count': len(self.pending_orders),
            'order_history_count': len(self.order_history),
            'scheduler': scheduler_status,
            'data_manager': data_status,
            'websocket_connected': getattr(self.websocket_manager, 'is_connected', lambda: True)() if callable(getattr(self.websocket_manager, 'is_connected', None)) else True,
            'stock_sync': sync_status,  # 🆕 동기화 상태 추가
            'realtime_cache_size': len(self.realtime_cache)  # 🆕 실시간 캐시 크기
        }

    def _check_stock_sync_status(self) -> Dict:
        """🆕 active_stocks와 selected_stocks 동기화 상태 확인"""
        try:
            # strategy_scheduler의 active_stocks에서 모든 종목 수집
            scheduler_stocks = set()
            for strategy_stocks in self.strategy_scheduler.active_stocks.values():
                scheduler_stocks.update(strategy_stocks)

            # main의 selected_stocks에서 현재 시간대 종목 수집
            current_selected = set(self.selected_stocks.get(self.current_time_slot or '', []))

            # 동기화 상태 분석
            sync_status = {
                'scheduler_stocks_count': len(scheduler_stocks),
                'selected_stocks_count': len(current_selected),
                'is_synced': scheduler_stocks == current_selected,
                'missing_in_selected': list(scheduler_stocks - current_selected),
                'extra_in_selected': list(current_selected - scheduler_stocks),
                'current_time_slot': self.current_time_slot,
                'last_sync_time': datetime.now().strftime('%H:%M:%S')
            }

            if not sync_status['is_synced']:
                logger.warning(f"⚠️ 종목 동기화 불일치 감지: scheduler={len(scheduler_stocks)}, selected={len(current_selected)}")

            return sync_status

        except Exception as e:
            logger.error(f"동기화 상태 체크 오류: {e}")
            return {
                'scheduler_stocks_count': 0,
                'selected_stocks_count': 0,
                'is_synced': False,
                'error': str(e)
            }

    async def cleanup(self):
        """정리 작업 - 안전한 종료"""
        logger.info("🧹 시스템 정리 중...")

        # 🚀 신호 처리 시스템 중지
        self.signal_processing_active = False
        logger.info("⚡ 신호 처리 시스템 중지")

        # 전략 스케줄러 중지
        self.strategy_scheduler.stop_scheduler()

        # 텔레그램 봇 중지
        if self.telegram_bot and self.telegram_bot.is_running():
            self.telegram_bot.stop_bot()
            logger.info("🤖 텔레그램 봇 중지")

        # WebSocket 연결 해제
        await self.websocket_manager.disconnect()

        # 데이터 매니저 정리
        self.data_manager.cleanup()

        # 데이터베이스 연결 종료
        db_manager.close_database()

        # 🚀 신호 처리 성능 통계 출력
        logger.info(
            f"📊 신호 처리 통계: "
            f"수신 {self.signal_stats['total_received']}개, "
            f"처리 {self.signal_stats['total_processed']}개, "
            f"필터링 {self.signal_stats['total_filtered']}개, "
            f"평균 처리시간 {self.signal_stats['average_processing_time']*1000:.0f}ms"
        )

        logger.info("✅ 시스템 정리 완료")

    def _signal_handler(self, signum, frame):
        """시그널 핸들러 - 안전한 종료"""
        logger.info(f"🛑 종료 신호 수신: {signum}")

        # 이미 종료 중이면 강제 종료
        if hasattr(self, '_shutting_down') and self._shutting_down:
            logger.warning("⚠️ 이미 종료 중 - 강제 종료")
            import sys
            sys.exit(1)

        self._shutting_down = True

        try:
            # 현재 이벤트 루프가 실행 중인지 확인
            loop = asyncio.get_running_loop()
            if loop and not loop.is_closed():
                # 이벤트 루프가 실행 중이면 cleanup task 생성
                task = loop.create_task(self._safe_cleanup_and_exit())
            else:
                # 이벤트 루프가 없으면 직접 종료
                import sys
                logger.warning("⚠️ 이벤트 루프 없음 - 직접 종료")
                sys.exit(0)
        except RuntimeError:
            # 이벤트 루프가 없는 경우 (아마 메인 스레드가 아님)
            logger.warning("⚠️ RuntimeError - 스레드에서 직접 종료")
            import sys
            sys.exit(0)
        except Exception as e:
            logger.error(f"❌ signal handler 오류: {e}")
            import sys
            sys.exit(1)

    async def _safe_cleanup_and_exit(self):
        """안전한 cleanup 후 종료"""
        try:
            logger.info("🧹 안전한 종료 시작...")

            # cleanup 실행
            await self.cleanup()

            logger.info("✅ cleanup 완료 - 프로그램 종료")

            # 짧은 대기 후 종료 (로그 출력 보장)
            await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"❌ cleanup 중 오류: {e}")
        finally:
            # 최종 종료
            import sys
            logger.info("🛑 프로그램 종료")
            sys.exit(0)

    def handle_time_slot_change(self, new_time_slot: str, new_stocks: List[str]):
        """
        시간대 변경 시 종목 처리 - 매수 종목 보호 로직

        Args:
            new_time_slot: 새로운 시간대
            new_stocks: 새로 선정된 종목 리스트
        """
        try:
            logger.info(f"⏰ 시간대 변경: {self.current_time_slot} → {new_time_slot}")

            # 🆕 selected_stocks 업데이트 (strategy_scheduler의 active_stocks와 동기화)
            self.selected_stocks[new_time_slot] = new_stocks.copy()
            logger.info(f"📋 {new_time_slot} 종목 선정: {len(new_stocks)}개")

            # 현재 보유 포지션 종목들 확인
            holding_stocks = set(self.positions.keys())

            # 이전 시간대 선정 종목들 중 매수한 종목들
            previous_selected = set(self.selected_stocks.get(self.current_time_slot or '', []))
            bought_stocks = holding_stocks.intersection(previous_selected)

            if bought_stocks:
                logger.info(f"📋 매수 종목 유지: {list(bought_stocks)} (포지션 보호)")

                # 매수한 종목들은 새 시간대에서도 HIGH 우선순위로 계속 모니터링
                for stock_code in bought_stocks:
                    self.data_manager.upgrade_priority(stock_code, DataPriority.HIGH)
                    # 손절/익절 콜백도 유지
                    self.data_manager.add_stock_request(
                        stock_code=stock_code,
                        priority=DataPriority.HIGH,
                        strategy_name="position_protection",
                        callback=self._create_position_protection_callback(stock_code)
                    )

            # 🆕 새로운 종목들의 실시간 캐시 준비 (웹소켓 구독으로 데이터 들어올 준비)
            new_monitoring_stocks = set(new_stocks) - holding_stocks
            if new_monitoring_stocks:
                logger.info(f"📈 신규 모니터링: {list(new_monitoring_stocks)}")
                # 실시간 캐시 초기화 (웹소켓 데이터 대기 상태)
                for stock_code in new_monitoring_stocks:
                    with self.cache_lock:
                        # 아직 가격 정보는 없지만 캐시 슬롯 준비
                        self.realtime_cache[stock_code] = {
                            'price': 0,
                            'timestamp': datetime.now(),
                            'source': 'websocket_ready',
                            'status': 'waiting_for_data'
                        }

            # 이전 시간대 정보 저장
            self.previous_time_slot = self.current_time_slot
            self.current_time_slot = new_time_slot

            # 포지션이 없는 이전 시간대 종목들은 BACKGROUND로 전환
            previous_only_stocks = previous_selected - bought_stocks
            if previous_only_stocks:
                logger.info(f"📉 백그라운드 전환: {list(previous_only_stocks)}")
                for stock_code in previous_only_stocks:
                    self.data_manager.upgrade_priority(stock_code, DataPriority.BACKGROUND)

            # 종목 변경 알림 (텔레그램 - 비동기)
            if self.telegram_bot and self.telegram_bot.is_running():
                change_msg = self._create_time_slot_change_message(
                    new_time_slot, bought_stocks, new_monitoring_stocks
                )
                asyncio.create_task(
                    self._send_telegram_notification_async(change_msg)
                )

        except Exception as e:
            logger.error(f"시간대 변경 처리 중 오류: {e}")

    def _create_position_protection_callback(self, stock_code: str):
        """포지션 보호용 콜백 함수 생성"""
        def callback(data: Dict, source: str):
            try:
                # 🆕 실시간 데이터 캐시 업데이트 (전역 사용 가능)
                current_price = data.get('current_price', {}).get('current_price', 0)
                if current_price > 0:
                    with self.cache_lock:
                        self.realtime_cache[stock_code] = {
                            'price': current_price,
                            'timestamp': datetime.now(),
                            'source': source,
                            'full_data': data
                        }

                # 포지션 보호 - 더 엄격한 손절/익절 적용
                if stock_code in self.positions and current_price > 0:
                    position = self.positions[stock_code]

                    if position['avg_price'] > 0:
                        profit_rate = (current_price - position['avg_price']) / position['avg_price']

                        # 시간대 변경 후 더 보수적인 손절/익절 (설정값 사용)
                        if profit_rate <= self.protection_stop_loss:
                            logger.warning(f"⚠️ 포지션 보호 손절: {stock_code} {profit_rate*100:.1f}%")
                            asyncio.create_task(
                                self.execute_strategy_order(
                                    stock_code=stock_code,
                                    order_type="SELL",
                                    quantity=position['quantity'],
                                    price=current_price,
                                    strategy_type="position_protection_stop"
                                )
                            )
                        elif profit_rate >= self.protection_take_profit:
                            logger.info(f"💰 포지션 보호 익절: {stock_code} {profit_rate*100:.1f}%")
                            asyncio.create_task(
                                self.execute_strategy_order(
                                    stock_code=stock_code,
                                    order_type="SELL",
                                    quantity=position['quantity'],
                                    price=current_price,
                                    strategy_type="position_protection_take"
                                )
                            )
            except Exception as e:
                logger.error(f"포지션 보호 콜백 오류 ({stock_code}): {e}")

        return callback

    def _create_time_slot_change_message(self, new_time_slot: str,
                                       bought_stocks: set, new_stocks: set) -> str:
        """시간대 변경 알림 메시지 생성"""
        slot_names = {
            'golden_time': '골든타임',
            'morning_leaders': '주도주 시간',
            'lunch_time': '점심시간',
            'closing_trend': '마감 추세'
        }

        slot_name = slot_names.get(new_time_slot, new_time_slot)

        msg = f"⏰ <b>{slot_name} 시작</b>\n\n"

        if bought_stocks:
            msg += f"🔒 <b>포지션 보호</b>\n"
            for stock in bought_stocks:
                position = self.positions.get(stock, {})
                qty = position.get('quantity', 0)
                avg_price = position.get('avg_price', 0)
                msg += f"  • {stock} {qty:,}주 @{avg_price:,}원\n"
            msg += "\n"

        if new_stocks:
            msg += f"📈 <b>신규 모니터링</b>\n"
            for stock in list(new_stocks)[:5]:  # 최대 5개만 표시
                msg += f"  • {stock}\n"
            if len(new_stocks) > 5:
                msg += f"  • 외 {len(new_stocks)-5}개\n"

        return msg

    async def _send_telegram_notification_async(self, message: str):
        """비동기 텔레그램 알림 전송"""
        try:
            if self.telegram_bot and self.telegram_bot.is_running():
                # 동기 메서드를 executor에서 비동기 실행
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    self.telegram_bot.send_notification_sync,
                    message
                )
        except Exception as e:
            logger.error(f"텔레그램 알림 전송 실패: {e}")

    def _safe_float(self, value, default: float) -> float:
        """안전한 float 변환"""
        try:
            return float(value) if value is not None else default
        except (ValueError, TypeError):
            return default

    def _safe_int(self, value, default: int) -> int:
        """안전한 int 변환"""
        try:
            return int(value) if value is not None else default
        except (ValueError, TypeError):
            return default

    def _load_trading_settings(self):
        """거래 관련 설정 로드 (config_loader 사용)"""
        try:
            # 손절/익절 설정
            self.stop_loss_threshold = self._safe_float(self.config_loader.get_config_value('risk_management', 'stop_loss_percent', -3.0), -3.0) / 100
            self.take_profit_threshold = self._safe_float(self.config_loader.get_config_value('risk_management', 'take_profit_percent', 5.0), 5.0) / 100

            # 포지션 보호 설정 (시간대 변경 시 더 보수적)
            self.protection_stop_loss = self._safe_float(self.config_loader.get_config_value('position_protection', 'stop_loss_percent', -2.0), -2.0) / 100
            self.protection_take_profit = self._safe_float(self.config_loader.get_config_value('position_protection', 'take_profit_percent', 2.0), 2.0) / 100

            # 모니터링 설정
            self.position_check_interval = self._safe_int(self.config_loader.get_config_value('monitoring', 'position_check_interval', 3), 3)
            self.no_position_wait_time = self._safe_int(self.config_loader.get_config_value('monitoring', 'no_position_wait_time', 10), 10)

            logger.info(f"⚙️ 거래 설정 로드 완료:")
            logger.info(f"   손절: {self.stop_loss_threshold*100:.1f}%, 익절: {self.take_profit_threshold*100:.1f}%")
            logger.info(f"   보호 손절: {self.protection_stop_loss*100:.1f}%, 보호 익절: {self.protection_take_profit*100:.1f}%")
            logger.info(f"   체크 간격: {self.position_check_interval}초, 무포지션 대기: {self.no_position_wait_time}초")

        except Exception as e:
            logger.error(f"❌ 설정 로드 오류: {e}. 기본값 사용")
            # 기본값 설정
            self.stop_loss_threshold = -0.03  # -3%
            self.take_profit_threshold = 0.05  # +5%
            self.protection_stop_loss = -0.02  # -2%
            self.protection_take_profit = 0.02  # +2%
            self.position_check_interval = 3
            self.no_position_wait_time = 10

    def _check_strategy_sell_conditions(self, stock_code: str, position: Dict,
                                       current_price: int, profit_rate: float) -> Optional[Dict]:
        """전략별 매도 조건 체크"""
        strategy_type = position.get('strategy_type', 'unknown')
        entry_time = position.get('entry_time', datetime.now())
        max_profit_rate = position.get('max_profit_rate', 0.0)
        holding_minutes = (datetime.now() - entry_time).total_seconds() / 60

        # 전략별 매도 조건
        if strategy_type == 'gap_trading' or 'gap_trading' in strategy_type:
            return self._check_gap_trading_sell(profit_rate, holding_minutes, max_profit_rate)

        elif strategy_type == 'volume_breakout' or 'volume_breakout' in strategy_type:
            return self._check_volume_breakout_sell(profit_rate, holding_minutes, max_profit_rate)

        elif strategy_type == 'momentum' or 'momentum' in strategy_type:
            return self._check_momentum_sell(profit_rate, holding_minutes, max_profit_rate)

        elif strategy_type in ['stop_loss', 'take_profit']:
            # 손절/익절로 매수한 경우 (재진입) - 더 보수적
            return self._check_conservative_sell(profit_rate, holding_minutes)

        else:
            # 기본 전략 (기존 로직)
            return self._check_default_sell(profit_rate)

    def _check_gap_trading_sell(self, profit_rate: float, holding_minutes: float,
                               max_profit_rate: float) -> Optional[Dict]:
        """갭 트레이딩 매도 조건 - 빠른 진입/탈출"""
        # 설정값 로드
        stop_loss = self._safe_float(self.config_loader.get_config_value('gap_trading_sell', 'stop_loss_percent', -2.0), -2.0) / 100
        take_profit = self._safe_float(self.config_loader.get_config_value('gap_trading_sell', 'take_profit_percent', 3.0), 3.0) / 100
        time_exit_min = self._safe_float(self.config_loader.get_config_value('gap_trading_sell', 'time_exit_minutes', 30), 30)
        time_exit_profit = self._safe_float(self.config_loader.get_config_value('gap_trading_sell', 'time_exit_profit_percent', 1.5), 1.5) / 100
        trailing_trigger = self._safe_float(self.config_loader.get_config_value('gap_trading_sell', 'trailing_stop_trigger', 2.0), 2.0) / 100
        trailing_stop = self._safe_float(self.config_loader.get_config_value('gap_trading_sell', 'trailing_stop_percent', 1.5), 1.5) / 100

        # 1. 타이트한 손절
        if profit_rate <= stop_loss:
            return {'reason': '갭_손절', 'type': 'stop_loss'}

        # 2. 빠른 익절
        if profit_rate >= take_profit:
            return {'reason': '갭_익절', 'type': 'take_profit'}

        # 3. 시간 기반 매도
        if holding_minutes >= time_exit_min and profit_rate >= time_exit_profit:
            return {'reason': '갭_시간익절', 'type': 'time_based'}

        # 4. Trailing stop
        if max_profit_rate >= trailing_trigger and profit_rate <= max_profit_rate - trailing_stop:
            return {'reason': '갭_트레일링', 'type': 'trailing_stop'}

        return None

    def _check_volume_breakout_sell(self, profit_rate: float, holding_minutes: float,
                                   max_profit_rate: float) -> Optional[Dict]:
        """볼륨 브레이크아웃 매도 조건 - 모멘텀 활용"""
        # 설정값 로드
        stop_loss = self._safe_float(self.config_loader.get_config_value('volume_breakout_sell', 'stop_loss_percent', -2.5), -2.5) / 100
        take_profit = self._safe_float(self.config_loader.get_config_value('volume_breakout_sell', 'take_profit_percent', 4.0), 4.0) / 100
        momentum_exit_min = self._safe_float(self.config_loader.get_config_value('volume_breakout_sell', 'momentum_exit_minutes', 60), 60)
        momentum_exit_profit = self._safe_float(self.config_loader.get_config_value('volume_breakout_sell', 'momentum_exit_profit_percent', 2.0), 2.0) / 100
        trailing_trigger = self._safe_float(self.config_loader.get_config_value('volume_breakout_sell', 'trailing_stop_trigger', 3.0), 3.0) / 100
        trailing_stop = self._safe_float(self.config_loader.get_config_value('volume_breakout_sell', 'trailing_stop_percent', 2.0), 2.0) / 100

        # 1. 표준 손절
        if profit_rate <= stop_loss:
            return {'reason': '볼륨_손절', 'type': 'stop_loss'}

        # 2. 적극적 익절
        if profit_rate >= take_profit:
            return {'reason': '볼륨_익절', 'type': 'take_profit'}

        # 3. 모멘텀 유지 체크
        if holding_minutes >= momentum_exit_min and profit_rate >= momentum_exit_profit:
            return {'reason': '볼륨_모멘텀익절', 'type': 'momentum_based'}

        # 4. Trailing stop
        if max_profit_rate >= trailing_trigger and profit_rate <= max_profit_rate - trailing_stop:
            return {'reason': '볼륨_트레일링', 'type': 'trailing_stop'}

        return None

    def _check_momentum_sell(self, profit_rate: float, holding_minutes: float,
                           max_profit_rate: float) -> Optional[Dict]:
        """모멘텀 매도 조건 - 트렌드 지속 기대"""
        # 설정값 로드
        stop_loss = self._safe_float(self.config_loader.get_config_value('momentum_sell', 'stop_loss_percent', -3.5), -3.5) / 100
        take_profit = self._safe_float(self.config_loader.get_config_value('momentum_sell', 'take_profit_percent', 6.0), 6.0) / 100
        long_term_min = self._safe_float(self.config_loader.get_config_value('momentum_sell', 'long_term_exit_minutes', 120), 120)
        long_term_profit = self._safe_float(self.config_loader.get_config_value('momentum_sell', 'long_term_exit_profit_percent', 3.0), 3.0) / 100
        trailing_trigger = self._safe_float(self.config_loader.get_config_value('momentum_sell', 'trailing_stop_trigger', 4.0), 4.0) / 100
        trailing_stop = self._safe_float(self.config_loader.get_config_value('momentum_sell', 'trailing_stop_percent', 2.5), 2.5) / 100

        # 1. 관대한 손절
        if profit_rate <= stop_loss:
            return {'reason': '모멘텀_손절', 'type': 'stop_loss'}

        # 2. 높은 익절
        if profit_rate >= take_profit:
            return {'reason': '모멘텀_익절', 'type': 'take_profit'}

        # 3. 장기 보유 전략
        if holding_minutes >= long_term_min and profit_rate >= long_term_profit:
            return {'reason': '모멘텀_장기익절', 'type': 'long_term'}

        # 4. Trailing stop
        if max_profit_rate >= trailing_trigger and profit_rate <= max_profit_rate - trailing_stop:
            return {'reason': '모멘텀_트레일링', 'type': 'trailing_stop'}

        return None

    def _check_conservative_sell(self, profit_rate: float, holding_minutes: float) -> Optional[Dict]:
        """보수적 매도 조건 - 손절/익절 재진입"""
        # 설정값 로드
        stop_loss = self._safe_float(self.config_loader.get_config_value('conservative_sell', 'stop_loss_percent', -1.5), -1.5) / 100
        take_profit = self._safe_float(self.config_loader.get_config_value('conservative_sell', 'take_profit_percent', 2.0), 2.0) / 100

        # 1. 엄격한 손절
        if profit_rate <= stop_loss:
            return {'reason': '보수_손절', 'type': 'stop_loss'}

        # 2. 빠른 익절
        if profit_rate >= take_profit:
            return {'reason': '보수_익절', 'type': 'take_profit'}

        return None

    def _check_default_sell(self, profit_rate: float) -> Optional[Dict]:
        """기본 매도 조건 - 기존 로직"""
        # 설정값 로드 (기본값으로 기존 threshold 사용)
        stop_loss = self._safe_float(self.config_loader.get_config_value('default_sell', 'stop_loss_percent', self.stop_loss_threshold * 100), self.stop_loss_threshold * 100) / 100
        take_profit = self._safe_float(self.config_loader.get_config_value('default_sell', 'take_profit_percent', self.take_profit_threshold * 100), self.take_profit_threshold * 100) / 100

        if profit_rate <= stop_loss:
            return {'reason': '기본_손절', 'type': 'stop_loss'}

        if profit_rate >= take_profit:
            return {'reason': '기본_익절', 'type': 'take_profit'}

        return None

    def update_realtime_cache(self, stock_code: str, current_price: int, source: str = "unknown"):
        """🆕 실시간 데이터 캐시 업데이트 (어디서든 호출 가능)"""
        if current_price > 0:
            with self.cache_lock:
                self.realtime_cache[stock_code] = {
                    'price': current_price,
                    'timestamp': datetime.now(),
                    'source': source
                }

    def get_realtime_price(self, stock_code: str, max_age_seconds: int = 10) -> Optional[Dict]:
        """🆕 실시간 캐시에서 현재가 조회 (fallback 없음)"""
        with self.cache_lock:
            cached_data = self.realtime_cache.get(stock_code)
            if cached_data:
                cache_age = (datetime.now() - cached_data['timestamp']).total_seconds()
                if cache_age <= max_age_seconds:
                    return {
                        'price': cached_data['price'],
                        'age_seconds': cache_age,
                        'source': cached_data['source']
                    }
        return None

    def get_cached_price_with_fallback(self, stock_code: str) -> Dict:
        """🆕 캐시 우선, fallback 포함 현재가 조회"""
        # 1. 실시간 캐시 시도
        realtime_data = self.get_realtime_price(stock_code, max_age_seconds=10)
        if realtime_data:
            return {
                'price': realtime_data['price'],
                'source': f"websocket({realtime_data['age_seconds']:.1f}s)",
                'success': True
            }

        # 2. HybridDataManager 시도
        try:
            current_data = self.data_manager.get_latest_data(stock_code)
            if current_data and 'current_price' in current_data:
                price = current_data['current_price']['current_price']
                if price > 0:
                    return {
                        'price': price,
                        'source': "hybrid_manager",
                        'success': True
                    }
        except Exception as e:
            logger.debug(f"HybridDataManager 조회 실패: {e}")

        # 3. 직접 REST API 호출 (최후 수단)
        try:
            price_data = self.trading_api.get_current_price(stock_code)
            if price_data and 'current_price' in price_data:
                price = price_data['current_price']
                if price > 0:
                    # 성공하면 실시간 캐시에도 저장
                    self.update_realtime_cache(stock_code, price, "rest_api_direct")
                    return {
                        'price': price,
                        'source': "rest_api_direct",
                        'success': True
                    }
        except Exception as e:
            logger.debug(f"직접 REST API 조회 실패: {e}")

        return {
            'price': 0,
            'source': "failed",
            'success': False
        }

    async def start_position_monitoring_signals(self):
        """🚀 포지션 모니터링을 신호로 변환하는 새로운 시스템"""
        logger.info("🛡️ 통합 포지션 모니터링 시작 (신호 기반)")
        last_check_time = {}

        while self.signal_processing_active:
            try:
                if not self.positions:
                    await asyncio.sleep(float(self.no_position_wait_time))
                    continue

                current_time = datetime.now()
                positions_to_check = []

                # 스레드 안전하게 포지션 복사
                with self.position_lock:
                    for stock_code, position in list(self.positions.items()):
                        # 종목별로 최소 5초 간격 체크
                        last_time = last_check_time.get(stock_code, current_time)
                        if (current_time - last_time).total_seconds() >= 5:
                            positions_to_check.append((stock_code, position.copy()))
                            last_check_time[stock_code] = current_time

                # 포지션별 모니터링 신호 생성
                for stock_code, position in positions_to_check:
                    try:
                        # 현재가 조회
                        price_result = self.get_cached_price_with_fallback(stock_code)
                        if not price_result['success']:
                            continue

                        current_price = price_result['price']
                        avg_price = position['avg_price']

                        if avg_price <= 0:
                            continue

                        profit_rate = (current_price - avg_price) / avg_price

                        # 최대 수익률 업데이트
                        if profit_rate > position.get('max_profit_rate', 0.0):
                            with self.position_lock:
                                if stock_code in self.positions:
                                    self.positions[stock_code]['max_profit_rate'] = profit_rate

                        # 🚀 매도 조건을 신호로 변환
                        sell_signal = self._check_strategy_sell_conditions(
                            stock_code, position, current_price, profit_rate
                        )

                        if sell_signal:
                            # 포지션 보호 신호를 우선순위 큐에 추가
                            protection_signal = {
                                'action': 'SELL',
                                'stock_code': stock_code,
                                'price': current_price,
                                'reason': sell_signal['reason'],
                                'strategy_type': 'position_protection',
                                'strength': 0.9,  # 높은 강도 (보호 목적)
                                'quantity': position['quantity'],
                                'source': 'position_monitoring'
                            }

                            # 우선순위 큐에 추가 (기존 매수/매도 신호와 동일한 방식)
                            await self.add_trading_signal(protection_signal)

                            logger.info(f"🛡️ 포지션 보호 신호 생성: {stock_code} - {sell_signal['reason']}")

                    except Exception as e:
                        logger.error(f"포지션 모니터링 오류 ({stock_code}): {e}")

                # 적응적 대기 시간
                position_count = len(self.positions)
                if position_count == 0:
                    sleep_time = float(self.no_position_wait_time)
                elif position_count <= 5:
                    sleep_time = float(self.position_check_interval)
                else:
                    sleep_time = max(2.0, float(self.position_check_interval) - 1.0)

                await asyncio.sleep(sleep_time)

            except Exception as e:
                logger.error(f"포지션 모니터링 전체 오류: {e}")
                await asyncio.sleep(10)

    async def start_pending_order_monitoring(self):
        """🕐 미체결 주문 모니터링 시스템"""
        logger.info("🕐 미체결 주문 모니터링 시작")

        # 설정값 로드
        timeout_seconds = self._safe_int(self.config_loader.get_config_value('trading', 'pending_order_timeout', 300), 300)
        check_interval = self._safe_int(self.config_loader.get_config_value('trading', 'pending_order_check_interval', 30), 30)
        buy_timeout_action = str(self.config_loader.get_config_value('trading', 'buy_timeout_action', 'price_adjust'))
        sell_timeout_action = str(self.config_loader.get_config_value('trading', 'sell_timeout_action', 'market_order'))
        price_adjust_percent = self._safe_float(self.config_loader.get_config_value('trading', 'price_adjust_percent', 0.5), 0.5)
        max_adjustments = self._safe_int(self.config_loader.get_config_value('trading', 'max_price_adjustments', 3), 3)
        force_market_time = self._safe_int(self.config_loader.get_config_value('trading', 'market_order_force_time', 600), 600)

        while self.signal_processing_active:
            try:
                current_time = datetime.now()
                timeout_orders = []

                # 스레드 안전하게 미체결 주문 복사
                with self.pending_order_lock:
                    for order_no, order_info in list(self.pending_orders.items()):
                        order_time = order_info.get('timestamp', current_time)
                        elapsed_seconds = (current_time - order_time).total_seconds()

                        # 타임아웃된 주문 찾기
                        if elapsed_seconds >= timeout_seconds:
                            timeout_orders.append((order_no, order_info.copy(), elapsed_seconds))

                # 타임아웃된 주문 처리
                for order_no, order_info, elapsed_seconds in timeout_orders:
                    try:
                        await self._handle_timeout_order(
                            order_no, order_info, elapsed_seconds,
                            buy_timeout_action, sell_timeout_action,
                            price_adjust_percent, max_adjustments, force_market_time
                        )
                    except Exception as e:
                        logger.error(f"타임아웃 주문 처리 오류 ({order_no}): {e}")

                await asyncio.sleep(check_interval)

            except Exception as e:
                logger.error(f"미체결 주문 모니터링 오류: {e}")
                await asyncio.sleep(60)  # 오류 시 1분 대기

    async def _handle_timeout_order(self, order_no: str, order_info: Dict, elapsed_seconds: float,
                                   buy_timeout_action: str, sell_timeout_action: str,
                                   price_adjust_percent: float, max_adjustments: int,
                                   force_market_time: float):
        """타임아웃된 주문 처리"""
        try:
            stock_code = order_info.get('stock_code', '')
            order_type = order_info.get('order_type', '')  # '매수' or '매도'
            original_price = order_info.get('price', 0)
            quantity = order_info.get('quantity', 0)
            strategy_type = order_info.get('strategy_type', 'unknown')

            logger.warning(f"⏰ 미체결 주문 타임아웃: {stock_code} {order_type} {elapsed_seconds:.0f}초 경과")

            # 현재 주문 상태 확인 (실제 API 호출)
            order_status = await self._check_order_status(order_no)

            if order_status == 'filled':
                # 이미 체결됨 - pending_orders에서만 제거
                logger.info(f"✅ 주문 이미 체결됨: {order_no}")
                with self.pending_order_lock:
                    if order_no in self.pending_orders:
                        del self.pending_orders[order_no]
                return

            if order_status == 'cancelled':
                # 이미 취소됨 - pending_orders에서만 제거
                logger.info(f"❌ 주문 이미 취소됨: {order_no}")
                with self.pending_order_lock:
                    if order_no in self.pending_orders:
                        del self.pending_orders[order_no]
                    if order_no in self.pending_order_adjustments:
                        del self.pending_order_adjustments[order_no]
                return

            # 시장가 강제 전환 시간 체크
            if elapsed_seconds >= force_market_time:
                logger.warning(f"🚨 강제 시장가 전환: {stock_code} {order_type}")
                await self._convert_to_market_order(order_no, order_info)
                return

            # 조정 횟수 확인
            adjustment_count = self.pending_order_adjustments.get(order_no, 0)
            if adjustment_count >= max_adjustments:
                logger.warning(f"⚠️ 최대 조정 횟수 초과: {stock_code} {order_type}")
                # 매도는 시장가, 매수는 취소
                if order_type == '매도':
                    await self._convert_to_market_order(order_no, order_info)
                else:
                    await self._cancel_order(order_no, order_info)
                return

            # 액션별 처리
            action = sell_timeout_action if order_type == '매도' else buy_timeout_action

            if action == 'cancel':
                await self._cancel_order(order_no, order_info)
            elif action == 'market_order':
                await self._convert_to_market_order(order_no, order_info)
            elif action == 'price_adjust':
                await self._adjust_order_price(order_no, order_info, price_adjust_percent)
            else:
                logger.warning(f"⚠️ 알 수 없는 액션: {action}")
                await self._cancel_order(order_no, order_info)

        except Exception as e:
            logger.error(f"타임아웃 주문 처리 전체 오류: {e}")

    async def _check_order_status(self, order_no: str) -> str:
        """주문 상태 확인"""
        try:
            # KIS API로 주문 상태 조회 (실제 구현은 trading_api에 따라 다름)
            # 임시로 pending 반환 (실제로는 get_order_detail 등의 메서드 사용)
            logger.debug(f"주문 상태 확인: {order_no}")
            return 'pending'  # 기본값 (실제 API 구현 후 수정 필요)
        except Exception as e:
            logger.error(f"주문 상태 확인 오류: {e}")
            return 'pending'

    async def _cancel_order(self, order_no: str, order_info: Dict):
        """주문 취소"""
        try:
            stock_code = order_info.get('stock_code', '')
            order_type = order_info.get('order_type', '')

            # KIS API로 주문 취소 (실제 구현은 trading_api에 따라 다름)
            try:
                # 실제 API 호출 (stock_code, quantity 파라미터 추가)
                quantity = order_info.get('quantity', 0)
                cancel_result = self.trading_api.cancel_order(
                    order_no=order_no,
                    stock_code=stock_code,
                    quantity=quantity
                )
                logger.info(f"📋 주문 취소 시도: {stock_code} {order_type}")
            except Exception as api_error:
                logger.error(f"API 주문 취소 오류: {api_error}")
                cancel_result = {'success': False}

            if cancel_result and cancel_result.get('success', False):
                logger.info(f"✅ 주문 취소 성공: {stock_code} {order_type}")

                # pending_orders에서 제거
                with self.pending_order_lock:
                    if order_no in self.pending_orders:
                        del self.pending_orders[order_no]
                    if order_no in self.pending_order_adjustments:
                        del self.pending_order_adjustments[order_no]

                # 텔레그램 알림
                if self.telegram_bot and self.telegram_bot.is_running():
                    cancel_msg = f"🚫 <b>주문 취소</b>\n📊 {stock_code} {order_type}\n⏰ 타임아웃으로 인한 자동 취소"
                    asyncio.create_task(self._send_telegram_notification_async(cancel_msg))
            else:
                logger.error(f"❌ 주문 취소 실패: {stock_code} {order_type}")

        except Exception as e:
            logger.error(f"주문 취소 오류: {e}")
        return {'success': False}

    async def _convert_to_market_order(self, order_no: str, order_info: Dict):
        """시장가 주문으로 전환"""
        try:
            stock_code = order_info.get('stock_code', '')
            order_type = order_info.get('order_type', '')
            quantity = order_info.get('quantity', 0)
            strategy_type = order_info.get('strategy_type', 'timeout_convert')

            logger.info(f"🔄 시장가 전환: {stock_code} {order_type}")

            # 기존 주문 취소
            cancel_result = self.trading_api.cancel_order(
                order_no=order_no,
                stock_code=stock_code,
                quantity=quantity
            )
            if not (cancel_result and cancel_result.get('success', False)):
                logger.warning(f"⚠️ 기존 주문 취소 실패: {order_no}")
                return

            # 시장가 주문 실행
            new_order_no = await self.execute_strategy_order(
                stock_code=stock_code,
                order_type="BUY" if order_type == '매수' else "SELL",
                quantity=quantity,
                price=0,  # 시장가
                strategy_type=f"{strategy_type}_market_convert"
            )

            # 기존 주문 정보 정리
            with self.pending_order_lock:
                if order_no in self.pending_orders:
                    del self.pending_orders[order_no]
                if order_no in self.pending_order_adjustments:
                    del self.pending_order_adjustments[order_no]

            if new_order_no:
                logger.info(f"✅ 시장가 전환 성공: {stock_code} {order_type} → {new_order_no}")

                # 텔레그램 알림
                if self.telegram_bot and self.telegram_bot.is_running():
                    convert_msg = f"🔄 <b>시장가 전환</b>\n📊 {stock_code} {order_type}\n⚡ 미체결로 인한 시장가 전환"
                    asyncio.create_task(self._send_telegram_notification_async(convert_msg))
            else:
                logger.error(f"❌ 시장가 전환 실패: {stock_code} {order_type}")

        except Exception as e:
            logger.error(f"시장가 전환 오류: {e}")

    async def _adjust_order_price(self, order_no: str, order_info: Dict, adjust_percent: float):
        """주문 가격 조정"""
        try:
            stock_code = order_info.get('stock_code', '')
            order_type = order_info.get('order_type', '')
            original_price = order_info.get('price', 0)
            quantity = order_info.get('quantity', 0)
            strategy_type = order_info.get('strategy_type', 'price_adjust')

            # 현재가 조회
            price_result = self.get_cached_price_with_fallback(stock_code)
            if not price_result['success']:
                logger.warning(f"⚠️ 현재가 조회 실패 - 시장가 전환: {stock_code}")
                await self._convert_to_market_order(order_no, order_info)
                return

            current_price = price_result['price']

            # 새로운 가격 계산
            if order_type == '매수':
                # 매수는 더 높은 가격으로 조정
                new_price = int(current_price * (1 + adjust_percent / 100))
            else:
                # 매도는 더 낮은 가격으로 조정
                new_price = int(current_price * (1 - adjust_percent / 100))

            # 가격이 같으면 시장가로 전환
            if new_price == original_price:
                logger.info(f"🔄 가격 동일 - 시장가 전환: {stock_code}")
                await self._convert_to_market_order(order_no, order_info)
                return

            logger.info(f"💰 가격 조정: {stock_code} {order_type} {original_price:,} → {new_price:,}원")

            # 기존 주문 취소
            cancel_result = self.trading_api.cancel_order(
                order_no=order_no,
                stock_code=stock_code,
                quantity=quantity
            )
            if not (cancel_result and cancel_result.get('success', False)):
                logger.warning(f"⚠️ 기존 주문 취소 실패: {order_no}")
                return

            # 새 가격으로 주문
            new_order_no = await self.execute_strategy_order(
                stock_code=stock_code,
                order_type="BUY" if order_type == '매수' else "SELL",
                quantity=quantity,
                price=new_price,
                strategy_type=f"{strategy_type}_price_adjusted"
            )

            # 조정 횟수 증가
            with self.pending_order_lock:
                if order_no in self.pending_orders:
                    del self.pending_orders[order_no]

                old_adjustments = self.pending_order_adjustments.get(order_no, 0)
                if order_no in self.pending_order_adjustments:
                    del self.pending_order_adjustments[order_no]

                # 새 주문번호로 조정 횟수 이관
                if new_order_no:
                    self.pending_order_adjustments[new_order_no] = old_adjustments + 1

            if new_order_no:
                adjustment_count = self.pending_order_adjustments.get(new_order_no, 0)
                logger.info(f"✅ 가격 조정 성공: {stock_code} {order_type} → {new_order_no} (조정 {adjustment_count}회)")

                # 텔레그램 알림
                if self.telegram_bot and self.telegram_bot.is_running():
                    adjust_msg = (f"💰 <b>가격 조정</b>\n📊 {stock_code} {order_type}\n"
                                f"💸 {original_price:,} → {new_price:,}원\n"
                                f"🔄 조정 {adjustment_count}회차")
                    asyncio.create_task(self._send_telegram_notification_async(adjust_msg))
            else:
                logger.error(f"❌ 가격 조정 실패: {stock_code} {order_type}")

        except Exception as e:
            logger.error(f"가격 조정 오류: {e}")

if __name__ == "__main__":
    import signal
    import sys

    # 봇 실행
    bot = StockBotMain()
    shutdown_requested = False

    # 시그널 핸들러 등록 (중복 호출 방지)
    def signal_handler(signum, frame):
        global shutdown_requested
        if shutdown_requested:
            print("\n🚨 강제 종료!")
            sys.exit(1)

        shutdown_requested = True
        print(f"\n🛑 종료 신호 수신 ({signum})")
        print("🔄 안전한 종료 중... (다시 Ctrl+C를 누르면 강제 종료)")

        # 모든 실행 중인 태스크 취소
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                # 메인 태스크들 취소
                for task in asyncio.all_tasks(loop):
                    if not task.done():
                        task.cancel()
        except RuntimeError:
            pass

        # cleanup은 finally 블록에서 실행

    # SIGINT (Ctrl+C), SIGTERM 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n👋 KeyboardInterrupt 감지")
        shutdown_requested = True
    except asyncio.CancelledError:
        print("\n🛑 태스크 취소됨")
    except Exception as e:
        print(f"❌ 시스템 오류: {e}")
        shutdown_requested = True
    finally:
        if shutdown_requested:
            print("🧹 시스템 정리 중...")
            try:
                # 새로운 이벤트 루프에서 cleanup 실행
                asyncio.run(bot.cleanup())
            except Exception as e:
                print(f"⚠️ 정리 중 오류: {e}")
        print("✅ StockBot 완전 종료")
