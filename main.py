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

        # 10. 거래 신호 큐 (strategy_scheduler → monitor_positions)
        self.trading_signals: asyncio.Queue = asyncio.Queue()
        self.signal_lock = threading.RLock()

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
        kst = pytz.timezone('Asia/Seoul')
        now = datetime.now(kst)
        is_market_open = self._is_market_open(now)

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

    def _is_market_open(self, current_time: datetime) -> bool:
        """
        장 시간 여부 확인

        Args:
            current_time: 확인할 시간 (timezone aware)

        Returns:
            장 시간 여부
        """
        # 평일 여부 확인 (0=월요일, 6=일요일)
        if current_time.weekday() >= 5:  # 토요일(5), 일요일(6)
            return False

        # 장 시간 확인 (09:00 ~ 15:30)
        market_open = current_time.replace(hour=9, minute=0, second=0, microsecond=0)
        market_close = current_time.replace(hour=15, minute=30, second=0, microsecond=0)

        return market_open <= current_time <= market_close

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
        # 메모리 포지션 업데이트
        if stock_code not in self.positions:
            self.positions[stock_code] = {
                'quantity': 0,
                'avg_price': 0,
                'total_amount': 0
            }

        position = self.positions[stock_code]
        new_total_amount = position['total_amount'] + (qty * price)
        new_quantity = position['quantity'] + qty
        new_avg_price = new_total_amount // new_quantity if new_quantity > 0 else 0

        self.positions[stock_code] = {
            'quantity': new_quantity,
            'avg_price': new_avg_price,
            'total_amount': new_total_amount
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
                'strategy_type': "unknown",  # 나중에 추가 정보로 업데이트
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

        logger.info(f"📈 매수 완료: {stock_code} 보유량 {new_quantity}주 (평단: {new_avg_price:,}원)")

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
        """포지션 모니터링 + 전체 거래 실행 - 통합 거래 관리"""
        logger.info("🔍 포지션 모니터링 + 거래 실행 시작")
        last_check_time = {}  # 종목별 마지막 체크 시간

        while True:
            try:
                # 1. 먼저 대기 중인 거래 신호 처리
                await self._process_pending_signals()

                # 2. 기존 포지션 모니터링 (손절/익절)
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
                        # 현재가 조회 (캐시된 데이터 우선 사용)
                        current_data = self.data_manager.get_latest_data(stock_code)
                        if not current_data or 'current_price' not in current_data:
                            logger.debug(f"현재가 데이터 없음: {stock_code}")
                            continue

                        current_price = current_data['current_price']['current_price']
                        avg_price = position['avg_price']
                        quantity = position['quantity']

                        # 안전장치: 가격이 0이면 스킵
                        if current_price <= 0 or avg_price <= 0:
                            continue

                        profit_rate = (current_price - avg_price) / avg_price

                        # 손절 우선 처리 (손실 제한)
                        if profit_rate <= self.stop_loss_threshold:
                            logger.warning(f"🔻 손절 신호: {stock_code} {profit_rate*100:.1f}%")
                            await self._execute_sell_order(stock_code, quantity, current_price, "손절")

                        # 익절 처리
                        elif profit_rate >= self.take_profit_threshold:
                            logger.info(f"📈 익절 신호: {stock_code} {profit_rate*100:.1f}%")
                            await self._execute_sell_order(stock_code, quantity, current_price, "익절")

                        # 현재 상태 로깅 (5분마다)
                        elif len(positions_to_check) <= 3:  # 포지션 적을 때만
                            logger.debug(f"📊 {stock_code}: {profit_rate*100:+.1f}% ({current_price:,}원)")

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
            target_investment = min(target_investment, balance['available_cash'] * 0.8)  # 가용 현금의 80%까지만

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
        """거래 신호를 큐에 추가 (strategy_scheduler에서 호출)"""
        try:
            await self.trading_signals.put(signal)
            logger.debug(f"📡 거래 신호 추가: {signal.get('stock_code')} {signal.get('action')}")
        except Exception as e:
            logger.error(f"거래 신호 추가 오류: {e}")

    async def _process_pending_signals(self):
        """대기 중인 거래 신호 처리 (monitor_positions에서 호출)"""
        try:
            # 큐에 있는 모든 신호를 한 번에 처리 (비블로킹)
            processed_signals = 0
            max_signals_per_cycle = 10  # 한 번에 처리할 최대 신호 수

            while not self.trading_signals.empty() and processed_signals < max_signals_per_cycle:
                try:
                    signal = self.trading_signals.get_nowait()
                    await self._execute_trading_signal(signal)
                    processed_signals += 1
                except asyncio.QueueEmpty:
                    break
                except Exception as e:
                    logger.error(f"신호 처리 오류: {e}")

            if processed_signals > 0:
                logger.debug(f"🔄 거래 신호 처리 완료: {processed_signals}개")

        except Exception as e:
            logger.error(f"신호 처리 전체 오류: {e}")

    async def _execute_trading_signal(self, signal: Dict):
        """개별 거래 신호 실행"""
        try:
            stock_code = signal.get('stock_code')
            action = signal.get('action')
            price = signal.get('price', 0)
            reason = signal.get('reason', 'unknown')
            strategy_type = signal.get('strategy_type', 'signal')

            # 필수 필드 검증
            if not stock_code or not action:
                logger.warning(f"⚠️ 신호 필수 필드 누락: stock_code={stock_code}, action={action}")
                return

            if action == 'BUY':
                # 새 포지션 오픈 가능 여부 체크
                if not self.can_open_new_position():
                    logger.info(f"⚠️ 신규 포지션 오픈 불가: {stock_code}")
                    return

                # 동적 포지션 사이징 계산
                position_info = self.calculate_position_size(
                    signal=signal,
                    stock_code=stock_code,
                    current_price=price
                )

                quantity = position_info['quantity']
                investment_amount = position_info['investment_amount']

                if quantity > 0:
                    await self.execute_strategy_order(
                        stock_code=stock_code,
                        order_type="BUY",
                        quantity=quantity,
                        price=price,
                        strategy_type=strategy_type
                    )
                    logger.info(
                        f"📈 매수 신호 실행: {stock_code} {quantity}주 @ {price:,}원 "
                        f"(투자금액: {investment_amount:,}원, {reason})"
                    )
                else:
                    logger.info(f"⚠️ 매수 수량 없음: {stock_code} (계산된 수량: {quantity})")

            elif action == 'SELL':
                # 보유 수량 확인 후 매도
                position = self.positions.get(stock_code, {})
                quantity = position.get('quantity', 0)

                if quantity > 0:
                    await self.execute_strategy_order(
                        stock_code=stock_code,
                        order_type="SELL",
                        quantity=quantity,
                        price=price,
                        strategy_type=strategy_type
                    )

                    # 예상 손익 계산
                    avg_price = position.get('avg_price', price)
                    expected_profit = (price - avg_price) * quantity
                    expected_profit_rate = (expected_profit / (avg_price * quantity)) * 100 if avg_price > 0 else 0

                    logger.info(
                        f"📉 매도 신호 실행: {stock_code} {quantity}주 @ {price:,}원 "
                        f"(예상손익: {expected_profit:+,}원 {expected_profit_rate:+.1f}%, {reason})"
                    )
                else:
                    logger.warning(f"⚠️ 매도할 포지션 없음: {stock_code}")

        except Exception as e:
            logger.error(f"거래 신호 실행 오류: {e}")



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

    async def run(self):
        """메인 실행 루프 - 비동기 태스크 관리"""
        try:
            # 초기화
            await self.initialize()

            # 핵심 태스크들 생성
            scheduler_task = asyncio.create_task(
                self.strategy_scheduler.start_scheduler(),
                name="strategy_scheduler"
            )
            monitor_task = asyncio.create_task(
                self.monitor_positions(),
                name="position_monitor"
            )
            listen_task = asyncio.create_task(
                self.websocket_manager.start_listening(),
                name="websocket_listener"
            )

            logger.info("🎮 StockBot 운영 시작 - 3개 핵심 태스크 실행")
            logger.info("   📅 전략 스케줄러 (종목 탐색 + 시간대별 전략)")
            logger.info("   🔍 포지션 모니터링 (손절/익절)")
            logger.info("   📡 WebSocket 리스너 (실시간 체결통보)")
            logger.info("   💰 잔고 갱신 (매수/매도 시마다 자동)")

            # 모든 핵심 태스크 동시 실행 및 관리
            await asyncio.gather(
                scheduler_task,  # 전략 스케줄러 (가장 중요!)
                monitor_task,    # 포지션 모니터링
                listen_task,     # WebSocket 수신
                return_exceptions=False  # 하나라도 실패하면 전체 중단
            )

        except KeyboardInterrupt:
            logger.info("👋 사용자 종료 요청")
        except Exception as e:
            logger.error(f"❌ 시스템 오류: {e}")
        finally:
            await self.cleanup()

    def get_system_status(self) -> Dict:
        """전체 시스템 상태 조회 - 대시보드용"""
        scheduler_status = self.strategy_scheduler.get_status()
        data_status = self.data_manager.get_system_status()

        return {
            'bot_running': True,
            'positions_count': len(self.positions),
            'pending_orders_count': len(self.pending_orders),
            'order_history_count': len(self.order_history),
            'scheduler': scheduler_status,
            'data_manager': data_status,
            'websocket_connected': getattr(self.websocket_manager, 'is_connected', lambda: True)() if callable(getattr(self.websocket_manager, 'is_connected', None)) else True
        }

    async def cleanup(self):
        """정리 작업 - 안전한 종료"""
        logger.info("🧹 시스템 정리 중...")

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

        logger.info("✅ 시스템 정리 완료")

    def _signal_handler(self, signum, frame):
        """시그널 핸들러 - 안전한 종료"""
        logger.info(f"종료 신호 수신: {signum}")
        # 별도 태스크로 cleanup 실행
        asyncio.create_task(self.cleanup())

    async def shutdown(self):
        """안전한 종료 - cleanup으로 통합"""
        await self.cleanup()

    def handle_time_slot_change(self, new_time_slot: str, new_stocks: List[str]):
        """
        시간대 변경 시 종목 처리 - 매수 종목 보호 로직

        Args:
            new_time_slot: 새로운 시간대
            new_stocks: 새로 선정된 종목 리스트
        """
        try:
            logger.info(f"⏰ 시간대 변경: {self.current_time_slot} → {new_time_slot}")

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

            # 이전 시간대 정보 저장
            self.previous_time_slot = self.current_time_slot
            self.current_time_slot = new_time_slot

            # 새로운 시간대 종목 저장
            self.selected_stocks[new_time_slot] = new_stocks.copy()

            # 포지션이 없는 이전 시간대 종목들은 BACKGROUND로 전환
            previous_only_stocks = previous_selected - bought_stocks
            if previous_only_stocks:
                logger.info(f"📉 백그라운드 전환: {list(previous_only_stocks)}")
                for stock_code in previous_only_stocks:
                    self.data_manager.upgrade_priority(stock_code, DataPriority.BACKGROUND)

            # 새로운 종목들 중 아직 모니터링하지 않는 종목들만 추가
            new_monitoring_stocks = set(new_stocks) - holding_stocks
            if new_monitoring_stocks:
                logger.info(f"📈 신규 모니터링: {list(new_monitoring_stocks)}")

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
                # 포지션 보호 - 더 엄격한 손절/익절 적용
                if stock_code in self.positions:
                    current_price = data.get('current_price', {}).get('current_price', 0)
                    position = self.positions[stock_code]

                    if current_price > 0 and position['avg_price'] > 0:
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
