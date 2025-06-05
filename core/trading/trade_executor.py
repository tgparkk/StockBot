#!/usr/bin/env python3
"""
거래 실행자 - 통합 거래 로직 처리
"""
import time
import threading
from typing import Dict, List, Optional, Any
from utils.logger import setup_logger
from ..api.kis_market_api import get_disparity_rank
from dataclasses import dataclass, field
from datetime import datetime
from .async_data_logger import get_async_logger, log_signal_failed, log_buy_failed, log_buy_success
from .order_execution_manager import OrderExecutionManager
from ..trading.trading_manager import TradingManager

logger = setup_logger(__name__)


@dataclass
class TradeConfig:
    """💰 거래 설정 - 여기서 매수 금액을 조정하세요!"""

    # 🎯 기본 매수 금액 설정 (가장 중요!)
    base_position_ratio: float = 0.2          # 기본 20%: 계좌 잔고의 20%씩 매수
    max_position_ratio: float = 0.50          # 최대 50%: 계좌 잔고의 50%를 넘지 않음
    max_investment_amount: int = 2000000      # 최대 200만원: 절대 한도액 (수정 가능)
    min_investment_amount: int = 300000       # 최소 30만원: 최소 투자금액 (수정 가능)

    # 💡 예시: 계좌에 1000만원이 있다면
    # - 기본 매수: 60만원 (6%)
    # - 최대 매수: 100만원 (10%)
    # - 하지만 max_investment_amount=50만원이므로 실제로는 50만원까지만 매수

    # 전략별 포지션 배수
    strategy_multipliers: Optional[Dict[str, float]] = None

    # 가격 프리미엄/할인
    buy_premiums: Optional[Dict[str, float]] = None
    sell_discounts: Optional[Dict[str, float]] = None

    def __post_init__(self):
        if self.strategy_multipliers is None:
            # 🎯 전략별 매수 금액 배수 (기본값 곱하기)
            self.strategy_multipliers = {
                'gap_trading': 0.7,           # 갭 거래: 보수적 (기본의 70% = 4.2%)
                'volume_breakout': 0.9,       # 거래량: 적극적 (기본의 90% = 5.4%)
                'momentum': 1.2,              # 모멘텀: 공격적 (기본의 120% = 7.2%)
                'disparity_reversal': 0.8,    # 이격도 반등: 적극적 (기본의 80% = 4.8%)
                'existing_holding': 0.5,      # 기존 보유: 매우 보수적 (기본의 50% = 3%)
                'default': 1.0                # 기본: 6%
            }

        if self.buy_premiums is None:
            self.buy_premiums = {
                'gap_trading': 0.001,         # 갭 거래: 0.1% 위
                'volume_breakout': 0.001,     # 거래량 돌파: 0.1% 위
                'momentum': 0.001,            # 모멘텀: 0.1% 위
                'disparity_reversal': 0.001,  # 🆕 이격도 반등: 0.1% 위
                'existing_holding': 0.001,    # 기존 보유: 0.1% 위
                'default': 0.001              # 기본: 0.1% 위
            }

        if self.sell_discounts is None:
            self.sell_discounts = {
                'gap_trading': 0.005,         # 갭 거래: 0.5% 아래
                'volume_breakout': 0.006,     # 거래량 돌파: 0.6% 아래
                'momentum': 0.004,            # 모멘텀: 0.4% 아래
                'disparity_reversal': 0.004,  # 🆕 이격도 반등: 0.4% 아래
                'default': 0.005              # 기본: 0.5% 아래
            }


@dataclass
class TradeResult:
    """거래 실행 결과"""
    success: bool
    stock_code: str
    order_type: str  # 'BUY' or 'SELL'
    quantity: int
    price: int
    total_amount: int
    order_no: str = ""
    error_message: str = ""
    is_pending: bool = False  # 🆕 웹소켓 NOTICE 대기 중인지 여부

    @property
    def is_buy(self) -> bool:
        return self.order_type == 'BUY'

    @property
    def is_sell(self) -> bool:
        return self.order_type == 'SELL'


class TradeExecutor:
    """거래 실행 전담 클래스"""

    def __init__(self, trading_manager : TradingManager, data_manager, trade_db, config: Optional[TradeConfig] = None):
        """초기화 (PositionManager 제거됨)"""
        self.trading_manager = trading_manager
        self.data_manager = data_manager
        self.trade_db = trade_db
        self.config = config or TradeConfig()

        # 중복 주문 방지
        self.pending_orders = set()

        # 🆕 비동기 데이터 로거 초기화
        self.async_logger = get_async_logger()

        # 🆕 웹소켓 NOTICE 기반 주문 실행 관리자 (PositionManager 제거)
        self.execution_manager = OrderExecutionManager(
            trade_db=trade_db,
            async_logger=self.async_logger
        )

        # 웹소켓 NOTICE 콜백 등록
        self._register_websocket_callbacks()

        # 중복 신호 방지
        self.last_signals = {}
        self.signal_cooldown = 300  # 5분

        logger.info("✅ TradeExecutor 초기화 완료 (실전투자 체결통보 연동, PositionManager 제거됨)")

    def _register_websocket_callbacks(self):
        """🔔 WebSocketMessageHandler에 OrderExecutionManager 직접 설정"""
        try:
            # 데이터 매니저의 웹소켓 매니저 확인
            if (hasattr(self.data_manager, 'websocket_manager') and
                self.data_manager.websocket_manager):

                websocket_manager = self.data_manager.websocket_manager

                # 🎯 WebSocketMessageHandler에 OrderExecutionManager 직접 설정
                if hasattr(websocket_manager, 'message_handler'):
                    message_handler = websocket_manager.message_handler
                    if hasattr(message_handler, 'set_execution_manager'):
                        message_handler.set_execution_manager(self.execution_manager)
                        logger.info("✅ WebSocketMessageHandler에 OrderExecutionManager 설정 완료")
                        logger.info("📡 체결통보는 WebSocketMessageHandler에서 직접 처리됨")
                        return
                    else:
                        logger.warning("⚠️ WebSocketMessageHandler에 set_execution_manager 메서드 없음")
                else:
                    logger.warning("⚠️ WebSocketManager에 message_handler 속성 없음")

                # 🔄 기존 콜백 시스템 사용 (fallback)
                logger.info("💡 기존 콜백 시스템을 fallback으로 사용")

                # 🎯 체결통보 처리 함수
                async def handle_execution_notice(data_type: str, data: Dict):
                    """체결통보 데이터 처리 - 🆕 data_type 포함 글로벌 콜백 시그니처"""
                    try:
                        # 🔍 체결통보 데이터만 처리
                        if data_type != 'STOCK_EXECUTION':
                            logger.debug(f"🔄 체결통보가 아닌 데이터 무시: {data_type}")
                            return

                        logger.info(f"🔔 체결통보 수신 (fallback): data_type={data_type}")

                        # OrderExecutionManager로 전달
                        await self.execution_manager.handle_execution_notice(data)

                    except Exception as e:
                        logger.error(f"❌ 체결통보 처리 오류: {e}")

                # 기존 콜백 시스템에 등록
                if hasattr(websocket_manager, 'add_global_callback'):
                    websocket_manager.add_global_callback('STOCK_EXECUTION', handle_execution_notice)
                    logger.info("✅ 체결통보 콜백 등록 완료 (fallback)")
                else:
                    logger.warning("⚠️ 웹소켓 매니저에 add_global_callback 메서드 없음")

            else:
                logger.warning("⚠️ 웹소켓 매니저가 없어 체결통보 처리 설정 실패")
                logger.info("💡 힌트: DataManager에 KisWebSocketManager 인스턴스가 필요합니다")

        except Exception as e:
            logger.error(f"❌ 웹소켓 콜백 등록 오류: {e}")

    def execute_buy_signal(self, signal: Dict) -> TradeResult:
        """
        매수 신호 실행 - 캔들차트 전략 전용 간소화 버전

        Args:
            signal: 매수 신호 딕셔너리
                - stock_code: 종목코드 (필수)
                - price: 매수가격 (필수)
                - quantity: 매수수량 (선택, 없으면 자동계산)
                - total_amount: 매수금액 (선택)
                - strategy: 전략명 (기본: 'candle')
                - pre_validated: 사전 검증 완료 여부 (캔들 시스템에서는 True)
        """
        stock_code = signal.get('stock_code', '')
        strategy = signal.get('strategy', 'candle')
        is_pre_validated = signal.get('pre_validated', False)

        try:
            logger.info(f"📈 캔들 매수 주문 실행: {stock_code}")

            # 💰 매수 가격 검증 및 수량 계산
            target_price = signal.get('price', 0)
            if target_price <= 0:
                return TradeResult(
                    success=False, stock_code=stock_code, order_type='BUY',
                    quantity=0, price=0, total_amount=0,
                    error_message="신호에 유효한 가격 정보 없음"
                )

            # 매수가격 조정 (틱 단위 맞춤)
            buy_price = self._calculate_buy_price(target_price, strategy, stock_code)

            # 매수 수량 계산
            if 'quantity' in signal and signal['quantity'] > 0:
                # 신호에서 수량 지정된 경우
                buy_quantity = int(signal['quantity'])
            elif 'total_amount' in signal and signal['total_amount'] > 0:
                # 신호에서 총 금액 지정된 경우
                buy_quantity = signal['total_amount'] // buy_price
            else:
                # 자동 계산 (계좌 잔고 기반)
                available_cash = self._get_available_cash()
                buy_quantity = self._calculate_buy_quantity_simple(buy_price, available_cash)

            if buy_quantity <= 0:
                return TradeResult(
                    success=False, stock_code=stock_code, order_type='BUY',
                    quantity=0, price=buy_price, total_amount=0,
                    error_message="매수 수량 부족"
                )

            total_amount = buy_quantity * buy_price

            # 🚀 실제 매수 주문 실행
            logger.info(f"💰 매수 주문: {stock_code} {buy_quantity:,}주 @ {buy_price:,}원 (총 {total_amount:,}원)")

            order_result = self.trading_manager.execute_order(
                stock_code=stock_code,
                order_type="BUY",
                quantity=buy_quantity,
                price=buy_price
            )

            if order_result:
                order_id = order_result  # execute_order는 order_no 문자열을 직접 반환

                # 📝 거래 기록 저장
                self._record_buy_trade(stock_code, buy_quantity, buy_price, strategy, signal, {'order_no': order_id})

                # 🎯 웹소켓 NOTICE 대기를 위해 OrderExecutionManager에 등록
                if order_id:
                    self.execution_manager.add_pending_order(
                        order_id=order_id, stock_code=stock_code, order_type='BUY',
                        quantity=buy_quantity, price=buy_price, strategy_type=strategy
                    )

                logger.info(f"✅ 매수 주문 성공: {stock_code} (주문번호: {order_id})")
                return TradeResult(
                    success=True, stock_code=stock_code, order_type='BUY',
                    quantity=buy_quantity, price=buy_price, total_amount=total_amount,
                    order_no=order_id, is_pending=True
                )
            else:
                logger.error(f"❌ 매수 주문 실패: {stock_code} - 주문 실행 실패")
                return TradeResult(
                    success=False, stock_code=stock_code, order_type='BUY',
                    quantity=buy_quantity, price=buy_price, total_amount=total_amount,
                    error_message="주문 실행 실패"
                )

        except Exception as e:
            logger.error(f"❌ 매수 주문 실행 중 오류: {stock_code} - {str(e)}")
            return TradeResult(
                success=False, stock_code=stock_code, order_type='BUY',
                quantity=0, price=signal.get('price', 0), total_amount=0,
                error_message=f"매수 실행 오류: {str(e)}"
            )

    def execute_sell_signal(self, signal: Dict) -> TradeResult:
        """
        매도 신호 실행 - 캔들차트 전략 전용 간소화 버전

        Args:
            signal: 매도 신호 딕셔너리
                - stock_code: 종목코드 (필수)
                - price: 매도가격 (필수)
                - quantity: 매도수량 (필수)
                - reason: 매도 이유 (선택)
                - strategy: 전략명 (기본: 'candle')
        """
        stock_code = signal.get('stock_code', '')
        strategy = signal.get('strategy', 'candle')
        reason = signal.get('reason', '매도신호')

        try:
            logger.info(f"📉 캔들 매도 주문 실행: {stock_code} ({reason})")

            # 필수 필드 검증
            required_fields = ['stock_code', 'price', 'quantity']
            for field in required_fields:
                if field not in signal or not signal[field]:
                    return TradeResult(
                        success=False, stock_code=stock_code, order_type='SELL',
                        quantity=0, price=signal.get('price', 0), total_amount=0,
                        error_message=f"필수 필드 누락: {field}"
                    )

            # 매도 정보 추출
            sell_price = int(signal.get('price', 0))
            sell_quantity = int(signal.get('quantity', 0))

            if sell_price <= 0 or sell_quantity <= 0:
                return TradeResult(
                    success=False, stock_code=stock_code, order_type='SELL',
                    quantity=sell_quantity, price=sell_price, total_amount=0,
                    error_message="유효하지 않은 가격 또는 수량"
                )

            total_amount = sell_quantity * sell_price

            # 🚀 실제 매도 주문 실행
            logger.info(f"💰 매도 주문: {stock_code} {sell_quantity:,}주 @ {sell_price:,}원 (총 {total_amount:,}원)")

            sell_result = self.trading_manager.execute_order(
                stock_code=stock_code,
                order_type="SELL",
                quantity=sell_quantity,
                price=sell_price,
                strategy_type=strategy
            )

            if sell_result:  # 주문번호가 반환되면 성공
                order_id = sell_result if isinstance(sell_result, str) else str(sell_result)

                # 📝 거래 기록 저장 (임시 포지션 정보)
                position = {
                    'strategy_type': strategy,
                    'stock_name': stock_code,
                    'buy_price': sell_price
                }
                sell_result_dict = {'order_no': order_id, 'status': 'pending'}
                self._record_sell_trade(stock_code, sell_quantity, sell_price, position, signal, sell_result_dict)

                # 🎯 웹소켓 NOTICE 대기를 위해 OrderExecutionManager에 등록
                if order_id:
                    self.execution_manager.add_pending_order(
                        order_id=order_id, stock_code=stock_code, order_type='SELL',
                        quantity=sell_quantity, price=sell_price, strategy_type=strategy
                    )

                logger.info(f"✅ 매도 주문 성공: {stock_code} (주문번호: {order_id})")
                return TradeResult(
                    success=True, stock_code=stock_code, order_type='SELL',
                    quantity=sell_quantity, price=sell_price, total_amount=total_amount,
                    order_no=order_id, is_pending=True
                )
            else:
                logger.error(f"❌ 매도 주문 실패: {stock_code}")
                return TradeResult(
                    success=False, stock_code=stock_code, order_type='SELL',
                    quantity=sell_quantity, price=sell_price, total_amount=total_amount,
                    error_message="매도 주문 API 실패"
                )

        except Exception as e:
            logger.error(f"❌ 매도 주문 실행 중 오류: {stock_code} - {str(e)}")
            return TradeResult(
                success=False, stock_code=stock_code, order_type='SELL',
                quantity=signal.get('quantity', 0), price=signal.get('price', 0), total_amount=0,
                error_message=f"매도 실행 오류: {str(e)}"
            )

    def execute_auto_sell(self, sell_signal: Dict) -> TradeResult:
        """자동 매도 실행 (손절/익절)"""
        try:
            stock_code = sell_signal['stock_code']
            reason = sell_signal['reason']
            current_price = sell_signal['current_price']
            quantity = sell_signal['quantity']
            strategy_type = sell_signal['strategy_type']

            logger.info(f"🤖 자동 매도 실행: {stock_code} - {reason}")

            # 실제 보유 수량 확인
            actual_quantity = self._get_actual_holding_quantity(stock_code)
            if actual_quantity <= 0:
                return TradeResult(
                    success=False,
                    stock_code=stock_code,
                    order_type='SELL',
                    quantity=0,
                    price=0,
                    total_amount=0,
                    error_message=f"실제 보유 수량 없음: {stock_code}"
                )

            # 매도 수량 결정
            sell_quantity = min(quantity, actual_quantity)

            # 자동 매도용 지정가 계산 (빠른 체결 우선)
            auto_sell_price = self._calculate_sell_price(current_price, strategy_type, is_auto_sell=True)

            # 자동 매도 실행
            order_result = self.trading_manager.execute_order(
                stock_code=stock_code,
                order_type="SELL",
                quantity=sell_quantity,
                price=auto_sell_price,
                strategy_type=f"auto_sell_{reason}"
            )

            if order_result:
                # 거래 기록 저장 (자동 매도)
                self._record_auto_sell_trade(stock_code, sell_quantity, auto_sell_price, reason, sell_signal, order_result)

                logger.info(f"✅ 자동 매도 완료: {stock_code} {sell_quantity:,}주 @ {auto_sell_price:,}원 - {reason}")

                return TradeResult(
                    success=True,
                    stock_code=stock_code,
                    order_type='SELL',
                    quantity=sell_quantity,
                    price=auto_sell_price,
                    total_amount=sell_quantity * auto_sell_price,
                    order_no=order_result
                )
            else:
                error_msg = f"자동 매도 주문 실패: {stock_code}"
                logger.error(f"❌ {error_msg}")

                return TradeResult(
                    success=False,
                    stock_code=stock_code,
                    order_type='SELL',
                    quantity=sell_quantity,
                    price=auto_sell_price,
                    total_amount=sell_quantity * auto_sell_price,
                    error_message=error_msg
                )

        except Exception as e:
            error_msg = f"자동 매도 실행 오류: {e}"
            logger.error(error_msg)

            return TradeResult(
                success=False,
                stock_code=sell_signal.get('stock_code', 'UNKNOWN'),
                order_type='SELL',
                quantity=0,
                price=0,
                total_amount=0,
                error_message=error_msg
            )

    # === 내부 헬퍼 메서드들 ===

    def _validate_buy_signal(self, signal: Dict, stock_code: str) -> bool:
        """🔍 매수 신호 검증 - 캔들 전용 (기본 검증만)"""
        return self._validate_buy_signal_basic(signal, stock_code)

    def _validate_buy_signal_basic(self, signal: Dict, stock_code: str) -> bool:
        """기본적인 매수 신호 검증 (캔들 전용 간소화 버전)"""
        try:
            # 필수 필드 확인
            required_fields = ['stock_code', 'price']
            for field in required_fields:
                if field not in signal or not signal[field]:
                    logger.warning(f"❌ 필수 필드 누락: {field} ({stock_code})")
                    return False

            # 가격 검증
            price = signal.get('price', 0)
            if price <= 0:
                logger.warning(f"❌ 유효하지 않은 가격: {price} ({stock_code})")
                return False

            # 쿨다운 확인 (30초)
            current_time = time.time()
            if stock_code in self.last_signals:
                if current_time - self.last_signals[stock_code] < 30:
                    logger.warning(f"❌ 쿨다운 중: {stock_code}")
                    return False

            # 마지막 신호 시간 업데이트
            self.last_signals[stock_code] = current_time
            return True

        except Exception as e:
            logger.error(f"기본 검증 오류 ({stock_code}): {e}")
            return False

    def _get_current_price(self, stock_code: str) -> int:
        """현재가 조회"""
        try:
            logger.debug(f"💰 현재가 조회 시작: {stock_code}")
            current_data = self.data_manager.get_latest_data(stock_code)
            if not current_data or current_data.get('status') != 'success':
                logger.error(f"❌ 현재가 조회 실패: {stock_code} - 데이터 매니저 응답: {current_data}")
                return 0

            current_price = current_data.get('current_price', 0)
            if current_price <= 0:
                logger.error(f"❌ 유효하지 않은 현재가: {stock_code} = {current_price}")
                return 0

            logger.debug(f"✅ 현재가 조회 성공: {stock_code} = {current_price:,}원")
            return current_price
        except Exception as e:
            logger.error(f"❌ 현재가 조회 오류 ({stock_code}): {e}")
            return 0

    def _get_available_cash(self) -> int:
        """사용 가능한 현금 조회"""
        try:
            logger.debug(f"💰 잔고 조회 시작")
            balance = self.trading_manager.get_balance()
            available_cash = balance.get('available_cash', 0)
            logger.debug(f"✅ 사용 가능한 현금: {available_cash:,}원")
            return available_cash
        except Exception as e:
            logger.error(f"❌ 잔고 조회 오류: {e}")
            return 0

    def _calculate_buy_price(self, current_price: int, strategy: str = 'default', stock_code: str = '') -> int:
        """매수 지정가 계산 - 🚨 실제 상하한가 조회 적용"""
        try:
            # 🆕 안전한 프리미엄 설정 (config가 None인 경우 대비)
            if hasattr(self.config, 'buy_premiums') and self.config.buy_premiums:
                base_premium = self.config.buy_premiums.get(strategy, 0.001)  # 기본 0.1%
            else:
                # config가 없거나 buy_premiums가 None인 경우 기본값 사용
                base_premium = 0.001  # 기본 0.1% 프리미엄

            # 시장 상황별 동적 조정
            volatility_adjustment = 0

            # 캔들 전략의 경우 더 적극적인 가격
            if strategy == 'candle':
                base_premium = 0.002  # 0.2% 프리미엄 (빠른 체결)

            # 최종 프리미엄 계산
            final_premium = base_premium + volatility_adjustment

            # 프리미엄 적용된 목표가 계산
            target_price = int(current_price * (1 + final_premium))

            # 🚨 실제 상하한가 조회
            price_limits = self._get_price_limits(stock_code, current_price)
            upper_limit = price_limits.get('upper_limit', current_price * 1.3)
            lower_limit = price_limits.get('lower_limit', current_price * 0.7)

            # 상한가 체크
            if target_price >= upper_limit:
                # 상한가 근처라면 상한가 직전 가격으로 조정
                target_price = int(upper_limit * 0.998)  # 상한가의 99.8%
                logger.warning(f"⚠️ {stock_code} 상한가 근처 - 매수가 조정: {target_price:,}원")

            # 틱 단위 조정
            final_price = self._adjust_to_tick_size(target_price)

            # 최종 검증 (현재가보다 너무 높으면 제한)
            max_buy_price = int(current_price * 1.05)  # 현재가의 105% 이하
            final_price = min(final_price, max_buy_price)

            logger.debug(f"💰 매수가 계산: {stock_code} 현재가{current_price:,}원 → 주문가{final_price:,}원 "
                        f"(프리미엄{final_premium*100:.2f}%)")

            return final_price

        except Exception as e:
            logger.error(f"매수가 계산 오류 ({stock_code}): {e}")
            # 오류시 현재가의 100.1% 반환 (안전장치)
            return int(current_price * 1.001)

    def _calculate_sell_price(self, current_price: int, strategy: str = 'default', is_auto_sell: bool = False, stock_code: str = '') -> int:
        """매도 지정가 계산 - 🚨 실제 상하한가 조회 적용"""
        try:
            if is_auto_sell:
                # 자동매도시 빠른 체결을 위해 더 낮은 가격
                discount = 0.008  # 0.8% 할인
            else:
                # 전략별 매도 할인 설정
                discount = self.config.sell_discounts.get(strategy, self.config.sell_discounts['default'])

            # 🚨 실제 상하한가 조회
            price_limits = self._get_price_limits(stock_code, current_price)
            upper_limit = price_limits['upper_limit']
            lower_limit = price_limits['lower_limit']
            base_price = price_limits['base_price']
            source = price_limits['source']

            # 상한가/하한가 여부 확인
            tick_size = self._get_tick_size(current_price)
            is_upper_limit = abs(current_price - upper_limit) <= tick_size
            is_lower_limit = abs(current_price - lower_limit) <= tick_size

            if is_upper_limit:
                logger.warning(f"🚨 상한가 종목 감지: {stock_code} {current_price:,}원 (상한가: {upper_limit:,}원, 기준가: {base_price:,}원)")
                # 상한가 종목은 현재가 이하에서만 매도 가능
                return current_price

            elif is_lower_limit:
                logger.warning(f"🚨 하한가 종목 감지: {stock_code} {current_price:,}원 (하한가: {lower_limit:,}원)")
                # 하한가 종목은 추가 하락 제한적이므로 할인 완화
                reduced_discount = discount * 0.3  # 할인율 70% 감소
                calculated_sell_price = int(current_price * (1 - reduced_discount))
                logger.info(f"💡 하한가 종목 할인 완화: {stock_code} {discount:.1%} → {reduced_discount:.1%}")
            else:
                # 일반 종목: 기본 계산
                calculated_sell_price = int(current_price * (1 - discount))

            # 상하한가 제한 적용
            if calculated_sell_price > upper_limit:
                logger.warning(f"🚨 매도가 상한가 초과 조정: {stock_code} {calculated_sell_price:,}원 → {upper_limit:,}원")
                calculated_sell_price = upper_limit
            elif calculated_sell_price < lower_limit:
                logger.warning(f"🚨 매도가 하한가 미만 조정: {stock_code} {calculated_sell_price:,}원 → {lower_limit:,}원")
                calculated_sell_price = lower_limit

            # 호가 단위로 최종 조정
            final_sell_price = self._adjust_to_tick_size(calculated_sell_price)

            # 최종 안전성 검증
            if final_sell_price > upper_limit:
                final_sell_price = upper_limit
                logger.warning(f"🚨 최종 상한가 제한 적용: {stock_code} {final_sell_price:,}원")
            elif final_sell_price < lower_limit:
                final_sell_price = lower_limit
                logger.warning(f"🚨 최종 하한가 제한 적용: {stock_code} {final_sell_price:,}원")

            logger.debug(f"💰 매도가 계산 (실제 상하한가): {stock_code} 현재{current_price:,}원 → 최종{final_sell_price:,}원 "
                        f"(할인: {discount:.1%}, 기준가: {base_price:,}원, 상한가: {upper_limit:,}원, 소스: {source})")

            return final_sell_price

        except Exception as e:
            logger.error(f"매도가 계산 오류: {stock_code} - {e}")
            # 오류시 기본 계산 (5% 할인 제한)
            backup_price = int(current_price * 0.995)
            emergency_lower = int(current_price * 0.90)
            return max(backup_price, emergency_lower)

    def _calculate_buy_quantity(self, current_price: int, buy_price: int, available_cash: int,
                               strategy: str, strength: float) -> int:
        """매수 수량 계산"""
        try:
            logger.debug(f"💰 매수 수량 계산 시작: 현재가={current_price:,}원, 매수가={buy_price:,}원, 잔고={available_cash:,}원")

            # 🔧 안전 여유분 적용 (주문 가능 금액 초과 방지)
            safe_available_cash = int(available_cash * 0.9)  # 90%만 사용 (10% 여유분)
            logger.debug(f"💰 안전 여유분 적용: {available_cash:,}원 → {safe_available_cash:,}원")

            if safe_available_cash < self.config.min_investment_amount:
                logger.warning(f"❌ 안전 잔고 부족: {safe_available_cash:,}원 < {self.config.min_investment_amount:,}원")
                return 0

            # 전략별 포지션 사이즈 조정
            strategy_multiplier = self.config.strategy_multipliers.get(strategy, 1.0)

            # 신호 강도 고려 (0.3 ~ 1.2 범위)
            strength_adjusted = max(0.3, min(strength, 1.2))

            # 최종 포지션 비율 계산
            final_position_ratio = self.config.base_position_ratio * strategy_multiplier * strength_adjusted

            logger.debug(f"💰 포지션 계산: 전략승수={strategy_multiplier:.2f}, 신호강도={strength_adjusted:.2f}, 최종비율={final_position_ratio:.2%}")

            # 최대 투자 금액 계산
            max_investment = min(
                safe_available_cash * final_position_ratio,  # 잔고 비율 기준
                safe_available_cash * self.config.max_position_ratio,  # 최대 비율 제한
                self.config.max_investment_amount       # 최대 금액 제한
            )

            logger.debug(f"💰 최대 투자 금액: {max_investment:,}원")

            # 수량 계산
            quantity = int(max_investment // buy_price) if buy_price > 0 else 0

            logger.debug(f"💰 계산된 수량: {quantity:,}주")

            # 최소 수량 체크
            if quantity * buy_price < self.config.min_investment_amount:
                old_quantity = quantity
                min_required_quantity = max(1, int(self.config.min_investment_amount // buy_price))
                # 안전 잔고를 초과하지 않는 범위에서 최소 수량 적용
                quantity = min(min_required_quantity, int(safe_available_cash // buy_price))
                logger.debug(f"💰 최소 투자금액 조정: {old_quantity:,}주 → {quantity:,}주 (안전잔고 고려)")

            # 최종 매수 금액 확인 및 재조정
            total_buy_amount = quantity * buy_price
            if total_buy_amount > safe_available_cash:
                quantity = int(safe_available_cash // buy_price)
                total_buy_amount = quantity * buy_price
                logger.warning(f"💰 매수 수량 재조정 (잔고 초과): {quantity:,}주, 총액={total_buy_amount:,}원")

            if quantity <= 0:
                logger.warning(f"❌ 매수 수량 부족: 계산 결과 {quantity}주")
                return 0

            logger.info(f"✅ 매수 수량 계산 완료: 전략={strategy}, 강도={strength:.2f}, 수량={quantity:,}주, 총액={total_buy_amount:,}원")
            return quantity

        except Exception as e:
            logger.error(f"매수 수량 계산 오류: {e}")
            return 0

    def _adjust_to_tick_size(self, price: int) -> int:
        """호가 단위로 가격 조정"""
        try:
            # 한국 주식 호가 단위
            if price < 1000:
                return price  # 1원 단위
            elif price < 5000:
                return (price // 5) * 5  # 5원 단위
            elif price < 10000:
                return (price // 10) * 10  # 10원 단위
            elif price < 50000:
                return (price // 50) * 50  # 50원 단위
            elif price < 100000:
                return (price // 100) * 100  # 100원 단위
            elif price < 500000:
                return (price // 500) * 500  # 500원 단위
            else:
                return (price // 1000) * 1000  # 1000원 단위
        except Exception as e:
            logger.error(f"호가 단위 조정 오류: {e}")
            return price

    def _get_actual_holding_quantity(self, stock_code: str) -> int:
        """실제 보유 수량 확인 - 🔧 KIS API 응답 필드 다중 확인"""
        try:
            logger.debug(f"📊 실제 보유 수량 조회 시작: {stock_code}")

            balance = self.trading_manager.get_balance()
            if not balance or not balance.get('success'):
                logger.error(f"❌ 잔고 조회 실패: {stock_code} - {balance}")
                return 0

            holdings = balance.get('holdings', [])
            logger.debug(f"📊 총 보유 종목 수: {len(holdings)}개")

            if not holdings:
                logger.warning(f"⚠️ 보유 종목 목록이 비어있음: {stock_code}")
                return 0

            # 🔧 KIS API 응답 필드명 다중 확인
            possible_code_fields = ['pdno', 'stock_code', 'stck_shrn_iscd', 'mksc_shrn_iscd']
            possible_quantity_fields = ['hldg_qty', 'quantity', 'ord_psbl_qty', 'available_quantity']

            for holding in holdings:
                logger.debug(f"📋 보유종목 확인: {holding}")

                # 종목코드 매칭 (다양한 필드명 확인)
                found_stock_code = None
                for field in possible_code_fields:
                    if field in holding:
                        code_value = str(holding.get(field, '')).strip()
                        if code_value == stock_code:
                            found_stock_code = code_value
                            logger.debug(f"✅ 종목코드 매칭: {field}={code_value}")
                            break

                if found_stock_code:
                    # 수량 확인 (다양한 필드명 확인)
                    for qty_field in possible_quantity_fields:
                        if qty_field in holding:
                            try:
                                quantity = int(holding.get(qty_field, 0))
                                logger.info(f"📊 실제 보유 수량 확인: {stock_code} = {quantity:,}주 (필드: {qty_field})")
                                return quantity
                            except (ValueError, TypeError):
                                logger.warning(f"⚠️ 수량 변환 실패: {qty_field}={holding.get(qty_field)}")
                                continue

                    # 수량 필드를 찾지 못한 경우
                    logger.error(f"❌ 수량 필드를 찾을 수 없음: {stock_code}")
                    logger.debug(f"   보유종목 데이터: {holding}")
                    return 0

            logger.warning(f"⚠️ 종목을 찾을 수 없음: {stock_code}")
            logger.debug(f"   확인한 종목코드들: {[h.get('pdno', h.get('stock_code', 'Unknown')) for h in holdings[:5]]}")
            return 0

        except Exception as e:
            logger.error(f"❌ 실제 보유 수량 확인 오류 ({stock_code}): {e}")
            return 0

    def _record_buy_trade(self, stock_code: str, quantity: int, buy_price: int,
                         strategy: str, signal: Dict, order_result: Dict):
        """매수 거래 기록 저장"""
        try:
            stock_name = stock_code  # 실제로는 종목명 조회 가능
            total_amount = quantity * buy_price
            reason = signal.get('reason', f'{strategy} 신호')
            strength = signal.get('strength', 0.5)

            trade_id = self.trade_db.record_buy_trade(
                stock_code=stock_code,
                stock_name=stock_name,
                quantity=quantity,
                price=buy_price,
                total_amount=total_amount,
                strategy_type=strategy,
                order_id=order_result.get('order_no', ''),
                status='SUCCESS',
                market_conditions={
                    'current_price': signal.get('price', buy_price),
                    'signal_strength': strength,
                    'reason': reason
                },
                notes=f"신호강도: {strength:.2f}, 사유: {reason}"
            )

            logger.info(f"💾 매수 기록 저장 완료 (ID: {trade_id})")

            # 선정된 종목과 거래 연결
            if trade_id > 0:
                try:
                    self.trade_db.link_trade_to_selected_stock(stock_code, trade_id)
                except Exception as e:
                    logger.error(f"선정 종목-거래 연결 오류: {e}")

        except Exception as e:
            logger.error(f"💾 매수 기록 저장 실패: {e}")

    def _record_sell_trade(self, stock_code: str, quantity: int, sell_price: int,
                          position: Dict, signal: Dict, sell_result: Dict):
        """매도 거래 기록 저장"""
        try:
            # 매수 거래 ID 찾기
            buy_trade_id = self.trade_db.find_buy_trade_for_sell(stock_code, quantity)

            # 🆕 포지션에서 전략 타입 직접 사용 (더 이상 복원 로직 불필요)
            strategy_type = position.get('strategy_type', 'unknown')

            # 수익률 계산
            buy_price = position.get('buy_price', sell_price)
            profit_rate = ((sell_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
            sell_type = "수동매도"
            condition_reason = signal.get('reason', '매도 신호')

            trade_id = self.trade_db.record_sell_trade(
                stock_code=stock_code,
                stock_name=position.get('stock_name', stock_code),
                quantity=quantity,
                price=sell_price,
                total_amount=quantity * sell_price,
                strategy_type=strategy_type,  # 🆕 포지션의 전략 타입 직접 사용
                buy_trade_id=buy_trade_id,
                order_id=sell_result.get('order_no', ''),
                status='SUCCESS',
                market_conditions={
                    'current_price': signal.get('price', sell_price),
                    'profit_rate': profit_rate,
                    'sell_reason': f"{sell_type}: {condition_reason}"
                },
                notes=f"매도사유: {sell_type}, 조건: {condition_reason}, 전략: {strategy_type}"
            )
            logger.info(f"💾 매도 기록 저장 완료 - 전략: {strategy_type} (ID: {trade_id})")

        except Exception as e:
            logger.error(f"💾 매도 기록 저장 실패: {e}")

    def _record_auto_sell_trade(self, stock_code: str, quantity: int, sell_price: int,
                               reason: str, sell_signal: Dict, order_result: str):
        """자동 매도 거래 기록 저장"""
        try:
            # 매수 거래 ID 찾기
            buy_trade_id = self.trade_db.find_buy_trade_for_sell(stock_code, quantity)

            # 🆕 매도 신호에서 전략 타입 직접 사용 (포지션 매니저에서 전달됨)
            strategy_type = sell_signal.get('strategy_type', 'unknown')

            # 수익률 계산
            current_price = sell_signal.get('current_price', sell_price)
            buy_price = sell_signal.get('buy_price', sell_price)
            profit_rate = ((sell_price - buy_price) / buy_price * 100) if buy_price > 0 else 0

            trade_id = self.trade_db.record_sell_trade(
                stock_code=stock_code,
                stock_name=sell_signal.get('stock_name', stock_code),
                quantity=quantity,
                price=sell_price,
                total_amount=quantity * sell_price,
                strategy_type=strategy_type,  # 🆕 신호의 전략 타입 직접 사용
                buy_trade_id=buy_trade_id,
                order_id=order_result,
                status='SUCCESS',
                market_conditions={
                    'current_price': current_price,
                    'profit_rate': profit_rate,
                    'sell_reason': f"자동매도: {reason}"
                },
                notes=f"자동매도 - {reason}, 현재가: {current_price:,}원, 전략: {strategy_type}"
            )
            logger.info(f"💾 자동매도 기록 저장 완료 - 전략: {strategy_type} (ID: {trade_id})")

        except Exception as e:
            logger.error(f"💾 자동매도 기록 저장 실패: {e}")

    def handle_signal(self, signal: Dict) -> Dict:
        """
        통합 신호 처리 메서드 - 매수/매도 신호를 통합 처리

        Args:
            signal: 거래 신호 딕셔너리
                - signal_type: 'BUY' 또는 'SELL'
                - stock_code: 종목코드
                - strategy: 전략명
                - price: 기준가격
                - strength: 신호 강도 (0.0~1.0)
                - reason: 신호 발생 사유

        Returns:
            Dict: 처리 결과
                - success: bool
                - message: str
                - order_executed: bool
                - trade_result: TradeResult (optional)
        """
        try:
            signal_type = signal.get('signal_type', '').upper()
            stock_code = signal.get('stock_code', '')
            strategy = signal.get('strategy', 'default')

            logger.info(f"🎯 거래 신호 처리: {signal_type} {stock_code} ({strategy})")

            if not signal_type or not stock_code:
                return {
                    'success': False,
                    'message': '필수 신호 정보 누락 (signal_type 또는 stock_code)',
                    'order_executed': False
                }

            # 중복 주문 방지
            if stock_code in self.pending_orders:
                return {
                    'success': False,
                    'message': f'이미 처리 중인 주문: {stock_code}',
                    'order_executed': False
                }

            self.pending_orders.add(stock_code)

            try:
                if signal_type == 'BUY':
                    # 매수 신호 처리
                    result = self.execute_buy_signal(signal)

                    if result.success:
                        return {
                            'success': True,
                            'message': f'매수 주문 완료: {stock_code} {result.quantity:,}주 @ {result.price:,}원',
                            'order_executed': True,
                            'trade_result': result
                        }
                    else:
                        return {
                            'success': False,
                            'message': f'매수 주문 실패: {result.error_message}',
                            'order_executed': False,
                            'trade_result': result
                        }

                elif signal_type == 'SELL':
                    # 매도 신호 처리 - 기존 포지션 확인
                    positions = self.position_manager.get_positions('active')

                    if stock_code not in positions:
                        return {
                            'success': False,
                            'message': f'매도할 포지션 없음: {stock_code}',
                            'order_executed': False
                        }

                    result = self.execute_sell_signal(signal)

                    if result.success:
                        return {
                            'success': True,
                            'message': f'매도 주문 완료: {stock_code} {result.quantity:,}주 @ {result.price:,}원',
                            'order_executed': True,
                            'trade_result': result
                        }
                    else:
                        return {
                            'success': False,
                            'message': f'매도 주문 실패: {result.error_message}',
                            'order_executed': False,
                            'trade_result': result
                        }

                else:
                    return {
                        'success': False,
                        'message': f'알 수 없는 신호 타입: {signal_type}',
                        'order_executed': False
                    }

            finally:
                # 처리 완료 후 pending에서 제거
                self.pending_orders.discard(stock_code)

        except Exception as e:
            # 예외 발생시에도 pending에서 제거
            if 'stock_code' in locals():
                self.pending_orders.discard(stock_code)

            error_msg = f"신호 처리 예외: {e}"
            logger.error(error_msg)

            return {
                'success': False,
                'message': error_msg,
                'order_executed': False
            }

    def get_execution_stats(self) -> Dict:
        """🆕 주문 실행 통계"""
        try:
            execution_stats = self.execution_manager.get_stats()
            pending_count = self.execution_manager.get_pending_orders_count()

            return {
                'traditional_orders': self.stats if hasattr(self, 'stats') else {},
                'websocket_executions': execution_stats,
                'pending_orders_count': pending_count,
                'total_success_rate': (
                    execution_stats['orders_filled'] /
                    max(execution_stats['orders_sent'], 1)
                ) if execution_stats['orders_sent'] > 0 else 0
            }
        except Exception as e:
            logger.error(f"❌ 실행 통계 조회 오류: {e}")
            return {}

    def cleanup_expired_orders(self):
        """🆕 만료된 주문 정리"""
        try:
            self.execution_manager.cleanup_expired_orders()
        except Exception as e:
            logger.error(f"❌ 만료 주문 정리 오류: {e}")

    def _get_price_limits(self, stock_code: str, current_price: int) -> Dict:
        """🚨 KIS API에서 실제 상하한가 정보 조회"""
        try:
            logger.debug(f"📊 상하한가 정보 조회 시작: {stock_code}")

            # KIS API에서 현재가 정보 조회 (상하한가 포함)
            current_data = self.data_manager.get_latest_data(stock_code)

            if not current_data or current_data.get('status') != 'success':
                logger.warning(f"⚠️ 가격 정보 조회 실패, 기본값 사용: {stock_code}")
                return self._get_fallback_price_limits(current_price)

            # KIS API 응답에서 상하한가 정보 추출
            data = current_data.get('data', {})

            # 다양한 필드명 확인
            base_price = self._extract_field(data, ['prdy_clpr', 'base_price', 'previous_close'])
            upper_limit = self._extract_field(data, ['stck_hgpr', 'upper_limit', 'high_limit'])
            lower_limit = self._extract_field(data, ['stck_lwpr', 'lower_limit', 'low_limit'])

            # 값 검증 및 변환
            try:
                if base_price:
                    base_price = int(base_price)
                if upper_limit:
                    upper_limit = int(upper_limit)
                if lower_limit:
                    lower_limit = int(lower_limit)
            except (ValueError, TypeError):
                logger.warning(f"⚠️ 상하한가 값 변환 실패: {stock_code}")
                return self._get_fallback_price_limits(current_price)

            # 상하한가가 직접 제공되는 경우
            if upper_limit and lower_limit and upper_limit > 0 and lower_limit > 0:
                logger.debug(f"✅ API에서 상하한가 직접 조회: {stock_code} - 상한가:{upper_limit:,}원, 하한가:{lower_limit:,}원")
                return {
                    'base_price': base_price or current_price,
                    'upper_limit': upper_limit,
                    'lower_limit': lower_limit,
                    'source': 'api_direct'
                }

            # 전일 종가만 제공되는 경우 계산
            elif base_price and base_price > 0:
                calculated_upper = int(base_price * 1.30)
                calculated_lower = int(base_price * 0.70)

                # 호가 단위로 조정
                calculated_upper = self._adjust_to_tick_size(calculated_upper)
                calculated_lower = self._adjust_to_tick_size(calculated_lower)

                logger.debug(f"✅ 전일종가 기준 계산: {stock_code} - 기준가:{base_price:,}원, 상한가:{calculated_upper:,}원")
                return {
                    'base_price': base_price,
                    'upper_limit': calculated_upper,
                    'lower_limit': calculated_lower,
                    'source': 'calculated'
                }

            else:
                logger.warning(f"⚠️ 상하한가 정보 부족, 기본값 사용: {stock_code}")
                return self._get_fallback_price_limits(current_price)

        except Exception as e:
            logger.error(f"❌ 상하한가 조회 오류: {stock_code} - {e}")
            return self._get_fallback_price_limits(current_price)

    def _extract_field(self, data: Dict, field_names: List[str]) -> Any:
        """딕셔너리에서 여러 필드명 중 하나를 찾아 반환"""
        for field in field_names:
            if field in data and data[field] is not None:
                return data[field]
        return None

    def _get_fallback_price_limits(self, current_price: int) -> Dict:
        """🔧 상하한가 조회 실패시 백업 계산"""
        try:
            # 현재가 기준으로 보수적 추정 (기존 로직)
            max_possible_base = int(current_price / 0.70)
            min_possible_base = int(current_price / 1.30)
            estimated_base = int((min_possible_base + max_possible_base) / 2)

            fallback_upper = int(estimated_base * 1.30)
            fallback_lower = int(estimated_base * 0.70)

            # 호가 단위로 조정
            fallback_upper = self._adjust_to_tick_size(fallback_upper)
            fallback_lower = self._adjust_to_tick_size(fallback_lower)

            logger.warning(f"🔧 백업 상하한가 계산: 추정기준가={estimated_base:,}원, 상한가={fallback_upper:,}원")

            return {
                'base_price': estimated_base,
                'upper_limit': fallback_upper,
                'lower_limit': fallback_lower,
                'source': 'fallback'
            }

        except Exception as e:
            logger.error(f"❌ 백업 계산 오류: {e}")
            return {
                'base_price': current_price,
                'upper_limit': int(current_price * 1.10),  # 보수적 10% 제한
                'lower_limit': int(current_price * 0.90),
                'source': 'emergency'
            }

    def _calculate_buy_quantity_simple(self, buy_price: float, available_cash: int) -> int:
        """간단한 매수 수량 계산 (캔들 전용)"""
        try:
            if available_cash < self.config.min_investment_amount:
                return 0

            # 기본 투자 금액 (계좌의 20%)
            target_amount = min(
                available_cash * self.config.base_position_ratio,
                self.config.max_investment_amount
            )

            quantity = int(target_amount // buy_price)
            return max(0, quantity)

        except Exception as e:
            logger.error(f"수량 계산 오류: {e}")
            return 0
