#!/usr/bin/env python3
"""
거래 실행자 - 간소화된 캔들 전략 전용
"""
import time
from typing import Dict, Optional
from utils.logger import setup_logger
from dataclasses import dataclass
from datetime import datetime
from .async_data_logger import get_async_logger
from .order_execution_manager import OrderExecutionManager
from ..trading.trading_manager import TradingManager
from collections import defaultdict

logger = setup_logger(__name__)


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
    """간소화된 거래 실행 전담 클래스 - 캔들 전략 전용"""

    def __init__(self, trading_manager: TradingManager, data_manager, trade_db):
        """초기화"""
        self.trading_manager = trading_manager
        self.data_manager = data_manager
        self.trade_db = trade_db

        # 🆕 기본 설정값들 (하드코딩)
        self.base_investment_amount = 500_000  # 기본 50만원
        self.max_investment_amount = 1_000_000  # 최대 100만원
        self.min_investment_amount = 100_000   # 최소 10만원
        self.buy_premium = 0.002              # 매수 프리미엄 0.2%
        self.sell_discount = 0.005            # 매도 할인 0.5%

        # 중복 주문 방지
        self.pending_orders = set()

        # 🆕 비동기 데이터 로거 초기화
        self.async_logger = get_async_logger()

        # 🆕 웹소켓 NOTICE 기반 주문 실행 관리자
        self.execution_manager = OrderExecutionManager(
            trade_db=trade_db,
            async_logger=self.async_logger
        )

        # 중복 신호 방지
        self.last_signals = {}
        self.signal_cooldown = 300  # 5분

        # 🆕 전략별 실행 통계
        self.strategy_stats = defaultdict(lambda: {
            'buy_orders': 0, 'sell_orders': 0, 'success_rate': 0.0, 'total_profit': 0.0
        })

        # 🎯 거래 기록 정책: 체결통보 시점에만 저장 (주문 제출시 저장 안함)
        logger.info("📋 거래 기록 정책: 체결통보(웹소켓 NOTICE) 시점에만 저장")
        logger.info("   💰 정확한 체결가/수량으로 기록, 주문 실패시 기록 안함")

        logger.info("✅ TradeExecutor 초기화 완료 (간소화 버전)")

    def set_candle_trade_manager(self, candle_trade_manager):
        """🎯 CandleTradeManager 참조 설정 - main.py에서 호출"""
        self.candle_trade_manager = candle_trade_manager
        logger.info("✅ TradeExecutor에 CandleTradeManager 참조 설정 완료")

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
            buy_price = self._calculate_buy_price(target_price)

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

            # 🔧 TradingManager는 성공시 order_no(str), 실패시 dict 반환
            if order_result is not None and isinstance(order_result, str):
                order_id = order_result if order_result.strip() else f"order_{int(datetime.now().timestamp() * 1000)}"

                # 🎯 웹소켓 NOTICE 대기를 위해 OrderExecutionManager에 등록 (체결시 거래 기록 저장됨)
                self.execution_manager.add_pending_order(
                    order_id=order_id, stock_code=stock_code, order_type='BUY',
                    quantity=buy_quantity, price=buy_price, strategy_type=strategy,
                    # 🆕 패턴 정보 추가
                    pattern_type=signal.get('pattern_type', ''),
                    pattern_confidence=signal.get('pattern_confidence', 0.0),
                    pattern_strength=signal.get('pattern_strength', 0),
                    # 🆕 기술적 지표 정보 추가
                    rsi_value=signal.get('rsi_value', None),
                    macd_value=signal.get('macd_value', None),
                    volume_ratio=signal.get('volume_ratio', None),
                    # 🆕 투자 정보 추가
                    investment_amount=total_amount,
                    investment_ratio=signal.get('investment_ratio', None)
                )

                logger.info(f"✅ 매수 주문 성공: {stock_code} (주문번호: {order_id}) - 체결 대기 중")
                return TradeResult(
                    success=True, stock_code=stock_code, order_type='BUY',
                    quantity=buy_quantity, price=buy_price, total_amount=total_amount,
                    order_no=order_id, is_pending=True
                )
            else:
                # 🆕 구체적인 오류 정보 처리
                if isinstance(order_result, dict) and not order_result.get('success', True):
                    error_code = order_result.get('error_code', 'UNKNOWN')
                    error_message = order_result.get('error_message', '알 수 없는 오류')
                    detailed_error = order_result.get('detailed_error', f"{error_code}: {error_message}")

                    # 🎯 주문가능금액 초과 오류 특별 처리
                    if 'APBK0952' in error_code or '주문가능금액을 초과' in error_message:
                        logger.error(f"💰 매수 주문 실패: {stock_code} - 주문가능금액 부족 ({detailed_error})")
                        failure_message = f"주문가능금액 부족: {error_message}"
                    else:
                        logger.error(f"❌ 매수 주문 실패: {stock_code} - {detailed_error}")
                        failure_message = f"주문 실패: {detailed_error}"

                else:
                    # 기존 방식 (None이나 기타 타입)
                    error_reason = f"TradingManager 반환값: {order_result} (타입: {type(order_result)})"
                    logger.error(f"❌ 매수 주문 실패: {stock_code} - {error_reason}")
                    failure_message = f"주문 실행 실패: {error_reason}"

                return TradeResult(
                    success=False, stock_code=stock_code, order_type='BUY',
                    quantity=buy_quantity, price=buy_price, total_amount=total_amount,
                    error_message=failure_message
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

            if sell_result and isinstance(sell_result, str):  # 주문번호가 반환되면 성공
                order_id = sell_result

                # 🎯 웹소켓 NOTICE 대기를 위해 OrderExecutionManager에 등록 (체결시 거래 기록 저장됨)
                self.execution_manager.add_pending_order(
                    order_id=order_id, stock_code=stock_code, order_type='SELL',
                    quantity=sell_quantity, price=sell_price, strategy_type=strategy
                )

                logger.info(f"✅ 매도 주문 성공: {stock_code} (주문번호: {order_id}) - 체결 대기 중")
                return TradeResult(
                    success=True, stock_code=stock_code, order_type='SELL',
                    quantity=sell_quantity, price=sell_price, total_amount=total_amount,
                    order_no=order_id, is_pending=True
                )
            else:
                # 🆕 구체적인 오류 정보 처리 (매도용)
                if isinstance(sell_result, dict) and not sell_result.get('success', True):
                    error_code = sell_result.get('error_code', 'UNKNOWN')
                    error_message = sell_result.get('error_message', '알 수 없는 오류')
                    detailed_error = sell_result.get('detailed_error', f"{error_code}: {error_message}")

                    logger.error(f"❌ 매도 주문 실패: {stock_code} - {detailed_error}")
                    failure_message = f"매도 실패: {detailed_error}"
                else:
                    logger.error(f"❌ 매도 주문 실패: {stock_code} - TradingManager 반환값: {sell_result}")
                    failure_message = "매도 주문 API 실패"

                return TradeResult(
                    success=False, stock_code=stock_code, order_type='SELL',
                    quantity=sell_quantity, price=sell_price, total_amount=total_amount,
                    error_message=failure_message
                )

        except Exception as e:
            logger.error(f"❌ 매도 주문 실행 중 오류: {stock_code} - {str(e)}")
            return TradeResult(
                success=False, stock_code=stock_code, order_type='SELL',
                quantity=signal.get('quantity', 0), price=signal.get('price', 0), total_amount=0,
                error_message=f"매도 실행 오류: {str(e)}"
            )



    # === 내부 헬퍼 메서드들 ===

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

    def _calculate_buy_price(self, current_price: int) -> int:
        """간소화된 매수 지정가 계산"""
        try:
            # 0.2% 프리미엄 적용
            target_price = int(current_price * (1 + self.buy_premium))

            # 틱 단위 조정
            final_price = self._adjust_to_tick_size(target_price)

            # 최대 5% 제한 (현재가의 105% 이하)
            max_buy_price = int(current_price * 1.05)
            final_price = min(final_price, max_buy_price)

            logger.debug(f"💰 매수가 계산: 현재가{current_price:,}원 → 주문가{final_price:,}원")
            return final_price

        except Exception as e:
            logger.error(f"매수가 계산 오류: {e}")
            return int(current_price * 1.002)  # 오류시 0.2% 프리미엄

    def _calculate_sell_price(self, current_price: int) -> int:
        """간소화된 매도 지정가 계산"""
        try:
            # 0.5% 할인 적용
            target_price = int(current_price * (1 - self.sell_discount))

            # 틱 단위 조정
            final_price = self._adjust_to_tick_size(target_price)

            # 최소 95% 보장 (현재가의 95% 이상)
            min_sell_price = int(current_price * 0.95)
            final_price = max(final_price, min_sell_price)

            logger.debug(f"💰 매도가 계산: 현재가{current_price:,}원 → 주문가{final_price:,}원")
            return final_price

        except Exception as e:
            logger.error(f"매도가 계산 오류: {e}")
            return int(current_price * 0.995)  # 오류시 0.5% 할인

    def _calculate_buy_quantity(self, buy_price: int, available_cash: int) -> int:
        """간소화된 매수 수량 계산"""
        try:
            # 안전 여유분 적용 (90%만 사용)
            safe_cash = int(available_cash * 0.9)

            if safe_cash < self.min_investment_amount:
                return 0

            # 기본 투자금액 사용
            target_amount = min(self.base_investment_amount, safe_cash, self.max_investment_amount)

            # 수량 계산
            quantity = int(target_amount // buy_price) if buy_price > 0 else 0

            # 최소 수량 체크
            if quantity * buy_price < self.min_investment_amount:
                min_quantity = max(1, int(self.min_investment_amount // buy_price))
                quantity = min(min_quantity, int(safe_cash // buy_price))

            logger.debug(f"💰 수량 계산: {quantity:,}주 (투자금액: {quantity * buy_price:,}원)")
            return max(0, quantity)

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
        """간소화된 실제 보유 수량 확인"""
        try:
            balance = self.trading_manager.get_balance()
            if not balance or not balance.get('success'):
                return 0

            holdings = balance.get('holdings', [])
            for holding in holdings:
                # 기본 필드 확인
                if holding.get('pdno') == stock_code:
                    return int(holding.get('hldg_qty', 0))
                if holding.get('stock_code') == stock_code:
                    return int(holding.get('quantity', 0))

            return 0

        except Exception as e:
            logger.error(f"❌ 보유 수량 확인 오류 ({stock_code}): {e}")
            return 0

    # ========== 🚫 DEPRECATED: 주문시 거래 기록 저장 함수들 ==========
    # 체결통보 시점(OrderExecutionManager)에서만 거래 기록을 저장하므로 더 이상 사용하지 않음

    def _record_buy_trade(self, stock_code: str, quantity: int, buy_price: int,
                         strategy: str, signal: Dict, order_result: Dict):
        """🚫 DEPRECATED: 매수 거래 기록 저장 (체결통보 시점에서만 저장하도록 변경)"""
        logger.debug(f"⚠️ DEPRECATED: _record_buy_trade 호출됨 - 체결통보 시점에서 저장됩니다")
        # 주문 제출시에는 기록하지 않고, 체결통보(OrderExecutionManager)에서만 기록
        pass

    def _record_sell_trade(self, stock_code: str, quantity: int, sell_price: int,
                          position: Dict, signal: Dict, sell_result: Dict):
        """🚫 DEPRECATED: 매도 거래 기록 저장 (체결통보 시점에서만 저장하도록 변경)"""
        logger.debug(f"⚠️ DEPRECATED: _record_sell_trade 호출됨 - 체결통보 시점에서 저장됩니다")
        # 주문 제출시에는 기록하지 않고, 체결통보(OrderExecutionManager)에서만 기록
        pass

    def get_execution_stats(self) -> Dict:
        """주문 실행 통계"""
        try:
            execution_stats = self.execution_manager.get_stats()
            pending_count = self.execution_manager.get_pending_orders_count()

            return {
                'websocket_executions': execution_stats,
                'pending_orders_count': pending_count
            }
        except Exception as e:
            logger.error(f"❌ 실행 통계 조회 오류: {e}")
            return {}

    def cleanup_expired_orders(self) -> int:
        """만료된 주문 정리"""
        try:
            return self.execution_manager.cleanup_expired_orders()
        except Exception as e:
            logger.error(f"❌ 만료 주문 정리 오류: {e}")
            return 0

    def _calculate_buy_quantity_simple(self, buy_price: float, available_cash: int) -> int:
        """간단한 매수 수량 계산 (캔들 전용)"""
        try:
            if available_cash < self.min_investment_amount:
                return 0

            # 기본 투자 금액 사용
            target_amount = min(self.base_investment_amount, self.max_investment_amount)
            quantity = int(target_amount // buy_price)
            return max(0, quantity)

        except Exception as e:
            logger.error(f"수량 계산 오류: {e}")
            return 0
