#!/usr/bin/env python3
"""
거래 실행 관리자 - 매수/매도 로직 전담
StockBot의 거래 실행 로직을 분리하여 단일 책임 원칙을 적용
"""
import logging
from typing import Dict, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class TradeConfig:
    """거래 설정"""
    # 포지션 사이즈 설정
    base_position_ratio: float = 0.08  # 기본 8%
    max_position_ratio: float = 0.12   # 최대 12%
    max_investment_amount: int = 500000  # 최대 50만원
    min_investment_amount: int = 50000   # 최소 5만원
    
    # 전략별 포지션 배수
    strategy_multipliers: Dict[str, float] = None
    
    # 가격 프리미엄/할인
    buy_premiums: Dict[str, float] = None
    sell_discounts: Dict[str, float] = None
    
    def __post_init__(self):
        if self.strategy_multipliers is None:
            self.strategy_multipliers = {
                'gap_trading': 0.7,      # 갭 거래: 보수적 (5.6%)
                'volume_breakout': 0.9,  # 거래량: 적극적 (7.2%)
                'momentum': 1.2,         # 모멘텀: 공격적 (9.6%)
                'existing_holding': 0.5, # 기존 보유: 매우 보수적 (4%)
                'default': 1.0           # 기본: 8%
            }
        
        if self.buy_premiums is None:
            self.buy_premiums = {
                'gap_trading': 0.003,      # 갭 거래: 0.3% 위
                'volume_breakout': 0.005,  # 거래량 돌파: 0.5% 위
                'momentum': 0.007,         # 모멘텀: 0.7% 위
                'existing_holding': 0.002, # 기존 보유: 0.2% 위
                'default': 0.003           # 기본: 0.3% 위
            }
        
        if self.sell_discounts is None:
            self.sell_discounts = {
                'gap_trading': 0.005,    # 갭 거래: 0.5% 아래
                'volume_breakout': 0.006, # 거래량 돌파: 0.6% 아래
                'momentum': 0.004,       # 모멘텀: 0.4% 아래
                'default': 0.005         # 기본: 0.5% 아래
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
    
    @property
    def is_buy(self) -> bool:
        return self.order_type == 'BUY'
    
    @property
    def is_sell(self) -> bool:
        return self.order_type == 'SELL'


class TradeExecutor:
    """거래 실행 전담 클래스"""
    
    def __init__(self, trading_manager, position_manager, data_manager, trade_db, config: TradeConfig = None):
        """초기화"""
        self.trading_manager = trading_manager
        self.position_manager = position_manager
        self.data_manager = data_manager
        self.trade_db = trade_db
        self.config = config or TradeConfig()
        
        # 중복 주문 방지
        self.pending_orders = set()
        
        logger.info("✅ TradeExecutor 초기화 완료")
    
    def execute_buy_signal(self, signal: Dict) -> TradeResult:
        """매수 신호 실행"""
        try:
            stock_code = signal['stock_code']
            strategy = signal['strategy']
            strength = signal.get('strength', 0.5)
            
            logger.info(f"🛒 매수 신호 처리 시작: {stock_code} (전략: {strategy}, 강도: {strength:.2f})")
            
            # 1. 기본 검증
            validation_result = self._validate_buy_signal(signal)
            if not validation_result[0]:
                return TradeResult(
                    success=False,
                    stock_code=stock_code,
                    order_type='BUY',
                    quantity=0,
                    price=0,
                    total_amount=0,
                    error_message=validation_result[1]
                )
            
            # 2. 현재가 조회
            current_price = self._get_current_price(stock_code)
            if current_price <= 0:
                return TradeResult(
                    success=False,
                    stock_code=stock_code,
                    order_type='BUY',
                    quantity=0,
                    price=0,
                    total_amount=0,
                    error_message=f"현재가 조회 실패: {current_price}"
                )
            
            # 3. 매수가 계산
            buy_price = self._calculate_buy_price(current_price, strategy)
            
            # 4. 매수 수량 계산
            available_cash = self._get_available_cash()
            quantity = self._calculate_buy_quantity(
                current_price=current_price,
                buy_price=buy_price,
                available_cash=available_cash,
                strategy=strategy,
                strength=strength
            )
            
            if quantity <= 0:
                return TradeResult(
                    success=False,
                    stock_code=stock_code,
                    order_type='BUY',
                    quantity=0,
                    price=buy_price,
                    total_amount=0,
                    error_message=f"매수 수량 부족: 현재가={current_price:,}원, 매수가={buy_price:,}원, 예산={available_cash:,}원"
                )
            
            # 5. 실제 매수 주문 실행
            total_amount = quantity * buy_price
            order_result = self.trading_manager.buy_order(
                stock_code=stock_code,
                quantity=quantity,
                price=buy_price
            )
            
            if order_result.get('status') == 'success':
                # 6. 거래 기록 저장
                self._record_buy_trade(stock_code, quantity, buy_price, strategy, signal, order_result)
                
                # 7. 포지션 추가
                self.position_manager.add_position(
                    stock_code=stock_code,
                    quantity=quantity,
                    buy_price=buy_price,
                    strategy_type=strategy
                )
                
                logger.info(f"✅ 매수 주문 완료: {stock_code} {quantity:,}주 @ {buy_price:,}원 (총 {total_amount:,}원)")
                
                return TradeResult(
                    success=True,
                    stock_code=stock_code,
                    order_type='BUY',
                    quantity=quantity,
                    price=buy_price,
                    total_amount=total_amount,
                    order_no=order_result.get('order_no', '')
                )
            else:
                error_msg = f"매수 주문 실패: {order_result.get('message', '알 수 없는 오류')}"
                logger.error(f"❌ {error_msg}")
                
                return TradeResult(
                    success=False,
                    stock_code=stock_code,
                    order_type='BUY',
                    quantity=quantity,
                    price=buy_price,
                    total_amount=total_amount,
                    error_message=error_msg
                )
        
        except Exception as e:
            error_msg = f"매수 신호 실행 오류: {e}"
            logger.error(error_msg)
            
            return TradeResult(
                success=False,
                stock_code=signal.get('stock_code', 'UNKNOWN'),
                order_type='BUY',
                quantity=0,
                price=0,
                total_amount=0,
                error_message=error_msg
            )
    
    def execute_sell_signal(self, signal: Dict) -> TradeResult:
        """매도 신호 실행"""
        try:
            stock_code = signal['stock_code']
            strategy = signal['strategy']
            
            logger.info(f"🏪 매도 신호 처리 시작: {stock_code} (전략: {strategy})")
            
            # 1. 포지션 확인
            existing_positions = self.position_manager.get_positions('active')
            if stock_code not in existing_positions:
                return TradeResult(
                    success=False,
                    stock_code=stock_code,
                    order_type='SELL',
                    quantity=0,
                    price=0,
                    total_amount=0,
                    error_message=f"보유하지 않은 종목: {stock_code}"
                )
            
            position = existing_positions[stock_code]
            position_quantity = position.get('quantity', 0)
            if position_quantity <= 0:
                return TradeResult(
                    success=False,
                    stock_code=stock_code,
                    order_type='SELL',
                    quantity=0,
                    price=0,
                    total_amount=0,
                    error_message=f"매도할 수량이 없음: {stock_code}"
                )
            
            # 2. 현재가 조회
            current_price = self._get_current_price(stock_code)
            if current_price <= 0:
                return TradeResult(
                    success=False,
                    stock_code=stock_code,
                    order_type='SELL',
                    quantity=0,
                    price=0,
                    total_amount=0,
                    error_message=f"현재가 조회 실패: {current_price}"
                )
            
            # 3. 매도가 계산
            sell_price = self._calculate_sell_price(current_price, strategy, is_auto_sell=False)
            
            # 4. 실제 보유 수량 검증
            actual_quantity = self._get_actual_holding_quantity(stock_code)
            verified_quantity = min(position_quantity, actual_quantity) if actual_quantity > 0 else 0
            
            if verified_quantity <= 0:
                return TradeResult(
                    success=False,
                    stock_code=stock_code,
                    order_type='SELL',
                    quantity=0,
                    price=sell_price,
                    total_amount=0,
                    error_message=f"실제 보유 수량 부족: 요청={position_quantity}, 실제={actual_quantity}"
                )
            
            # 5. 매도 주문 실행
            total_amount = verified_quantity * sell_price
            sell_result = self.trading_manager.sell_order(
                stock_code=stock_code,
                quantity=verified_quantity,
                price=sell_price
            )
            
            if sell_result.get('status') == 'success':
                # 6. 거래 기록 저장
                self._record_sell_trade(stock_code, verified_quantity, sell_price, position, signal, sell_result)
                
                # 7. 포지션에서 제거
                self.position_manager.remove_position(stock_code, verified_quantity, sell_price)
                
                logger.info(f"✅ 매도 주문 완료: {stock_code} {verified_quantity:,}주 @ {sell_price:,}원 (총 {total_amount:,}원)")
                
                return TradeResult(
                    success=True,
                    stock_code=stock_code,
                    order_type='SELL',
                    quantity=verified_quantity,
                    price=sell_price,
                    total_amount=total_amount,
                    order_no=sell_result.get('order_no', '')
                )
            else:
                error_msg = f"매도 주문 실패: {sell_result.get('message', '알 수 없는 오류')}"
                logger.error(f"❌ {error_msg}")
                
                return TradeResult(
                    success=False,
                    stock_code=stock_code,
                    order_type='SELL',
                    quantity=verified_quantity,
                    price=sell_price,
                    total_amount=total_amount,
                    error_message=error_msg
                )
        
        except Exception as e:
            error_msg = f"매도 신호 실행 오류: {e}"
            logger.error(error_msg)
            
            return TradeResult(
                success=False,
                stock_code=signal.get('stock_code', 'UNKNOWN'),
                order_type='SELL',
                quantity=0,
                price=0,
                total_amount=0,
                error_message=error_msg
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
    
    def _validate_buy_signal(self, signal: Dict) -> Tuple[bool, str]:
        """매수 신호 검증"""
        stock_code = signal.get('stock_code')
        if not stock_code:
            return False, "종목코드 누락"
        
        # 포지션 중복 체크
        existing_positions = self.position_manager.get_positions('active')
        if stock_code in existing_positions:
            return False, f"이미 보유 중인 종목: {stock_code}"
        
        # 중복 주문 체크
        if stock_code in self.pending_orders:
            return False, f"이미 주문 진행 중인 종목: {stock_code}"
        
        return True, "검증 통과"
    
    def _get_current_price(self, stock_code: str) -> int:
        """현재가 조회"""
        try:
            current_data = self.data_manager.get_latest_data(stock_code)
            if not current_data or current_data.get('status') != 'success':
                logger.error(f"현재가 조회 실패: {stock_code}")
                return 0
            
            current_price = current_data.get('current_price', 0)
            if current_price <= 0:
                logger.error(f"유효하지 않은 현재가: {stock_code} = {current_price}")
                return 0
            
            return current_price
        except Exception as e:
            logger.error(f"현재가 조회 오류 ({stock_code}): {e}")
            return 0
    
    def _get_available_cash(self) -> int:
        """사용 가능한 현금 조회"""
        try:
            balance = self.trading_manager.get_balance()
            return balance.get('available_cash', 0)
        except Exception as e:
            logger.error(f"잔고 조회 오류: {e}")
            return 0
    
    def _calculate_buy_price(self, current_price: int, strategy: str = 'default') -> int:
        """매수 지정가 계산"""
        try:
            base_premium = self.config.buy_premiums.get(strategy, self.config.buy_premiums['default'])
            
            # 시장 상황별 동적 조정
            volatility_adjustment = 0
            if current_price < 5000:
                volatility_adjustment = 0.002   # 저가주: +0.2%
            elif current_price > 100000:
                volatility_adjustment = -0.001  # 고가주: -0.1%
            
            # 최종 프리미엄 계산
            final_premium = base_premium + volatility_adjustment
            final_premium = max(0.001, min(final_premium, 0.01))  # 0.1%~1.0% 범위 제한
            
            # 계산된 매수가
            buy_price = int(current_price * (1 + final_premium))
            
            # 호가 단위로 조정
            buy_price = self._adjust_to_tick_size(buy_price)
            
            logger.debug(f"💰 매수가 계산: {current_price:,}원 → {buy_price:,}원 (프리미엄: {final_premium:.1%}, 전략: {strategy})")
            return buy_price
            
        except Exception as e:
            logger.error(f"매수가 계산 오류: {e}")
            return int(current_price * 1.003)  # 기본 0.3% 프리미엄
    
    def _calculate_sell_price(self, current_price: int, strategy: str = 'default', is_auto_sell: bool = False) -> int:
        """매도 지정가 계산"""
        try:
            if is_auto_sell:
                # 자동매도시 빠른 체결을 위해 더 낮은 가격
                discount = 0.008  # 0.8% 할인
            else:
                # 전략별 매도 할인 설정
                discount = self.config.sell_discounts.get(strategy, self.config.sell_discounts['default'])
            
            # 계산된 매도가
            sell_price = int(current_price * (1 - discount))
            
            # 호가 단위로 조정
            sell_price = self._adjust_to_tick_size(sell_price)
            
            logger.debug(f"💰 매도가 계산: {current_price:,}원 → {sell_price:,}원 (할인: {discount:.1%})")
            return sell_price
            
        except Exception as e:
            logger.error(f"매도가 계산 오류: {e}")
            return int(current_price * 0.995)  # 기본 0.5% 할인
    
    def _calculate_buy_quantity(self, current_price: int, buy_price: int, available_cash: int, 
                               strategy: str, strength: float) -> int:
        """매수 수량 계산"""
        try:
            if available_cash < self.config.min_investment_amount:
                logger.warning(f"잔고 부족: {available_cash:,}원 < {self.config.min_investment_amount:,}원")
                return 0
            
            # 전략별 포지션 사이즈 조정
            strategy_multiplier = self.config.strategy_multipliers.get(strategy, 1.0)
            
            # 신호 강도 고려 (0.3 ~ 1.2 범위)
            strength_adjusted = max(0.3, min(strength, 1.2))
            
            # 최종 포지션 비율 계산
            final_position_ratio = self.config.base_position_ratio * strategy_multiplier * strength_adjusted
            
            # 최대 투자 금액 계산
            max_investment = min(
                available_cash * final_position_ratio,  # 잔고 비율 기준
                available_cash * self.config.max_position_ratio,  # 최대 비율 제한
                self.config.max_investment_amount       # 최대 금액 제한
            )
            
            # 수량 계산
            quantity = int(max_investment // buy_price) if buy_price > 0 else 0
            
            # 최소 수량 체크
            if quantity * buy_price < self.config.min_investment_amount:
                quantity = max(1, int(self.config.min_investment_amount // buy_price))
            
            # 최종 매수 금액 확인 및 재조정
            total_buy_amount = quantity * buy_price
            if total_buy_amount > available_cash:
                quantity = int(available_cash // buy_price)
                total_buy_amount = quantity * buy_price
                logger.debug(f"💰 매수 수량 재조정: {quantity:,}주, 총액={total_buy_amount:,}원")
            
            logger.info(f"💰 매수 수량 계산: 전략={strategy}, 강도={strength:.2f}, 비율={final_position_ratio:.1%}, 수량={quantity:,}주")
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
        """실제 보유 수량 확인"""
        try:
            balance = self.trading_manager.get_balance()
            holdings = balance.get('holdings', [])
            
            for holding in holdings:
                if holding.get('pdno') == stock_code:
                    quantity = int(holding.get('hldg_qty', 0))
                    logger.debug(f"📊 실제 보유 수량: {stock_code} = {quantity:,}주")
                    return quantity
            
            logger.debug(f"📊 실제 보유 수량: {stock_code} = 0주 (보유하지 않음)")
            return 0
            
        except Exception as e:
            logger.error(f"실제 보유 수량 확인 오류 ({stock_code}): {e}")
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
                strategy_type=position.get('strategy_type', 'unknown'),
                buy_trade_id=buy_trade_id,
                order_id=sell_result.get('order_no', ''),
                status='SUCCESS',
                market_conditions={
                    'current_price': signal.get('price', sell_price),
                    'profit_rate': profit_rate,
                    'sell_reason': f"{sell_type}: {condition_reason}"
                },
                notes=f"매도사유: {sell_type}, 조건: {condition_reason}"
            )
            logger.info(f"💾 매도 기록 저장 완료 (ID: {trade_id})")
            
        except Exception as e:
            logger.error(f"💾 매도 기록 저장 실패: {e}")
    
    def _record_auto_sell_trade(self, stock_code: str, quantity: int, sell_price: int,
                               reason: str, sell_signal: Dict, order_result: str):
        """자동 매도 거래 기록 저장"""
        try:
            # 매수 거래 ID 찾기
            buy_trade_id = self.trade_db.find_buy_trade_for_sell(stock_code, quantity)
            
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
                strategy_type=sell_signal.get('strategy_type', 'auto_sell'),
                buy_trade_id=buy_trade_id,
                order_id=order_result,
                status='SUCCESS',
                market_conditions={
                    'current_price': current_price,
                    'profit_rate': profit_rate,
                    'sell_reason': f"자동매도: {reason}"
                },
                notes=f"자동매도 - {reason}, 현재가: {current_price:,}원"
            )
            logger.info(f"💾 자동매도 기록 저장 완료 (ID: {trade_id})")
            
        except Exception as e:
            logger.error(f"💾 자동매도 기록 저장 실패: {e}") 