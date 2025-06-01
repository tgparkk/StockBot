#!/usr/bin/env python3
"""
거래 실행자 - 통합 거래 로직 처리
"""
import time
import threading
from typing import Dict, List, Optional, Any
from utils.logger import setup_logger
from ..api.kis_market_api import get_disparity_rank, get_multi_period_disparity
from dataclasses import dataclass, field
from datetime import datetime
from .async_data_logger import get_async_logger, log_signal_failed, log_buy_failed, log_buy_success
from .order_execution_manager import OrderExecutionManager

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
    strategy_multipliers: Dict[str, float] = None

    # 가격 프리미엄/할인
    buy_premiums: Dict[str, float] = None
    sell_discounts: Dict[str, float] = None

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

    def __init__(self, trading_manager, position_manager, data_manager, trade_db, config: TradeConfig = None):
        """초기화"""
        self.trading_manager = trading_manager
        self.position_manager = position_manager
        self.data_manager = data_manager
        self.trade_db = trade_db
        self.config = config or TradeConfig()

        # 중복 주문 방지
        self.pending_orders = set()

        # 🆕 비동기 데이터 로거 초기화
        self.async_logger = get_async_logger()
        
        # 🆕 웹소켓 NOTICE 기반 주문 실행 관리자
        self.execution_manager = OrderExecutionManager(
            position_manager=position_manager,
            trade_db=trade_db,
            async_logger=self.async_logger
        )
        
        # 웹소켓 NOTICE 콜백 등록
        self._register_websocket_callbacks()
        
        # 중복 신호 방지
        self.last_signals = {}
        self.signal_cooldown = 300  # 5분
        
        logger.info("✅ TradeExecutor 초기화 완료 (실전투자 체결통보 연동)")

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
        """매수 신호 실행 - 🆕 웹소켓 NOTICE 기반으로 개선"""
        stock_code = signal.get('stock_code', '')
        strategy = signal.get('strategy', 'unknown')
        signal_strength = signal.get('strength', 0.0)
        
        # 🆕 매수 시도 시작 로깅
        attempt_start_time = time.time()
        validation_details = {}
        
        try:
            logger.info(f"📈 매수 신호 처리 시작: {stock_code} (전략: {strategy}, 강도: {signal_strength:.2f})")

            # 1. 기본 신호 검증
            validation_result = self._validate_buy_signal_enhanced(signal, stock_code)
            validation_details['enhanced_validation'] = validation_result
            
            if not validation_result:
                reason = "강화된 매수 검증 실패"
                # 🆕 매수 실패 로깅
                log_buy_failed(
                    stock_code=stock_code,
                    reason=reason,
                    signal_data=signal,
                    validation_details=validation_details
                )
                return TradeResult(
                    success=False,
                    stock_code=stock_code,
                    order_type='BUY',
                    quantity=0,
                    price=signal.get('price', 0),
                    total_amount=0,
                    error_message=reason
                )

            # 2. 잔고 확인
            balance = self.trading_manager.get_balance()
            available_cash = balance.get('available_cash', 0)
            validation_details['available_cash'] = available_cash
            
            if available_cash < self.config.min_investment_amount:
                reason = f"잔고 부족: {available_cash:,}원 < {self.config.min_investment_amount:,}원"
                # 🆕 매수 실패 로깅
                log_buy_failed(
                    stock_code=stock_code,
                    reason=reason,
                    signal_data=signal,
                    validation_details=validation_details
                )
                return TradeResult(
                    success=False,
                    stock_code=stock_code,
                    order_type='BUY',
                    quantity=0,
                    price=signal.get('price', 0),
                    total_amount=0,
                    error_message=reason
                )

            # 3. 매수 가격 및 수량 계산
            current_price = signal.get('price', 0)
            if current_price <= 0:
                reason = "유효하지 않은 가격"
                log_buy_failed(stock_code, reason, signal, validation_details)
                return TradeResult(False, stock_code, 'BUY', 0, 0, 0, reason)

            # 호가 단위로 조정된 매수 가격
            buy_price = self._adjust_to_tick_size(current_price)
            validation_details['buy_price'] = buy_price
            
            # 매수 수량 계산
            quantity = self._calculate_buy_quantity(
                current_price, buy_price, available_cash, strategy, signal_strength
            )
            validation_details['calculated_quantity'] = quantity
            
            if quantity <= 0:
                reason = "매수 수량 부족"
                log_buy_failed(stock_code, reason, signal, validation_details)
                return TradeResult(False, stock_code, 'BUY', 0, buy_price, 0, reason)

            total_amount = quantity * buy_price
            validation_details['total_amount'] = total_amount

            # 4. 최종 잔고 재확인
            if total_amount > available_cash:
                reason = f"총 매수금액 초과: {total_amount:,}원 > {available_cash:,}원"
                log_buy_failed(stock_code, reason, signal, validation_details)
                return TradeResult(False, stock_code, 'BUY', quantity, buy_price, total_amount, reason)

            # 5. 실제 매수 주문 실행
            logger.info(f"💰 매수 주문 실행: {stock_code} {quantity:,}주 @{buy_price:,}원 (총 {total_amount:,}원)")
            
            buy_result = self.trading_manager.execute_order(
                stock_code=stock_code,
                order_type="BUY",
                quantity=quantity,
                price=buy_price,
                strategy_type=strategy
            )

            if buy_result and buy_result.get('success', False):
                order_id = buy_result.get('order_id', '')
                
                # 🆕 웹소켓 NOTICE 대기를 위해 OrderExecutionManager에 주문 등록
                if order_id:
                    success = self.execution_manager.add_pending_order(
                        order_id=order_id,
                        stock_code=stock_code,
                        order_type='BUY',
                        quantity=quantity,
                        price=buy_price,
                        strategy_type=strategy
                    )
                    
                    if success:
                        logger.info(f"✅ 매수 주문 전송 성공 - 웹소켓 NOTICE 대기 중: {stock_code} (주문ID: {order_id})")
                        return TradeResult(
                            success=True,
                            stock_code=stock_code,
                            order_type='BUY',
                            quantity=quantity,
                            price=buy_price,
                            total_amount=total_amount,
                            order_no=order_id,
                            is_pending=True  # 🆕 웹소켓 NOTICE 대기 중
                        )
                    else:
                        reason = "주문 대기 등록 실패"
                        log_buy_failed(stock_code, reason, signal, validation_details)
                        return TradeResult(False, stock_code, 'BUY', quantity, buy_price, total_amount, reason)
                else:
                    # 🚨 주문ID가 없는 경우도 체결통보를 기다려야 함
                    logger.warning(f"⚠️ 주문ID 없음 - 체결통보 기반 처리 불가: {stock_code}")
                    
                    # 🆕 임시 주문ID 생성하여 대기 목록에 추가
                    temp_order_id = f"TEMP_{stock_code}_{int(time.time())}"
                    
                    success = self.execution_manager.add_pending_order(
                        order_id=temp_order_id,
                        stock_code=stock_code,
                        order_type='BUY',
                        quantity=quantity,
                        price=buy_price,
                        strategy_type=strategy
                    )
                    
                    if success:
                        logger.info(f"⏳ 임시 주문ID로 체결통보 대기: {stock_code} (임시ID: {temp_order_id})")
                        return TradeResult(
                            success=True,
                            stock_code=stock_code,
                            order_type='BUY',
                            quantity=quantity,
                            price=buy_price,
                            total_amount=total_amount,
                            order_no=temp_order_id,
                            is_pending=True  # 체결통보 대기 중
                        )
                    else:
                        # 대기 등록 실패시에만 기존 방식 사용 (최후 수단)
                        logger.error(f"❌ 임시 주문ID 등록 실패 - 기존방식 적용: {stock_code}")
                        
                        # 기존 방식: 즉시 포지션에 추가 (비추천, 최후 수단)
                        self.position_manager.add_position(
                            stock_code=stock_code,
                            quantity=quantity,
                            buy_price=buy_price,
                            strategy_type=strategy
                        )
                        
                        # 기존 방식: 즉시 성공 로깅 (비추천, 최후 수단)
                        log_buy_success(
                            stock_code=stock_code,
                            buy_price=buy_price,
                            quantity=quantity,
                            strategy=strategy,
                            signal_data=signal
                        )
                        
                        logger.warning(f"⚠️ 비추천 방식으로 매수 처리됨: {stock_code} {quantity:,}주 @{buy_price:,}원")
                        return TradeResult(
                            success=True,
                            stock_code=stock_code,
                            order_type='BUY',
                            quantity=quantity,
                            price=buy_price,
                            total_amount=total_amount,
                            order_no=buy_result.get('order_id', temp_order_id)
                        )
            else:
                reason = f"매수 주문 실패: {buy_result.get('error_message', '알 수 없는 오류') if buy_result else '주문 결과 없음'}"
                log_buy_failed(stock_code, reason, signal, validation_details)
                return TradeResult(False, stock_code, 'BUY', quantity, buy_price, total_amount, reason)

        except Exception as e:
            reason = f"매수 신호 처리 오류: {e}"
            logger.error(reason)
            # 🆕 예외 상황 로깅
            log_buy_failed(
                stock_code=stock_code,
                reason=reason,
                signal_data=signal,
                validation_details=validation_details
            )
            return TradeResult(False, stock_code, 'BUY', 0, 0, 0, reason)

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
            sell_result = self.trading_manager.execute_order(
                stock_code=stock_code,
                order_type="SELL",
                quantity=verified_quantity,
                price=sell_price,
                strategy_type=strategy
            )

            if sell_result:  # 주문번호가 반환되면 성공
                # 🆕 체결통보 기반 매도 처리
                order_id = sell_result if isinstance(sell_result, str) else str(sell_result)
                
                if order_id:
                    # 웹소켓 NOTICE 대기를 위해 OrderExecutionManager에 주문 등록
                    success = self.execution_manager.add_pending_order(
                        order_id=order_id,
                        stock_code=stock_code,
                        order_type='SELL',
                        quantity=verified_quantity,
                        price=sell_price,
                        strategy_type=strategy
                    )
                    
                    if success:
                        # 📝 거래 기록은 저장하되 포지션 제거는 체결통보 후
                        sell_result_dict = {'order_no': order_id, 'status': 'pending'}
                        self._record_sell_trade(stock_code, verified_quantity, sell_price, position, signal, sell_result_dict)
                        
                        logger.info(f"📤 매도 주문 전송 성공 - 체결통보 대기 중: {stock_code} {verified_quantity:,}주 @{sell_price:,}원")
                        
                        return TradeResult(
                            success=True,
                            stock_code=stock_code,
                            order_type='SELL',
                            quantity=verified_quantity,
                            price=sell_price,
                            total_amount=total_amount,
                            order_no=order_id,
                            is_pending=True  # 체결통보 대기 중
                        )
                    else:
                        # 대기 등록 실패시 기존 방식 (최후 수단)
                        logger.warning(f"⚠️ 매도 주문 대기 등록 실패 - 기존방식 적용: {stock_code}")
                        
                        # 기존 방식: 즉시 처리
                        sell_result_dict = {'order_no': order_id, 'status': 'success'}
                        self._record_sell_trade(stock_code, verified_quantity, sell_price, position, signal, sell_result_dict)
                        
                        # 포지션에서 제거 (비추천, 최후 수단)
                        self.position_manager.remove_position(stock_code, verified_quantity, sell_price)
                        
                        logger.warning(f"⚠️ 비추천 방식으로 매도 처리됨: {stock_code} {verified_quantity:,}주 @{sell_price:,}원")
                        
                        return TradeResult(
                            success=True,
                            stock_code=stock_code,
                            order_type='SELL',
                            quantity=verified_quantity,
                            price=sell_price,
                            total_amount=total_amount,
                            order_no=order_id
                        )
                else:
                    # 주문ID가 없는 경우 임시ID 생성
                    temp_order_id = f"TEMP_SELL_{stock_code}_{int(time.time())}"
                    
                    success = self.execution_manager.add_pending_order(
                        order_id=temp_order_id,
                        stock_code=stock_code,
                        order_type='SELL',
                        quantity=verified_quantity,
                        price=sell_price,
                        strategy_type=strategy
                    )
                    
                    if success:
                        # 거래 기록 저장
                        sell_result_dict = {'order_no': temp_order_id, 'status': 'pending'}
                        self._record_sell_trade(stock_code, verified_quantity, sell_price, position, signal, sell_result_dict)
                        
                        logger.info(f"⏳ 임시ID로 매도 체결통보 대기: {stock_code} (임시ID: {temp_order_id})")
                        
                        return TradeResult(
                            success=True,
                            stock_code=stock_code,
                            order_type='SELL',
                            quantity=verified_quantity,
                            price=sell_price,
                            total_amount=total_amount,
                            order_no=temp_order_id,
                            is_pending=True
                        )
                    else:
                        error_msg = f"매도 주문 대기 등록 실패: {stock_code}"
                        logger.error(f"❌ {error_msg}")
                        return TradeResult(False, stock_code, 'SELL', verified_quantity, sell_price, total_amount, error_msg)

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

    def _validate_buy_signal(self, signal: Dict, stock_code: str) -> bool:
        """기본 매수 신호 검증"""
        try:
            # 1. 필수 필드 확인
            required_fields = ['stock_code', 'strategy', 'strength', 'price']
            for field in required_fields:
                if field not in signal:
                    reason = f"필수 필드 누락: {field}"
                    # 🆕 필수 필드 누락 로깅
                    log_signal_failed(
                        stock_code=stock_code,
                        strategy=signal.get('strategy', 'unknown'),
                        signal_strength=signal.get('strength', 0.0),
                        threshold=1.0,
                        reason=reason,
                        market_data=signal
                    )
                    logger.warning(f"❌ 매수 신호 검증 실패: {reason}")
                    return False

            # 2. 신호 강도 확인
            strength = signal.get('strength', 0.0)
            min_strength = 0.3  # 최소 신호 강도
            if strength < min_strength:
                reason = f"신호 강도 부족 ({strength:.2f} < {min_strength})"
                # 🆕 신호 강도 부족 로깅
                log_signal_failed(
                    stock_code=stock_code,
                    strategy=signal.get('strategy', 'unknown'),
                    signal_strength=strength,
                    threshold=min_strength,
                    reason=reason,
                    market_data=signal
                )
                logger.debug(f"🔽 신호 강도 부족: {stock_code} - {reason}")
                return False

            # 3. 쿨다운 확인 (같은 종목 중복 신호 방지)
            current_time = time.time()
            if stock_code in self.last_signals:
                last_signal_time = self.last_signals[stock_code]
                if current_time - last_signal_time < self.signal_cooldown:
                    remaining_time = int(self.signal_cooldown - (current_time - last_signal_time))
                    reason = f"신호 쿨다운 중 (남은 시간: {remaining_time}초)"
                    # 🆕 쿨다운 로깅
                    log_signal_failed(
                        stock_code=stock_code,
                        strategy=signal.get('strategy', 'unknown'),
                        signal_strength=strength,
                        threshold=self.signal_cooldown,
                        reason=reason,
                        market_data=signal
                    )
                    logger.debug(f"⏰ 신호 쿨다운: {stock_code} - {reason}")
                    return False

            # 4. 중복 포지션 확인
            existing_positions = self.position_manager.get_positions('active')
            if stock_code in existing_positions:
                reason = "이미 보유 중인 종목"
                # 🆕 중복 포지션 로깅
                log_signal_failed(
                    stock_code=stock_code,
                    strategy=signal.get('strategy', 'unknown'),
                    signal_strength=strength,
                    threshold=1.0,
                    reason=reason,
                    market_data=signal
                )
                logger.debug(f"📊 중복 포지션: {stock_code} - {reason}")
                return False

            # 5. 진행중인 주문 확인
            if stock_code in self.pending_orders:
                reason = "처리 중인 주문 존재"
                # 🆕 중복 주문 로깅
                log_signal_failed(
                    stock_code=stock_code,
                    strategy=signal.get('strategy', 'unknown'),
                    signal_strength=strength,
                    threshold=1.0,
                    reason=reason,
                    market_data=signal
                )
                logger.debug(f"⚠️ 중복 주문: {stock_code} - {reason}")
                return False

            # 6. 가격 유효성 확인
            price = signal.get('price', 0)
            if price <= 0:
                reason = f"유효하지 않은 가격: {price}"
                # 🆕 가격 오류 로깅
                log_signal_failed(
                    stock_code=stock_code,
                    strategy=signal.get('strategy', 'unknown'),
                    signal_strength=strength,
                    threshold=1.0,
                    reason=reason,
                    market_data=signal
                )
                logger.warning(f"💰 가격 오류: {stock_code} - {reason}")
                return False

            # 7. 거래 시간 확인 (선택사항)
            # 현재는 시간 제한 없이 모든 시간에 거래 허용

            # 모든 검증 통과
            self.last_signals[stock_code] = current_time
            logger.debug(f"✅ 기본 매수 신호 검증 통과: {stock_code}")
            return True

        except Exception as e:
            reason = f"검증 과정 오류: {e}"
            logger.error(f"❌ 매수 신호 검증 오류: {reason}")
            # 🆕 검증 오류 로깅
            log_signal_failed(
                stock_code=stock_code,
                strategy=signal.get('strategy', 'unknown'),
                signal_strength=signal.get('strength', 0.0),
                threshold=1.0,
                reason=reason,
                market_data=signal
            )
            return False

    def _validate_buy_signal_enhanced(self, signal: Dict, stock_code: str) -> bool:
        """🆕 강화된 매수 신호 검증 (고도화된 다중 이격도 활용)"""
        try:
            # 기본 검증
            basic_validation = self._validate_buy_signal(signal, stock_code)
            if not basic_validation:
                # 🆕 기본 검증 실패 로깅
                log_signal_failed(
                    stock_code=stock_code,
                    strategy=signal.get('strategy', 'unknown'),
                    signal_strength=signal.get('strength', 0.0),
                    threshold=0.3,  # 기본 임계값
                    reason="기본 검증 실패",
                    market_data=signal
                )
                logger.warning(f"🚫 강화된 매수 검증 실패: 기본 검증 단계에서 실패 ({stock_code})")
                return False

            # 🎯 다중 기간 이격도 종합 검증
            try:
                # 특정 종목에 대한 5일, 20일, 60일 이격도 확인
                d5_data = get_disparity_rank(
                    fid_input_iscd="0000",
                    fid_hour_cls_code="5",
                    fid_vol_cnt="10000"
                )
                d20_data = get_disparity_rank(
                    fid_input_iscd="0000",
                    fid_hour_cls_code="20",
                    fid_vol_cnt="10000"
                )
                d60_data = get_disparity_rank(
                    fid_input_iscd="0000",
                    fid_hour_cls_code="60",
                    fid_vol_cnt="10000"
                )

                # 해당 종목의 다중 이격도 검증
                d5_val = d20_val = d60_val = None

                if d5_data is not None and not d5_data.empty:
                    d5_row = d5_data[d5_data['mksc_shrn_iscd'] == stock_code]
                    if not d5_row.empty:
                        d5_val = float(d5_row.iloc[0].get('d5_dsrt', 100))

                if d20_data is not None and not d20_data.empty:
                    d20_row = d20_data[d20_data['mksc_shrn_iscd'] == stock_code]
                    if not d20_row.empty:
                        d20_val = float(d20_row.iloc[0].get('d20_dsrt', 100))

                if d60_data is not None and not d60_data.empty:
                    d60_row = d60_data[d60_data['mksc_shrn_iscd'] == stock_code]
                    if not d60_row.empty:
                        d60_val = float(d60_row.iloc[0].get('d60_dsrt', 100))

                # 🎯 다중 이격도 기반 매수 검증 로직
                if all(val is not None for val in [d5_val, d20_val, d60_val]):
                    # 전략별 차별화된 검증
                    strategy = signal.get('strategy', 'default')

                    if strategy == 'disparity_reversal':
                        # 이격도 반등 전략: 과매도 구간에서만 매수
                        if d20_val <= 90 and d60_val <= 95:
                            logger.info(f"🎯 이격도반등 매수 허용: {stock_code} "
                                      f"D5:{d5_val:.1f} D20:{d20_val:.1f} D60:{d60_val:.1f}")
                            return True
                        else:
                            # 🆕 이격도 반등 전략 실패 로깅
                            log_signal_failed(
                                stock_code=stock_code,
                                strategy=strategy,
                                signal_strength=signal.get('strength', 0.0),
                                threshold=90.0,  # D20 임계값
                                reason=f"이격도반등 조건 미달 (D20:{d20_val:.1f} > 90, D60:{d60_val:.1f} > 95)",
                                market_data={**signal, 'disparity_5d': d5_val, 'disparity_20d': d20_val, 'disparity_60d': d60_val}
                            )
                            logger.warning(f"🎯 이격도반등 매수 거부: {stock_code} "
                                         f"D5:{d5_val:.1f} D20:{d20_val:.1f} D60:{d60_val:.1f} (과매도 미달)")
                            return False

                    elif strategy in ['gap_trading', 'volume_breakout', 'momentum']:
                        # 기존 전략들: 과매수 구간 매수 금지
                        if d5_val >= 135 or d20_val >= 125:  # 단기/중기 과매수 (1단계 완화)
                            # 🆕 과매수 구간 매수 거부 로깅
                            log_signal_failed(
                                stock_code=stock_code,
                                strategy=strategy,
                                signal_strength=signal.get('strength', 0.0),
                                threshold=125.0,  # D20 임계값
                                reason=f"과매수 구간 매수 금지 (D5:{d5_val:.1f} >= 135 또는 D20:{d20_val:.1f} >= 125)",
                                market_data={**signal, 'disparity_5d': d5_val, 'disparity_20d': d20_val, 'disparity_60d': d60_val}
                            )
                            logger.warning(f"🎯 {strategy} 매수 거부: {stock_code} "
                                         f"D5:{d5_val:.1f} D20:{d20_val:.1f} D60:{d60_val:.1f} (과매수)")
                            return False
                        elif d20_val <= 90:  # 중기 과매도 구간 = 매수 우대
                            logger.info(f"🎯 {strategy} 매수 우대: {stock_code} "
                                       f"D5:{d5_val:.1f} D20:{d20_val:.1f} D60:{d60_val:.1f} (과매도)")
                            return True
                        else:  # 중립 구간
                            logger.debug(f"🎯 {strategy} 매수 중립: {stock_code} "
                                        f"D5:{d5_val:.1f} D20:{d20_val:.1f} D60:{d60_val:.1f}")
                            return True

                    else:
                        # 기타 전략: 기본 검증
                        if d20_val >= 125:  # 과매수 매수 금지 (1단계 완화)
                            # 🆕 기타 전략 과매수 거부 로깅
                            log_signal_failed(
                                stock_code=stock_code,
                                strategy=strategy,
                                signal_strength=signal.get('strength', 0.0),
                                threshold=125.0,
                                reason=f"기타전략 과매수 구간 (D20:{d20_val:.1f} >= 125)",
                                market_data={**signal, 'disparity_20d': d20_val}
                            )
                            logger.warning(f"🎯 기타전략 매수 거부: {stock_code} "
                                         f"D20:{d20_val:.1f} (과매수)")
                            return False
                        else:
                            return True

                elif d20_val is not None:
                    # 20일 이격도만 확인 가능한 경우 (기존 로직)
                    if d20_val <= 90:
                        logger.info(f"🎯 20일 이격도 매수 허용: {stock_code} D20:{d20_val:.1f}% (과매도)")
                        return True
                    elif d20_val >= 125:  # 1단계 완화
                        # 🆕 20일 이격도만으로 과매수 거부 로깅
                        log_signal_failed(
                            stock_code=stock_code,
                            strategy=signal.get('strategy', 'unknown'),
                            signal_strength=signal.get('strength', 0.0),
                            threshold=125.0,
                            reason=f"20일 이격도 과매수 (D20:{d20_val:.1f} >= 125)",
                            market_data={**signal, 'disparity_20d': d20_val}
                        )
                        logger.warning(f"🎯 20일 이격도 매수 거부: {stock_code} D20:{d20_val:.1f}% (과매수)")
                        return False
                    else:
                        logger.debug(f"🎯 20일 이격도 중립: {stock_code} D20:{d20_val:.1f}%")
                        return True

            except Exception as e:
                logger.warning(f"🚫 다중 이격도 확인 실패 ({stock_code}): {e}")
                logger.info(f"🎯 이격도 확인 실패로 기본 검증 결과 사용 ({stock_code})")
                # 이격도 확인 실패시 기본 검증 결과 사용
                pass

            logger.debug(f"✅ 강화된 매수 검증 통과: {stock_code} (이격도 조건 만족 또는 확인 불가)")
            return True  # 기본 검증 통과시 매수 허용

        except Exception as e:
            logger.error(f"🚫 강화된 매수 신호 검증 오류 ({stock_code}): {e}")
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
