"""
캔들 전략 매매 실행 전용 모듈
"""
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any
from utils.logger import setup_logger

from .candle_trade_candidate import CandleTradeCandidate, CandleStatus
from .candle_risk_manager import CandleRiskManager

logger = setup_logger(__name__)


class CandleExecutionManager:
    """캔들 전략 매매 실행 전용 클래스"""

    def __init__(self, trade_executor, trade_db, config: Dict[str, Any]):
        self.trade_executor = trade_executor
        self.trade_db = trade_db
        self.config = config
        self.korea_tz = timezone(timedelta(hours=9))

        # 실행 관련 설정
        self.execution_config = {
            'trading_start_time': config.get('trading_start_time', '09:00'),
            'trading_end_time': config.get('trading_end_time', '15:20'),
            'investment_calculation': config.get('investment_calculation', {}),
            'min_investment': config.get('min_investment', 100_000)
        }

        logger.info("✅ CandleExecutionManager 초기화 완료")

    # ==========================================
    # 🎯 메인 실행 메서드
    # ==========================================

    async def execute_entry(self, candidate: CandleTradeCandidate, entry_params: Dict) -> bool:
        """실제 매수 진입 실행"""
        try:
            # 매개변수 추출
            current_price = entry_params['current_price']
            position_amount = entry_params['position_amount']
            quantity = entry_params['quantity']
            signal = entry_params['signal']

            # 거래 시간 체크
            if not self._is_trading_time():
                logger.warning(f"⏰ {candidate.stock_code} 거래 시간 외 매수 차단")
                return False

            # 실제 매수 주문 실행
            if self.trade_executor:
                try:
                    result = self.trade_executor.execute_buy_signal(signal)
                    if not result.success:
                        logger.error(f"❌ 매수 주문 실패: {candidate.stock_code} - {result.error_message}")
                        return False
                    logger.info(f"✅ 실제 매수 주문 성공: {candidate.stock_code} (주문번호: {result.order_no})")
                except Exception as e:
                    logger.error(f"❌ 매수 주문 실행 오류: {candidate.stock_code} - {e}")
                    return False
            else:
                # 테스트 모드
                logger.info(f"📈 매수 주문 (테스트): {candidate.stock_code} {quantity}주 {current_price:,.0f}원")

            # 포지션 진입 기록
            quantity_int = int(quantity) if isinstance(quantity, float) else quantity
            candidate.enter_position(current_price, quantity_int)
            candidate.risk_management.position_amount = position_amount

            # 캔들 전략 매수 정보 DB 저장
            try:
                if self.trade_db:
                    candle_strategy_data = self._prepare_candle_strategy_data(
                        candidate, current_price, quantity_int, position_amount
                    )
                    saved_id = await self._save_candle_position_to_db(candle_strategy_data)
                    if saved_id:
                        candidate.metadata['db_position_id'] = saved_id
                        logger.info(f"📚 {candidate.stock_code} 캔들 전략 정보 DB 저장 완료 (ID: {saved_id})")
            except Exception as db_error:
                logger.warning(f"⚠️ {candidate.stock_code} 캔들 전략 DB 저장 오류: {db_error}")

            return True

        except Exception as e:
            logger.error(f"❌ 매수 진입 실행 오류 ({candidate.stock_code}): {e}")
            return False

    async def execute_exit(self, position: CandleTradeCandidate, exit_price: float, reason: str) -> bool:
        """매도 청산 실행"""
        try:
            # 거래 시간 재확인
            if not self._is_trading_time():
                logger.warning(f"⏰ {position.stock_code} 거래 시간 외 매도 차단 - {reason}")
                return False

            # 실제 보유 여부 확인
            actual_quantity = await self._verify_actual_holding(position)
            if actual_quantity <= 0:
                logger.warning(f"⚠️ {position.stock_code} 실제 보유하지 않는 종목 - 매도 취소")
                return False

            # 안전한 매도가 계산
            risk_manager = CandleRiskManager(self.config)
            safe_sell_price = risk_manager.calculate_safe_sell_price(exit_price, reason)

            # 매도 신호 생성
            signal = {
                'stock_code': position.stock_code,
                'action': 'sell',
                'strategy': 'candle_pattern',
                'price': safe_sell_price,
                'quantity': actual_quantity,
                'total_amount': int(safe_sell_price * actual_quantity),
                'reason': reason,
                'pattern_type': str(position.detected_patterns[0].pattern_type) if position.detected_patterns else 'unknown',
                'pre_validated': True
            }

            # 실제 매도 주문 실행
            if self.trade_executor:
                try:
                    result = self.trade_executor.execute_sell_signal(signal)
                    if not result.success:
                        logger.error(f"❌ 매도 주문 실패: {position.stock_code} - {result.error_message}")
                        return False
                    logger.info(f"✅ 실제 매도 주문 성공: {position.stock_code} (주문번호: {result.order_no})")
                except Exception as e:
                    logger.error(f"❌ 매도 주문 실행 오류: {position.stock_code} - {e}")
                    return False
            else:
                # 테스트 모드
                logger.info(f"📉 매도 주문 (테스트): {position.stock_code} {actual_quantity}주")

            # 포지션 청산 기록
            position.exit_position(exit_price, reason)

            # 캔들 전략 매도 정보 DB 업데이트
            try:
                if self.trade_db and 'db_position_id' in position.metadata:
                    exit_data = {
                        'exit_price': exit_price,
                        'exit_reason': reason,
                        'realized_pnl': position.performance.realized_pnl or 0.0,
                        'exit_time': datetime.now().isoformat(),
                        'holding_duration': str(datetime.now() - position.performance.entry_time) if position.performance.entry_time else None
                    }
                    logger.info(f"📚 {position.stock_code} 캔들 전략 매도 기록 업데이트 대상")
                    logger.debug(f"매도 정보: {exit_data}")
            except Exception as db_error:
                logger.warning(f"⚠️ {position.stock_code} 캔들 전략 매도 DB 업데이트 오류: {db_error}")

            return True

        except Exception as e:
            logger.error(f"매도 청산 실행 오류 ({position.stock_code}): {e}")
            return False

    async def calculate_investment_amount(self, candidate: CandleTradeCandidate) -> Tuple[int, int, str]:
        """투자 금액 및 수량 계산"""
        try:
            # 최신 가격 확인
            from ..api.kis_market_api import get_inquire_price
            current_data = get_inquire_price("J", candidate.stock_code)

            if current_data is None or current_data.empty:
                return 0, 0, "가격조회실패"

            current_price = float(current_data.iloc[0].get('stck_prpr', 0))
            if current_price <= 0:
                return 0, 0, "가격정보없음"

            candidate.update_price(current_price)

            # 투자 가능 금액 계산
            try:
                from ..api.kis_market_api import get_account_balance
                balance_info = get_account_balance()
                inv_config = self.execution_config['investment_calculation']

                if balance_info and balance_info.get('total_value', 0) > 0:
                    total_evaluation = balance_info.get('total_value', 0)

                    # 실제 매수가능금액 조회
                    try:
                        if self.trade_executor and hasattr(self.trade_executor, 'trading_manager'):
                            trading_balance = self.trade_executor.trading_manager.get_balance()
                            if trading_balance.get('success'):
                                actual_cash = trading_balance.get('available_cash', 0)
                                calculation_method = "실제매수가능금액"
                            else:
                                actual_cash = 0
                                calculation_method = "매수가능금액조회실패"
                        else:
                            actual_cash = 0
                            calculation_method = "TradeExecutor없음"
                    except Exception as e:
                        logger.warning(f"⚠️ 매수가능금액 조회 오류: {e}")
                        actual_cash = 0
                        calculation_method = "오류시백업"

                    # 투자 가능 금액 결정
                    cash_based_amount = int(actual_cash * inv_config.get('cash_usage_ratio', 0.8)) if actual_cash > 0 else 0
                    portfolio_based_amount = int(total_evaluation * inv_config.get('portfolio_usage_ratio', 0.2))

                    if cash_based_amount >= inv_config.get('min_cash_threshold', 500_000):
                        total_available = cash_based_amount
                        calculation_method += " → 현금잔고기준"
                    else:
                        total_available = min(portfolio_based_amount, inv_config.get('max_portfolio_limit', 3_000_000))
                        calculation_method += " → 총평가액기준"
                else:
                    total_available = inv_config.get('default_investment', 1_000_000)
                    calculation_method = "기본값"

            except Exception as e:
                logger.warning(f"📊 잔고 조회 오류 - 기본 투자 금액 사용: {e}")
                inv_config = self.execution_config['investment_calculation']
                total_available = inv_config.get('default_investment', 1_000_000)
                calculation_method = "오류시기본값"

            # 포지션별 실제 투자 금액 계산
            position_amount = int(total_available * candidate.risk_management.position_size_pct / 100)

            # 매수 수량 계산
            quantity = position_amount // current_price

            # 최소 투자 조건 체크
            min_investment = self.execution_config.get('min_investment', 100_000)
            if quantity <= 0 or position_amount < min_investment:
                logger.warning(f"❌ {candidate.stock_code}: 투자조건 미달")
                return 0, 0, f"투자조건미달(최소:{min_investment:,}원)"

            return position_amount, quantity, calculation_method

        except Exception as e:
            logger.error(f"❌ 투자 금액 계산 오류: {e}")
            return 0, 0, "계산오류"

    # ==========================================
    # 🔍 검증 및 조회 메서드
    # ==========================================

    async def verify_actual_holding(self, position: CandleTradeCandidate) -> int:
        """실제 보유 여부 및 수량 확인"""
        try:
            from ..api.kis_market_api import get_account_balance
            account_info = get_account_balance()

            if account_info and 'stocks' in account_info:
                for stock in account_info['stocks']:
                    if stock.get('stock_code') == position.stock_code:
                        actual_quantity = stock.get('quantity', 0)
                        if actual_quantity > 0:
                            # 시스템 수량과 실제 수량 비교
                            system_quantity = position.performance.entry_quantity or 0
                            final_quantity = min(system_quantity, actual_quantity)

                            logger.info(f"✅ {position.stock_code} 보유 확인: "
                                       f"시스템{system_quantity}주 → 실제{actual_quantity}주 → 매도{final_quantity}주")
                            return final_quantity
                        else:
                            logger.warning(f"⚠️ {position.stock_code} 실제 보유 수량 없음")
                            return 0

                logger.warning(f"⚠️ {position.stock_code} 보유 종목 목록에 없음")
                return 0
            else:
                logger.warning(f"⚠️ {position.stock_code} 계좌 정보 조회 실패")
                # 백업: 시스템 수량 사용
                return position.performance.entry_quantity or 0

        except Exception as e:
            logger.warning(f"⚠️ {position.stock_code} 보유 확인 오류: {e}")
            return position.performance.entry_quantity or 0

    def is_trading_time(self) -> bool:
        """거래 시간 체크 (공개 메서드)"""
        return self._is_trading_time()

    def get_account_status(self) -> Dict[str, Any]:
        """계좌 상태 조회"""
        try:
            from ..api.kis_market_api import get_account_balance
            balance_info = get_account_balance()

            if not balance_info:
                return {'success': False, 'error': '계좌 정보 조회 실패'}

            # 매수 가능 금액 조회
            available_cash = 0
            if self.trade_executor and hasattr(self.trade_executor, 'trading_manager'):
                try:
                    trading_balance = self.trade_executor.trading_manager.get_balance()
                    if trading_balance.get('success'):
                        available_cash = trading_balance.get('available_cash', 0)
                except Exception as e:
                    logger.debug(f"매수 가능 금액 조회 오류: {e}")

            return {
                'success': True,
                'total_value': balance_info.get('total_value', 0),
                'cash_balance': balance_info.get('cash_balance', 0),
                'available_cash': available_cash,
                'stock_value': balance_info.get('stock_value', 0),
                'profit_loss': balance_info.get('profit_loss', 0),
                'profit_loss_rate': balance_info.get('profit_loss_rate', 0.0),
                'total_stocks': balance_info.get('total_stocks', 0),
                'holdings': balance_info.get('stocks', [])
            }

        except Exception as e:
            logger.error(f"계좌 상태 조회 오류: {e}")
            return {'success': False, 'error': str(e)}

    # ==========================================
    # 🔧 내부 유틸리티 메서드
    # ==========================================

    def _is_trading_time(self) -> bool:
        """거래 시간 체크"""
        try:
            current_time = datetime.now().time()
            trading_start = datetime.strptime(self.execution_config['trading_start_time'], '%H:%M').time()
            trading_end = datetime.strptime(self.execution_config['trading_end_time'], '%H:%M').time()
            return trading_start <= current_time <= trading_end
        except Exception as e:
            logger.error(f"거래 시간 체크 오류: {e}")
            return False

    def _prepare_candle_strategy_data(self, candidate: CandleTradeCandidate,
                                    current_price: float, quantity: int, position_amount: int) -> Dict:
        """캔들 전략 데이터 준비"""
        try:
            return {
                'stock_code': candidate.stock_code,
                'stock_name': candidate.stock_name,
                'entry_price': current_price,
                'entry_quantity': quantity,
                'position_amount': position_amount,
                'target_price': candidate.risk_management.target_price,
                'stop_loss_price': candidate.risk_management.stop_loss_price,
                'trailing_stop_pct': candidate.risk_management.trailing_stop_pct,
                'max_holding_hours': candidate.risk_management.max_holding_hours,
                'pattern_type': str(candidate.detected_patterns[0].pattern_type) if candidate.detected_patterns else 'unknown',
                'pattern_strength': candidate.detected_patterns[0].strength if candidate.detected_patterns else 0,
                'pattern_confidence': candidate.detected_patterns[0].confidence if candidate.detected_patterns else 0.0,
                'signal_strength': candidate.signal_strength,
                'entry_priority': candidate.entry_priority,
                'strategy_config': {
                    'position_size_pct': candidate.risk_management.position_size_pct,
                    'risk_score': candidate.risk_management.risk_score,
                    'pattern_description': candidate.detected_patterns[0].description if candidate.detected_patterns else '',
                    'market_type': candidate.market_type,
                    'created_at': candidate.created_at.isoformat() if candidate.created_at else None
                }
            }
        except Exception as e:
            logger.error(f"캔들 전략 데이터 준비 오류: {e}")
            return {}

    async def _save_candle_position_to_db(self, strategy_data: Dict) -> Optional[int]:
        """캔들 전략 포지션 정보를 DB에 저장"""
        try:
            if not self.trade_db:
                return None

            # 기존 DB 함수 활용
            position_id = self.trade_db.record_candle_candidate(
                stock_code=strategy_data['stock_code'],
                stock_name=strategy_data['stock_name'],
                current_price=strategy_data['entry_price'],
                pattern_type=strategy_data['pattern_type'],
                pattern_strength=strategy_data['pattern_strength'],
                signal_strength='HIGH' if strategy_data['signal_strength'] >= 80 else 'MEDIUM',
                entry_reason=f"캔들전략매수:{strategy_data['pattern_type']}",
                risk_score=strategy_data['strategy_config'].get('risk_score', 50),
                target_price=strategy_data['target_price'],
                stop_loss_price=strategy_data['stop_loss_price']
            )

            return position_id

        except Exception as e:
            logger.error(f"캔들 포지션 DB 저장 오류: {e}")
            return None

    # ==========================================
    # 🔧 매수/매도 조건 검증
    # ==========================================

    def validate_buy_conditions(self, candidate: CandleTradeCandidate) -> Tuple[bool, List[str]]:
        """매수 조건 종합 검증"""
        try:
            issues = []

            # 거래 시간 체크
            if not self._is_trading_time():
                issues.append("거래 시간 외")

            # 가격 정보 체크
            if candidate.current_price <= 0:
                issues.append("가격 정보 없음")

            # 리스크 관리 설정 체크
            if not candidate.risk_management:
                issues.append("리스크 관리 설정 없음")

            # 패턴 정보 체크
            if not candidate.detected_patterns:
                issues.append("패턴 정보 없음")

            # 신호 강도 체크
            if candidate.signal_strength < 60:
                issues.append(f"신호 강도 부족 ({candidate.signal_strength})")

            is_valid = len(issues) == 0
            return is_valid, issues

        except Exception as e:
            logger.error(f"매수 조건 검증 오류: {e}")
            return False, [f"검증 오류: {e}"]

    def validate_sell_conditions(self, position: CandleTradeCandidate, reason: str) -> Tuple[bool, List[str]]:
        """매도 조건 종합 검증"""
        try:
            issues = []

            # 거래 시간 체크
            if not self._is_trading_time():
                issues.append("거래 시간 외")

            # 진입 상태 체크
            if position.status != CandleStatus.ENTERED:
                issues.append(f"진입 상태 아님 ({position.status})")

            # 진입 정보 체크
            if not position.performance.entry_price:
                issues.append("진입 정보 없음")

            # 수량 체크
            if not position.performance.entry_quantity or position.performance.entry_quantity <= 0:
                issues.append("매도할 수량 없음")

            is_valid = len(issues) == 0
            return is_valid, issues

        except Exception as e:
            logger.error(f"매도 조건 검증 오류: {e}")
            return False, [f"검증 오류: {e}"]
