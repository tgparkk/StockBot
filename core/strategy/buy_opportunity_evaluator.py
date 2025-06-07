"""
매수 기회 평가자
캔들 기반 매매 전략의 진입 기회 평가 및 매수 실행을 담당
"""
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any, TYPE_CHECKING
from utils.logger import setup_logger

if TYPE_CHECKING:
    from .candle_trade_manager import CandleTradeManager

from .candle_trade_candidate import CandleTradeCandidate, CandleStatus, TradeSignal

logger = setup_logger(__name__)


class BuyOpportunityEvaluator:
    """매수 기회 평가 및 실행 관리자"""

    def __init__(self, candle_trade_manager: 'CandleTradeManager'):
        """
        Args:
            candle_trade_manager: CandleTradeManager 인스턴스
        """
        self.manager = candle_trade_manager
        logger.info("✅ BuyOpportunityEvaluator 초기화 완료")

    async def evaluate_entry_opportunities(self):
        """💰 매수 실행 전용 - 모든 분석은 _periodic_signal_evaluation에서 완료됨"""
        try:
            # 🎯 매수 준비 상태인 종목들만 필터링 (이미 모든 검증 완료됨)
            buy_ready_candidates = [
                candidate for candidate in self.manager.stock_manager._all_stocks.values()
                if candidate.status == CandleStatus.BUY_READY and candidate.is_ready_for_entry()
            ]

            if not buy_ready_candidates:
                logger.debug("📊 매수 준비된 종목이 없습니다")
                return

            logger.info(f"💰 매수 실행: {len(buy_ready_candidates)}개 준비된 종목")

            # 🚀 개별 종목별로 순차 매수 실행 (잔액 실시간 반영)
            successful_orders = 0
            for candidate in buy_ready_candidates:
                try:
                    # 💰 매수 직전 최신 계좌 정보 조회 (잔액 실시간 반영)
                    account_info = await self._get_account_info()
                    if not account_info:
                        logger.warning(f"⚠️ {candidate.stock_code} 계좌 정보 조회 실패 - 매수 스킵")
                        continue

                    # 💰 현재 가용 투자 자금 계산 (매수마다 업데이트)
                    available_funds = self._calculate_available_funds(account_info)
                    logger.info(f"💰 {candidate.stock_code} 가용 투자 자금: {available_funds:,.0f}원")

                    if available_funds <= 0:
                        logger.warning(f"⚠️ {candidate.stock_code} 가용 자금 부족 - 매수 중단")
                        break  # 자금 부족시 추가 매수 중단

                    # 🎯 개별 종목 투자금액 계산
                    current_positions = len([c for c in self.manager.stock_manager._all_stocks.values()
                                           if c.status in [CandleStatus.ENTERED, CandleStatus.PENDING_ORDER]])
                    investment_amount = self._calculate_entry_params(candidate, available_funds, current_positions)

                    if investment_amount < self.manager.config['investment_calculation']['min_investment']:
                        logger.warning(f"⚠️ {candidate.stock_code} 투자금액 부족: {investment_amount:,.0f}원")
                        continue

                    # 📈 매수 주문 실행
                    success = await self._execute_entry(candidate, investment_amount)
                    if success:
                        successful_orders += 1

                        # 🔧 매수 주문 성공시 PENDING_ORDER 상태로 변경
                        candidate.status = CandleStatus.PENDING_ORDER
                        self.manager.stock_manager.update_candidate(candidate)

                        logger.info(f"✅ {candidate.stock_code} 매수 주문 성공 - PENDING_ORDER 상태 전환")
                    else:
                        # 🔧 매수 주문 실패시 원래 상태 유지 (BUY_READY)
                        logger.warning(f"❌ {candidate.stock_code} 매수 주문 실패 - BUY_READY 상태 유지")

                    # 주문 간 간격 (API 부하 방지)
                    await asyncio.sleep(0.5)

                except Exception as e:
                    logger.error(f"❌ {candidate.stock_code} 매수 처리 오류: {e}")
                    continue

            if successful_orders > 0:
                logger.info(f"🎯 총 {successful_orders}개 종목 매수 주문 완료")
            else:
                logger.debug("📊 매수 실행된 종목이 없습니다")

        except Exception as e:
            logger.error(f"❌ 매수 기회 평가 오류: {e}")

    async def evaluate_watching_stocks_for_entry(self, watching_candidates: List[CandleTradeCandidate]) -> int:
        """🔍 관찰 중인 종목들의 진입 가능성 평가 및 BUY_READY 전환 (candle_trade_manager에서 이관)"""
        try:
            converted_count = 0

            for candidate in watching_candidates:
                try:
                    # 강한 매수 신호인 경우에만 세부 검증 실행
                    if candidate.trade_signal in [TradeSignal.STRONG_BUY, TradeSignal.BUY]:

                        # 세부 진입 조건 검증 수행
                        entry_validation_passed = await self._validate_detailed_entry_conditions(candidate)

                        if entry_validation_passed:
                            # BUY_READY 상태로 전환
                            old_status = candidate.status
                            candidate.status = CandleStatus.BUY_READY
                            candidate.metadata['buy_ready_time'] = datetime.now(self.manager.korea_tz).isoformat()

                            logger.info(f"🎯 {candidate.stock_code} 매수 준비 완료: "
                                       f"{old_status.value} → {candidate.status.value} "
                                       f"(신호:{candidate.trade_signal.value}, 강도:{candidate.signal_strength})")
                            converted_count += 1
                        else:
                            logger.debug(f"📋 {candidate.stock_code} 세부 진입 조건 미충족 - WATCHING 유지")

                except Exception as e:
                    logger.debug(f"관찰 종목 진입 평가 오류 ({candidate.stock_code}): {e}")
                    continue

            return converted_count

        except Exception as e:
            logger.error(f"관찰 종목 진입 평가 오류: {e}")
            return 0

    async def _validate_detailed_entry_conditions(self, candidate: CandleTradeCandidate) -> bool:
        """🔍 세부 진입 조건 검증 (candle_trade_manager에서 이관)"""
        try:
            # 1. 최신 가격 정보 조회 (이미 comprehensive_signal_analysis에서 수행했지만 최신성 확보)
            from ..api.kis_market_api import get_inquire_price
            current_data = get_inquire_price("J", candidate.stock_code)

            if current_data is None or current_data.empty:
                logger.debug(f"❌ {candidate.stock_code} 가격 정보 조회 실패")
                return False

            current_price = float(current_data.iloc[0].get('stck_prpr', 0))
            if current_price <= 0:
                logger.debug(f"❌ {candidate.stock_code} 유효하지 않은 가격: {current_price}")
                return False

            # 가격 업데이트
            candidate.update_price(current_price)
            stock_info_dict = current_data.iloc[0].to_dict()

            # 2. 🔍 기본 필터 체크
            if not self.manager._passes_basic_filters(current_price, stock_info_dict):
                logger.debug(f"❌ {candidate.stock_code} 기본 필터 미통과")
                return False

            # 3. 🔍 상세 진입 조건 체크
            entry_conditions = await self.manager._check_entry_conditions(candidate, stock_info_dict)

            if not entry_conditions.overall_passed:
                logger.debug(f"❌ {candidate.stock_code} 상세 진입 조건 미통과: {', '.join(entry_conditions.fail_reasons)}")
                return False

            # 4. 🔍 추가 안전성 검증
            safety_check = self._perform_additional_safety_checks(candidate, current_price, stock_info_dict)

            if not safety_check:
                logger.debug(f"❌ {candidate.stock_code} 안전성 검증 미통과")
                return False

            logger.info(f"✅ {candidate.stock_code} 세부 진입 조건 모두 통과 - 매수 준비 완료")
            return True

        except Exception as e:
            logger.debug(f"세부 진입 조건 검증 오류 ({candidate.stock_code}): {e}")
            return False

    def _perform_additional_safety_checks(self, candidate: CandleTradeCandidate, current_price: float, stock_info: Dict) -> bool:
        """🛡️ 추가 안전성 검증"""
        try:
            # 1. 급등/급락 상태 체크
            day_change_pct = float(stock_info.get('prdy_ctrt', 0))
            if abs(day_change_pct) > 15.0:  # 15% 이상 급등락시 제외
                logger.debug(f"❌ {candidate.stock_code} 급등락 상태: {day_change_pct:.2f}%")
                return False

            # 2. 거래 정지 상태 체크
            trading_halt = stock_info.get('mrkt_warn_cls_code', '')
            if trading_halt and trading_halt not in ['00', '']:
                logger.debug(f"❌ {candidate.stock_code} 거래 제한 상태: {trading_halt}")
                return False

            # 3. 최근 신호 생성 시간 체크 (너무 오래된 신호 제외)
            if candidate.signal_updated_at:
                signal_age = datetime.now(self.manager.korea_tz) - candidate.signal_updated_at
                if signal_age.total_seconds() > 300:  # 5분 이상 된 신호
                    logger.debug(f"❌ {candidate.stock_code} 신호가 너무 오래됨: {signal_age}")
                    return False

            # 4. 패턴 신뢰도 재확인
            if candidate.detected_patterns:
                primary_pattern = candidate.detected_patterns[0]
                if primary_pattern.confidence < 0.6:  # 60% 미만 신뢰도
                    logger.debug(f"❌ {candidate.stock_code} 패턴 신뢰도 부족: {primary_pattern.confidence:.2f}")
                    return False

            return True

        except Exception as e:
            logger.debug(f"안전성 검증 오류 ({candidate.stock_code}): {e}")
            return False

    def _calculate_entry_params(self, candidate: CandleTradeCandidate, available_funds: float, position_count: int) -> float:
        """개별 종목 투자금액 계산"""
        try:
            # 기본 투자금액 계산
            investment_config = self.manager.config['investment_calculation']
            base_amount = investment_config['default_investment']

            # 우선순위에 따른 조정 (높은 우선순위일수록 더 많이)
            priority_multiplier = min(1.5, candidate.entry_priority / 100 + 0.5)
            adjusted_amount = base_amount * priority_multiplier

            # 포지션 분산을 위한 조정 (여러 종목에 분산)
            max_single_investment = available_funds * 0.4  # 가용 자금의 40% 이하
            adjusted_amount = min(adjusted_amount, max_single_investment)

            # 최소/최대 제한 적용
            min_investment = investment_config['min_investment']
            adjusted_amount = max(min_investment, adjusted_amount)

            logger.debug(f"💰 {candidate.stock_code} 투자금액: {adjusted_amount:,.0f}원 "
                        f"(우선순위:{candidate.entry_priority}, 배수:{priority_multiplier:.2f})")

            return adjusted_amount

        except Exception as e:
            logger.error(f"투자금액 계산 오류: {e}")
            return self.manager.config['investment_calculation']['default_investment']

    async def _get_account_info(self) -> Optional[Dict]:
        """계좌 정보 조회"""
        try:
            from ..api.kis_market_api import get_account_balance
            return get_account_balance()
        except Exception as e:
            logger.error(f"계좌 정보 조회 오류: {e}")
            return None

    def _calculate_available_funds(self, account_info: Dict) -> float:
        """🆕 가용 투자 자금 계산 (현금 + 평가액 기반)"""
        try:
            investment_config = self.manager.config['investment_calculation']

            # 현금 잔고
            cash_balance = float(account_info.get('cash_balance', 0))

            # 총 평가액
            total_evaluation = float(account_info.get('total_evaluation', 0))

            # 현금이 충분한 경우 현금 위주 사용
            if cash_balance >= investment_config['min_cash_threshold']:
                available_funds = cash_balance * investment_config['cash_usage_ratio']
                logger.debug(f"💰 현금 기반 투자: {available_funds:,.0f}원 (현금잔고: {cash_balance:,.0f}원)")
            else:
                # 현금이 부족한 경우 평가액 일부 활용
                portfolio_based = total_evaluation * investment_config['portfolio_usage_ratio']
                portfolio_based = min(portfolio_based, investment_config['max_portfolio_limit'])

                available_funds = cash_balance * 0.9 + portfolio_based  # 현금 90% + 평가액 일부
                logger.debug(f"💰 평가액 기반 투자: {available_funds:,.0f}원 "
                           f"(현금: {cash_balance:,.0f}원, 평가액활용: {portfolio_based:,.0f}원)")

            # 최소 투자금액 확보 여부 체크
            min_required = investment_config['min_investment']
            if available_funds < min_required:
                logger.warning(f"⚠️ 가용자금 부족: {available_funds:,.0f}원 < {min_required:,.0f}원")
                return 0

            return available_funds

        except Exception as e:
            logger.error(f"가용 자금 계산 오류: {e}")
            return self.manager.config['investment_calculation']['default_investment']

    async def _execute_entry(self, candidate: CandleTradeCandidate, investment_amount: float) -> bool:
        """매수 실행 - 주문만 하고 체결은 웹소켓에서 확인"""
        try:
            current_price = candidate.current_price
            quantity = int(investment_amount / current_price)

            if quantity <= 0:
                logger.warning(f"❌ {candidate.stock_code} 매수 수량 계산 오류 (수량: {quantity})")
                return False

            # 매수 신호 생성
            signal = {
                'stock_code': candidate.stock_code,
                'action': 'buy',
                'strategy': 'candle_pattern',
                'price': current_price,
                'quantity': quantity,
                'total_amount': int(current_price * quantity),
                'pattern_type': str(candidate.detected_patterns[0].pattern_type) if candidate.detected_patterns else 'unknown',
                'signal_strength': candidate.signal_strength,
                'entry_priority': candidate.entry_priority,
                'pre_validated': True  # 캔들 시스템에서 이미 검증 완료
            }

            # 실제 매수 주문 실행 (TradeExecutor 사용)
            if hasattr(self.manager, 'trade_executor') and self.manager.trade_executor:
                try:
                    result = self.manager.trade_executor.execute_buy_signal(signal)
                    if not result.success:
                        logger.error(f"❌ 매수 주문 실패: {candidate.stock_code} - {result.error_message}")
                        return False

                    # 🔧 수정: 새로운 주문 추적 시스템 사용
                    order_no = getattr(result, 'order_no', None)
                    candidate.set_pending_order(order_no or f"unknown_{datetime.now().strftime('%H%M%S')}", 'buy')

                    logger.info(f"📈 매수 주문 제출 성공: {candidate.stock_code} {quantity}주 {current_price:,.0f}원 "
                               f"(주문번호: {order_no})")

                    logger.debug(f"📋 {candidate.stock_code} 매수 주문 대기 중 - 체결은 웹소켓에서 확인")

                except Exception as e:
                    logger.error(f"❌ 매수 주문 실행 오류: {candidate.stock_code} - {e}")
                    return False
            else:
                # TradeExecutor가 없는 경우 로그만 출력 (테스트 모드)
                logger.info(f"📈 매수 주문 (테스트): {candidate.stock_code} {quantity}주 {current_price:,.0f}원")

                # 🔧 테스트 모드에서도 주문 추적
                test_order_no = f"test_buy_{candidate.stock_code}_{datetime.now().strftime('%H%M%S')}"
                candidate.set_pending_order(test_order_no, 'buy')

            # 일일 통계는 주문 제출 시점에 카운트 (나중에 체결 실패시 조정 가능)
            self.manager.daily_stats['trades_count'] += 1

            return True

        except Exception as e:
            logger.error(f"매수 실행 오류 ({candidate.stock_code}): {e}")
            return False

    async def _save_candle_position_to_db(self, candidate: CandleTradeCandidate, investment_amount: float):
        """🆕 캔들 전략 매수 정보를 데이터베이스에 저장"""
        try:
            if not self.manager.trade_db:
                logger.debug(f"📚 {candidate.stock_code} DB 없음 - 저장 스킵")
                return

            # 매수 정보 구성
            entry_data = {
                'stock_code': candidate.stock_code,
                'stock_name': candidate.stock_name,
                'strategy_type': 'candle_pattern',
                'entry_price': candidate.performance.entry_price,
                'entry_quantity': candidate.performance.entry_quantity,
                'investment_amount': investment_amount,
                'pattern_type': candidate.detected_patterns[0].pattern_type.value if candidate.detected_patterns else 'unknown',
                'pattern_strength': candidate.detected_patterns[0].strength if candidate.detected_patterns else 0,
                'signal_strength': candidate.signal_strength,
                'entry_priority': candidate.entry_priority,
                'target_price': candidate.risk_management.target_price,
                'stop_loss_price': candidate.risk_management.stop_loss_price,
                'max_holding_hours': candidate.risk_management.max_holding_hours,
                'entry_time': datetime.now().isoformat()
            }

            # 임시로 로그만 출력 (실제 DB 스키마에 따라 구현)
            logger.info(f"📚 {candidate.stock_code} 캔들 전략 매수 정보 DB 저장 대상")
            logger.debug(f"매수 정보: {entry_data}")

            # 🆕 메타데이터에 DB 저장 정보 기록
            candidate.metadata['db_saved'] = True
            candidate.metadata['original_entry_source'] = 'candle_strategy'
            candidate.metadata['entry_timestamp'] = entry_data['entry_time']

        except Exception as e:
            logger.warning(f"⚠️ {candidate.stock_code} 캔들 전략 매수 DB 저장 오류: {e}")
            # DB 저장 실패해도 거래는 계속 진행
