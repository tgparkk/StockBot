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
        """진입 기회 평가 및 매수 실행 (기존 _evaluate_entry_opportunities 로직)"""
        try:
            # 🎯 매수 준비 상태인 종목들만 필터링
            buy_ready_candidates = [
                candidate for candidate in self.manager.stock_manager._all_stocks.values()
                if candidate.status == CandleStatus.BUY_READY and candidate.is_ready_for_entry()
            ]

            if not buy_ready_candidates:
                return

            logger.info(f"🎯 매수 기회 평가: {len(buy_ready_candidates)}개 후보")

            # 💰 현재 계좌 정보 조회 (투자금액 계산용)
            account_info = await self._get_account_info()
            if not account_info:
                logger.warning("⚠️ 계좌 정보 조회 실패 - 매수 스킵")
                return

            # 🎯 5개씩 배치로 나누어 병렬 처리 (API 부하 방지)
            batch_size = 5
            valid_candidates = []

            for i in range(0, len(buy_ready_candidates), batch_size):
                batch = buy_ready_candidates[i:i + batch_size]

                # 진입 조건 재검증
                batch_results = await self._process_entry_validation_batch(batch)
                valid_candidates.extend(batch_results)

                # 배치 간 짧은 대기
                if i + batch_size < len(buy_ready_candidates):
                    await asyncio.sleep(0.3)

            if not valid_candidates:
                logger.info("📊 진입 조건을 만족하는 종목이 없습니다")
                return

            # 💰 투자금액 계산
            available_funds = self._calculate_available_funds(account_info)
            logger.info(f"💰 가용 투자 자금: {available_funds:,.0f}원")

            if available_funds < self.manager.config['investment_calculation']['min_investment']:
                logger.warning(f"⚠️ 투자 가능 자금 부족: {available_funds:,.0f}원")
                return

            # 🎯 우선순위 정렬 및 포지션 제한 체크
            valid_candidates.sort(key=lambda x: x.entry_priority, reverse=True)

            # 현재 포지션 수 체크
            current_positions = len([c for c in self.manager.stock_manager._all_stocks.values() if c.status == CandleStatus.ENTERED])
            max_positions = self.manager.config['max_positions']

            if current_positions >= max_positions:
                logger.info(f"📊 최대 포지션 수 도달: {current_positions}/{max_positions}")
                return

            # 🎯 실제 매수 실행
            executed_count = 0
            remaining_positions = max_positions - current_positions

            for candidate in valid_candidates[:remaining_positions]:
                try:
                    # 개별 투자금액 계산
                    investment_amount = self._calculate_entry_params(candidate, available_funds, executed_count + 1)

                    if investment_amount < self.manager.config['investment_calculation']['min_investment']:
                        logger.info(f"⚠️ {candidate.stock_code} 투자금액 부족: {investment_amount:,.0f}원")
                        continue

                    # 매수 실행
                    success = await self._execute_entry(candidate, investment_amount)

                    if success:
                        executed_count += 1
                        available_funds -= investment_amount

                        # 🆕 캔들 전략 매수 정보 DB 저장
                        await self._save_candle_position_to_db(candidate, investment_amount)

                        logger.info(f"✅ {candidate.stock_code} 매수 완료 ({executed_count}번째)")

                        # 연속 매수 간 간격
                        await asyncio.sleep(1.0)
                    else:
                        logger.warning(f"❌ {candidate.stock_code} 매수 실패")

                except Exception as e:
                    logger.error(f"매수 실행 오류 ({candidate.stock_code}): {e}")
                    continue

            if executed_count > 0:
                logger.info(f"🎯 매수 실행 완료: {executed_count}개 종목")
            else:
                logger.info("📊 매수 실행된 종목이 없습니다")

        except Exception as e:
            logger.error(f"진입 기회 평가 오류: {e}")

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

    async def _process_entry_validation_batch(self, candidates: List[CandleTradeCandidate]) -> List[CandleTradeCandidate]:
        """진입 조건 검증 배치 처리"""
        try:
            valid_candidates = []

            # 각 후보별 검증
            validation_tasks = [
                self._validate_single_entry_candidate(candidate) for candidate in candidates
            ]

            # 병렬 실행
            validation_results = await asyncio.gather(*validation_tasks, return_exceptions=True)

            # 결과 필터링
            for candidate, is_valid in zip(candidates, validation_results):
                if isinstance(is_valid, Exception):
                    logger.debug(f"검증 오류 ({candidate.stock_code}): {is_valid}")
                    continue

                if is_valid:
                    valid_candidates.append(candidate)

            return valid_candidates

        except Exception as e:
            logger.error(f"배치 검증 오류: {e}")
            return []

    async def _validate_single_entry_candidate(self, candidate: CandleTradeCandidate) -> bool:
        """개별 후보 진입 조건 재검증"""
        try:
            # 1. 최신 가격 정보 조회
            from ..api.kis_market_api import get_inquire_price
            current_data = get_inquire_price("J", candidate.stock_code)

            if current_data is None or current_data.empty:
                return False

            current_price = float(current_data.iloc[0].get('stck_prpr', 0))
            if current_price <= 0:
                return False

            # 2. 가격 업데이트
            candidate.update_price(current_price)

            # 3. 기본 필터 재검증
            if not self.manager._passes_basic_filters(current_price, current_data.iloc[0].to_dict()):
                return False

            # 4. 강한 매수 신호 재확인
            if candidate.trade_signal not in [TradeSignal.STRONG_BUY, TradeSignal.BUY]:
                return False

            # 5. 최종 진입 조건 체크
            entry_conditions = await self.manager._check_entry_conditions(candidate, current_data.iloc[0].to_dict())

            return entry_conditions.overall_passed

        except Exception as e:
            logger.debug(f"개별 검증 오류 ({candidate.stock_code}): {e}")
            return False

    async def _execute_entry(self, candidate: CandleTradeCandidate, investment_amount: float) -> bool:
        """매수 실행"""
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
                    logger.info(f"✅ 실제 매수 주문 성공: {candidate.stock_code} {quantity}주 {current_price:,.0f}원 "
                               f"(주문번호: {result.order_no})")
                except Exception as e:
                    logger.error(f"❌ 매수 주문 실행 오류: {candidate.stock_code} - {e}")
                    return False
            else:
                # TradeExecutor가 없는 경우 로그만 출력 (테스트 모드)
                logger.info(f"📈 매수 주문 (테스트): {candidate.stock_code} {quantity}주 {current_price:,.0f}원")

            # 포지션 진입 기록
            candidate.enter_position(current_price, quantity)

            # 🆕 _all_stocks 상태 업데이트 (BUY_READY → ENTERED)
            candidate.status = CandleStatus.ENTERED
            if candidate.stock_code in self.manager.stock_manager._all_stocks:
                self.manager.stock_manager._all_stocks[candidate.stock_code] = candidate
                logger.debug(f"🔄 {candidate.stock_code} stock_manager._all_stocks 상태 업데이트: → ENTERED")

            # 일일 통계 업데이트
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
