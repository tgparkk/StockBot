"""
매수 기회 평가자
캔들 기반 매매 전략의 진입 기회 평가 및 매수 실행을 담당
"""
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any, TYPE_CHECKING
from utils.logger import setup_logger
import time

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
            # 🔍 전체 종목 상태 분석
            all_stocks = self.manager.stock_manager._all_stocks.values()

            # 🎯 매수 준비 상태인 종목들만 필터링 (이미 모든 검증 완료됨) + 중복 주문 방지
            buy_ready_candidates = []
            for candidate in all_stocks:
                if not candidate.is_ready_for_entry():
                    continue

                # 🚨 PENDING_ORDER 상태 종목 제외
                if candidate.status == CandleStatus.PENDING_ORDER:
                    logger.debug(f"🚫 {candidate.stock_code} PENDING_ORDER 상태 - 매수 스킵")
                    continue

                # 🚨 매수 주문 대기 중인 종목 제외
                if candidate.has_pending_order('buy'):
                    logger.debug(f"🚫 {candidate.stock_code} 매수 주문 대기 중 - 매수 스킵")
                    continue

                # 🚨 최근에 매수 주문을 낸 종목 제외 (5분 내)
                recent_order_time = candidate.metadata.get('last_buy_order_time')
                if recent_order_time:
                    time_since_order = time.time() - recent_order_time
                    # 🔧 config에서 최소 주문 간격 가져오기
                    min_order_interval = self.manager.config.get('min_order_interval_seconds', 300)  # 기본 5분
                    if time_since_order < min_order_interval:
                        logger.debug(f"🚫 {candidate.stock_code} 최근 주문 후 {time_since_order:.0f}초 경과 - 매수 스킵")
                        continue

                buy_ready_candidates.append(candidate)

            if not buy_ready_candidates:
                logger.info("📊 매수 준비된 종목이 없습니다")

                # 🔍 BUY_READY 상태인데 is_ready_for_entry()가 False인 종목 체크
                buy_ready_status_only = [
                    candidate for candidate in all_stocks
                    if candidate.status == CandleStatus.BUY_READY
                ]

                return

            #logger.info(f"💰 매수 실행: {len(buy_ready_candidates)}개 준비된 종목")

            # 🚀 개별 종목별로 순차 매수 실행 (잔액 실시간 반영)
            successful_orders = 0
            for candidate in buy_ready_candidates:
                try:
                    # 💰 매수 직전 최신 계좌 정보 조회 (잔액 실시간 반영)
                    #logger.info(f"🔍 {candidate.stock_code} 계좌 정보 조회 시작...")
                    account_info = await self._get_account_info()
                    if not account_info:
                        logger.warning(f"⚠️ {candidate.stock_code} 계좌 정보 조회 실패 - 매수 스킵")
                        continue

                    # 💰 현재 가용 투자 자금 계산 (매수마다 업데이트)
                    available_funds = self._calculate_available_funds(account_info)
                    #logger.info(f"💰 {candidate.stock_code} 가용 투자 자금: {available_funds:,.0f}원")

                    if available_funds <= 0:
                        logger.warning(f"⚠️ {candidate.stock_code} 가용 자금 부족 ({available_funds:,.0f}원) - 매수 중단")
                        break  # 자금 부족시 추가 매수 중단

                    # 🎯 개별 종목 투자금액 계산
                    current_positions = len([c for c in self.manager.stock_manager._all_stocks.values()
                                           if c.status in [CandleStatus.ENTERED, CandleStatus.PENDING_ORDER]])
                    min_investment = self.manager.config['investment_calculation']['min_investment']

                    #logger.info(f"🔍 {candidate.stock_code} 투자금액 계산: 현재포지션={current_positions}개, "
                    #           f"가용자금={available_funds:,.0f}원, 최소투자금={min_investment:,.0f}원")

                    investment_amount = self._calculate_entry_params(candidate, available_funds, current_positions)
                    logger.debug(f"💰 {candidate.stock_code} 계산된 투자금액: {investment_amount:,.0f}원")

                    if investment_amount < min_investment:
                        logger.warning(f"⚠️ {candidate.stock_code} 투자금액 부족: {investment_amount:,.0f}원 < {min_investment:,.0f}원")
                        continue

                    # 📈 매수 주문 실행
                    success = await self._execute_entry(candidate, investment_amount)
                    if success:
                        successful_orders += 1

                        # 🔧 매수 주문 성공시 stock_manager 업데이트 (set_pending_order에서 이미 PENDING_ORDER 설정됨)
                        self.manager.stock_manager.update_candidate(candidate)

                        # 🔍 상태 변경 확인
                        updated_candidate = self.manager.stock_manager._all_stocks.get(candidate.stock_code)
                        if updated_candidate:
                            actual_status = updated_candidate.status.value
                            is_ready_check = updated_candidate.is_ready_for_entry()
                            logger.info(f"✅ {candidate.stock_code} 매수 주문 성공 - {actual_status} 상태 "
                                       f"(is_ready={is_ready_check}, 주문번호: {updated_candidate.get_pending_order_no('buy')})")
                        else:
                            logger.error(f"❌ {candidate.stock_code} stock_manager 업데이트 실패!")
                    else:
                        # 🔧 매수 주문 실패시 원래 상태 유지 (BUY_READY)
                        logger.warning(f"❌ {candidate.stock_code} 매수 주문 실패 - BUY_READY 상태 유지")

                    # 주문 간 간격 (API 부하 방지)
                    #await asyncio.sleep(0.5)

                except Exception as e:
                    logger.error(f"❌ {candidate.stock_code} 매수 처리 오류: {e}")
                    continue

            if successful_orders > 0:
                logger.info(f"🎯 총 {successful_orders}개 종목 매수 주문 완료")
            else:
                logger.debug("📊 매수 실행된 종목이 없습니다")

        except Exception as e:
            logger.error(f"❌ 매수 기회 평가 오류: {e}")

    async def evaluate_watching_stocks_for_entry(self, watching_candidates: List[CandleTradeCandidate], current_data_dict: Optional[Dict[str, Any]] = None) -> int:
        """🔍 관찰 중인 종목들의 진입 가능성 평가 및 BUY_READY 전환 (current_data 파라미터 추가)"""
        try:
            converted_count = 0

            # 🔍 디버깅: 입력 종목 현황 파악
            logger.debug(f"🔍 BUY_READY 전환 검토: {len(watching_candidates)}개 관찰 종목")

            # 신호별 분류
            signal_counts = {}
            for candidate in watching_candidates:
                signal = candidate.trade_signal.value
                signal_counts[signal] = signal_counts.get(signal, 0) + 1

            logger.debug(f"📊 신호별 현황: {signal_counts}")

            for candidate in watching_candidates:
                try:
                    logger.debug(f"🔍 {candidate.stock_code} 진입 검토 시작: "
                                f"신호={candidate.trade_signal.value}, 강도={candidate.signal_strength}, "
                                f"상태={candidate.status.value}")

                    # 강한 매수 신호인 경우에만 세부 검증 실행
                    if candidate.trade_signal in [TradeSignal.STRONG_BUY, TradeSignal.BUY]:
                        logger.debug(f"🎯 {candidate.stock_code} 매수 신호 감지 - 세부 검증 시작")

                        # 해당 종목의 current_data 가져오기
                        stock_current_data = current_data_dict.get(candidate.stock_code) if current_data_dict else None

                        # 세부 진입 조건 검증 수행 (current_data 전달)
                        entry_validation_passed = await self._validate_detailed_entry_conditions(candidate, stock_current_data)

                        if entry_validation_passed:
                            # BUY_READY 상태로 전환
                            old_status = candidate.status
                            candidate.status = CandleStatus.BUY_READY
                            candidate.metadata['buy_ready_time'] = datetime.now(self.manager.korea_tz).isoformat()

                            # 🔧 stock_manager 업데이트 (중요!)
                            self.manager.stock_manager.update_candidate(candidate)

                            # 🔍 상태 변경 확인
                            actual_status = self.manager.stock_manager._all_stocks.get(candidate.stock_code)
                            if actual_status:
                                # 🔍 is_ready_for_entry() 체크
                                ready_check = actual_status.is_ready_for_entry()
                                logger.debug(f"🔍 {candidate.stock_code} is_ready_for_entry(): {ready_check}")
                            else:
                                logger.error(f"❌ {candidate.stock_code} stock_manager 업데이트 실패!")

                            converted_count += 1
                        else:
                            continue
                    else:
                        logger.debug(f"📋 {candidate.stock_code} 매수 신호 아님 ({candidate.trade_signal.value}) - 스킵")

                except Exception as e:
                    logger.error(f"❌ 관찰 종목 진입 평가 오류 ({candidate.stock_code}): {e}")
                    continue

            logger.debug(f"✅ BUY_READY 전환 완료: {converted_count}/{len(watching_candidates)}개")
            return converted_count

        except Exception as e:
            logger.error(f"관찰 종목 진입 평가 오류: {e}")
            return 0

    async def _validate_detailed_entry_conditions(self, candidate: CandleTradeCandidate, current_data: Optional[Any] = None) -> bool:
        """🚀 세부 진입 조건 검증 - 새로운 빠른 매수 판단 함수 사용"""
        try:
            # 🆕 빠른 매수 판단 함수 사용 (장전 패턴분석 + 현재가격 기반)
            buy_decision_result = await self.manager.candle_analyzer.quick_buy_decision(candidate, current_data)
            
            if buy_decision_result is None:
                logger.debug(f"❌ {candidate.stock_code} 빠른 매수 판단 실패 - 결과 없음")
                return False

            # 매수 결정 확인
            buy_decision = buy_decision_result.get('buy_decision', 'reject')
            buy_score = buy_decision_result.get('buy_score', 0)
            current_price = buy_decision_result.get('current_price', 0)

            if buy_decision == 'buy':
                logger.info(f"✅ {candidate.stock_code} 빠른 매수 판단 통과: 점수 {buy_score}/100, 현재가 {current_price:,}원")
                
                # 🔧 기존 시스템과의 호환성을 위해 entry_conditions 설정
                from .candle_trade_candidate import EntryConditions
                candidate.entry_conditions = EntryConditions(
                    overall_passed=True,
                    fail_reasons=[],
                    pass_reasons=[f'빠른_매수_판단_통과(점수:{buy_score})']
                )
                return True
                
            elif buy_decision == 'wait':
                reason = buy_decision_result.get('reason', '알 수 없음')
                logger.debug(f"⏸️ {candidate.stock_code} 빠른 매수 대기: {reason} (점수: {buy_score}/100)")
                return False
                
            else:  # 'reject'
                reason = buy_decision_result.get('reason', '알 수 없음')
                logger.debug(f"❌ {candidate.stock_code} 빠른 매수 거부: {reason}")
                return False

        except Exception as e:
            logger.error(f"❌ {candidate.stock_code} 빠른 매수 판단 검증 오류: {e}")
            # 오류시 기존 방식으로 폴백
            return await self._validate_detailed_entry_conditions_fallback(candidate, current_data)

    async def _validate_detailed_entry_conditions_fallback(self, candidate: CandleTradeCandidate, current_data: Optional[Any] = None) -> bool:
        """🔧 기존 세부 진입 조건 검증 (폴백용)"""
        try:
            # 1. 가격 정보 (파라미터로 받거나 API 조회)
            if current_data is None:
                from ..api.kis_market_api import get_inquire_price
                current_data = get_inquire_price("J", candidate.stock_code)

            if current_data is None or current_data.empty:
                logger.debug(f"❌ {candidate.stock_code} 가격 정보 조회 실패")
                return False

            current_price = float(current_data.iloc[0].get('stck_prpr', 0))
            if current_price <= 0:
                logger.debug(f"❌ {candidate.stock_code} 유효하지 않은 가격 {current_price}")
                return False

            logger.debug(f"✅ {candidate.stock_code} 가격 확인: {current_price:,}원")

            # 가격 업데이트
            candidate.update_price(current_price)
            stock_info_dict = current_data.iloc[0].to_dict()

            # 2. 🔍 기본 필터 체크
            if not self.manager._passes_basic_filters(current_price, stock_info_dict):
                logger.debug(f"❌ {candidate.stock_code} 2단계 실패: 기본 필터 미통과")
                return False

            logger.debug(f"✅ {candidate.stock_code} 2단계 통과: 기본 필터")

            # 3. 🔍 상세 진입 조건 체크 (일봉 데이터 조회)
            daily_data = None
            try:
                # 캐시된 일봉 데이터 우선 사용
                daily_data = candidate.get_ohlcv_data()
                if daily_data is None:
                    # 캐시에 없으면 API 조회
                    from ..api.kis_market_api import get_inquire_daily_itemchartprice
                    daily_data = get_inquire_daily_itemchartprice(
                        output_dv="2",  # 일자별 차트 데이터 배열
                        itm_no=candidate.stock_code,
                        period_code="D",  # 일봉
                        adj_prc="1"
                    )
                    # 조회 성공시 캐싱
                    if daily_data is not None and not daily_data.empty:
                        candidate.cache_ohlcv_data(daily_data)
            except Exception as e:
                logger.debug(f"일봉 데이터 조회 오류 ({candidate.stock_code}): {e}")
                daily_data = None

            entry_conditions = await self.check_entry_conditions(candidate, stock_info_dict, daily_data)

            if not entry_conditions.overall_passed:
                #logger.info(f"❌ {candidate.stock_code} 3단계 실패: 상세 진입 조건 미통과 - {', '.join(entry_conditions.fail_reasons)}")
                return False

            logger.debug(f"✅ {candidate.stock_code} 3단계 통과: 상세 진입 조건")

            # 4. 🔍 추가 안전성 검증
            safety_check = self._perform_additional_safety_checks(candidate, current_price, stock_info_dict)

            if not safety_check:
                #logger.info(f"❌ {candidate.stock_code} 4단계 실패: 안전성 검증 미통과")
                return False

            logger.debug(f"✅ {candidate.stock_code} 4단계 통과: 안전성 검증")

            # 🔧 중요! entry_conditions 업데이트 (is_ready_for_entry() 체크용)
            candidate.entry_conditions = entry_conditions

            #logger.info(f"✅ {candidate.stock_code} 모든 세부 진입 조건 통과 - entry_conditions 업데이트 완료")
            return True

        except Exception as e:
            logger.info(f"❌ {candidate.stock_code} 폴백 진입 조건 검증 오류: {e}")
            return False

    def _perform_additional_safety_checks(self, candidate: CandleTradeCandidate, current_price: float, stock_info: Dict) -> bool:
        """🛡️ 추가 안전성 검증"""
        try:
            # 1. 급등/급락 상태 체크
            day_change_pct = float(stock_info.get('prdy_ctrt', 0))
            # 🔧 config에서 급등락 임계값 가져오기
            max_day_change = self.manager.config.get('max_day_change_pct', 15.0)
            if abs(day_change_pct) > max_day_change:
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
                # 🔧 config에서 신호 유효시간 가져오기
                max_signal_age = self.manager.config.get('max_signal_age_seconds', 300)  # 기본 5분
                if signal_age.total_seconds() > max_signal_age:
                    logger.debug(f"❌ {candidate.stock_code} 신호가 너무 오래됨: {signal_age}")
                    return False

            # 4. 패턴 신뢰도 재확인
            if candidate.detected_patterns:
                primary_pattern = candidate.detected_patterns[0]
                # 🔧 config에서 최소 신뢰도 가져오기
                min_confidence = self.manager.config.get('pattern_confidence_threshold', 0.6)
                if primary_pattern.confidence < min_confidence:
                    logger.debug(f"❌ {candidate.stock_code} 패턴 신뢰도 부족: {primary_pattern.confidence:.2f}")
                    return False

            return True

        except Exception as e:
            logger.debug(f"안전성 검증 오류 ({candidate.stock_code}): {e}")
            return False

    def _calculate_entry_params(self, candidate: CandleTradeCandidate, available_funds: float, position_count: int) -> float:
        """개별 종목 투자금액 계산 (🆕 시장상황 반영)"""
        try:
            # 🌍 시장 상황 가져오기
            market_condition = self.manager.market_analyzer.get_current_condition()
            market_adjustments = self.manager.config.get('market_condition_adjustments', {})

            # 기본 우선순위 배수 계산
            max_priority_multiplier = self.manager.config.get('max_priority_multiplier', 1.5)
            base_multiplier = self.manager.config.get('base_priority_multiplier', 0.5)
            priority_multiplier = min(max_priority_multiplier, candidate.entry_priority / 100 + base_multiplier)

            # 🆕 시장 추세에 따른 포지션 크기 조정
            position_size_multiplier = 1.0

            # 상승장/하락장에 따른 조정
            market_trend = market_condition.get('market_trend', 'neutral_market')
            if market_trend == 'bull_market':
                bull_config = market_adjustments.get('bull_market', {})
                position_size_multiplier = bull_config.get('position_size_multiplier', 1.2)
                logger.debug(f"🐂 상승장 감지 - 포지션 크기 {position_size_multiplier:.1f}배 조정")

            elif market_trend == 'bear_market':
                bear_config = market_adjustments.get('bear_market', {})
                position_size_multiplier = bear_config.get('position_size_multiplier', 0.7)
                logger.debug(f"🐻 하락장 감지 - 포지션 크기 {position_size_multiplier:.1f}배 축소")

            # 🆕 변동성에 따른 추가 조정
            volatility_multiplier = 1.0
            volatility = market_condition.get('volatility', 'low_volatility')
            if volatility == 'high_volatility':
                high_vol_config = market_adjustments.get('high_volatility', {})
                volatility_multiplier = high_vol_config.get('position_size_reduction', 0.8)
                logger.debug(f"📈 고변동성 - 포지션 크기 {volatility_multiplier:.1f}배 축소")

            # 기본 투자금액 계산
            max_single_investment_ratio = self.manager.config.get('max_single_investment_ratio', 0.4)
            base_investment = available_funds * max_single_investment_ratio

            # 🆕 시장상황 종합 반영
            adjusted_amount = base_investment * position_size_multiplier * volatility_multiplier * priority_multiplier

            # 최소/최대 제한 적용
            min_investment = self.manager.config['investment_calculation']['min_investment']
            adjusted_amount = max(min_investment, adjusted_amount)

            logger.debug(f"💰 {candidate.stock_code} 투자금액: {adjusted_amount:,.0f}원 "
                        f"(기본배수:{priority_multiplier:.2f}, 시장조정:{position_size_multiplier:.2f}, "
                        f"변동성조정:{volatility_multiplier:.2f})")

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
        """🆕 가용 투자 자금 계산 (KIS API dnca_tot_amt 매수가능금액 활용)"""
        try:
            investment_config = self.manager.config['investment_calculation']

            # 🎯 KIS API에서 제공하는 실제 매수가능금액 사용 (dnca_tot_amt)
            available_amount = float(account_info.get('available_amount', 0))  # dnca_tot_amt
            cash_balance = float(account_info.get('cash_balance', 0))          # 현금잔고
            total_value = float(account_info.get('total_value', 0))           # 총평가액

            logger.debug(f"💰 계좌 정보: 매수가능금액={available_amount:,.0f}원, "
                       f"현금잔고={cash_balance:,.0f}원, 총평가액={total_value:,.0f}원")

            # 🎯 매수가능금액이 있으면 이를 기준으로 사용 (가장 정확한 값)
            if available_amount > 0:
                # 매수가능금액의 일정 비율만 사용 (안전 마진)
                safe_ratio = investment_config.get('available_amount_ratio', 0.9)  # 90% 사용
                available_funds = available_amount * safe_ratio

                logger.debug(f"💰 매수가능금액 기반 투자: {available_funds:,.0f}원 "
                           f"(매수가능금액의 {safe_ratio*100:.0f}%)")

            # 매수가능금액 정보가 없으면 기존 로직 사용 (폴백)
            elif cash_balance > 0:
                # 현금 잔고 기반 계산
                cash_usage_ratio = investment_config.get('cash_usage_ratio', 0.8)
                available_funds = cash_balance * cash_usage_ratio

                logger.warning(f"⚠️ 매수가능금액 정보 없음 - 현금잔고 기반: {available_funds:,.0f}원 "
                              f"(현금잔고의 {cash_usage_ratio*100:.0f}%)")

            else:
                logger.error("❌ 매수가능금액과 현금잔고 모두 0원 또는 정보 없음")
                return 0

            # 최소 투자금액 확보 여부 체크
            min_required = investment_config['min_investment']
            if available_funds < min_required:
                logger.warning(f"⚠️ 가용자금 부족: {available_funds:,.0f}원 < {min_required:,.0f}원")
                return 0

            logger.debug(f"✅ 최종 가용 투자자금: {available_funds:,.0f}원")
            return available_funds

        except Exception as e:
            logger.error(f"가용 자금 계산 오류: {e}")
            return 0

    async def _execute_entry(self, candidate: CandleTradeCandidate, investment_amount: float) -> bool:
        """매수 실행 - 주문만 하고 체결은 웹소켓에서 확인"""
        try:
            # 🚨 최종 중복 주문 방지 체크
            if candidate.status == CandleStatus.PENDING_ORDER:
                logger.warning(f"🚫 {candidate.stock_code} 이미 PENDING_ORDER 상태 - 매수 중단")
                return False

            if candidate.has_pending_order('buy'):
                logger.warning(f"🚫 {candidate.stock_code} 이미 매수 주문 대기 중 - 매수 중단")
                return False

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
                'pattern_type': str(candidate.detected_patterns[0].pattern_type.value) if candidate.detected_patterns else 'unknown',
                'pattern_confidence': candidate.detected_patterns[0].confidence if candidate.detected_patterns else 0.0,
                'pattern_strength': candidate.detected_patterns[0].strength if candidate.detected_patterns else 0,
                'signal_strength': candidate.signal_strength,
                'entry_priority': candidate.entry_priority,
                'pre_validated': True,  # 캔들 시스템에서 이미 검증 완료
                # 🆕 기술적 지표 정보 추가 (진입 조건 체크에서 계산된 값들)
                'rsi_value': getattr(candidate, '_rsi_value', None),
                'macd_value': getattr(candidate, '_macd_value', None),
                'volume_ratio': getattr(candidate, '_volume_ratio', None),
                'investment_amount': int(current_price * quantity),
                'investment_ratio': investment_amount / max(available_funds, 1) if available_funds > 0 else 0.0
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

            # 일일 통계는 주문 제출 시점에 카운트 (나중에 체결 실패시 조정 가능)
            self.manager.daily_stats['trades_count'] += 1

            return True

        except Exception as e:
            logger.error(f"매수 실행 오류 ({candidate.stock_code}): {e}")
            return False


    # ========== 🆕 진입 조건 체크 ==========

    def should_update_buy_signal(self, candidate: CandleTradeCandidate, analysis_result: Dict) -> bool:
        """🚀 매수 신호 업데이트 필요 여부 판단 - quick_buy_decision용"""
        try:
            buy_decision = analysis_result['buy_decision']
            buy_score = analysis_result.get('buy_score', 50)
            
            # 매수 결정을 TradeSignal로 변환
            if buy_decision == 'buy':
                new_signal = TradeSignal.STRONG_BUY if buy_score >= 85 else TradeSignal.BUY
            elif buy_decision == 'wait':
                new_signal = TradeSignal.HOLD
            else:  # 'reject'
                new_signal = TradeSignal.HOLD

            # 1. 신호 종류가 변경된 경우
            signal_changed = new_signal != candidate.trade_signal
            if signal_changed:
                logger.debug(f"🚀 {candidate.stock_code} 매수 신호 변경: {candidate.trade_signal.value} → {new_signal.value}")
                return True

            # 2. 점수 변화 체크 (매수 신호는 더 민감하게)
            score_diff = abs(buy_score - candidate.signal_strength)
            
            # 매수 신호에서는 10점 차이로 민감하게 반응
            if score_diff >= 10:
                logger.debug(f"🚀 {candidate.stock_code} 매수 점수 변화: {candidate.signal_strength} → {buy_score} (차이:{score_diff:.1f})")
                return True

            # 3. 매수 결정 변화 (buy_decision이 바뀐 경우)
            prev_decision = getattr(candidate, '_last_buy_decision', None)
            if prev_decision != buy_decision:
                candidate._last_buy_decision = buy_decision
                logger.debug(f"🚀 {candidate.stock_code} 매수 결정 변화: {prev_decision} → {buy_decision}")
                return True

            return False

        except Exception as e:
            logger.debug(f"매수 신호 업데이트 판단 오류: {e}")
            return False

    async def check_entry_conditions(self, candidate: CandleTradeCandidate,
                                   current_info: Dict, daily_data: Optional[Any] = None):
        """🔍 진입 조건 종합 체크 (CandleTradeManager에서 이관)"""
        try:
            from .candle_trade_candidate import EntryConditions

            conditions = EntryConditions()

            # 1. 거래량 조건
            current_volume = int(current_info.get('acml_vol', 0))  # 🎯 현재 누적 거래량
            avg_volume = int(current_info.get('avrg_vol', 1))
            volume_ratio = current_volume / max(avg_volume, 1)

            conditions.volume_check = volume_ratio >= self.manager.config['min_volume_ratio']
            if not conditions.volume_check:
                conditions.fail_reasons.append(f"거래량 부족 ({volume_ratio:.1f}배)")

            # 2. 기술적 지표 조건 (RSI, MACD, 볼린저밴드 등) - 전달받은 daily_data 사용
            try:
                conditions.rsi_check = True  # 기본값
                conditions.technical_indicators = {}  # 🆕 기술적 지표 저장

                if daily_data is not None and not daily_data.empty and len(daily_data) >= 20:
                    from ..analysis.technical_indicators import TechnicalIndicators

                    # OHLCV 데이터 추출
                    ohlcv_data = []
                    for _, row in daily_data.iterrows():
                        try:
                            open_price = float(row.get('stck_oprc', 0))
                            high_price = float(row.get('stck_hgpr', 0))
                            low_price = float(row.get('stck_lwpr', 0))
                            close_price = float(row.get('stck_clpr', 0))
                            daily_volume = int(row.get('acml_vol', 0))  # 🎯 일봉별 거래량

                            if all(x > 0 for x in [open_price, high_price, low_price, close_price]):
                                ohlcv_data.append({
                                    'open': open_price,
                                    'high': high_price,
                                    'low': low_price,
                                    'close': close_price,
                                    'volume': daily_volume
                                })
                        except (ValueError, TypeError):
                            continue

                    if len(ohlcv_data) >= 14:
                        close_prices = [x['close'] for x in ohlcv_data]
                        high_prices = [x['high'] for x in ohlcv_data]
                        low_prices = [x['low'] for x in ohlcv_data]
                        volumes = [x['volume'] for x in ohlcv_data]

                        # 🔥 1. RSI 계산 및 체크
                        rsi_values = TechnicalIndicators.calculate_rsi(close_prices)
                        current_rsi = rsi_values[-1] if rsi_values else 50.0
                        conditions.technical_indicators['rsi'] = current_rsi
                        
                        # 🆕 candidate에 기술적 지표 값 저장
                        candidate._rsi_value = current_rsi
                        candidate._volume_ratio = volume_ratio

                        # RSI 과매수 구간 (65 이상) 체크
                        conditions.rsi_check = current_rsi < 65  # 65 미만일 때 진입 허용
                        if not conditions.rsi_check:
                            conditions.fail_reasons.append(f"RSI 과매수 ({current_rsi:.1f})")

                        # 🔥 2. MACD 계산 및 추가 확인
                        try:
                            macd_line, macd_signal, macd_histogram = TechnicalIndicators.calculate_macd(close_prices)
                            if macd_line and macd_signal and macd_histogram:
                                current_macd = macd_line[-1]
                                current_signal = macd_signal[-1]
                                current_histogram = macd_histogram[-1]

                                conditions.technical_indicators['macd'] = float(current_macd)
                                conditions.technical_indicators['macd_signal'] = float(current_signal)
                                conditions.technical_indicators['macd_histogram'] = float(current_histogram)
                                
                                # 🆕 candidate에 MACD 값 저장
                                candidate._macd_value = float(current_histogram)

                                # MACD가 상승 전환 중이면 가점 (RSI 과매수여도 진입 고려)
                                if float(current_macd) > float(current_signal) and float(current_histogram) > 0.0:
                                    if not conditions.rsi_check and current_rsi < 75:  # RSI가 75 미만이면 MACD 우선
                                        conditions.rsi_check = True
                                        conditions.fail_reasons = [r for r in conditions.fail_reasons if 'RSI' not in r]
                                        logger.debug(f"📊 {candidate.stock_code} MACD 상승전환으로 RSI 조건 완화")
                        except Exception as e:
                            logger.debug(f"📊 {candidate.stock_code} MACD 계산 오류: {e}")
                            candidate._macd_value = None

                        # 🔥 3. 볼린저 밴드 계산 (추가 확인)
                        try:
                            bb_upper, bb_middle, bb_lower = TechnicalIndicators.calculate_bollinger_bands(close_prices, 20, 2)
                            if bb_upper and bb_middle and bb_lower:
                                current_price = float(close_prices[-1])
                                bb_position = (current_price - float(bb_lower[-1])) / (float(bb_upper[-1]) - float(bb_lower[-1]))

                                conditions.technical_indicators['bb_position'] = bb_position

                                # 볼린저 밴드 하단 근처(20% 이하)면 RSI 과매수 조건 완화
                                if bb_position <= 0.2 and not conditions.rsi_check and current_rsi < 70:
                                    conditions.rsi_check = True
                                    conditions.fail_reasons = [r for r in conditions.fail_reasons if 'RSI' not in r]
                                    logger.debug(f"📊 {candidate.stock_code} 볼린저밴드 하단으로 RSI 조건 완화")
                        except Exception as e:
                            logger.debug(f"📊 {candidate.stock_code} 볼린저밴드 계산 오류: {e}")

                        logger.debug(f"📊 {candidate.stock_code} 기술지표 - RSI:{current_rsi:.1f}, "
                                   f"MACD:{conditions.technical_indicators.get('macd_histogram', 0):.3f}, "
                                   f"BB위치:{conditions.technical_indicators.get('bb_position', 0.5):.2f}")

                    else:
                        conditions.rsi_check = True  # 데이터 부족시 통과
                        logger.debug(f"📊 {candidate.stock_code} 기술지표 데이터 부족 - 통과")
                else:
                    conditions.rsi_check = True  # 데이터 없을 시 통과
                    logger.debug(f"📊 {candidate.stock_code} 일봉 데이터 없음 - 기술지표 체크 통과")

            except Exception as e:
                logger.error(f"기술지표 계산 오류 ({candidate.stock_code}): {e}")
                conditions.rsi_check = True  # 오류시 통과

            # 3. 시간대 조건
            current_time = datetime.now().time()
            from datetime import datetime as dt
            trading_start = dt.strptime(self.manager.config['trading_start_time'], '%H:%M').time()
            trading_end = dt.strptime(self.manager.config['trading_end_time'], '%H:%M').time()

            conditions.time_check = trading_start <= current_time <= trading_end
            if not conditions.time_check:
                conditions.fail_reasons.append("거래 시간 외")
            conditions.time_check = True

            # 4. 가격대 조건
            price = candidate.current_price
            conditions.price_check = self.manager.config['min_price'] <= price <= self.manager.config['max_price']
            if not conditions.price_check:
                conditions.fail_reasons.append(f"가격대 부적합 ({price:,.0f}원)")

            # 5. 시가총액 조건 (간접 추정)
            conditions.market_cap_check = price >= 5000  # 간단한 추정

            # 6. 일일 거래대금 조건 (현재 누적 거래량 × 현재가)
            daily_amount = current_volume * price
            conditions.daily_volume_check = daily_amount >= self.manager.config['min_daily_volume']
            if not conditions.daily_volume_check:
                conditions.fail_reasons.append(f"거래대금 부족 ({daily_amount/100000000:.0f}억원)")

            # 전체 통과 여부
            conditions.overall_passed = all([
                conditions.volume_check,
                conditions.rsi_check,
                conditions.time_check,
                conditions.price_check,
                conditions.market_cap_check,
                conditions.daily_volume_check
            ])

            return conditions

        except Exception as e:
            logger.error(f"진입 조건 체크 오류: {e}")
            return EntryConditions()
