"""
매도 포지션 관리자
캔들 기반 매매 전략의 기존 포지션 관리를 담당
"""
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, TYPE_CHECKING
from utils.logger import setup_logger

if TYPE_CHECKING:
    from .candle_trade_manager import CandleTradeManager

from .candle_trade_candidate import CandleTradeCandidate, CandleStatus

logger = setup_logger(__name__)


class SellPositionManager:
    """매도 포지션 관리 및 매도 실행 관리자"""

    def __init__(self, candle_trade_manager: 'CandleTradeManager'):
        """
        Args:
            candle_trade_manager: CandleTradeManager 인스턴스
        """
        self.manager = candle_trade_manager
        logger.info("✅ SellPositionManager 초기화 완료")

    async def manage_existing_positions(self):
        """기존 포지션 관리 (손절/익절/추적손절) - _all_stocks 통합 버전"""
        try:
            # 🆕 _all_stocks에서 ENTERED 상태인 모든 종목 관리 (기존 보유 + 새로 매수)
            entered_positions = [
                stock for stock in self.manager.stock_manager._all_stocks.values()
                if stock.status == CandleStatus.ENTERED
            ]

            if not entered_positions:
                return

            logger.debug(f"📊 포지션 관리: {len(entered_positions)}개 포지션 (_all_stocks 통합)")

            for position in entered_positions:
                try:
                    await self._manage_single_position(position)
                except Exception as e:
                    logger.error(f"포지션 관리 오류 ({position.stock_code}): {e}")

        except Exception as e:
            logger.error(f"포지션 관리 오류: {e}")

    async def _manage_single_position(self, position: CandleTradeCandidate):
        """개별 포지션 관리"""
        try:
            # 🕐 거래 시간 체크 (매도 시간 제한)
            # current_time = datetime.now().time()
            # trading_start = datetime.strptime(self.manager.config['trading_start_time'], '%H:%M').time()
            # trading_end = datetime.strptime(self.manager.config['trading_end_time'], '%H:%M').time()

            # is_trading_time = trading_start <= current_time <= trading_end
            # if not is_trading_time:
            #     logger.debug(f"⏰ {position.stock_code} 거래 시간 외 - 매도 대기 중")

            # 최신 가격 조회
            # from ..api.kis_market_api import get_inquire_price
            # current_data = get_inquire_price("J", position.stock_code)

            # if current_data is None or current_data.empty:
            #     return

            # current_price = float(current_data.iloc[0].get('stck_prpr', 0))
            # if current_price <= 0:
            #     logger.warning(f"⚠️ {position.stock_code} 잘못된 현재가: {current_price}")
            #     return

            # # 포지션 정보 업데이트
            # position.update_price(current_price)

            # 📊 매도 조건 체크
            should_exit = False
            exit_reason = ""

            # 🆕 실시간 캔들 패턴 재분석 (DB 의존 제거)
            target_profit_pct, stop_loss_pct, max_hours, pattern_based = self._get_pattern_based_target(position)

            # 1. 손절 체크 (패턴별)
            if position.performance.pnl_pct is not None and position.performance.pnl_pct <= -stop_loss_pct:
                should_exit = True
                exit_reason = "손절"

            # 2. 익절 체크 (패턴별)
            elif position.performance.pnl_pct is not None and position.performance.pnl_pct >= target_profit_pct:
                should_exit = True
                exit_reason = "목표가 도달"

            # 3. 시간 청산 체크 (패턴별)
            elif self._should_time_exit_pattern_based(position, max_hours):
                should_exit = True
                exit_reason = "시간 청산"

            # 🆕 동적 추적 손절 업데이트 (손절가가 계속 조정됨)
            if position.performance.entry_price:
                self._update_trailing_stop(position, position.current_price)

            # 매도 실행
            if should_exit:
                logger.info(f"📉 {position.stock_code} 매도 조건 충족: {exit_reason} "
                           f"(수익률: {position.performance.pnl_pct:+.2f}%)")
                await self._execute_exit(position, position.current_price, exit_reason)

        except Exception as e:
            logger.error(f"개별 포지션 관리 오류 ({position.stock_code}): {e}")

    def _get_pattern_based_target(self, position: CandleTradeCandidate) -> Tuple[float, float, int, bool]:
        """🎯 캔들 패턴별 수익률 목표, 손절, 시간 설정 결정"""
        try:
            # 1. 캔들 전략으로 매수한 종목인지 확인
            is_candle_strategy = (
                position.metadata.get('restored_from_db', False) or  # DB에서 복원됨
                position.metadata.get('original_entry_source') == 'candle_strategy' or  # 캔들 전략 매수
                len(position.detected_patterns) > 0  # 패턴 정보가 있음
            )

            if not is_candle_strategy:
                # 수동/앱 매수 종목: 큰 수익/손실 허용 (🎯 3% 목표, 3% 손절) - 사용자 수정 반영
                logger.debug(f"📊 {position.stock_code} 수동 매수 종목 - 기본 설정 적용")
                return 3.0, 3.0, 24, False

            # 2. 🔄 실시간 캔들 패턴 재분석 (🆕 캐싱 활용)
            original_pattern = None

            # 🆕 캐시된 일봉 데이터 우선 사용
            ohlcv_data = position.get_ohlcv_data()

            if ohlcv_data is None:
                # 캐시에 없으면 API 호출
                try:
                    from ..api.kis_market_api import get_inquire_daily_itemchartprice
                    ohlcv_data = get_inquire_daily_itemchartprice(
                        output_dv="2",
                        itm_no=position.stock_code,
                        period_code="D",
                        adj_prc="1"
                    )

                    # 🆕 조회 성공시 캐싱
                    if ohlcv_data is not None and not ohlcv_data.empty:
                        position.cache_ohlcv_data(ohlcv_data)
                        logger.debug(f"📥 {position.stock_code} 일봉 데이터 조회 및 캐싱 완료")
                    else:
                        logger.debug(f"❌ {position.stock_code} 일봉 데이터 조회 실패")

                except Exception as e:
                    logger.debug(f"일봉 데이터 조회 오류 ({position.stock_code}): {e}")
                    ohlcv_data = None
            else:
                logger.debug(f"📄 {position.stock_code} 캐시된 일봉 데이터 사용")

            # 🆕 실시간 캔들 패턴 분석 (캐시된 데이터 활용)
            if ohlcv_data is not None and not ohlcv_data.empty:
                try:
                    pattern_result = self.manager.pattern_detector.analyze_stock_patterns(position.stock_code, ohlcv_data)
                    if pattern_result and len(pattern_result) > 0:
                        strongest_pattern = max(pattern_result, key=lambda p: p.strength)
                        original_pattern = strongest_pattern.pattern_type.value
                        logger.debug(f"🔄 {position.stock_code} 실시간 패턴 분석: {original_pattern} (강도: {strongest_pattern.strength})")
                except Exception as e:
                    logger.debug(f"패턴 분석 오류 ({position.stock_code}): {e}")

            # DB에서 복원된 경우 (백업)
            if not original_pattern and 'original_pattern_type' in position.metadata:
                original_pattern = position.metadata['original_pattern_type']
                logger.debug(f"📚 {position.stock_code} DB에서 패턴 복원: {original_pattern}")

            # 기존 패턴 정보 활용 (백업)
            elif not original_pattern and position.detected_patterns and len(position.detected_patterns) > 0:
                strongest_pattern = max(position.detected_patterns, key=lambda p: p.strength)
                original_pattern = strongest_pattern.pattern_type.value
                logger.debug(f"📊 {position.stock_code} 기존 패턴 정보 활용: {original_pattern}")

            # 3. 패턴별 목표, 손절, 시간 설정 적용
            if original_pattern:
                # 패턴명을 소문자로 변환하여 config에서 조회
                pattern_key = original_pattern.lower().replace('_', '_')
                pattern_config = self.manager.config['pattern_targets'].get(pattern_key)

                if pattern_config:
                    target_pct = pattern_config['target']
                    stop_pct = pattern_config['stop']
                    max_hours = pattern_config['max_hours']

                    logger.debug(f"📊 {position.stock_code} 패턴 '{original_pattern}' - "
                                f"목표:{target_pct}%, 손절:{stop_pct}%, 시간:{max_hours}h")
                    return target_pct, stop_pct, max_hours, True
                else:
                    # 패턴 config에 없으면 패턴 강도에 따라 결정 (🎯 큰 수익/손실 허용)
                    if position.detected_patterns:
                        strongest_pattern = max(position.detected_patterns, key=lambda p: p.strength)
                        if strongest_pattern.strength >= 90:
                            target_pct, stop_pct, max_hours = 15.0, 4.0, 8  # 매우 강한 패턴
                        elif strongest_pattern.strength >= 80:
                            target_pct, stop_pct, max_hours = 12.0, 3.0, 6  # 강한 패턴
                        elif strongest_pattern.strength >= 70:
                            target_pct, stop_pct, max_hours = 8.0, 3.0, 4  # 중간 패턴
                        else:
                            target_pct, stop_pct, max_hours = 5.0, 2.0, 2  # 약한 패턴

                        logger.debug(f"📊 {position.stock_code} 패턴 강도 {strongest_pattern.strength} - "
                                    f"목표:{target_pct}%, 손절:{stop_pct}%, 시간:{max_hours}h")
                        return target_pct, stop_pct, max_hours, True

            # 4. 기본값: 캔들 전략이지만 패턴 정보 없음 (🎯 3% 목표, 3% 손절) - 사용자 수정 반영
            logger.debug(f"📊 {position.stock_code} 캔들 전략이나 패턴 정보 없음 - 기본 캔들 설정 적용")
            return 3.0, 3.0, 6, True

        except Exception as e:
            logger.error(f"패턴별 설정 결정 오류 ({position.stock_code}): {e}")
            # 오류시 안전하게 기본값 반환 (🎯 3% 목표, 3% 손절) - 사용자 수정 반영
            return 3.0, 3.0, 24, False

    def _should_time_exit_pattern_based(self, position: CandleTradeCandidate, max_hours: int) -> bool:
        """🆕 패턴별 시간 청산 조건 체크"""
        try:
            if not position.performance.entry_time:
                return False

            # 보유 시간 계산
            holding_time = datetime.now() - position.performance.entry_time
            max_holding = timedelta(hours=max_hours)

            # 패턴별 최대 보유시간 초과시 청산
            if holding_time >= max_holding:
                logger.info(f"⏰ {position.stock_code} 패턴별 최대 보유시간({max_hours}h) 초과 청산: {holding_time}")
                return True

            # 새로운 시간 기반 청산 규칙 적용 (선택적)
            time_rules = self.manager.config.get('time_exit_rules', {})

            # 수익 중 시간 청산 (패턴별 시간의 절반 후)
            profit_exit_hours = max_hours // 2  # 패턴별 시간의 절반
            min_profit = time_rules.get('min_profit_for_time_exit', 0.5) / 100

            if (holding_time >= timedelta(hours=profit_exit_hours) and
                position.performance.pnl_pct and
                position.performance.pnl_pct >= min_profit):
                logger.info(f"⏰ {position.stock_code} 패턴별 시간 기반 수익 청산: {holding_time}")
                return True

            return False

        except Exception as e:
            logger.error(f"패턴별 시간 청산 체크 오류: {e}")
            return False

    async def _execute_exit(self, position: CandleTradeCandidate, exit_price: float, reason: str) -> bool:
        """매도 청산 실행"""
        try:
            # 🕐 거래 시간 재확인 (매도 실행 직전 체크)
            current_time = datetime.now().time()
            trading_start = datetime.strptime(self.manager.config['trading_start_time'], '%H:%M').time()
            trading_end = datetime.strptime(self.manager.config['trading_end_time'], '%H:%M').time()

            is_trading_time = trading_start <= current_time <= trading_end
            if not is_trading_time:
                logger.warning(f"⏰ {position.stock_code} 거래 시간 외 매도 차단 - {reason}")
                logger.info(f"현재 시간: {current_time}, 거래 시간: {trading_start} ~ {trading_end}")

            # 🔍 실제 보유 여부 확인 (매도 전 필수 체크)
            try:
                from ..api.kis_market_api import get_account_balance
                account_info = get_account_balance()

                if account_info and 'stocks' in account_info:
                    # 실제 보유 종목에서 해당 종목 찾기
                    actual_holding = None
                    for stock in account_info['stocks']:
                        if stock.get('stock_code') == position.stock_code:
                            actual_holding = stock
                            break

                    if not actual_holding:
                        logger.warning(f"⚠️ {position.stock_code} 실제 보유하지 않는 종목 - 매도 취소")
                        return False

                    actual_quantity = actual_holding.get('quantity', 0)
                    if actual_quantity <= 0:
                        logger.warning(f"⚠️ {position.stock_code} 실제 보유 수량 없음 ({actual_quantity}주) - 매도 취소")
                        return False

                    # 매도할 수량을 실제 보유 수량으로 조정
                    quantity = min(position.performance.entry_quantity or 0, actual_quantity)
                    logger.info(f"✅ {position.stock_code} 보유 확인: 시스템{position.performance.entry_quantity}주 → 실제{actual_quantity}주 → 매도{quantity}주")
                else:
                    logger.warning(f"⚠️ {position.stock_code} 계좌 정보 조회 실패 - 매도 진행")
                    quantity = position.performance.entry_quantity

            except Exception as e:
                logger.warning(f"⚠️ {position.stock_code} 보유 확인 오류: {e} - 기존 수량으로 진행")
                quantity = position.performance.entry_quantity

            if not quantity or quantity <= 0:
                logger.warning(f"❌ {position.stock_code} 매도할 수량 없음 ({quantity}주)")
                return False

            # 🆕 안전한 매도가 계산 (현재가 직접 사용 금지)
            safe_sell_price = self._calculate_safe_sell_price(exit_price, reason)

            # 매도 신호 생성
            signal = {
                'stock_code': position.stock_code,
                'action': 'sell',
                'strategy': 'candle_pattern',
                'price': safe_sell_price,  # 🎯 계산된 안전한 매도가 사용
                'quantity': quantity,
                'total_amount': int(safe_sell_price * quantity),
                'reason': reason,
                'pattern_type': str(position.detected_patterns[0].pattern_type) if position.detected_patterns else 'unknown',
                'pre_validated': True  # 캔들 시스템에서 이미 검증 완료
            }

            # 실제 매도 주문 실행 (TradeExecutor 사용)
            if hasattr(self.manager, 'trade_executor') and self.manager.trade_executor:
                try:
                    result = self.manager.trade_executor.execute_sell_signal(signal)
                    if not result.success:
                        logger.error(f"❌ 매도 주문 실패: {position.stock_code} - {result.error_message}")
                        return False
                    logger.info(f"✅ 실제 매도 주문 성공: {position.stock_code} "
                               f"현재가{exit_price:,.0f}원 → 주문가{safe_sell_price:,.0f}원 "
                               f"(주문번호: {result.order_no})")
                except Exception as e:
                    logger.error(f"❌ 매도 주문 실행 오류: {position.stock_code} - {e}")
                    return False
            else:
                # TradeExecutor가 없는 경우 로그만 출력 (테스트 모드)
                logger.info(f"📉 매도 주문 (테스트): {position.stock_code} {quantity}주 "
                           f"현재가{exit_price:,.0f}원 → 주문가{safe_sell_price:,.0f}원")

            # 포지션 청산 기록 (원래 exit_price로 기록)
            position.exit_position(exit_price, reason)

            # 🆕 _all_stocks 상태 업데이트 (ENTERED → EXITED)
            if position.stock_code in self.manager.stock_manager._all_stocks:
                self.manager.stock_manager._all_stocks[position.stock_code].status = CandleStatus.EXITED
                self.manager.stock_manager._all_stocks[position.stock_code].exit_position(exit_price, reason)
                logger.debug(f"🔄 {position.stock_code} stock_manager._all_stocks 상태 업데이트: → EXITED")

            # 일일 통계 업데이트
            if position.performance.realized_pnl:
                self.manager.daily_stats['total_profit_loss'] += position.performance.realized_pnl

                if position.performance.realized_pnl > 0:
                    self.manager.daily_stats['successful_trades'] += 1
                else:
                    self.manager.daily_stats['failed_trades'] += 1

            return True

        except Exception as e:
            logger.error(f"매도 청산 실행 오류 ({position.stock_code}): {e}")
            return False

    def _calculate_safe_sell_price(self, current_price: float, reason: str) -> int:
        """안전한 매도가 계산 (틱 단위 맞춤)"""
        try:
            # 매도 이유별 할인율 적용
            if reason == "손절":
                discount_pct = 0.008  # 0.8% 할인 (빠른 체결 우선)
            elif reason in ["목표가 도달", "익절"]:
                discount_pct = 0.003  # 0.3% 할인 (적당한 체결)
            elif reason == "시간 청산":
                discount_pct = 0.005  # 0.5% 할인 (중간 속도)
            else:
                discount_pct = 0.005  # 기본 0.5% 할인

            # 할인된 가격 계산
            target_price = int(current_price * (1 - discount_pct))

            # 틱 단위 맞춤
            tick_unit = self._get_tick_unit(target_price)
            safe_price = (target_price // tick_unit) * tick_unit

            # 최소 가격 보정 (너무 낮으면 안됨)
            min_price = int(current_price * 0.97)  # 현재가의 97% 이상
            safe_price = max(safe_price, min_price)

            logger.debug(f"💰 매도가 계산: 현재가{current_price:,.0f}원 → 주문가{safe_price:,.0f}원 "
                        f"({reason}, 할인{discount_pct*100:.1f}%)")

            return safe_price

        except Exception as e:
            logger.error(f"안전한 매도가 계산 오류: {e}")
            # 오류시 현재가의 99% 반환 (안전장치)
            return int(current_price * 0.99)

    def _get_tick_unit(self, price: int) -> int:
        """호가단위 계산"""
        try:
            if price < 2000:
                return 1
            elif price < 5000:
                return 5
            elif price < 20000:
                return 10
            elif price < 50000:
                return 50
            elif price < 200000:
                return 100
            elif price < 500000:
                return 500
            else:
                return 1000
        except:
            return 100  # 기본값

    def _update_trailing_stop(self, position: CandleTradeCandidate, current_price: float):
        """🔄 패턴 기반 동적 목표/손절 조정 시스템 (개선된 버전)"""
        try:
            # 🆕 캐시된 OHLCV 데이터 사용 (API 호출 제거)
            ohlcv_data = position.get_ohlcv_data()
            if ohlcv_data is None:
                logger.debug(f"📄 {position.stock_code} 캐시된 일봉 데이터 없음 - 기본 trailing stop 적용")
                # 캐시된 데이터가 없으면 기존 방식으로 폴백
                self._fallback_trailing_stop(position, current_price)
                return

            # 🆕 1단계: 실시간 캔들 패턴 재분석 (OHLCV 데이터 전달)
            pattern_update = self._analyze_realtime_pattern_changes(position.stock_code, current_price, ohlcv_data)

            # 🆕 2단계: 수익률 기반 동적 조정
            profit_based_update = self._calculate_profit_based_adjustments(position, current_price)

            # 🆕 3단계: 추세 강도 기반 조정 (OHLCV 데이터 전달)
            trend_based_update = self._calculate_trend_based_adjustments(position, current_price, ohlcv_data)

            # 🆕 4단계: 종합 판단 및 업데이트
            self._apply_dynamic_adjustments(position, current_price, pattern_update, profit_based_update, trend_based_update)

        except Exception as e:
            logger.error(f"동적 목표/손절 조정 오류 ({position.stock_code}): {e}")
            # 기존 방식으로 폴백
            self._fallback_trailing_stop(position, current_price)

    def _analyze_realtime_pattern_changes(self, stock_code: str, current_price: float, ohlcv_data: Optional[Any] = None) -> Dict:
        """🔄 실시간 캔들 패턴 변화 분석"""
        try:
            # 🆕 전달받은 OHLCV 데이터만 사용 (API 호출 제거)
            if ohlcv_data is None or ohlcv_data.empty:
                logger.debug(f"📄 {stock_code} OHLCV 데이터 없음 - 패턴 분석 불가")
                return {'pattern_strength_changed': False, 'new_patterns': []}

            # 현재 패턴 분석
            current_patterns = self.manager.pattern_detector.analyze_stock_patterns(stock_code, ohlcv_data)

            if not current_patterns:
                return {'pattern_strength_changed': False, 'new_patterns': []}

            # 가장 강한 패턴 선택
            strongest_pattern = max(current_patterns, key=lambda p: p.strength)

            # 패턴 강도 변화 분석
            pattern_strength_tier = self._get_pattern_strength_tier(strongest_pattern.strength)

            return {
                'pattern_strength_changed': True,
                'new_patterns': current_patterns,
                'strongest_pattern': strongest_pattern,
                'strength_tier': pattern_strength_tier,
                'pattern_type': strongest_pattern.pattern_type.value,
                'confidence': strongest_pattern.confidence
            }

        except Exception as e:
            logger.debug(f"실시간 패턴 분석 오류 ({stock_code}): {e}")
            return {'pattern_strength_changed': False, 'new_patterns': []}

    def _get_pattern_strength_tier(self, strength: int) -> str:
        """패턴 강도를 티어로 분류"""
        if strength >= 90:
            return 'ULTRA_STRONG'  # 15% 목표, 4% 손절
        elif strength >= 80:
            return 'STRONG'        # 12% 목표, 3% 손절
        elif strength >= 70:
            return 'MEDIUM'        # 8% 목표, 3% 손절
        elif strength >= 60:
            return 'WEAK'          # 5% 목표, 2% 손절
        else:
            return 'VERY_WEAK'     # 3% 목표, 1.5% 손절

    def _calculate_profit_based_adjustments(self, position: CandleTradeCandidate, current_price: float) -> Dict:
        """💰 수익률 기반 동적 조정 계산 (마이너스 상황 포함)"""
        try:
            if not position.performance.entry_price:
                return {'target_multiplier': 1.0, 'stop_tightening': 1.0}

            # 현재 수익률 계산
            pnl_pct = ((current_price - position.performance.entry_price) / position.performance.entry_price) * 100

            # 🆕 수익률별 동적 조정 (마이너스 구간 추가)
            if pnl_pct >= 5.0:
                # 5% 이상 수익: 목표 1.5배 확장, 손절 50% 강화
                return {'target_multiplier': 1.5, 'stop_tightening': 0.5, 'reason': '고수익구간'}
            elif pnl_pct >= 3.0:
                # 3% 이상 수익: 목표 1.3배 확장, 손절 70% 강화
                return {'target_multiplier': 1.3, 'stop_tightening': 0.7, 'reason': '수익구간'}
            elif pnl_pct >= 1.0:
                # 1% 이상 수익: 목표 1.1배 확장, 손절 80% 강화
                return {'target_multiplier': 1.1, 'stop_tightening': 0.8, 'reason': '소폭수익'}
            elif pnl_pct >= -1.0:
                # 소폭 마이너스(-1% 이내): 기본 설정 유지
                return {'target_multiplier': 1.0, 'stop_tightening': 1.0, 'reason': '소폭손실'}
            elif pnl_pct >= -3.0:
                # 🆕 중간 마이너스(-3% 이내): 패턴 강화시 기회 확대
                return {'target_multiplier': 1.0, 'stop_relaxation': 1.2, 'allow_lower_stop': True, 'reason': '중간손실_회복대기'}
            elif pnl_pct >= -5.0:
                # 🆕 큰 마이너스(-5% 이내): 강한 반전 패턴시에만 기다림
                return {'target_multiplier': 1.0, 'stop_relaxation': 1.5, 'allow_lower_stop': True, 'reason': '큰손실_반전대기'}
            else:
                # 🆕 매우 큰 마이너스(-5% 초과): 매우 강한 패턴에서만 추가 대기
                return {'target_multiplier': 1.0, 'stop_relaxation': 1.8, 'allow_lower_stop': True, 'reason': '심각손실_특수패턴대기'}

        except Exception as e:
            logger.debug(f"수익률 기반 조정 계산 오류: {e}")
            return {'target_multiplier': 1.0, 'stop_tightening': 1.0}

    def _calculate_trend_based_adjustments(self, position: CandleTradeCandidate, current_price: float, ohlcv_data: Optional[Any] = None) -> Dict:
        """📈 추세 강도 기반 조정 계산"""
        try:
            # 🆕 전달받은 OHLCV 데이터만 사용 (API 호출 제거)
            if ohlcv_data is None or ohlcv_data.empty or len(ohlcv_data) < 5:
                logger.debug(f"📄 {position.stock_code} OHLCV 데이터 부족 - 추세 분석 불가")
                return {'trend_strength': 'NEUTRAL', 'trend_multiplier': 1.0}

            # 최근 5일 종가 추출
            recent_closes = []
            for _, row in ohlcv_data.head(5).iterrows():
                try:
                    close_price = float(row.get('stck_clpr', 0))
                    if close_price > 0:
                        recent_closes.append(close_price)
                except (ValueError, TypeError):
                    continue

            if len(recent_closes) < 3:
                return {'trend_strength': 'NEUTRAL', 'trend_multiplier': 1.0}

            # 추세 강도 계산 (최신가 vs 과거가 비교)
            trend_pct = ((recent_closes[0] - recent_closes[-1]) / recent_closes[-1]) * 100

            if trend_pct >= 10:
                return {'trend_strength': 'VERY_STRONG_UP', 'trend_multiplier': 1.4, 'reason': '강한상승추세'}
            elif trend_pct >= 5:
                return {'trend_strength': 'STRONG_UP', 'trend_multiplier': 1.2, 'reason': '상승추세'}
            elif trend_pct >= 2:
                return {'trend_strength': 'WEAK_UP', 'trend_multiplier': 1.1, 'reason': '약한상승'}
            elif trend_pct <= -5:
                return {'trend_strength': 'STRONG_DOWN', 'trend_multiplier': 0.8, 'reason': '하락추세'}
            else:
                return {'trend_strength': 'NEUTRAL', 'trend_multiplier': 1.0, 'reason': '중립'}

        except Exception as e:
            logger.debug(f"추세 분석 오류: {e}")
            return {'trend_strength': 'NEUTRAL', 'trend_multiplier': 1.0}

    def _apply_dynamic_adjustments(self, position: CandleTradeCandidate, current_price: float,
                                 pattern_update: Dict, profit_update: Dict, trend_update: Dict):
        """🎯 동적 조정 적용 (마이너스 상황 특수 로직 포함)"""
        try:
            entry_price = position.performance.entry_price
            if not entry_price:
                return

            # 🆕 현재 목표가/손절가 백업
            original_target = position.risk_management.target_price
            original_stop = position.risk_management.stop_loss_price

            # 🆕 1단계: 패턴 기반 기본 목표/손절 재계산
            if pattern_update.get('pattern_strength_changed'):
                new_target_pct, new_stop_pct = self._get_pattern_tier_targets(pattern_update['strength_tier'])
            else:
                # 기존 설정 유지를 위한 역계산
                new_target_pct = ((original_target - entry_price) / entry_price) * 100
                new_stop_pct = ((entry_price - original_stop) / entry_price) * 100

            # 🆕 2단계: 수익률 기반 조정 적용 (마이너스 로직 추가)
            target_multiplier = profit_update.get('target_multiplier', 1.0)

            # 마이너스 상황에서의 특수 처리
            if profit_update.get('stop_relaxation'):
                # 손절 완화 적용 (마이너스 상황)
                stop_relaxation = profit_update.get('stop_relaxation', 1.0)
                adjusted_stop_pct = new_stop_pct * stop_relaxation
                allow_lower_stop = profit_update.get('allow_lower_stop', False)
            else:
                # 기존 로직 (수익 상황)
                stop_tightening = profit_update.get('stop_tightening', 1.0)
                adjusted_stop_pct = new_stop_pct * stop_tightening
                allow_lower_stop = False

            adjusted_target_pct = new_target_pct * target_multiplier

            # 🆕 3단계: 추세 기반 조정 적용
            trend_multiplier = trend_update.get('trend_multiplier', 1.0)
            final_target_pct = adjusted_target_pct * trend_multiplier

            # 🆕 4단계: 새로운 목표가/손절가 계산
            new_target_price = entry_price * (1 + final_target_pct / 100)
            new_stop_price = entry_price * (1 - adjusted_stop_pct / 100)

            # 🆕 5단계: 패턴 강도 기반 마이너스 특수 조건 검사
            strong_reversal_pattern = False
            if pattern_update.get('pattern_strength_changed'):
                strongest_pattern_obj = pattern_update.get('strongest_pattern')
                pattern_tier = pattern_update.get('strength_tier', '')

                # CandlePatternInfo 객체에서 직접 속성 접근
                if strongest_pattern_obj:
                    pattern_strength = strongest_pattern_obj.strength

                    # 강한 반전 패턴 감지 (STRONG 이상)
                    if pattern_tier in ['ULTRA_STRONG', 'STRONG'] and pattern_strength >= 80:
                        strong_reversal_pattern = True

            # 🆕 6단계: 안전성 검증 및 적용 (마이너스 로직 추가)
            # 목표가 업데이트
            if new_target_price > original_target:
                position.risk_management.target_price = new_target_price
                target_updated = True
            else:
                target_updated = False

            # 🆕 손절가 업데이트 (마이너스 상황에서 조건부 하향 허용)
            if allow_lower_stop and strong_reversal_pattern:
                # 🎯 마이너스 + 강한 반전 패턴: 손절가 하향 조정 허용
                if new_stop_price != original_stop:  # 변경이 있을 때만
                    position.risk_management.stop_loss_price = new_stop_price
                    stop_updated = True
                    logger.info(f"🔄 {position.stock_code} 마이너스 특수조정: 강한 반전패턴으로 손절가 완화")
                else:
                    stop_updated = False
            else:
                # 기존 로직: 상향만 허용
                if new_stop_price > original_stop:
                    position.risk_management.stop_loss_price = new_stop_price
                    stop_updated = True
                else:
                    stop_updated = False

            # 🆕 6단계: 변경사항 로깅
            if target_updated or stop_updated:
                pnl_pct = ((current_price - entry_price) / entry_price) * 100

                logger.info(f"🔄 {position.stock_code} 동적 조정 적용 (수익률: {pnl_pct:+.1f}%):")

                if target_updated:
                    logger.info(f"   📈 목표가: {original_target:,.0f}원 → {new_target_price:,.0f}원 "
                               f"({((new_target_price - entry_price) / entry_price * 100):+.1f}%)")

                if stop_updated:
                    logger.info(f"   🛡️ 손절가: {original_stop:,.0f}원 → {new_stop_price:,.0f}원 "
                               f"({((entry_price - new_stop_price) / entry_price * 100):+.1f}%)")

                # 조정 사유 로깅
                reasons = []
                if pattern_update.get('pattern_strength_changed'):
                    reasons.append(f"패턴강도: {pattern_update['strength_tier']}")
                if profit_update.get('reason'):
                    reasons.append(f"수익: {profit_update['reason']}")
                if trend_update.get('reason'):
                    reasons.append(f"추세: {trend_update['reason']}")

                if reasons:
                    logger.info(f"   📋 조정사유: {', '.join(reasons)}")

        except Exception as e:
            logger.error(f"동적 조정 적용 오류 ({position.stock_code}): {e}")

    def _get_pattern_tier_targets(self, strength_tier: str) -> Tuple[float, float]:
        """패턴 강도 티어별 목표/손절 퍼센트 반환"""
        tier_settings = {
            'ULTRA_STRONG': (15.0, 4.0),   # 15% 목표, 4% 손절
            'STRONG': (12.0, 3.0),         # 12% 목표, 3% 손절
            'MEDIUM': (8.0, 3.0),          # 8% 목표, 3% 손절
            'WEAK': (5.0, 2.0),            # 5% 목표, 2% 손절
            'VERY_WEAK': (3.0, 1.5)        # 3% 목표, 1.5% 손절
        }
        return tier_settings.get(strength_tier, (5.0, 2.0))

    def _fallback_trailing_stop(self, position: CandleTradeCandidate, current_price: float):
        """기존 방식 추적 손절 (폴백용)"""
        try:
            trailing_pct = position.risk_management.trailing_stop_pct / 100
            new_trailing_stop = current_price * (1 - trailing_pct)

            # 기존 손절가보다 높을 때만 업데이트
            if new_trailing_stop > position.risk_management.stop_loss_price:
                position.risk_management.stop_loss_price = new_trailing_stop
                logger.debug(f"📈 {position.stock_code} 기본 추적손절 업데이트: {new_trailing_stop:,.0f}원")

        except Exception as e:
            logger.error(f"기본 추적 손절 오류: {e}")
