"""
캔들 전략 리스크 관리 전용 모듈
"""
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any
from utils.logger import setup_logger

from .candle_trade_candidate import (
    CandleTradeCandidate, PatternType, RiskManagement, CandleStatus
)
from .candle_pattern_detector import CandlePatternDetector

logger = setup_logger(__name__)


class CandleRiskManager:
    """캔들 전략 리스크 관리 전용 클래스"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.korea_tz = timezone(timedelta(hours=9))

        # 리스크 관리 기본 설정
        self.risk_config = {
            'max_position_size_pct': config.get('max_position_size_pct', 30),
            'default_stop_loss_pct': config.get('default_stop_loss_pct', 1.8),
            'default_target_profit_pct': config.get('default_target_profit_pct', 3),
            'max_holding_hours': config.get('max_holding_hours', 6),
            'pattern_targets': config.get('pattern_targets', {}),
            'time_exit_rules': config.get('time_exit_rules', {}),
            'investment_calculation': config.get('investment_calculation', {})
        }

        logger.info("✅ CandleRiskManager 초기화 완료")

    # ==========================================
    # 🎯 메인 리스크 관리 메서드
    # ==========================================

    def calculate_risk_management(self, candidate: CandleTradeCandidate) -> RiskManagement:
        """리스크 관리 설정 계산"""
        try:
            current_price = candidate.current_price

            # 패턴별 포지션 크기 조정
            if candidate.primary_pattern:
                pattern_type = candidate.primary_pattern.pattern_type
                confidence = candidate.primary_pattern.confidence

                # 강한 패턴일수록 큰 포지션
                if pattern_type in [PatternType.MORNING_STAR, PatternType.BULLISH_ENGULFING]:
                    base_position_pct = min(30, self.risk_config['max_position_size_pct'])
                elif pattern_type in [PatternType.HAMMER, PatternType.INVERTED_HAMMER]:
                    base_position_pct = 20
                else:
                    base_position_pct = 15

                # 신뢰도에 따른 조정
                position_size_pct = base_position_pct * confidence
            else:
                position_size_pct = 10

            # 손절가/목표가 계산
            stop_loss_pct = self.risk_config['default_stop_loss_pct']
            target_profit_pct = self.risk_config['default_target_profit_pct']

            # 패턴별 목표 설정 적용
            if candidate.primary_pattern:
                pattern_name = candidate.primary_pattern.pattern_type.value.lower()
                pattern_config = self.risk_config['pattern_targets'].get(pattern_name)

                if pattern_config:
                    target_profit_pct = pattern_config['target']
                    stop_loss_pct = pattern_config['stop']
                    logger.debug(f"📊 {candidate.stock_code} 패턴별 목표 적용: {pattern_name}")
                else:
                    # 패턴 강도에 따른 기본 조정
                    if candidate.primary_pattern.pattern_type == PatternType.MORNING_STAR:
                        target_profit_pct = 2.5
                        stop_loss_pct = 1.5
                    elif candidate.primary_pattern.pattern_type == PatternType.BULLISH_ENGULFING:
                        target_profit_pct = 2.0
                        stop_loss_pct = 1.5
                    elif candidate.primary_pattern.pattern_type == PatternType.HAMMER:
                        target_profit_pct = 1.5
                        stop_loss_pct = 1.5

            stop_loss_price = current_price * (1 - stop_loss_pct / 100)
            target_price = current_price * (1 + target_profit_pct / 100)

            # 추적 손절 설정
            trailing_stop_pct = stop_loss_pct * 0.6

            # 최대 보유 시간 (패턴별 조정)
            max_holding_hours = self.risk_config['max_holding_hours']
            if candidate.primary_pattern:
                pattern_name = candidate.primary_pattern.pattern_type.value.lower()
                pattern_config = self.risk_config['pattern_targets'].get(pattern_name)

                if pattern_config and 'max_hours' in pattern_config:
                    max_holding_hours = pattern_config['max_hours']

            # 위험도 점수 계산
            risk_score = self._calculate_risk_score({'stck_prpr': current_price})

            return RiskManagement(
                position_size_pct=position_size_pct,
                position_amount=0,  # 실제 투자금액은 진입시 계산
                stop_loss_price=stop_loss_price,
                target_price=target_price,
                trailing_stop_pct=trailing_stop_pct,
                max_holding_hours=max_holding_hours,
                risk_score=risk_score
            )

        except Exception as e:
            logger.error(f"리스크 관리 계산 오류: {e}")
            return RiskManagement(0, 0, 0, 0, 0, 8, 100)

    def calculate_position_amount(self, candidate: CandleTradeCandidate,
                                total_available: float) -> Tuple[int, int]:
        """포지션 크기 및 수량 계산"""
        try:
            current_price = candidate.current_price

            # 포지션별 실제 투자 금액 계산
            position_amount = int(total_available * candidate.risk_management.position_size_pct / 100)

            # 매수 수량 계산 (소수점 이하 버림)
            quantity = position_amount // current_price

            return position_amount, quantity

        except Exception as e:
            logger.error(f"포지션 크기 계산 오류: {e}")
            return 0, 0

    def get_pattern_based_target(self, position: CandleTradeCandidate) -> Tuple[float, float, int, bool]:
        """🎯 캔들 패턴별 수익률 목표, 손절, 시간 설정 결정"""
        try:
            # 1. 캔들 전략으로 매수한 종목인지 확인
            is_candle_strategy = (
                position.metadata.get('restored_from_db', False) or
                position.metadata.get('original_entry_source') == 'candle_strategy' or
                len(position.detected_patterns) > 0
            )

            if not is_candle_strategy:
                # 수동/앱 매수 종목: 큰 수익/손실 허용
                logger.debug(f"📊 {position.stock_code} 수동 매수 종목 - 기본 설정 적용")
                return 10.0, 5.0, 24, False

            # 2. 실시간 캔들 패턴 재분석
            original_pattern = None

            try:
                from ..api.kis_market_api import get_inquire_daily_itemchartprice
                ohlcv_data = get_inquire_daily_itemchartprice(
                    output_dv="2",
                    itm_no=position.stock_code,
                    period_code="D",
                    adj_prc="1"
                )

                if ohlcv_data is not None and not ohlcv_data.empty:
                    pattern_detector = CandlePatternDetector()
                    pattern_result = pattern_detector.analyze_stock_patterns(position.stock_code, ohlcv_data)
                    if pattern_result and len(pattern_result) > 0:
                        strongest_pattern = max(pattern_result, key=lambda p: p.strength)
                        original_pattern = strongest_pattern.pattern_type.value
                        logger.debug(f"🔄 {position.stock_code} 실시간 패턴 분석: {original_pattern}")
            except Exception as e:
                logger.debug(f"실시간 패턴 분석 오류 ({position.stock_code}): {e}")

            # DB에서 복원된 경우 (백업)
            if not original_pattern and 'original_pattern_type' in position.metadata:
                original_pattern = position.metadata['original_pattern_type']
                logger.debug(f"📚 {position.stock_code} DB에서 패턴 복원: {original_pattern}")

            # 기존 패턴 정보 활용 (백업)
            elif not original_pattern and position.detected_patterns:
                strongest_pattern = max(position.detected_patterns, key=lambda p: p.strength)
                original_pattern = strongest_pattern.pattern_type.value
                logger.debug(f"📊 {position.stock_code} 기존 패턴 정보 활용: {original_pattern}")

            # 3. 패턴별 목표, 손절, 시간 설정 적용
            if original_pattern:
                pattern_key = original_pattern.lower().replace('_', '_')
                pattern_config = self.risk_config['pattern_targets'].get(pattern_key)

                if pattern_config:
                    target_pct = pattern_config['target']
                    stop_pct = pattern_config['stop']
                    max_hours = pattern_config['max_hours']

                    logger.debug(f"📊 {position.stock_code} 패턴 '{original_pattern}' - "
                                f"목표:{target_pct}%, 손절:{stop_pct}%, 시간:{max_hours}h")
                    return target_pct, stop_pct, max_hours, True
                else:
                    # 패턴 config에 없으면 패턴 강도에 따라 결정
                    if position.detected_patterns:
                        strongest_pattern = max(position.detected_patterns, key=lambda p: p.strength)
                        if strongest_pattern.strength >= 90:
                            target_pct, stop_pct, max_hours = 15.0, 4.0, 8
                        elif strongest_pattern.strength >= 80:
                            target_pct, stop_pct, max_hours = 12.0, 3.0, 6
                        elif strongest_pattern.strength >= 70:
                            target_pct, stop_pct, max_hours = 8.0, 3.0, 4
                        else:
                            target_pct, stop_pct, max_hours = 5.0, 2.0, 2

                        logger.debug(f"📊 {position.stock_code} 패턴 강도 {strongest_pattern.strength}")
                        return target_pct, stop_pct, max_hours, True

            # 4. 기본값: 캔들 전략이지만 패턴 정보 없음
            logger.debug(f"📊 {position.stock_code} 캔들 전략이나 패턴 정보 없음 - 기본 캔들 설정")
            return 10.0, 5.0, 6, True

        except Exception as e:
            logger.error(f"패턴별 설정 결정 오류 ({position.stock_code}): {e}")
            return 10.0, 5.0, 24, False

    # ==========================================
    # 🔄 동적 리스크 관리 메서드들
    # ==========================================

    def update_trailing_stop(self, position: CandleTradeCandidate, current_price: float):
        """🔄 패턴 기반 동적 목표/손절 조정 시스템"""
        try:
            # OHLCV 데이터 조회
            from ..api.kis_market_api import get_inquire_daily_itemchartprice
            ohlcv_data = get_inquire_daily_itemchartprice(
                output_dv="2",
                itm_no=position.stock_code,
                period_code="D",
                adj_prc="1"
            )

            # 실시간 패턴 재분석
            pattern_update = self._analyze_realtime_pattern_changes(position.stock_code, current_price, ohlcv_data)

            # 수익률 기반 동적 조정
            profit_based_update = self._calculate_profit_based_adjustments(position, current_price)

            # 추세 강도 기반 조정
            trend_based_update = self._calculate_trend_based_adjustments(position, current_price, ohlcv_data)

            # 종합 판단 및 업데이트
            self._apply_dynamic_adjustments(position, current_price, pattern_update, profit_based_update, trend_based_update)

        except Exception as e:
            logger.error(f"동적 목표/손절 조정 오류 ({position.stock_code}): {e}")
            # 기존 방식으로 폴백
            self._fallback_trailing_stop(position, current_price)

    def should_exit_position(self, position: CandleTradeCandidate) -> bool:
        """포지션 매도 조건 확인"""
        try:
            if not position.performance.entry_price:
                return False

            current_price = position.current_price
            entry_price = position.performance.entry_price

            # 패턴별 설정 가져오기
            target_profit_pct, stop_loss_pct, max_hours, pattern_based = self.get_pattern_based_target(position)

            # 손절 체크 (패턴별)
            pnl_pct = ((current_price - entry_price) / entry_price) * 100
            if pnl_pct <= -stop_loss_pct:
                return True

            # 익절 체크 (패턴별)
            if pnl_pct >= target_profit_pct:
                return True

            # 기존 목표가/손절가 도달
            if current_price <= position.risk_management.stop_loss_price:
                return True
            if current_price >= position.risk_management.target_price:
                return True

            # 시간 청산 체크 (패턴별)
            if self.should_time_exit_pattern_based(position, max_hours):
                return True

            return False

        except Exception as e:
            logger.debug(f"매도 조건 확인 오류: {e}")
            return False

    def should_time_exit_pattern_based(self, position: CandleTradeCandidate, max_hours: int) -> bool:
        """🆕 패턴별 시간 청산 조건 체크"""
        try:
            if not position.performance.entry_time:
                return False

            # 보유 시간 계산
            holding_time = datetime.now() - position.performance.entry_time
            max_holding = timedelta(hours=max_hours)

            # 패턴별 최대 보유시간 초과시 청산
            if holding_time >= max_holding:
                logger.info(f"⏰ {position.stock_code} 패턴별 최대 보유시간({max_hours}h) 초과 청산")
                return True

            # 시간 기반 청산 규칙 적용
            time_rules = self.risk_config.get('time_exit_rules', {})

            # 수익 중 시간 청산 (패턴별 시간의 절반 후)
            profit_exit_hours = max_hours // 2
            min_profit = time_rules.get('min_profit_for_time_exit', 0.5) / 100

            if (holding_time >= timedelta(hours=profit_exit_hours) and
                position.performance.pnl_pct and
                position.performance.pnl_pct >= min_profit):
                logger.info(f"⏰ {position.stock_code} 패턴별 시간 기반 수익 청산")
                return True

            return False

        except Exception as e:
            logger.error(f"패턴별 시간 청산 체크 오류: {e}")
            return False

    def calculate_safe_sell_price(self, current_price: float, reason: str) -> int:
        """안전한 매도가 계산 (틱 단위 맞춤)"""
        try:
            # 매도 이유별 할인율 적용
            if reason == "손절":
                discount_pct = 0.008  # 0.8% 할인
            elif reason in ["목표가 도달", "익절"]:
                discount_pct = 0.003  # 0.3% 할인
            elif reason == "시간 청산":
                discount_pct = 0.005  # 0.5% 할인
            else:
                discount_pct = 0.005  # 기본 0.5% 할인

            # 할인된 가격 계산
            target_price = int(current_price * (1 - discount_pct))

            # 틱 단위 맞춤
            tick_unit = self._get_tick_unit(target_price)
            safe_price = (target_price // tick_unit) * tick_unit

            # 최소 가격 보정
            min_price = int(current_price * 0.97)
            safe_price = max(safe_price, min_price)

            logger.debug(f"💰 매도가 계산: 현재가{current_price:,.0f}원 → 주문가{safe_price:,.0f}원")
            return safe_price

        except Exception as e:
            logger.error(f"안전한 매도가 계산 오류: {e}")
            return int(current_price * 0.99)

    # ==========================================
    # 🔧 내부 계산 메서드들
    # ==========================================

    def _calculate_risk_score(self, stock_info: dict) -> int:
        """위험도 점수 계산 (0-100)"""
        try:
            risk_score = 50  # 기본 점수

            current_price = float(stock_info.get('stck_prpr', 0))
            change_rate = float(stock_info.get('prdy_ctrt', 0))

            # 가격대별 위험도
            if current_price < 5000:
                risk_score += 20  # 저가주 위험
            elif current_price > 100000:
                risk_score += 10  # 고가주 위험

            # 변동률별 위험도
            if abs(change_rate) > 10:
                risk_score += 30  # 급등락 위험
            elif abs(change_rate) > 5:
                risk_score += 15

            return min(100, max(0, risk_score))
        except:
            return 50

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
            return 100

    def _analyze_realtime_pattern_changes(self, stock_code: str, current_price: float,
                                        ohlcv_data: Optional[Any] = None) -> Dict:
        """🔄 실시간 캔들 패턴 변화 분석"""
        try:
            if ohlcv_data is None or ohlcv_data.empty:
                return {'pattern_strength_changed': False, 'new_patterns': []}

            # 현재 패턴 분석
            pattern_detector = CandlePatternDetector()
            current_patterns = pattern_detector.analyze_stock_patterns(stock_code, ohlcv_data)

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
            return 'ULTRA_STRONG'
        elif strength >= 80:
            return 'STRONG'
        elif strength >= 70:
            return 'MEDIUM'
        elif strength >= 60:
            return 'WEAK'
        else:
            return 'VERY_WEAK'

    def _calculate_profit_based_adjustments(self, position: CandleTradeCandidate, current_price: float) -> Dict:
        """💰 수익률 기반 동적 조정 계산"""
        try:
            if not position.performance.entry_price:
                return {'target_multiplier': 1.0, 'stop_tightening': 1.0}

            # 현재 수익률 계산
            pnl_pct = ((current_price - position.performance.entry_price) / position.performance.entry_price) * 100

            # 수익률별 동적 조정
            if pnl_pct >= 5.0:
                return {'target_multiplier': 1.5, 'stop_tightening': 0.5, 'reason': '고수익구간'}
            elif pnl_pct >= 3.0:
                return {'target_multiplier': 1.3, 'stop_tightening': 0.7, 'reason': '수익구간'}
            elif pnl_pct >= 1.0:
                return {'target_multiplier': 1.1, 'stop_tightening': 0.8, 'reason': '소폭수익'}
            elif pnl_pct >= -1.0:
                return {'target_multiplier': 1.0, 'stop_tightening': 1.0, 'reason': '소폭손실'}
            elif pnl_pct >= -3.0:
                return {'target_multiplier': 1.0, 'stop_relaxation': 1.2, 'allow_lower_stop': True, 'reason': '중간손실_회복대기'}
            elif pnl_pct >= -5.0:
                return {'target_multiplier': 1.0, 'stop_relaxation': 1.5, 'allow_lower_stop': True, 'reason': '큰손실_반전대기'}
            else:
                return {'target_multiplier': 1.0, 'stop_relaxation': 1.8, 'allow_lower_stop': True, 'reason': '심각손실_특수패턴대기'}

        except Exception as e:
            logger.debug(f"수익률 기반 조정 계산 오류: {e}")
            return {'target_multiplier': 1.0, 'stop_tightening': 1.0}

    def _calculate_trend_based_adjustments(self, position: CandleTradeCandidate, current_price: float,
                                         ohlcv_data: Optional[Any] = None) -> Dict:
        """📈 추세 강도 기반 조정 계산"""
        try:
            if ohlcv_data is None or ohlcv_data.empty or len(ohlcv_data) < 5:
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

            # 추세 강도 계산
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
        """🎯 동적 조정 적용"""
        try:
            entry_price = position.performance.entry_price
            if not entry_price:
                return

            # 현재 목표가/손절가 백업
            original_target = position.risk_management.target_price
            original_stop = position.risk_management.stop_loss_price

            # 패턴 기반 기본 목표/손절 재계산
            if pattern_update.get('pattern_strength_changed'):
                new_target_pct, new_stop_pct = self._get_pattern_tier_targets(pattern_update['strength_tier'])
            else:
                new_target_pct = ((original_target - entry_price) / entry_price) * 100
                new_stop_pct = ((entry_price - original_stop) / entry_price) * 100

            # 수익률 기반 조정 적용
            target_multiplier = profit_update.get('target_multiplier', 1.0)

            if profit_update.get('stop_relaxation'):
                stop_relaxation = profit_update.get('stop_relaxation', 1.0)
                adjusted_stop_pct = new_stop_pct * stop_relaxation
                allow_lower_stop = profit_update.get('allow_lower_stop', False)
            else:
                stop_tightening = profit_update.get('stop_tightening', 1.0)
                adjusted_stop_pct = new_stop_pct * stop_tightening
                allow_lower_stop = False

            adjusted_target_pct = new_target_pct * target_multiplier

            # 추세 기반 조정 적용
            trend_multiplier = trend_update.get('trend_multiplier', 1.0)
            final_target_pct = adjusted_target_pct * trend_multiplier

            # 새로운 목표가/손절가 계산
            new_target_price = entry_price * (1 + final_target_pct / 100)
            new_stop_price = entry_price * (1 - adjusted_stop_pct / 100)

            # 패턴 강도 기반 특수 조건 검사
            strong_reversal_pattern = False
            if pattern_update.get('pattern_strength_changed'):
                pattern_tier = pattern_update.get('strength_tier', '')
                if pattern_tier in ['ULTRA_STRONG', 'STRONG']:
                    strong_reversal_pattern = True

            # 안전성 검증 및 적용
            target_updated = False
            stop_updated = False

            # 목표가 업데이트
            if new_target_price > original_target:
                position.risk_management.target_price = new_target_price
                target_updated = True

            # 손절가 업데이트
            if allow_lower_stop and strong_reversal_pattern:
                if new_stop_price != original_stop:
                    position.risk_management.stop_loss_price = new_stop_price
                    stop_updated = True
            else:
                if new_stop_price > original_stop:
                    position.risk_management.stop_loss_price = new_stop_price
                    stop_updated = True

            # 변경사항 로깅
            if target_updated or stop_updated:
                pnl_pct = ((current_price - entry_price) / entry_price) * 100
                logger.info(f"🔄 {position.stock_code} 동적 조정 적용 (수익률: {pnl_pct:+.1f}%)")

        except Exception as e:
            logger.error(f"동적 조정 적용 오류 ({position.stock_code}): {e}")

    def _get_pattern_tier_targets(self, strength_tier: str) -> Tuple[float, float]:
        """패턴 강도 티어별 목표/손절 퍼센트 반환"""
        tier_settings = {
            'ULTRA_STRONG': (15.0, 4.0),
            'STRONG': (12.0, 3.0),
            'MEDIUM': (8.0, 3.0),
            'WEAK': (5.0, 2.0),
            'VERY_WEAK': (3.0, 1.5)
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
                logger.debug(f"📈 {position.stock_code} 기본 추적손절 업데이트")

        except Exception as e:
            logger.error(f"기본 추적 손절 오류: {e}")
