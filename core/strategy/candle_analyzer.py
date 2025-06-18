"""
캔들 분석 전용 클래스
기술적 지표, 패턴 분석, 종합 신호 분석 등을 담당
"""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd

from .candle_trade_candidate import (
    CandleTradeCandidate, PatternType, TradeSignal, CandlePatternInfo, CandleStatus
)
from .candle_pattern_detector import CandlePatternDetector
from utils.logger import setup_logger

logger = setup_logger(__name__)


def calculate_business_hours_analyzer(start_time: datetime, end_time: datetime) -> float:
    """🕒 실제 거래시간만 계산 (9:00-15:30, 주말 제외) - candle_analyzer용"""
    try:
        # 시작시간이 종료시간보다 늦으면 0 반환
        if start_time >= end_time:
            return 0.0

        # 거래시간 설정 (9:00 - 15:30)
        TRADING_START_HOUR = 9
        TRADING_START_MINUTE = 0
        TRADING_END_HOUR = 15
        TRADING_END_MINUTE = 30

        total_hours = 0.0
        current = start_time

        # 하루씩 계산하면서 주말 및 장외시간 제외
        while current < end_time:
            # 현재 날짜의 요일 확인 (0=월요일, 6=일요일)
            weekday = current.weekday()

            # 주말(토요일=5, 일요일=6) 제외
            if weekday < 5:  # 월~금요일만
                # 이 날의 거래시간 범위 설정
                trading_start = current.replace(
                    hour=TRADING_START_HOUR, 
                    minute=TRADING_START_MINUTE, 
                    second=0, 
                    microsecond=0
                )
                trading_end = current.replace(
                    hour=TRADING_END_HOUR, 
                    minute=TRADING_END_MINUTE, 
                    second=0, 
                    microsecond=0
                )

                # 실제 계산할 시간 범위 결정
                day_start = max(current, trading_start)  # 거래시작 이후부터
                day_finish = min(end_time, trading_end)   # 거래종료 이전까지

                # 유효한 거래시간이 있는 경우에만 계산
                if day_start < day_finish:
                    day_hours = (day_finish - day_start).total_seconds() / 3600
                    total_hours += day_hours
                    
                    # 디버깅용 로그 (상세)
                    logger.debug(f"📊 거래시간 계산: {day_start.strftime('%m/%d %H:%M')} ~ "
                               f"{day_finish.strftime('%m/%d %H:%M')} = {day_hours:.2f}h")

            # 다음 날로 이동
            current = (current + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

        logger.debug(f"📊 총 거래시간: {total_hours:.2f}h (거래시간 9:00-15:30, 주말제외)")
        return total_hours

    except Exception as e:
        logger.error(f"❌ 거래시간 계산 오류: {e}")
        # 오류시 기존 방식으로 폴백 (주말만 제외)
        try:
            total_hours = 0.0
            current = start_time
            
            while current < end_time:
                weekday = current.weekday()
                if weekday < 5:  # 월~금요일만
                    day_end = current.replace(hour=23, minute=59, second=59, microsecond=999999)
                    day_start = current
                    day_finish = min(day_end, end_time)
                    day_hours = (day_finish - day_start).total_seconds() / 3600
                    total_hours += day_hours
                current = (current + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            
            logger.warning(f"⚠️ 거래시간 계산 폴백 사용: {total_hours:.2f}h")
            return total_hours
        except:
            return (end_time - start_time).total_seconds() / 3600


class CandleAnalyzer:
    """캔들 패턴 및 기술적 지표 분석 전용 클래스"""

    def __init__(self, pattern_detector: CandlePatternDetector, config: Dict, korea_tz: timezone):
        """
        Args:
            pattern_detector: 캔들 패턴 감지기
            config: 설정 딕셔너리
            korea_tz: 한국 시간대
        """
        self.pattern_detector = pattern_detector
        self.config = config
        self.korea_tz = korea_tz

        logger.info("✅ CandleAnalyzer 초기화 완료")

    async def analyze_current_patterns(self, stock_code: str, current_price: float, ohlcv_data: Optional[Any]) -> Dict:
        """📊 최신 캔들 패턴 분석"""
        try:
            # 전달받은 OHLCV 데이터 사용
            if ohlcv_data is None or ohlcv_data.empty:
                return {'signal': 'neutral', 'strength': 0, 'patterns': []}

            # 패턴 감지
            pattern_result = self.pattern_detector.analyze_stock_patterns(stock_code, ohlcv_data)

            if not pattern_result:
                return {'signal': 'neutral', 'strength': 0, 'patterns': []}

            # 가장 강한 패턴 기준
            strongest_pattern = max(pattern_result, key=lambda p: p.strength)

            # 패턴 기반 신호 생성
            if strongest_pattern.pattern_type in [PatternType.HAMMER, PatternType.INVERTED_HAMMER,
                                                PatternType.BULLISH_ENGULFING,
                                                PatternType.RISING_THREE_METHODS]:
                signal = 'bullish'
            elif strongest_pattern.pattern_type in [PatternType.BEARISH_ENGULFING, PatternType.EVENING_STAR,
                                                   PatternType.FALLING_THREE_METHODS]:
                signal = 'bearish'
            else:
                signal = 'neutral'

            return {
                'signal': signal,
                'strength': strongest_pattern.strength,
                'confidence': strongest_pattern.confidence,
                'patterns': pattern_result,
                'primary_pattern': strongest_pattern.pattern_type.value
            }

        except Exception as e:
            logger.debug(f"패턴 분석 오류 ({stock_code}): {e}")
            return {'signal': 'neutral', 'strength': 0, 'patterns': []}

    async def analyze_technical_indicators(self, stock_code: str, current_price: float, ohlcv_data: Optional[Any]) -> Dict:
        """📈 기술적 지표 분석"""
        try:
            # 전달받은 OHLCV 데이터 사용
            if ohlcv_data is None or ohlcv_data.empty or len(ohlcv_data) < 20:
                return {'signal': 'neutral', 'rsi': 50.0, 'trend': 'neutral'}

            # 종가 데이터 추출
            close_prices = []
            for _, row in ohlcv_data.head(20).iterrows():  # 최근 20일
                try:
                    close_price = float(row.get('stck_clpr', 0))
                    if close_price > 0:
                        close_prices.append(close_price)
                except (ValueError, TypeError):
                    continue

            if len(close_prices) < 14:
                return {'signal': 'neutral', 'rsi': 50.0, 'trend': 'neutral'}

            # RSI 계산
            from ..analysis.technical_indicators import TechnicalIndicators
            rsi_values = TechnicalIndicators.calculate_rsi(close_prices)
            current_rsi = rsi_values[-1] if rsi_values else 50.0

            # 이동평균 추세
            if len(close_prices) >= 5:
                ma_5 = sum(close_prices[:5]) / 5
                ma_20 = sum(close_prices[:20]) / 20 if len(close_prices) >= 20 else ma_5

                if current_price > ma_5 > ma_20:
                    trend = 'uptrend'
                elif current_price < ma_5 < ma_20:
                    trend = 'downtrend'
                else:
                    trend = 'neutral'
            else:
                trend = 'neutral'

            # 종합 신호
            # 🔧 config에서 RSI 임계값 가져오기
            rsi_oversold = self.config.get('rsi_oversold_threshold', 30)
            rsi_overbought = self.config.get('rsi_overbought_threshold', 70)

            if current_rsi < rsi_oversold and trend in ['uptrend', 'neutral']:
                signal = 'oversold_bullish'
            elif current_rsi > rsi_overbought and trend in ['downtrend', 'neutral']:
                signal = 'overbought_bearish'
            elif current_rsi < rsi_oversold:
                signal = 'oversold'
            elif current_rsi > rsi_overbought:
                signal = 'overbought'
            else:
                signal = 'neutral'

            return {
                'signal': signal,
                'rsi': current_rsi,
                'trend': trend,
                'ma_5': ma_5 if 'ma_5' in locals() else current_price,
                'ma_20': ma_20 if 'ma_20' in locals() else current_price
            }

        except Exception as e:
            logger.debug(f"기술적 지표 분석 오류 ({stock_code}): {e}")
            return {'signal': 'neutral', 'rsi': 50.0, 'trend': 'neutral'}

    def analyze_time_conditions(self, candidate: CandleTradeCandidate) -> Dict:
        """🕯️ 단순화된 시간 조건 분석 - 24시간 내에는 시간 압박 없음"""
        try:
            # 기본값 설정
            signal = 'normal'
            time_pressure = 'low'
            trading_hours = self._is_trading_time()

            # 진입한 종목만 시간 분석
            if candidate.status != CandleStatus.ENTERED or not candidate.performance.entry_time:
                return {
                    'signal': signal,
                    'time_pressure': time_pressure,
                    'trading_hours': trading_hours,
                    'holding_duration': '0h (미진입)'
                }

            # 시간 계산
            entry_time = candidate.performance.entry_time
            current_time = datetime.now(self.korea_tz)
            if entry_time.tzinfo is None:
                entry_time = entry_time.replace(tzinfo=self.korea_tz)

            # 거래시간 기준 보유 시간 계산
            holding_hours = calculate_business_hours_analyzer(entry_time, current_time)
            holding_duration = f"{holding_hours:.1f}h (거래시간만)"

            # 패턴별 최대 보유시간 가져오기
            _, _, max_holding_hours, _ = self._get_pattern_based_target(candidate)

            logger.debug(f"⏰ {candidate.stock_code} 시간 조건: {holding_hours:.1f}h / {max_holding_hours}h")

            # 🆕 단순화된 시간 신호 로직
            if holding_hours < 24.0:
                # 24시간 이내: 시간 압박 없음
                time_pressure = 'low'
                signal = 'normal'
                logger.debug(f"⏰ {candidate.stock_code} 24시간 이내 - 시간 압박 없음")
            
            elif holding_hours >= max_holding_hours:
                # 최대 보유시간 초과: 강제 청산 신호
                time_pressure = 'high'
                signal = 'time_exit'
                logger.info(f"⏰ {candidate.stock_code} 최대 보유시간 초과 - 시간 청산 신호")
            
            elif holding_hours >= max_holding_hours * 0.9:
                # 90% 경과: 주의 신호
                time_pressure = 'medium'
                signal = 'time_caution'
                logger.debug(f"⏰ {candidate.stock_code} 보유시간 90% 경과 - 주의")
            
            else:
                # 정상 범위
                time_pressure = 'low'
                signal = 'normal'

            return {
                'signal': signal,
                'time_pressure': time_pressure,
                'trading_hours': trading_hours,
                'holding_duration': holding_duration,
                'holding_hours': holding_hours,
                'max_holding_hours': max_holding_hours
            }

        except Exception as e:
            logger.error(f"❌ {candidate.stock_code} 시간 조건 분석 오류: {e}")
            return {
                'signal': 'normal',
                'time_pressure': 'low',
                'trading_hours': True,
                'holding_duration': '0h (오류)'
            }

    def analyze_risk_conditions(self, candidate: CandleTradeCandidate, current_price: float) -> Dict:
        """💰 리스크 조건 분석"""
        try:
            if not candidate.risk_management:
                return {'signal': 'neutral', 'risk_level': 'medium'}

            # 현재 수익률 계산
            if candidate.performance.entry_price:
                pnl_pct = ((current_price - candidate.performance.entry_price) / candidate.performance.entry_price) * 100
            else:
                pnl_pct = 0.0

            # 손절/익절 조건 체크
            target_price = candidate.risk_management.target_price
            stop_loss_price = candidate.risk_management.stop_loss_price

            # 패턴별 목표/손절 기준 가져오기
            target_profit_pct, stop_loss_pct, _, _ = self._get_pattern_based_target(candidate)

            if current_price >= target_price:
                signal = 'target_reached'
                risk_level = 'profit_secure'
            elif current_price <= stop_loss_price:
                signal = 'stop_loss'
                risk_level = 'high_risk'
            elif pnl_pct >= target_profit_pct:
                signal = 'profit_target'
                risk_level = 'profit_zone'
            elif pnl_pct <= -stop_loss_pct:
                signal = 'loss_limit'
                risk_level = 'loss_zone'
            elif pnl_pct >= 1.0:
                signal = 'profit_zone'
                risk_level = 'low_risk'
            elif pnl_pct <= -1.0:
                signal = 'loss_zone'
                risk_level = 'medium_risk'
            else:
                signal = 'neutral'
                risk_level = 'medium'

            return {
                'signal': signal,
                'risk_level': risk_level,
                'pnl_pct': pnl_pct,
                'target_distance': ((target_price - current_price) / current_price * 100) if target_price else 0,
                'stop_distance': ((current_price - stop_loss_price) / current_price * 100) if stop_loss_price else 0
            }

        except Exception as e:
            logger.debug(f"리스크 조건 분석 오류: {e}")
            return {'signal': 'neutral', 'risk_level': 'medium'}



    async def quick_buy_decision(self, candidate: CandleTradeCandidate, current_data: Optional[Any] = None) -> Optional[Dict]:
        """🚀 매수 실행 가능 여부 빠른 판단 - 확정된 신호 기반, 현재 가격에서 매수 가능한지만 체크"""
        try:
            stock_code = candidate.stock_code

            # 1️⃣ 현재가격 확보 (가장 중요!)
            if current_data is None:
                from ..api.kis_market_api import get_inquire_price
                current_data = get_inquire_price("J", stock_code)

            if current_data is None or current_data.empty:
                return None

            current_price = float(current_data.iloc[0].get('stck_prpr', 0))
            if current_price <= 0:
                return None

            # 현재가 업데이트
            candidate.update_price(current_price)

            logger.debug(f"🔍 {stock_code} 매수 실행 가능성 판단: 현재가 {current_price:,}원")

            # 2️⃣ 빠른 기본 조건 체크 (실패시 즉시 리턴)
            basic_check = self._check_basic_buy_conditions(candidate, current_price, current_data)
            if not basic_check['passed']:
                logger.debug(f"❌ {stock_code} 기본 조건 실패: {basic_check['fail_reason']}")
                return {
                    'buy_decision': 'reject',
                    'reason': basic_check['fail_reason'],
                    'current_price': current_price,
                    'analysis_type': 'quick_buy_decision'
                }

            # 3️⃣ 시가 근처 매수 조건 체크 (핵심!)
            entry_timing = self._check_entry_timing_conditions(candidate, current_price, current_data)
            if not entry_timing['good_timing']:
                logger.debug(f"⏰ {stock_code} 진입 타이밍 부적절: {entry_timing['reason']}")
                return {
                    'buy_decision': 'wait',
                    'reason': entry_timing['reason'],
                    'current_price': current_price,
                    'analysis_type': 'quick_buy_decision',
                    'timing_info': entry_timing
                }

            # 4️⃣ 기존 패턴 존재 확인 (재검증 아님)
            pattern_validity = self._validate_existing_patterns(candidate, current_price)
            if not pattern_validity['valid']:
                logger.debug(f"📊 {stock_code} 패턴 정보 없음: {pattern_validity['reason']}")
                return {
                    'buy_decision': 'reject',
                    'reason': pattern_validity['reason'],
                    'current_price': current_price,
                    'analysis_type': 'quick_buy_decision'
                }

            # 5️⃣ 최종 매수 결정 (확정된 신호 + 현재 조건 기반)
            buy_score = self._calculate_quick_buy_score(candidate, current_price, entry_timing, pattern_validity)
            
            # 임계값 체크
            min_buy_score = self.config.get('trading_thresholds', {}).get('min_quick_buy_score', 75)
            
            if buy_score >= min_buy_score:
                decision = 'buy'
                logger.info(f"✅ {stock_code} 매수 실행 가능: 점수 {buy_score}/100 (임계값: {min_buy_score})")
            else:
                decision = 'wait'
                logger.debug(f"⏸️ {stock_code} 매수 대기: 점수 {buy_score}/100 (임계값: {min_buy_score})")

            return {
                'buy_decision': decision,
                'buy_score': buy_score,
                'current_price': current_price,
                'analysis_type': 'quick_buy_decision',
                'basic_check': basic_check,
                'entry_timing': entry_timing,
                'pattern_validity': pattern_validity,
                'execution_time': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"❌ 매수 실행 가능성 판단 오류 ({candidate.stock_code}): {e}")
            return None

    def _check_basic_buy_conditions(self, candidate: CandleTradeCandidate, current_price: float, current_data: Any) -> Dict:
        """🔍 기본 매수 조건 빠른 체크 - 신호 재검증 제거"""
        try:
            # 1. 가격대 체크
            if not (self.config['min_price'] <= current_price <= self.config['max_price']):
                return {'passed': False, 'fail_reason': f'가격대 범위 외 ({current_price:,}원)'}

            # 2. 거래 시간 체크
            if not self._is_trading_time():
                return {'passed': False, 'fail_reason': '장 시간 외'}

            # 3. 최소 거래량 체크
            volume = int(current_data.iloc[0].get('acml_vol', 0))
            min_volume = self.config.get('min_volume', 10000)
            if volume < min_volume:
                return {'passed': False, 'fail_reason': f'거래량 부족 ({volume:,}주 < {min_volume:,}주)'}

            # 🔧 신호 재검증 제거 - 이미 확정된 신호는 그대로 사용
            # 기존: candidate.trade_signal 재체크 → 제거
            # 이유: 신호가 확정되면 그 신호를 바탕으로 매매만 하면 됨

            return {'passed': True, 'fail_reason': None}

        except Exception as e:
            logger.error(f"기본 매수 조건 체크 오류: {e}")
            return {'passed': False, 'fail_reason': f'체크 오류: {str(e)}'}

    def _check_entry_timing_conditions(self, candidate: CandleTradeCandidate, current_price: float, current_data: Any) -> Dict:
        """⏰ 진입 타이밍 조건 체크 - 현실적인 시가 기준 적용"""
        try:
            # 오늘 시가 가져오기
            today_open = float(current_data.iloc[0].get('stck_oprc', 0))
            if today_open <= 0:
                return {'good_timing': False, 'reason': '시가 정보 없음', 'today_open': 0}

            # 시가 대비 현재가 위치 계산
            price_diff_pct = ((current_price - today_open) / today_open) * 100

            # 🆕 현실적인 시간대별 차등 기준 적용
            from datetime import datetime
            current_time = datetime.now().time()
            
            # 시간대별 허용 범위 설정
            if current_time < datetime.strptime("09:30", "%H:%M").time():
                # 장 초반 (9:00-9:30): 엄격한 기준
                max_allowed_pct = 2.0
                timing_phase = "장초반"
            elif current_time < datetime.strptime("11:00", "%H:%M").time():
                # 장 전반 (9:30-11:00): 중간 기준
                max_allowed_pct = 4.0
                timing_phase = "장전반"
            elif current_time < datetime.strptime("14:00", "%H:%M").time():
                # 장 중반 (11:00-14:00): 관대한 기준
                max_allowed_pct = 6.0
                timing_phase = "장중반"
            else:
                # 장 후반 (14:00-15:30): 매우 관대한 기준
                max_allowed_pct = 8.0
                timing_phase = "장후반"

            # 🆕 상승/하락 방향별 추가 조정
            if price_diff_pct > 0:  # 상승한 경우
                # 상승시에는 기본 기준 적용
                final_max_pct = max_allowed_pct
                direction_note = "상승"
            else:  # 하락한 경우  
                # 하락시에는 더 관대하게 (+50% 허용)
                final_max_pct = max_allowed_pct * 1.5
                direction_note = "하락"

            # 📊 타이밍 품질 평가 (더 관대한 기준)
            abs_diff = abs(price_diff_pct)
            
            if abs_diff <= 1.0:
                timing_quality = 'excellent'
                timing_score = 100
            elif abs_diff <= 3.0:
                timing_quality = 'good'  
                timing_score = 80
            elif abs_diff <= final_max_pct:
                timing_quality = 'acceptable'
                timing_score = 60
            else:
                timing_quality = 'poor'
                timing_score = 20

            # 🚨 매수 가능 여부 최종 판단
            if abs_diff <= final_max_pct:
                reason = f'{timing_phase} {direction_note} 진입 가능 (시가대비 {price_diff_pct:+.2f}%)'
                
                # 🆕 품질별 메시지 추가
                if timing_quality == 'excellent':
                    reason += ' [최우수 타이밍]'
                elif timing_quality == 'good':
                    reason += ' [양호한 타이밍]'
                elif abs_diff > final_max_pct * 0.8:
                    reason += f' [주의: 허용한도 {final_max_pct:.1f}% 근접]'
                
                return {
                    'good_timing': True,
                    'reason': reason,
                    'timing_quality': timing_quality,
                    'timing_score': timing_score,
                    'price_diff_pct': price_diff_pct,
                    'today_open': today_open,
                    'timing_phase': timing_phase,
                    'max_allowed_diff': final_max_pct,
                    'suggested_buy_price': min(today_open * 1.01, current_price * 1.005)  # 🆕 더 관대한 추천가
                }
            else:
                return {
                    'good_timing': False,
                    'reason': f'{timing_phase} 허용범위 초과 (시가대비 {price_diff_pct:+.2f}% > 한도 ±{final_max_pct:.1f}%)',
                    'timing_quality': 'poor',
                    'timing_score': 0,
                    'price_diff_pct': price_diff_pct,
                    'today_open': today_open,
                    'timing_phase': timing_phase,
                    'max_allowed_diff': final_max_pct
                }

        except Exception as e:
            logger.error(f"진입 타이밍 체크 오류: {e}")
            return {'good_timing': False, 'reason': f'타이밍 체크 오류: {str(e)}', 'today_open': 0}

    def _validate_existing_patterns(self, candidate: CandleTradeCandidate, current_price: float) -> Dict:
        """📊 기존 패턴 존재 여부만 간단 확인 - 재검증 제거"""
        try:
            if not candidate.detected_patterns:
                return {'valid': False, 'reason': '패턴 정보 없음'}

            primary_pattern = candidate.detected_patterns[0]
            
            # 🔧 패턴 재검증 제거 - 이미 확정된 패턴은 그대로 사용
            # 기존: 신뢰도, 강도, 생성시간 재체크 → 제거
            # 이유: 패턴이 감지되어 신호가 확정되었다면 그 결과를 신뢰

            return {
                'valid': True,
                'reason': '기존 패턴 사용',
                'pattern_type': primary_pattern.pattern_type.value,
                'confidence': primary_pattern.confidence,
                'strength': primary_pattern.strength
            }

        except Exception as e:
            logger.error(f"패턴 존재 확인 오류: {e}")
            return {'valid': False, 'reason': f'확인 오류: {str(e)}'}

    def _calculate_quick_buy_score(self, candidate: CandleTradeCandidate, current_price: float, 
                                 entry_timing: Dict, pattern_validity: Dict) -> int:
        """🧮 빠른 매수 점수 계산 - 현실적인 타이밍 기준"""
        try:
            score = 0

            # 1. 기본 패턴 점수 (40점)
            if candidate.detected_patterns:
                primary_pattern = candidate.detected_patterns[0]
                score += int(primary_pattern.confidence * 20)  # 0.7 신뢰도 = 14점
                score += int(primary_pattern.strength * 0.2)   # 80 강도 = 16점

            # 2. 진입 타이밍 점수 (30점) - 더 관대한 점수 배분
            timing_quality = entry_timing.get('timing_quality', 'poor')
            timing_scores = {
                'excellent': 30,    # 최우수 (1% 이내)
                'good': 25,        # 양호 (3% 이내) - 상향 조정
                'acceptable': 20,   # 허용 가능 (시간대별 한도 이내) - 상향 조정
                'fair': 15,        # 보통 - 추가
                'poor': 5          # 나쁨 - 최소 점수 부여
            }
            timing_score = timing_scores.get(timing_quality, 5)
            score += timing_score

            # 3. 신호 강도 점수 (20점)  
            signal_strength = candidate.signal_strength
            score += int(signal_strength * 0.2)  # 80 강도 = 16점

            # 4. 우선순위 점수 (10점)
            entry_priority = candidate.entry_priority
            score += int(entry_priority * 0.1)  # 80 우선순위 = 8점

            # 🆕 5. 시간대별 보너스 점수 (추가 5점)
            timing_phase = entry_timing.get('timing_phase', '')
            phase_bonus = {
                '장초반': 5,    # 장 초반 보너스
                '장전반': 3,    # 장 전반 보너스  
                '장중반': 2,    # 장 중반 보너스
                '장후반': 1     # 장 후반 보너스
            }
            score += phase_bonus.get(timing_phase, 0)

            # 정규화 (0~100)
            final_score = min(100, max(0, score))

            # 🆕 상세 로깅
            if candidate.detected_patterns:
                primary_pattern = candidate.detected_patterns[0]
                logger.debug(f"📊 {candidate.stock_code} 매수 점수: {final_score}/100 "
                            f"(패턴:{int(primary_pattern.confidence * 20)}+{int(primary_pattern.strength * 0.2)} "
                            f"+ 타이밍:{timing_score}({timing_quality}) "
                            f"+ 신호:{int(signal_strength * 0.2)} "
                            f"+ 우선순위:{int(entry_priority * 0.1)} "
                            f"+ 시간대:{phase_bonus.get(timing_phase, 0)}({timing_phase}))")

            return final_score

        except Exception as e:
            logger.error(f"빠른 매수 점수 계산 오류: {e}")
            return 0

    async def _analyze_intraday_confirmation(self, stock_code: str, current_price: float, daily_ohlcv: Any) -> Dict:
        """🕐 장중 데이터 보조 분석 (현재가 추적 + 거래량 급증 감지만)"""
        try:
            # 🎯 단순화된 장중 분석 - 핵심만 추출
            intraday_analysis = {
                'valid': False,
                'volume_surge': False,
                'current_price_updated': True,
                'analysis_source': 'simplified_intraday'
            }

            # 🆕 장중 분석이 필요한지 간단히 판단
            if not self._is_trading_time():
                logger.debug(f"📊 {stock_code} 장시간 외 - 장중 분석 스킵")
                return intraday_analysis

            # 🆕 분봉 데이터로 거래량 급증만 확인 (선택적)
            minute_data = await self._get_minute_candle_data(stock_code, period_minutes=5, count=10)
            if minute_data is not None and not minute_data.empty:
                intraday_analysis['valid'] = True

                # 거래량 급증 감지만 수행
                volume_surge = self._detect_volume_surge(minute_data)
                intraday_analysis['volume_surge'] = volume_surge

                if volume_surge:
                    logger.info(f"📈 {stock_code} 거래량 급증 감지 - 패턴 확정 가능성 높음")

            return intraday_analysis

        except Exception as e:
            logger.debug(f"❌ {stock_code} 장중 데이터 분석 오류: {e}")
            return {'valid': False, 'volume_surge': False, 'analysis_source': 'error'}



    async def _get_minute_candle_data(self, stock_code: str, period_minutes: int = 5, count: int = 20) -> Optional[Any]:
        """분봉 데이터 조회 (KIS API 활용) - 최대 30분봉만 제공"""
        try:
            from ..api.kis_market_api import get_inquire_time_itemchartprice
            from datetime import datetime, timedelta

            # 🔧 현실적 제한: 최대 30분봉만 조회 가능 (KIS API 30건 제한)
            now = datetime.now()
            thirty_minutes_ago = now - timedelta(minutes=30)
            
            # 시간 형식 변환 (HHMMSS)
            input_hour = thirty_minutes_ago.strftime("%H%M%S")
            
            logger.debug(f"📊 {stock_code} 분봉 데이터 조회: 최근 30분 (제한된 범위)")

            # KIS API 호출 - 최대 30분 전부터 현재까지
            minute_data = get_inquire_time_itemchartprice(
                output_dv="2",              # 분봉 데이터 배열 (output2)
                div_code="J",               # 조건시장분류코드 (J: 주식)
                itm_no=stock_code,          # 입력종목코드
                input_hour=input_hour,      # 30분 전 시간 설정
                past_data_yn="Y",           # 과거데이터포함여부 (Y: 포함)
                etc_cls_code=""             # 기타구분코드 (공백)
            )

            if minute_data is not None and not minute_data.empty:
                # 데이터 품질 확인
                data_count = len(minute_data)
                if data_count > 0:
                    first_time = minute_data.iloc[0].get('stck_cntg_hour', 'N/A')
                    last_time = minute_data.iloc[-1].get('stck_cntg_hour', 'N/A')
                    logger.debug(f"✅ {stock_code} 분봉 데이터 조회 성공: {data_count}건 (최대 30분)")
                    logger.debug(f"   📅 시간 범위: {first_time} ~ {last_time}")
                    
                    # 최신 데이터 우선 정렬 (최신순)
                    minute_data = minute_data.sort_values('stck_cntg_hour', ascending=False).reset_index(drop=True)
                    
                    # count 제한 적용 (기본 20개)
                    if data_count > count:
                        limited_data = minute_data.head(count)
                        logger.debug(f"   📊 데이터 제한 적용: {data_count}건 → {count}건 (최신순)")
                        return limited_data
                    
                    return minute_data
                else:
                    logger.debug(f"⚠️ {stock_code} 분봉 데이터 조회 결과 없음")
                    return None
            else:
                logger.debug(f"⚠️ {stock_code} 분봉 데이터 조회 결과 없음")
                return None

        except Exception as e:
            logger.debug(f"❌ {stock_code} 분봉 데이터 조회 오류: {e}")
            # 분봉 데이터 조회 실패시에도 장중 분석은 계속 진행
            return None



    def _detect_volume_surge(self, minute_data: Any) -> bool:
        """거래량 급증 감지"""
        try:
            if minute_data is None or minute_data.empty or len(minute_data) < 10:
                return False

            # 최근 3개 분봉 vs 이전 7개 분봉 거래량 비교 (KIS API 컬럼명 사용)
            recent_volumes = []
            for _, row in minute_data.head(3).iterrows():
                try:
                    volume = int(row.get('cntg_vol', row.get('acml_vol', 0)))
                    if volume > 0:
                        recent_volumes.append(volume)
                except (ValueError, TypeError):
                    continue

            previous_volumes = []
            for _, row in minute_data.iloc[3:10].iterrows():
                try:
                    volume = int(row.get('cntg_vol', row.get('acml_vol', 0)))
                    if volume > 0:
                        previous_volumes.append(volume)
                except (ValueError, TypeError):
                    continue

            if not recent_volumes or not previous_volumes:
                return False

            avg_recent = sum(recent_volumes) / len(recent_volumes)
            avg_previous = sum(previous_volumes) / len(previous_volumes)

            # 거래량이 2배 이상 증가했으면 급증으로 판단
            return avg_recent > avg_previous * 2.0

        except Exception as e:
            logger.debug(f"거래량 급증 감지 오류: {e}")
            return False



    def _refine_position_signals_with_intraday(self, position_signals: Dict, intraday_signals: Dict, focus_on_exit: bool) -> Dict:
        """장중 데이터로 포지션 신호 정밀화 (단순화)"""
        try:
            if not intraday_signals.get('valid', False):
                return position_signals

            refined_signals = position_signals.copy()
            volume_surge = intraday_signals.get('volume_surge', False)

            # 🎯 거래량 급증시에만 신호 강화
            if volume_surge:
                if focus_on_exit:
                    # 매도 신호 + 거래량 급증 = 신호 강화
                    if position_signals.get('signal') in ['strong_sell', 'sell']:
                        refined_signals['intraday_enhancement'] = 'volume_surge_exit'
                        logger.debug("📈 장중 분석: 거래량 급증으로 매도 신호 강화")
                else:
                    # 매수 신호 + 거래량 급증 = 신호 강화
                    if position_signals.get('signal') in ['buy_ready', 'strong_buy']:
                        refined_signals['intraday_enhancement'] = 'volume_surge_entry'
                        logger.debug("💰 장중 분석: 거래량 급증으로 매수 신호 강화")

            # 장중 분석 메타데이터 추가
            refined_signals['volume_surge'] = volume_surge

            return refined_signals

        except Exception as e:
            logger.debug(f"장중 데이터 신호 정밀화 오류: {e}")
            return position_signals

    async def _ensure_fresh_ohlcv_data(self, candidate: CandleTradeCandidate, stock_code: str) -> Optional[Any]:
        """ 최신 일봉 데이터 확보 (캔들패턴의 핵심)"""
        try:
            # 기존 캐시된 데이터 확인
            cached_data = candidate.get_ohlcv_data()

            # 🕒 일봉 데이터 갱신 필요성 체크
            need_update = self._should_update_daily_candle_data(cached_data)

            if not need_update and cached_data is not None:
                logger.debug(f"📊 {stock_code} 캐시된 일봉 데이터 사용")
                return cached_data

            # 🆕 최신 일봉 데이터 조회
            logger.debug(f"📥 {stock_code} 최신 일봉 데이터 조회")
            from ..api.kis_market_api import get_inquire_daily_itemchartprice
            fresh_ohlcv = get_inquire_daily_itemchartprice(
                output_dv="2",  # 일자별 차트 데이터
                itm_no=stock_code,
                period_code="D",
                adj_prc="1"
            )

            if fresh_ohlcv is not None and not fresh_ohlcv.empty:
                # 캐시 업데이트
                candidate.cache_ohlcv_data(fresh_ohlcv)
                logger.debug(f"✅ {stock_code} 일봉 데이터 갱신 완료")
                return fresh_ohlcv

            # 폴백: 캐시된 데이터라도 사용
            return cached_data

        except Exception as e:
            logger.error(f"❌ {stock_code} 일봉 데이터 확보 오류: {e}")
            return candidate.get_ohlcv_data()  # 기존 캐시 반환

    def _should_update_daily_candle_data(self, cached_data: Optional[Any]) -> bool:
        """일봉 데이터 갱신 필요성 판단"""
        try:
            if cached_data is None:
                return True

            # 🕒 장중에는 1시간마다, 장후에는 하루에 한번 갱신
            current_time = datetime.now()

            # 장 시간 확인
            if self._is_trading_time():
                # 장중: 1시간마다 갱신 (오늘 새로운 캔들 형성 중)
                return True  # 실제로는 candidate의 마지막 업데이트 시간 체크 필요
            else:
                # 장후: 하루에 한번만 갱신
                return current_time.hour >= 16 and current_time.minute >= 0

        except Exception as e:
            logger.error(f"일봉 갱신 판단 오류: {e}")
            return True  # 오류시 갱신

    async def _analyze_pattern_changes(self, candidate: CandleTradeCandidate, stock_code: str,
                                     current_price: float, ohlcv_data: Any) -> Dict:
        """🔄 캔들패턴 전환 감지 분석 (핵심!)"""
        try:
            # 현재 감지된 패턴들
            current_patterns = self.pattern_detector.analyze_stock_patterns(stock_code, ohlcv_data)

            # 기존에 감지된 패턴들 (진입 근거)
            existing_patterns = candidate.detected_patterns

            analysis = {
                'has_pattern_change': False,
                'new_patterns': [],
                'disappeared_patterns': [],
                'pattern_reversal_detected': False,
                'reversal_strength': 0,
                'action_required': 'none',  # 'immediate_exit', 'caution', 'none'
                'ohlcv_updated': True
            }

            if not current_patterns:
                if existing_patterns:
                    analysis['disappeared_patterns'] = [p.pattern_type.value for p in existing_patterns]
                    analysis['has_pattern_change'] = True
                    analysis['action_required'] = 'caution'
                return analysis

            # 🔄 패턴 변화 감지
            current_pattern_types = {p.pattern_type for p in current_patterns}
            existing_pattern_types = {p.pattern_type for p in existing_patterns} if existing_patterns else set()

            # 새로운 패턴 발견
            new_pattern_types = current_pattern_types - existing_pattern_types
            if new_pattern_types:
                analysis['new_patterns'] = [p.value for p in new_pattern_types]
                analysis['has_pattern_change'] = True

                # 🚨 패턴 반전 감지 (매우 중요!)
                reversal_detected, reversal_strength = self._detect_pattern_reversal(
                    existing_patterns, current_patterns
                )

                if reversal_detected:
                    analysis['pattern_reversal_detected'] = True
                    analysis['reversal_strength'] = reversal_strength

                    # 강한 반전 시그널이면 즉시 조치 필요
                    if reversal_strength >= 80:
                        analysis['action_required'] = 'immediate_exit'
                        logger.warning(f"🚨 {stock_code} 강한 패턴 반전 감지! 즉시 매도 검토 필요")
                    elif reversal_strength >= 60:
                        analysis['action_required'] = 'caution'
                        logger.info(f"⚠️ {stock_code} 패턴 반전 징후 감지")

            # 사라진 패턴
            disappeared_pattern_types = existing_pattern_types - current_pattern_types
            if disappeared_pattern_types:
                analysis['disappeared_patterns'] = [p.value for p in disappeared_pattern_types]
                analysis['has_pattern_change'] = True

            return analysis

        except Exception as e:
            logger.error(f"❌ 패턴 변화 분석 오류: {e}")
            return {'has_pattern_change': False, 'action_required': 'none', 'ohlcv_updated': False}

    def _detect_pattern_reversal(self, existing_patterns: List, current_patterns: List) -> Tuple[bool, int]:
        """패턴 반전 감지"""
        try:
            if not existing_patterns or not current_patterns:
                return False, 0

            # 기존 패턴의 방향성 (상승/하락)
            existing_bullish = any(p.pattern_type in [
                PatternType.HAMMER, PatternType.INVERTED_HAMMER,
                PatternType.BULLISH_ENGULFING, PatternType.MORNING_STAR
            ] for p in existing_patterns)

            # 현재 패턴의 방향성
            current_bearish = any(p.pattern_type in [
                PatternType.BEARISH_ENGULFING, PatternType.EVENING_STAR,
                PatternType.FALLING_THREE_METHODS
            ] for p in current_patterns)

            # 상승 → 하락 반전
            if existing_bullish and current_bearish:
                strongest_bearish = max([p for p in current_patterns if p.pattern_type in [
                    PatternType.BEARISH_ENGULFING, PatternType.EVENING_STAR
                ]], key=lambda x: x.strength, default=None)

                if strongest_bearish:
                    return True, strongest_bearish.strength

            return False, 0

        except Exception as e:
            logger.error(f"패턴 반전 감지 오류: {e}")
            return False, 0

    async def _analyze_daily_candle_patterns(self, stock_code: str, current_price: float, ohlcv_data: Any) -> Dict:
        """일봉 기준 캔들 패턴 분석 (기존 analyze_current_patterns 개선)"""
        try:
            # 기존 로직 재사용하되, 일봉 관점 강화
            base_result = await self.analyze_current_patterns(stock_code, current_price, ohlcv_data)

            # 🆕 일봉 캔들패턴 특화 정보 추가
            if base_result.get('patterns'):
                strongest_pattern = max(base_result['patterns'], key=lambda p: p.strength)

                # 패턴 완성도 계산 (오늘 캔들이 패턴을 더 강화하는지)
                pattern_completion = self._calculate_pattern_completion(strongest_pattern, ohlcv_data)

                base_result.update({
                    'pattern_completion_rate': pattern_completion,
                    'daily_candle_strength': self._calculate_daily_candle_strength(ohlcv_data),
                    'pattern_reliability': self._assess_pattern_reliability(strongest_pattern, ohlcv_data)
                })

            return base_result

        except Exception as e:
            logger.error(f"❌ 일봉 패턴 분석 오류: {e}")
            return {'signal': 'neutral', 'strength': 0, 'patterns': []}

    def _calculate_pattern_completion(self, pattern, ohlcv_data) -> float:
        """패턴 완성도 계산 (0~1)"""
        try:
            # 오늘 캔들이 패턴을 더 확실하게 만드는지 평가
            if ohlcv_data is None or ohlcv_data.empty or len(ohlcv_data) < 3:
                return 0.5

            # 간단한 완성도: 최근 3일 캔들의 일관성
            recent_candles = ohlcv_data.head(3)

            # 상승 패턴의 경우: 고가가 점진적으로 올라가는지
            if pattern.pattern_type in [PatternType.HAMMER, PatternType.BULLISH_ENGULFING]:
                highs = [float(row.get('stck_hgpr', 0)) for _, row in recent_candles.iterrows()]
                if len(highs) >= 2:
                    return 0.8 if highs[0] > highs[1] else 0.4

            return 0.6  # 기본 완성도

        except Exception as e:
            logger.error(f"패턴 완성도 계산 오류: {e}")
            return 0.5

    def _calculate_daily_candle_strength(self, ohlcv_data) -> float:
        """오늘 캔들의 강도 계산"""
        try:
            if ohlcv_data is None or ohlcv_data.empty:
                return 0.0

            today_candle = ohlcv_data.iloc[0]
            open_price = float(today_candle.get('stck_oprc', 0))
            close_price = float(today_candle.get('stck_clpr', 0))
            high_price = float(today_candle.get('stck_hgpr', 0))
            low_price = float(today_candle.get('stck_lwpr', 0))

            if high_price <= low_price:
                return 0.0

            # 실체 비율 (몸통/전체 범위)
            body_size = abs(close_price - open_price)
            total_range = high_price - low_price
            body_ratio = body_size / total_range if total_range > 0 else 0

            return min(1.0, body_ratio * 1.5)  # 0~1 범위로 정규화

        except Exception as e:
            logger.error(f"일봉 강도 계산 오류: {e}")
            return 0.0

    def _assess_pattern_reliability(self, pattern, ohlcv_data) -> float:
        """패턴 신뢰도 평가 (거래량, 위치 등 고려)"""
        try:
            base_reliability = pattern.confidence

            # 🔍 거래량 확인 (패턴 + 거래량 증가 = 신뢰도 상승)
            volume_factor = self._calculate_volume_factor(ohlcv_data)

            # 🔍 가격 위치 확인 (지지/저항 근처 패턴 = 신뢰도 상승)
            position_factor = self._calculate_position_factor(ohlcv_data)

            # 종합 신뢰도 (최대 0.95)
            adjusted_reliability = min(0.95, base_reliability * volume_factor * position_factor)

            return adjusted_reliability

        except Exception as e:
            logger.error(f"패턴 신뢰도 평가 오류: {e}")
            return pattern.confidence if pattern else 0.5

    def _calculate_volume_factor(self, ohlcv_data) -> float:
        """거래량 요인 계산"""
        try:
            if ohlcv_data is None or ohlcv_data.empty or len(ohlcv_data) < 5:
                return 1.0

            # 최근 5일 평균 거래량 대비 오늘 거래량
            recent_volumes = []
            for _, row in ohlcv_data.head(5).iterrows():
                volume = int(row.get('acml_vol', 0))
                if volume > 0:
                    recent_volumes.append(volume)

            if len(recent_volumes) < 2:
                return 1.0

            today_volume = recent_volumes[0]
            avg_volume = sum(recent_volumes[1:]) / len(recent_volumes[1:])

            volume_ratio = today_volume / avg_volume if avg_volume > 0 else 1.0

            # 거래량 2배 이상 = 1.2배 신뢰도, 절반 이하 = 0.8배 신뢰도
            if volume_ratio >= 2.0:
                return 1.2
            elif volume_ratio >= 1.5:
                return 1.1
            elif volume_ratio <= 0.5:
                return 0.8
            else:
                return 1.0

        except Exception as e:
            logger.error(f"거래량 요인 계산 오류: {e}")
            return 1.0

    def _calculate_position_factor(self, ohlcv_data) -> float:
        """가격 위치 요인 계산 (지지/저항 근처)"""
        try:
            if ohlcv_data is None or ohlcv_data.empty or len(ohlcv_data) < 20:
                return 1.0

            # 20일 고가/저가 범위에서의 현재 위치
            recent_data = ohlcv_data.head(20)
            current_price = float(ohlcv_data.iloc[0].get('stck_clpr', 0))

            highs = [float(row.get('stck_hgpr', 0)) for _, row in recent_data.iterrows()]
            lows = [float(row.get('stck_lwpr', 0)) for _, row in recent_data.iterrows()]

            max_high = max(highs)
            min_low = min(lows)

            if max_high <= min_low:
                return 1.0

            # 상대적 위치 (0~1)
            relative_position = (current_price - min_low) / (max_high - min_low)

            # 지지선(0.1~0.3) 또는 저항선(0.7~0.9) 근처에서 패턴이 나타나면 신뢰도 증가
            if 0.1 <= relative_position <= 0.3 or 0.7 <= relative_position <= 0.9:
                return 1.15  # 15% 신뢰도 증가
            else:
                return 1.0

        except Exception as e:
            logger.error(f"가격 위치 요인 계산 오류: {e}")
            return 1.0

    def _analyze_candle_exit_conditions(self, candidate: CandleTradeCandidate, current_price: float, pattern_change_analysis: Dict) -> Dict:
        """🎯 새로운 매도 조건 분석 - 패턴별 target/stop 기준으로 단순화"""
        try:
            # 기본 매도 조건들
            should_exit = False
            exit_reasons = []
            exit_priority = 'normal'

            logger.debug(f"🔍 {candidate.stock_code} 매도 조건 분석 시작 (현재가: {current_price:,}원)")

            # 🚨 1. 패턴 반전 감지시 즉시 매도 (최우선)
            if pattern_change_analysis.get('action_required') == 'immediate_exit':
                should_exit = True
                exit_reasons.append('강한 패턴 반전 감지')
                exit_priority = 'emergency'
                logger.warning(f"🚨 {candidate.stock_code} 강한 패턴 반전 감지 - 즉시 매도")
                return {
                    'signal': 'strong_sell',
                    'should_exit': should_exit,
                    'exit_reasons': exit_reasons,
                    'exit_priority': exit_priority,
                    'pattern_reversal_exit': True
                }

            # 🆕 2. 보유 시간 계산 (거래시간 기준)
            if not candidate.performance.entry_time:
                logger.debug(f"⚠️ {candidate.stock_code} 진입 시간 정보 없음 - 매도 조건 스킵")
                return {'signal': 'hold', 'should_exit': False, 'exit_reasons': [], 'exit_priority': 'normal'}

            entry_time = candidate.performance.entry_time
            current_time = datetime.now(self.korea_tz)
            if entry_time.tzinfo is None:
                entry_time = entry_time.replace(tzinfo=self.korea_tz)

            # 거래시간 기준 보유 시간 계산
            holding_hours = calculate_business_hours_analyzer(entry_time, current_time)
            logger.debug(f"⏰ {candidate.stock_code} 보유시간: {holding_hours:.1f}h (거래시간만)")

            # 🆕 3. 수익률 계산
            if not candidate.performance.entry_price:
                logger.debug(f"⚠️ {candidate.stock_code} 진입가 정보 없음 - 매도 조건 스킵")
                return {'signal': 'hold', 'should_exit': False, 'exit_reasons': [], 'exit_priority': 'normal'}

            pnl_pct = ((current_price - candidate.performance.entry_price) / candidate.performance.entry_price) * 100
            logger.debug(f"💰 {candidate.stock_code} 현재 수익률: {pnl_pct:+.2f}% (진입가: {candidate.performance.entry_price:,}원)")

            # 🎯 4. 패턴별 설정 가져오기
            target_profit_pct, stop_loss_pct, max_hours, pattern_based = self._get_pattern_based_target(candidate)
            logger.debug(f"📊 {candidate.stock_code} 패턴별 설정: 목표{target_profit_pct}%, 손절{stop_loss_pct}%, 최대{max_hours}h")

            # 🎯 5. 새로운 매도 판단 로직
            # 5-1. 목표 수익률 도달시 즉시 매도
            if pnl_pct >= target_profit_pct:
                should_exit = True
                exit_reasons.append(f'목표 수익률 달성 ({target_profit_pct}%)')
                exit_priority = 'high'
                logger.info(f"🎯 {candidate.stock_code} 목표 수익률 달성: {pnl_pct:+.2f}% >= {target_profit_pct}%")
            
            # 5-2. 손절 기준 도달시 즉시 매도
            elif pnl_pct <= -stop_loss_pct:
                should_exit = True
                exit_reasons.append(f'손절 기준 도달 ({stop_loss_pct}%)')
                exit_priority = 'high'
                logger.info(f"🛑 {candidate.stock_code} 손절 기준 도달: {pnl_pct:+.2f}% <= -{stop_loss_pct}%")
            
            # 5-3. stop~target 사이에서 24시간 초과시 강제 매도
            elif holding_hours >= 24.0:
                should_exit = True
                exit_reasons.append('24시간 보유 완료')
                exit_priority = 'normal'
                logger.info(f"⏰ {candidate.stock_code} 24시간 보유 완료: {holding_hours:.1f}h >= 24h")
            
            # 5-4. stop~target 사이에서 24시간 내면 보유 지속
            else:
                logger.info(f"⏸️ {candidate.stock_code} 보유 지속: 수익률 {pnl_pct:+.2f}% ({-stop_loss_pct}% ~ {target_profit_pct}% 범위), 보유시간 {holding_hours:.1f}h/24h")
                return {
                    'signal': 'hold', 
                    'should_exit': False, 
                    'exit_reasons': ['목표 범위 내 24시간 보유'], 
                    'exit_priority': 'normal',
                    'holding_hours': holding_hours,
                    'pnl_pct': pnl_pct,
                    'target_range': f"{-stop_loss_pct}% ~ {target_profit_pct}%"
                }

            # 신호 결정
            if should_exit:
                if exit_priority == 'emergency':
                    signal = 'strong_sell'
                elif exit_priority == 'high':
                    signal = 'strong_sell'
                else:
                    signal = 'sell'
            else:
                signal = 'hold'

            logger.debug(f"🔍 {candidate.stock_code} 매도 조건 분석 완료: {signal} (우선순위: {exit_priority}, 이유: {exit_reasons})")

            return {
                'signal': signal,
                'should_exit': should_exit,
                'exit_reasons': exit_reasons,
                'exit_priority': exit_priority,
                'holding_hours': holding_hours,
                'pnl_pct': pnl_pct,
                'target_profit_pct': target_profit_pct,
                'stop_loss_pct': stop_loss_pct
            }

        except Exception as e:
            logger.error(f"❌ {candidate.stock_code} 매도 조건 분석 오류: {e}")
            return {'signal': 'hold', 'should_exit': False, 'exit_reasons': [], 'exit_priority': 'normal'}

    def _analyze_candle_entry_conditions(self, candidate: CandleTradeCandidate, current_price: float, current_pattern_signals: Dict) -> Dict:
        """🎯 캔들패턴 기반 진입 조건 분석 (관찰 중인 종목용)"""
        try:
            # 기본 진입 조건 체크
            can_enter = True
            entry_reasons = []

            # 1. 가격대 체크
            if not (self.config['min_price'] <= current_price <= self.config['max_price']):
                can_enter = False
            else:
                entry_reasons.append('가격대 적정')

            # 2. 거래 시간 체크
            if self._is_trading_time():
                entry_reasons.append('거래 시간')
            else:
                can_enter = False

            # 3. 🆕 패턴 신호 강도 체크 (config에서 임계값 가져오기)
            pattern_strength = current_pattern_signals.get('strength', 0)
            min_pattern_strength = self.config.get('trading_thresholds', {}).get('min_pattern_strength', 70)

            if pattern_strength >= min_pattern_strength:
                entry_reasons.append(f'강한 패턴 신호 ({pattern_strength})')
            elif pattern_strength >= min_pattern_strength * 0.8:  # 80% 수준까지 허용
                entry_reasons.append(f'적정 패턴 신호 ({pattern_strength})')
            else:
                can_enter = False

            # 4. 🆕 패턴 신뢰도 체크 (config에서 임계값 가져오기)
            pattern_reliability = current_pattern_signals.get('pattern_reliability', 0.0)
            min_pattern_confidence = self.config.get('trading_thresholds', {}).get('min_pattern_confidence', 0.65)

            if pattern_reliability >= min_pattern_confidence:
                entry_reasons.append(f'높은 패턴 신뢰도 ({pattern_reliability:.2f})')
            elif pattern_reliability < min_pattern_confidence * 0.8:  # 80% 수준 미만은 차단
                can_enter = False

            signal = 'buy_ready' if can_enter else 'wait'

            return {
                'signal': signal,
                'can_enter': can_enter,
                'entry_reasons': entry_reasons,
                'pattern_strength': pattern_strength,
                'pattern_reliability': pattern_reliability
            }

        except Exception as e:
            logger.debug(f"진입 조건 분석 오류: {e}")
            return {'signal': 'wait', 'can_enter': False, 'entry_reasons': []}

    def _calculate_candle_focused_signal(self, current_pattern_signals: Dict, pattern_change_analysis: Dict,
                                         technical_harmony: float, pattern_time_signals: Dict,
                                         pattern_risk_signals: Dict, position_signals: Dict,
                                         focus_on_exit: bool = False) -> Tuple[TradeSignal, int]:
        """🧮 캔들패턴 중심의 종합 신호 계산 - 24시간 내 시간 신호 무시"""
        try:
            # 🚨 패턴 반전 감지시 즉시 매도 (최우선)
            if pattern_change_analysis.get('action_required') == 'immediate_exit':
                return TradeSignal.STRONG_SELL, 95

            # 가중치 설정 (캔들패턴 중심)
            if focus_on_exit:
                # 매도 신호 중심 - 패턴 반전이 중요
                weights = {
                    'pattern_change': 0.4,   # 패턴 변화가 가장 중요
                    'risk': 0.3,            # 리스크 조건
                    'position': 0.2,        # 포지션 분석
                    'time': 0.1,            # 시간 조건
                    'pattern': 0.0          # 현재 패턴은 덜 중요 (변화가 더 중요)
                }
            else:
                # 매수 신호 중심 - 패턴과 조화성이 중요
                weights = {
                    'pattern': 0.4,         # 현재 패턴이 가장 중요
                    'technical_harmony': 0.25,  # 패턴-기술지표 조화성
                    'position': 0.2,        # 진입 조건
                    'risk': 0.1,            # 리스크 조건
                    'time': 0.05           # 시간은 덜 중요
                }

            # 각 신호의 점수 계산 (0~100)
            pattern_score = self._get_signal_score(current_pattern_signals.get('signal', 'neutral'), 'pattern')

            # 🔧 technical_harmony는 float이므로 0~100 스케일로 변환
            technical_score = technical_harmony * 100 if isinstance(technical_harmony, (int, float)) else 50

            # 시간 신호 처리
            time_signal = pattern_time_signals.get('signal', 'normal')
            time_score = self._get_signal_score(time_signal, 'time')
            
            risk_score = self._get_signal_score(pattern_risk_signals.get('signal', 'neutral'), 'risk')
            position_score = self._get_signal_score(position_signals.get('signal', 'wait'), 'position')

            # 패턴 변화 점수
            if pattern_change_analysis.get('action_required') == 'caution':
                pattern_change_score = 70  # 주의 레벨
            elif pattern_change_analysis.get('has_pattern_change'):
                pattern_change_score = 60  # 일반적인 변화
            else:
                pattern_change_score = 50  # 변화 없음

            # 가중 평균 계산
            if focus_on_exit:
                total_score = (
                    pattern_change_score * weights['pattern_change'] +
                    risk_score * weights['risk'] +
                    position_score * weights['position'] +
                    time_score * weights['time']
                )
            else:
                total_score = (
                    pattern_score * weights['pattern'] +
                    technical_score * weights['technical_harmony'] +
                    position_score * weights['position'] +
                    risk_score * weights['risk'] +
                    time_score * weights['time']
                )

            # 디버깅 로그 (매도 신호 중심)
            if focus_on_exit:
                logger.debug(f"🧮 매도 신호 계산: 패턴변화({pattern_change_score:.0f}×{weights['pattern_change']:.1f}) + "
                           f"리스크({risk_score:.0f}×{weights['risk']:.1f}) + "
                           f"포지션({position_score:.0f}×{weights['position']:.1f}) + "
                           f"시간({time_score:.0f}×{weights['time']:.1f}) = {total_score:.0f}점")

            # 신호 결정
            if focus_on_exit:
                # 매도 신호
                if total_score >= 80:
                    return TradeSignal.STRONG_SELL, int(total_score)
                elif total_score >= 60:
                    return TradeSignal.SELL, int(total_score)
                else:
                    return TradeSignal.HOLD, int(total_score)
            else:
                # 매수 신호 (config에서 임계값 가져오기)
                min_buy_signal_score = self.config.get('trading_thresholds', {}).get('min_buy_signal_score', 70)
                min_strong_buy_score = self.config.get('trading_thresholds', {}).get('min_strong_buy_score', 85)

                if total_score >= min_strong_buy_score:
                    return TradeSignal.STRONG_BUY, int(total_score)
                elif total_score >= min_buy_signal_score:
                    return TradeSignal.BUY, int(total_score)
                else:
                    return TradeSignal.HOLD, int(total_score)

        except Exception as e:
            logger.debug(f"캔들패턴 중심 신호 계산 오류: {e}")
            return TradeSignal.HOLD, 50

    def _get_signal_score(self, signal: str, signal_type: str) -> float:
        """신호를 점수로 변환 (0~100)"""
        try:
            if signal_type == 'pattern':
                scores = {
                    'bullish': 80, 'bearish': 20, 'neutral': 50
                }
            elif signal_type == 'technical':
                scores = {
                    'oversold_bullish': 85, 'overbought_bearish': 15,
                    'oversold': 70, 'overbought': 30, 'neutral': 50
                }
            elif signal_type == 'time':
                scores = {
                    'time_exit': 80, 'time_caution': 60, 'normal': 50, 'closed_market': 30
                }
            elif signal_type == 'risk':
                scores = {
                    'target_reached': 90, 'stop_loss': 90, 'profit_target': 85,
                    'loss_limit': 85, 'profit_zone': 60, 'loss_zone': 40, 'neutral': 50,
                    'pattern_risk_high': 90, 'pattern_risk_medium': 60
                }
            elif signal_type == 'position':
                scores = {
                    'strong_sell': 90, 'buy_ready': 80, 'hold': 50, 'wait': 30
                }
            else:
                scores = {'neutral': 50}

            return scores.get(signal, 50)

        except Exception as e:
            logger.debug(f"신호 점수 변환 오류: {e}")
            return 50

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
                # 수동/앱 매수 종목: 큰 수익/손실 허용 (🎯 10% 목표, 5% 손절)
                logger.debug(f"📊 {position.stock_code} 수동 매수 종목 - 기본 설정 적용")
                return 1.4, 2.0, 24, False

            # 2. 🔄 실시간 캔들 패턴 재분석 (🆕 캐시된 데이터 활용)
            original_pattern = None

            # 🆕 캐시된 OHLCV 데이터 사용 (API 호출 제거)
            ohlcv_data = position.get_ohlcv_data()

            if ohlcv_data is not None and not ohlcv_data.empty:
                try:
                    pattern_result = self.pattern_detector.analyze_stock_patterns(position.stock_code, ohlcv_data)
                    if pattern_result and len(pattern_result) > 0:
                        strongest_pattern = max(pattern_result, key=lambda p: p.strength)
                        original_pattern = strongest_pattern.pattern_type.value
                        logger.debug(f"🔄 {position.stock_code} 캐시된 데이터로 패턴 분석: {original_pattern} (강도: {strongest_pattern.strength})")
                except Exception as e:
                    logger.debug(f"캐시된 데이터 패턴 분석 오류 ({position.stock_code}): {e}")
            else:
                logger.debug(f"📄 {position.stock_code} 캐시된 일봉 데이터 없음")

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
                pattern_config = self.config['pattern_targets'].get(pattern_key)

                if pattern_config:
                    target_pct = pattern_config['target']
                    stop_pct = pattern_config['stop']
                    max_hours = pattern_config['max_hours']

                    logger.debug(f"📊 {position.stock_code} 패턴 '{original_pattern}' - "
                                f"목표:{target_pct}%, 손절:{stop_pct}%, 시간:{max_hours}h")
                    return target_pct, stop_pct, max_hours, True
                else:
                    logger.debug(f"📊 {position.stock_code} 패턴 '{original_pattern}' Config 없음 - 기본 설정 적용 (목표:1.5%, 손절:2.0%)")
                    return 1.4, 2.0, 24, True

            # 4. 기본값: 캔들 전략이지만 패턴 정보 없음 (🎯 큰 수익/손실 허용)
            logger.debug(f"📊 {position.stock_code} 캔들 전략이나 패턴 정보 없음 - 기본 캔들 설정 적용 (목표:1.5%, 손절:2.0%)")
            return 1.4, 2.0, 6, True

        except Exception as e:
            logger.error(f"패턴별 설정 결정 오류 ({position.stock_code}): {e}")
            # 오류시 안전하게 기본값 반환 (🎯 큰 수익/손실 허용)
            logger.debug(f"📊 {position.stock_code} 오류로 인한 기본 설정 적용 (목표:1.5%, 손절:2.0%)")
            return 1.4, 2.0, 24, False

    def _should_time_exit_pattern_based(self, position: CandleTradeCandidate, max_hours: int) -> bool:
        """🆕 단순화된 시간 기반 매도 판단 - 24시간 내에는 시간 매도 금지"""
        try:
            if not position.performance.entry_time:
                logger.debug(f"⚠️ {position.stock_code} 진입 시간 정보 없음 - 시간 청산 불가")
                return False

            # 거래시간 기준 보유 시간 계산
            entry_time = position.performance.entry_time
            current_time = datetime.now(self.korea_tz)
            if entry_time.tzinfo is None:
                entry_time = entry_time.replace(tzinfo=self.korea_tz)

            holding_hours = calculate_business_hours_analyzer(entry_time, current_time)

            logger.debug(f"⏰ {position.stock_code} 시간 청산 체크: {holding_hours:.1f}h / {max_hours}h (거래시간만)")

            # 🆕 24시간 내에는 시간 기반 매도 금지
            if holding_hours < 24.0:
                logger.debug(f"⏰ {position.stock_code} 24시간 이내 보유 - 시간 청산 금지")
                return False

            # 🎯 최대 보유시간 초과시에만 시간 청산
            if holding_hours >= max_hours:
                logger.info(f"⏰ {position.stock_code} 최대 거래시간({max_hours}h) 초과 청산: {holding_hours:.1f}h")
                return True

            return False

        except Exception as e:
            logger.error(f"❌ {position.stock_code} 시간 청산 판단 오류: {e}")
            return False

    def _is_trading_time(self) -> bool:
        """거래 시간 체크"""
        try:
            current_time = datetime.now().time()
            trading_start = datetime.strptime(self.config['trading_start_time'], '%H:%M').time()
            trading_end = datetime.strptime(self.config['trading_end_time'], '%H:%M').time()

            return trading_start <= current_time <= trading_end
        except Exception as e:
            logger.error(f"거래 시간 체크 오류: {e}")
            return False

    # ========== 🆕 패턴 기반 매매 신호 생성 ==========

    def generate_trade_signal_from_patterns(self, patterns: List[CandlePatternInfo]) -> Tuple[TradeSignal, int]:
        """🎯 패턴 기반 매매 신호 생성"""
        try:
            if not patterns:
                return TradeSignal.HOLD, 0

            # 가장 강한 패턴 기준
            primary_pattern = max(patterns, key=lambda p: p.strength)

            # 패턴별 신호 맵핑
            bullish_patterns = {
                PatternType.HAMMER,
                PatternType.INVERTED_HAMMER,
                PatternType.BULLISH_ENGULFING,
                PatternType.RISING_THREE_METHODS
            }

            bearish_patterns = {
                PatternType.BEARISH_ENGULFING,
                PatternType.EVENING_STAR,
                PatternType.FALLING_THREE_METHODS
            }

            neutral_patterns = {
                PatternType.DOJI
            }

            # 신호 결정
            if primary_pattern.pattern_type in bullish_patterns:
                if primary_pattern.confidence >= 0.85 and primary_pattern.strength >= 90:
                    return TradeSignal.STRONG_BUY, primary_pattern.strength
                elif primary_pattern.confidence >= 0.70:
                    return TradeSignal.BUY, primary_pattern.strength
                else:
                    return TradeSignal.HOLD, primary_pattern.strength

            elif primary_pattern.pattern_type in bearish_patterns:
                if primary_pattern.confidence >= 0.85:
                    return TradeSignal.STRONG_SELL, primary_pattern.strength
                else:
                    return TradeSignal.SELL, primary_pattern.strength

            elif primary_pattern.pattern_type in neutral_patterns:
                return TradeSignal.HOLD, primary_pattern.strength

            else:
                return TradeSignal.HOLD, 0

        except Exception as e:
            logger.error(f"매매 신호 생성 오류: {e}")
            return TradeSignal.HOLD, 0

    # ========== 🆕 위험도 및 우선순위 계산 ==========

    def calculate_risk_score(self, stock_info: dict) -> int:
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

    def calculate_entry_priority(self, candidate: CandleTradeCandidate) -> int:
        """🎯 진입 우선순위 계산 (0~100) - 🆕 시장상황 반영"""
        try:
            priority = 0

            # 1. 신호 강도 (25%)
            priority += candidate.signal_strength * 0.25

            # 2. 패턴 점수 (25%)
            priority += candidate.pattern_score * 0.25

            # 3. 패턴 신뢰도 (20%)
            if candidate.primary_pattern:
                priority += candidate.primary_pattern.confidence * 100 * 0.2

            # 4. 패턴별 기본 가중치 (15%)
            if candidate.primary_pattern:
                from .candle_trade_candidate import PatternType
                pattern_weights = {
                    PatternType.BULLISH_ENGULFING: 15,  # 최고 신뢰도
                    PatternType.HAMMER: 13,
                    PatternType.INVERTED_HAMMER: 11,
                    PatternType.RISING_THREE_METHODS: 9,
                    PatternType.DOJI: 6,               # 가장 낮음
                }
                weight = pattern_weights.get(candidate.primary_pattern.pattern_type, 8)
                priority += weight

            # 🆕 5. 시장상황 조정 (15%) - CandleTradeManager 접근
            try:
                # CandleTradeManager 인스턴스 찾기 (전역 참조 또는 config를 통해)
                market_adjustment = self._calculate_market_priority_adjustment(candidate)
                priority += market_adjustment

                if market_adjustment != 0:
                    logger.debug(f"🌍 {candidate.stock_code} 시장상황 우선순위 조정: {market_adjustment:+.1f}점")

            except Exception as market_err:
                logger.debug(f"시장상황 우선순위 조정 오류: {market_err}")
                # 오류시 기본값 사용

            # 정규화 (0~100)
            return min(100, max(0, int(priority)))

        except Exception as e:
            logger.error(f"진입 우선순위 계산 오류: {e}")
            return 50

    def _calculate_market_priority_adjustment(self, candidate: CandleTradeCandidate) -> float:
        """🌍 시장상황에 따른 우선순위 조정 점수 계산"""
        try:
            # CandleTradeManager의 MarketConditionAnalyzer 접근 시도
            # (순환 참조 방지를 위해 런타임에 접근)

            # 임시로 더미 시장 조건 생성 (실제로는 MarketConditionAnalyzer에서 가져와야 함)
            import random

            adjustment = 0.0

            # 🐂 상승장에서 상승 패턴 보너스
            if candidate.primary_pattern:
                from .candle_trade_candidate import PatternType
                bullish_patterns = [
                    PatternType.BULLISH_ENGULFING,
                    PatternType.HAMMER,
                    PatternType.INVERTED_HAMMER,
                    PatternType.RISING_THREE_METHODS
                ]

                # 더미 시장 상황 (실제로는 market_analyzer에서 가져와야 함)
                simulated_bull_market = random.choice([True, False])
                simulated_high_volatility = random.choice([True, False])

                if candidate.primary_pattern.pattern_type in bullish_patterns:
                    if simulated_bull_market:
                        adjustment += 10  # 상승장에서 상승 패턴 보너스

                    if simulated_high_volatility:
                        adjustment -= 5   # 고변동성에서는 약간 페널티

                # 🐻 하락장에서는 보수적 접근
                if not simulated_bull_market:
                    adjustment -= 8  # 하락장에서는 전반적 페널티

            return adjustment

        except Exception as e:
            logger.debug(f"시장상황 조정 계산 오류: {e}")
            return 0.0

    async def _basic_risk_analysis_only(self, candidate: CandleTradeCandidate, current_price: float, focus_on_exit: bool) -> Optional[Dict]:
        """🔧 패턴 분석 불가시 기본 리스크 관리만 수행"""
        try:
            # 기본 리스크 조건만 분석
            risk_signals = self.analyze_risk_conditions(candidate, current_price)
            time_signals = self.analyze_time_conditions(candidate)

            # 매도 조건만 체크 (패턴 없이)
            if focus_on_exit:
                if risk_signals.get('signal') in ['target_reached', 'stop_loss', 'loss_limit']:
                    final_signal = TradeSignal.SELL
                    signal_strength = 70
                else:
                    final_signal = TradeSignal.HOLD
                    signal_strength = 50
            else:
                final_signal = TradeSignal.HOLD
                signal_strength = 30  # 패턴 없으면 낮은 신호

            return {
                'new_signal': final_signal,
                'signal_strength': signal_strength,
                'price_change_pct': 0.0,
                'analysis_type': 'basic_risk_only',
                'pattern_analysis_failed': True,
                'risk_signals': risk_signals,
                'time_signals': time_signals,
                'analysis_time': datetime.now()
            }

        except Exception as e:
            logger.error(f"❌ 기본 리스크 분석 오류: {e}")
            return None

    async def _analyze_pattern_technical_harmony(self, stock_code: str, current_price: float,
                                               ohlcv_data: Any, current_pattern_signals: Dict) -> float:
        """📈 패턴과 기술적 지표의 조화성 분석"""
        try:
            # 기존 기술적 지표 분석
            technical_signals = await self.analyze_technical_indicators(stock_code, current_price, ohlcv_data)

            pattern_signal = current_pattern_signals.get('signal', 'neutral')
            technical_signal = technical_signals.get('signal', 'neutral')
            rsi = technical_signals.get('rsi', 50.0)

            # 조화성 점수 계산 (0.0 ~ 1.0)
            harmony_score = 0.5  # 기본값

            # 패턴과 기술지표가 같은 방향
            if pattern_signal == 'bullish' and technical_signal in ['oversold_bullish', 'oversold']:
                harmony_score = 0.9  # 매우 좋은 조화
            elif pattern_signal == 'bullish' and rsi < 50:
                harmony_score = 0.7  # 좋은 조화
            elif pattern_signal == 'bearish' and technical_signal in ['overbought_bearish', 'overbought']:
                harmony_score = 0.9  # 매우 좋은 조화
            elif pattern_signal == 'bearish' and rsi > 50:
                harmony_score = 0.7  # 좋은 조화

            # 상반된 신호
            elif pattern_signal == 'bullish' and rsi > 70:
                harmony_score = 0.3  # 조화 부족 (상승패턴 + 과매수)
            elif pattern_signal == 'bearish' and rsi < 30:
                harmony_score = 0.3  # 조화 부족 (하락패턴 + 과매도)

            return harmony_score

        except Exception as e:
            logger.error(f"❌ 패턴-기술지표 조화성 분석 오류: {e}")
            return 0.5

    def _analyze_candle_pattern_timing(self, candidate: CandleTradeCandidate) -> Dict:
        """🕯️ 단순화된 캔들패턴 시간 분석 - 24시간 내에는 시간 압박 없음"""
        try:
            # 기본값 설정
            signal = 'normal'
            time_pressure = 'low'
            trading_hours = True

            # 진입 시간 확인
            if not candidate.performance.entry_time:
                logger.debug(f"⚠️ {candidate.stock_code} 진입 시간 정보 없음")
                return {'signal': signal, 'time_pressure': time_pressure, 'trading_hours': trading_hours}

            # 시간 계산
            entry_time = candidate.performance.entry_time
            current_time = datetime.now(self.korea_tz)
            if entry_time.tzinfo is None:
                entry_time = entry_time.replace(tzinfo=self.korea_tz)

            # 거래시간 기준 보유 시간 계산
            holding_hours = calculate_business_hours_analyzer(entry_time, current_time)

            # 패턴별 최대 보유시간 가져오기
            _, _, max_holding_hours, _ = self._get_pattern_based_target(candidate)

            logger.debug(f"⏰ {candidate.stock_code} 시간 분석: {holding_hours:.1f}h / {max_holding_hours}h (거래시간만)")

            # 🆕 단순화된 시간 압박 로직
            if holding_hours < 24.0:
                # 24시간 이내: 시간 압박 없음
                time_pressure = 'low'
                signal = 'normal'
                logger.debug(f"⏰ {candidate.stock_code} 24시간 이내 - 시간 압박 없음")
            
            elif holding_hours >= max_holding_hours:
                # 최대 보유시간 초과: 강제 청산
                time_pressure = 'high'
                signal = 'time_exit'
                logger.info(f"⏰ {candidate.stock_code} 최대 보유시간 초과 - 강제 청산 신호")
            
            elif holding_hours >= max_holding_hours * 0.9:
                # 90% 경과: 주의 신호
                time_pressure = 'medium'
                signal = 'time_caution'
                logger.debug(f"⏰ {candidate.stock_code} 보유시간 90% 경과 - 주의 신호")
            
            else:
                # 정상 범위
                time_pressure = 'low'
                signal = 'normal'

            return {
                'signal': signal,
                'time_pressure': time_pressure,
                'trading_hours': trading_hours,
                'holding_hours': holding_hours,
                'max_holding_hours': max_holding_hours
            }

        except Exception as e:
            logger.error(f"❌ {candidate.stock_code} 시간 분석 오류: {e}")
            return {'signal': 'normal', 'time_pressure': 'low', 'trading_hours': True}

    def _analyze_pattern_specific_risks(self, candidate: CandleTradeCandidate, current_price: float, current_pattern_signals: Dict) -> Dict:
        """💰 패턴별 특화 리스크 조건 분석"""
        try:
            # 기본 리스크 분석
            base_risk = self.analyze_risk_conditions(candidate, current_price)

            # 패턴별 추가 리스크 요소
            pattern_risk_adjustments = []

            # 패턴 완성도가 낮으면 리스크 증가
            pattern_completion = current_pattern_signals.get('pattern_completion_rate', 0.5)
            if pattern_completion < 0.4:
                pattern_risk_adjustments.append('낮은 패턴 완성도')

            # 패턴 신뢰도가 낮으면 리스크 증가
            pattern_reliability = current_pattern_signals.get('pattern_reliability', 0.5)
            if pattern_reliability < 0.6:
                pattern_risk_adjustments.append('낮은 패턴 신뢰도')

            # 일봉 강도가 약하면 리스크 증가
            daily_strength = current_pattern_signals.get('daily_candle_strength', 0.5)
            if daily_strength < 0.3:
                pattern_risk_adjustments.append('약한 일봉 강도')

            # 종합 리스크 레벨 조정
            base_risk_level = base_risk.get('risk_level', 'medium')

            if len(pattern_risk_adjustments) >= 2:
                adjusted_risk_level = 'high_risk'
                signal = 'pattern_risk_high'
            elif len(pattern_risk_adjustments) == 1:
                adjusted_risk_level = 'medium_risk'
                signal = 'pattern_risk_medium'
            else:
                adjusted_risk_level = base_risk_level
                signal = base_risk.get('signal', 'neutral')

            result = base_risk.copy()
            result.update({
                'signal': signal,
                'risk_level': adjusted_risk_level,
                'pattern_risk_adjustments': pattern_risk_adjustments,
                'pattern_completion': pattern_completion,
                'pattern_reliability': pattern_reliability,
                'daily_strength': daily_strength
            })

            return result

        except Exception as e:
            logger.debug(f"패턴별 리스크 조건 분석 오류: {e}")
            return {'signal': 'neutral', 'risk_level': 'medium', 'pattern_risk_adjustments': []}

