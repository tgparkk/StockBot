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
    """🕒 주말을 제외한 영업시간 계산 (시간 단위) - candle_analyzer용"""
    try:
        # 시작시간이 종료시간보다 늦으면 0 반환
        if start_time >= end_time:
            return 0.0

        total_hours = 0.0
        current = start_time

        # 하루씩 계산하면서 주말 제외
        while current < end_time:
            # 현재 날짜의 요일 확인 (0=월요일, 6=일요일)
            weekday = current.weekday()

            # 주말(토요일=5, 일요일=6) 제외
            if weekday < 5:  # 월~금요일만
                # 하루의 끝 시간 계산
                day_end = current.replace(hour=23, minute=59, second=59, microsecond=999999)

                # 이 날에서 계산할 시간 범위
                day_start = current
                day_finish = min(day_end, end_time)

                # 이 날의 시간 추가
                day_hours = (day_finish - day_start).total_seconds() / 3600
                total_hours += day_hours

            # 다음 날로 이동
            current = (current + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

        return total_hours

    except Exception as e:
        logger.error(f"❌ 영업시간 계산 오류: {e}")
        # 오류시 기존 방식으로 폴백
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
                                                PatternType.BULLISH_ENGULFING, PatternType.MORNING_STAR,
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
        """⏰ 시간 기반 조건 분석 (주말 제외)"""
        try:
            current_time = datetime.now(self.korea_tz)

            # 거래 시간 체크
            trading_hours = self._is_trading_time()

            # 보유 시간 분석 (진입한 종목의 경우, 주말 제외)
            holding_duration = None
            time_pressure = 'none'

            if (candidate.status == CandleStatus.ENTERED and
                candidate.performance and
                candidate.performance.entry_time):

                entry_time = candidate.performance.entry_time
                # timezone 통일
                if entry_time.tzinfo is None:
                    entry_time = entry_time.replace(tzinfo=self.korea_tz)

                # 🆕 주말을 제외한 보유시간 계산
                holding_hours = calculate_business_hours_analyzer(entry_time, current_time)
                holding_duration = f"{holding_hours:.1f}h (주말제외)"

                max_holding_hours = candidate.risk_management.max_holding_hours

                if holding_hours >= max_holding_hours * 0.8:  # 80% 경과
                    time_pressure = 'high'
                elif holding_hours >= max_holding_hours * 0.5:  # 50% 경과
                    time_pressure = 'medium'
                else:
                    time_pressure = 'low'

            # 시간 기반 신호
            if not trading_hours:
                signal = 'closed_market'
            elif time_pressure == 'high':
                signal = 'time_exit'
            elif time_pressure == 'medium':
                signal = 'time_caution'
            else:
                signal = 'normal'

            return {
                'signal': signal,
                'trading_hours': trading_hours,
                'holding_duration': holding_duration,
                'time_pressure': time_pressure
            }

        except Exception as e:
            logger.debug(f"❌ {candidate.stock_code} 시간 조건 분석 오류: {e}")
            return {'signal': 'normal', 'trading_hours': True, 'time_pressure': 'none'}

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

            if current_price >= target_price:
                signal = 'target_reached'
                risk_level = 'profit_secure'
            elif current_price <= stop_loss_price:
                signal = 'stop_loss'
                risk_level = 'high_risk'
            elif pnl_pct >= self.config.get('default_target_profit_pct', 3.0):  # 기본 3% 수익
                signal = 'profit_target'
                risk_level = 'profit_zone'
            elif pnl_pct <= -self.config.get('default_stop_loss_pct', 3.0):  # 기본 3% 손실
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

    async def comprehensive_signal_analysis(self, candidate: CandleTradeCandidate, focus_on_exit: bool = False) -> Optional[Dict]:
        """🔍 캔들패턴 거래 관점의 종합 신호 분석"""
        try:
            stock_code = candidate.stock_code

            # 1. 최신 가격 정보 조회
            from ..api.kis_market_api import get_inquire_price
            current_data = get_inquire_price("J", stock_code)

            if current_data is None or current_data.empty:
                return None

            current_price = float(current_data.iloc[0].get('stck_prpr', 0))
            if current_price <= 0:
                return None

            # 가격 업데이트
            old_price = candidate.current_price
            candidate.update_price(current_price)

            # 🔄 일봉 데이터 확인 및 업데이트 (캔들패턴의 핵심)
            ohlcv_data = await self._ensure_fresh_ohlcv_data(candidate, stock_code)

            # 🎯 캔들패턴 전략에서는 일봉 데이터가 필수
            if ohlcv_data is None or ohlcv_data.empty:
                logger.warning(f"📊 {stock_code} 일봉 데이터 없음 - 캔들패턴 분석 불가")
                # 패턴 분석 없이 기본 리스크 관리만 수행
                return await self._basic_risk_analysis_only(candidate, current_price, focus_on_exit)

            # 🆕 2-1. 장중 데이터 보조 분석 (선택적)
            intraday_signals = await self._analyze_intraday_confirmation(stock_code, current_price, ohlcv_data)

            # 2. 🕯️ 패턴 전환 감지 (매우 중요!)
            pattern_change_analysis = await self._analyze_pattern_changes(candidate, stock_code, current_price, ohlcv_data)

            # 3. 📊 현재 패턴 상태 분석 (일봉 기준)
            current_pattern_signals = await self._analyze_daily_candle_patterns(stock_code, current_price, ohlcv_data)

            # 4. 📈 기술적 지표와 패턴 조화성 분석
            technical_harmony = await self._analyze_pattern_technical_harmony(stock_code, current_price, ohlcv_data, current_pattern_signals)

            # 5. ⏰ 캔들패턴 시간 조건 분석 (일봉 기준)
            pattern_time_signals = self._analyze_candle_pattern_timing(candidate)

            # 6. 💰 패턴별 리스크 조건 분석
            pattern_risk_signals = self._analyze_pattern_specific_risks(candidate, current_price, current_pattern_signals)

            # 7. 🎯 캔들패턴 기반 포지션 분석
            if focus_on_exit:
                position_signals = self._analyze_candle_exit_conditions(candidate, current_price, pattern_change_analysis)
            else:
                position_signals = self._analyze_candle_entry_conditions(candidate, current_price, current_pattern_signals)

            # 🆕 7-1. 장중 데이터로 포지션 신호 정밀화
            if intraday_signals and intraday_signals.get('valid', False):
                position_signals = self._refine_position_signals_with_intraday(position_signals, intraday_signals, focus_on_exit)

            # 8. 🧮 캔들패턴 중심의 종합 신호 계산
            final_signal, signal_strength = self._calculate_candle_focused_signal(
                current_pattern_signals, pattern_change_analysis, technical_harmony,
                pattern_time_signals, pattern_risk_signals, position_signals, focus_on_exit
            )

            return {
                'new_signal': final_signal,
                'signal_strength': signal_strength,
                'price_change_pct': ((current_price - old_price) / old_price * 100) if old_price > 0 else 0,

                # 🆕 캔들패턴 전용 분석 결과
                'current_pattern_signals': current_pattern_signals,
                'pattern_change_analysis': pattern_change_analysis,
                'technical_harmony': technical_harmony,
                'pattern_time_signals': pattern_time_signals,
                'pattern_risk_signals': pattern_risk_signals,
                'position_signals': position_signals,

                # 🆕 장중 데이터 분석 결과
                'intraday_signals': intraday_signals,

                # 분석 메타데이터
                'analysis_time': datetime.now(),
                'analysis_type': 'candle_pattern_focused_with_intraday',
                'daily_candle_updated': pattern_change_analysis.get('ohlcv_updated', False)
            }

        except Exception as e:
            logger.error(f"❌ 캔들패턴 신호 분석 오류 ({candidate.stock_code}): {e}")
            return None

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
        """분봉 데이터 조회 (KIS API 활용)"""
        try:
            # 🆕 KIS API는 당일 분봉만 제공하고 간격 설정이 제한적임
            # 현재는 기본 당일 분봉 데이터만 조회 (추후 개선 예정)
            from ..api.kis_market_api import get_inquire_time_itemchartprice

            logger.debug(f"📊 {stock_code} 당일 분봉 데이터 조회 시작 (최대 {count}개)")

            # KIS API 호출 - 당일 분봉 데이터
            minute_data = get_inquire_time_itemchartprice(
                output_dv="2",              # 분봉 데이터 배열 (output2)
                div_code="J",               # 조건시장분류코드 (J: 주식)
                itm_no=stock_code,          # 입력종목코드
                input_hour=None,            # 입력시간1 (None시 현재시간)
                past_data_yn="Y",           # 과거데이터포함여부
                etc_cls_code=""             # 기타구분코드 (공백)
            )

            if minute_data is not None and not minute_data.empty:
                # count 개수만큼 제한
                limited_data = minute_data.head(count)
                logger.debug(f"✅ {stock_code} 당일 분봉 데이터 조회 성공: {len(limited_data)}개")

                # 🔍 데이터 구조 확인 (디버깅용)
                if len(limited_data) > 0:
                    first_row = limited_data.iloc[0]
                    # KIS API의 실제 컬럼명 사용
                    time_info = first_row.get('stck_cntg_hour', first_row.get('stck_bsop_date', 'N/A'))
                    close_price = first_row.get('stck_clpr', 'N/A')
                    volume = first_row.get('cntg_vol', first_row.get('acml_vol', 'N/A'))

                    logger.debug(f"📊 첫 번째 분봉 데이터 샘플: 시간={time_info} "
                               f"종가={close_price}원 거래량={volume}")

                return limited_data
            else:
                logger.debug(f"⚠️ {stock_code} 당일 분봉 데이터 조회 결과 없음")
                return None

        except Exception as e:
            logger.debug(f"❌ {stock_code} 분봉 데이터 조회 오류: {e}")
            # 🔧 분봉 데이터 조회 실패시에도 장중 분석은 계속 진행
            # (일봉 기반 분석으로 폴백 가능)
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
        """�� 최신 일봉 데이터 확보 (캔들패턴의 핵심)"""
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
        """🎯 매도 조건 분석 (진입한 종목용)"""
        try:
            # 기본 매도 조건들
            should_exit = False
            exit_reasons = []

            # 🚨 패턴 반전 감지시 즉시 매도
            if pattern_change_analysis.get('action_required') == 'immediate_exit':
                should_exit = True
                exit_reasons.append('강한 패턴 반전 감지')
                return {
                    'signal': 'strong_sell',
                    'should_exit': should_exit,
                    'exit_reasons': exit_reasons,
                    'pattern_reversal_exit': True
                }

            # 🆕 패턴별 설정 가져오기
            target_profit_pct, stop_loss_pct, max_hours, pattern_based = self._get_pattern_based_target(candidate)

            # 1. 수익률 기반 매도 (패턴별 설정 사용)
            if candidate.performance.entry_price:
                pnl_pct = ((current_price - candidate.performance.entry_price) / candidate.performance.entry_price) * 100

                if pnl_pct >= target_profit_pct:
                    should_exit = True
                    exit_reasons.append(f'{target_profit_pct}% 수익 달성')
                elif pnl_pct <= -stop_loss_pct:
                    should_exit = True
                    exit_reasons.append(f'{stop_loss_pct}% 손절')

            # 2. 목표가/손절가 도달
            if current_price >= candidate.risk_management.target_price:
                should_exit = True
                exit_reasons.append('목표가 도달')
            elif current_price <= candidate.risk_management.stop_loss_price:
                should_exit = True
                exit_reasons.append('손절가 도달')

            # 3. 시간 청산 (패턴별)
            if self._should_time_exit_pattern_based(candidate, max_hours):
                should_exit = True
                exit_reasons.append(f'{max_hours}시간 청산')

            # 4. 패턴 변화 주의 (caution 레벨)
            if pattern_change_analysis.get('action_required') == 'caution':
                exit_reasons.append('패턴 변화 주의')

            signal = 'strong_sell' if should_exit else 'hold'

            return {
                'signal': signal,
                'should_exit': should_exit,
                'exit_reasons': exit_reasons
            }

        except Exception as e:
            logger.debug(f"매도 조건 분석 오류: {e}")
            return {'signal': 'hold', 'should_exit': False, 'exit_reasons': []}

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
        """🧮 캔들패턴 중심의 종합 신호 계산"""
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

            time_score = self._get_signal_score(pattern_time_signals.get('signal', 'normal'), 'time')
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
                return 2.0, 3.0, 24, False

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
                    return 3.0, 3.0, 24, True

            # 4. 기본값: 캔들 전략이지만 패턴 정보 없음 (🎯 큰 수익/손실 허용)
            logger.debug(f"📊 {position.stock_code} 캔들 전략이나 패턴 정보 없음 - 기본 캔들 설정 적용")
            return 3.0, 3.0, 6, True

        except Exception as e:
            logger.error(f"패턴별 설정 결정 오류 ({position.stock_code}): {e}")
            # 오류시 안전하게 기본값 반환 (🎯 큰 수익/손실 허용)
            return 3.0, 3.0, 24, False

    def _should_time_exit_pattern_based(self, position: CandleTradeCandidate, max_hours: int) -> bool:
        """🆕 패턴별 시간 청산 조건 체크 (개선된 버전 + 주말 제외)"""
        try:
            if not position.performance or not position.performance.entry_time:
                return False

            # 🆕 보유 시간 계산 (주말 제외)
            current_time = datetime.now(self.korea_tz)
            entry_time = position.performance.entry_time

            # timezone 통일
            if entry_time.tzinfo is None:
                entry_time = entry_time.replace(tzinfo=self.korea_tz)

            holding_hours = calculate_business_hours_analyzer(entry_time, current_time)

            # 패턴별 최대 보유시간 초과시 청산 (영업일 기준)
            if holding_hours >= max_hours:
                logger.info(f"⏰ {position.stock_code} 패턴별 최대 보유시간({max_hours}h) 초과 청산: {holding_hours:.1f}h (주말제외)")
                return True

            # 🔧 현재 수익률 재계산 (정확성 보장)
            current_price = position.current_price
            entry_price = position.performance.entry_price

            if not entry_price or entry_price <= 0:
                logger.debug(f"⚠️ {position.stock_code} 진입가 정보 없음 - 시간 청산 불가")
                return False

            # 🆕 실시간 수익률 계산
            current_pnl_pct = ((current_price - entry_price) / entry_price) * 100

            # 새로운 시간 기반 청산 규칙 적용 (선택적)
            time_rules = self.config.get('time_exit_rules', {})

            # 🔧 수익 중 시간 청산 (패턴별 시간의 절반 후, 영업일 기준)
            profit_exit_hours = max_hours // 2  # 패턴별 시간의 절반
            min_profit = time_rules.get('min_profit_for_time_exit', 1.0) / 100  # 🔧 기본값 1.0%

            if (holding_hours >= profit_exit_hours and
                current_pnl_pct >= min_profit):  # 🔧 실시간 계산된 수익률 사용
                logger.info(f"⏰ {position.stock_code} 패턴별 시간 기반 수익 청산: {holding_hours:.1f}h "
                           f"(실제수익률: {current_pnl_pct:+.2f}%, 기준: {min_profit*100:.1f}%, 주말제외)")
                return True

            # 🆕 손실 상황에서는 시간 청산 차단 (추가 안전장치)
            if current_pnl_pct < 0:
                logger.debug(f"🛡️ {position.stock_code} 손실 상황 - 시간 청산 차단 (수익률: {current_pnl_pct:+.2f}%)")
                return False

            return False

        except Exception as e:
            logger.error(f"❌ {position.stock_code} 패턴별 시간 청산 체크 오류: {e}")
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
                PatternType.MORNING_STAR,
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
        """🎯 진입 우선순위 계산 (0~100)"""
        try:
            priority = 0

            # 1. 신호 강도 (30%)
            priority += candidate.signal_strength * 0.3

            # 2. 패턴 점수 (30%)
            priority += candidate.pattern_score * 0.3

            # 3. 패턴 신뢰도 (20%)
            if candidate.primary_pattern:
                priority += candidate.primary_pattern.confidence * 100 * 0.2

            # 4. 패턴별 가중치 (20%)
            if candidate.primary_pattern:
                from .candle_trade_candidate import PatternType
                pattern_weights = {
                    PatternType.MORNING_STAR: 20,      # 최고 신뢰도
                    PatternType.BULLISH_ENGULFING: 18,
                    PatternType.HAMMER: 15,
                    PatternType.INVERTED_HAMMER: 15,
                    PatternType.RISING_THREE_METHODS: 12,
                    PatternType.DOJI: 8,               # 가장 낮음
                }
                weight = pattern_weights.get(candidate.primary_pattern.pattern_type, 10)
                priority += weight

            # 정규화 (0~100)
            return min(100, max(0, int(priority)))

        except Exception as e:
            logger.error(f"진입 우선순위 계산 오류: {e}")
            return 50

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
        """⏰ 캔들패턴 시간 조건 분석 (일봉 기준)"""
        try:
            current_time = datetime.now(self.korea_tz)

            # 거래 시간 체크
            trading_hours = self._is_trading_time()

            # 패턴 형성 시간 분석
            pattern_timing_score = 0.5  # 기본값

            # 현재 시간을 naive datetime으로도 준비 (시간대 비교용)
            current_time_naive = datetime.now()

            # 장 시작 1시간 이내 = 좋은 타이밍
            if trading_hours and current_time_naive.hour == 9:
                pattern_timing_score = 0.8
            # 장 중반 = 보통 타이밍
            elif trading_hours and 10 <= current_time_naive.hour <= 14:
                pattern_timing_score = 0.6
            # 장 마감 1시간 전 = 주의 필요
            elif trading_hours and current_time_naive.hour >= 14:
                pattern_timing_score = 0.4

            # 보유 시간 분석 (진입한 종목의 경우)
            holding_duration = None
            time_pressure = 'none'

            if (candidate.status == CandleStatus.ENTERED and
                candidate.performance and
                candidate.performance.entry_time):

                holding_duration = current_time - candidate.performance.entry_time
                max_holding = timedelta(hours=candidate.risk_management.max_holding_hours)

                if holding_duration >= max_holding * 0.8:  # 80% 경과
                    time_pressure = 'high'
                elif holding_duration >= max_holding * 0.5:  # 50% 경과
                    time_pressure = 'medium'
                else:
                    time_pressure = 'low'

            # 시간 기반 신호
            if not trading_hours:
                signal = 'closed_market'
            elif time_pressure == 'high':
                signal = 'time_exit'
            elif time_pressure == 'medium':
                signal = 'time_caution'
            else:
                signal = 'normal'

            return {
                'signal': signal,
                'trading_hours': trading_hours,
                'holding_duration': str(holding_duration) if holding_duration else None,
                'time_pressure': time_pressure,
                'pattern_timing_score': pattern_timing_score
            }

        except Exception as e:
            logger.debug(f"❌ {candidate.stock_code} 캔들패턴 시간 조건 분석 오류: {e}")
            return {'signal': 'normal', 'trading_hours': True, 'time_pressure': 'none', 'pattern_timing_score': 0.5}

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
