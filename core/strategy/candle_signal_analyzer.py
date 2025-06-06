"""
캔들 신호 분석 전용 모듈
"""
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum

from utils.logger import setup_logger
from .candle_trade_candidate import (
    CandleTradeCandidate, TradeSignal, PatternType, CandlePatternInfo
)
from .candle_pattern_detector import CandlePatternDetector

logger = setup_logger(__name__)


class SignalStrength(Enum):
    """신호 강도"""
    VERY_WEAK = 0
    WEAK = 25
    MEDIUM = 50
    STRONG = 75
    VERY_STRONG = 100


class CandleSignalAnalyzer:
    """캔들 기반 신호 분석 전용 클래스"""

    def __init__(self, pattern_detector: CandlePatternDetector):
        self.pattern_detector = pattern_detector
        self.korea_tz = timezone(timedelta(hours=9))

        # 신호 분석 설정
        self.signal_config = {
            'pattern_confidence_threshold': 0.6,
            'rsi_oversold': 30,
            'rsi_overbought': 70,
            'volume_threshold': 1.5,
            'trend_strength_threshold': 5.0,
        }

        logger.info("✅ CandleSignalAnalyzer 초기화 완료")

    # ==========================================
    # 🎯 메인 신호 생성 메서드
    # ==========================================

    def generate_trade_signal(self, candidate: CandleTradeCandidate,
                            patterns: List[CandlePatternInfo]) -> Tuple[TradeSignal, int]:
        """패턴 기반 매매 신호 생성"""
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

            else:
                return TradeSignal.HOLD, primary_pattern.strength

        except Exception as e:
            logger.error(f"매매 신호 생성 오류: {e}")
            return TradeSignal.HOLD, 0

    async def comprehensive_signal_analysis(self, candidate: CandleTradeCandidate,
                                          focus_on_exit: bool = False) -> Optional[Dict]:
        """🔍 다각도 종합 신호 분석"""
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

            # OHLCV 데이터 조회 (한 번만)
            from ..api.kis_market_api import get_inquire_daily_itemchartprice
            ohlcv_data = get_inquire_daily_itemchartprice(
                output_dv="2",
                itm_no=stock_code,
                period_code="D",
                adj_prc="1"
            )

            # 2. 각 분야별 신호 분석
            pattern_signals = await self._analyze_current_patterns(stock_code, current_price, ohlcv_data)
            technical_signals = await self._analyze_technical_indicators(stock_code, current_price, ohlcv_data)
            time_signals = self._analyze_time_conditions(candidate)
            risk_signals = self._analyze_risk_conditions(candidate, current_price)

            if focus_on_exit:
                position_signals = self._analyze_exit_conditions(candidate, current_price, ohlcv_data)
            else:
                position_signals = self._analyze_entry_conditions(candidate, current_price)

            # 3. 종합 신호 계산
            final_signal, signal_strength = self._calculate_comprehensive_signal(
                pattern_signals, technical_signals, time_signals,
                risk_signals, position_signals, focus_on_exit
            )

            return {
                'new_signal': final_signal,
                'signal_strength': signal_strength,
                'price_change_pct': ((current_price - old_price) / old_price * 100) if old_price > 0 else 0,
                'pattern_signals': pattern_signals,
                'technical_signals': technical_signals,
                'time_signals': time_signals,
                'risk_signals': risk_signals,
                'position_signals': position_signals,
                'analysis_time': datetime.now()
            }

        except Exception as e:
            logger.debug(f"종합 신호 분석 오류 ({candidate.stock_code}): {e}")
            return None

    # ==========================================
    # 📊 개별 분야별 분석 메서드들
    # ==========================================

    async def _analyze_current_patterns(self, stock_code: str, current_price: float,
                                       ohlcv_data: Optional[Any]) -> Dict:
        """📊 최신 캔들 패턴 분석"""
        try:
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
                                                PatternType.BULLISH_ENGULFING, PatternType.MORNING_STAR]:
                signal = 'bullish'
            elif strongest_pattern.pattern_type in [PatternType.BEARISH_ENGULFING, PatternType.EVENING_STAR]:
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

    async def _analyze_technical_indicators(self, stock_code: str, current_price: float,
                                          ohlcv_data: Optional[Any]) -> Dict:
        """📈 기술적 지표 분석"""
        try:
            if ohlcv_data is None or ohlcv_data.empty or len(ohlcv_data) < 20:
                return {'signal': 'neutral', 'rsi': 50.0, 'trend': 'neutral'}

            # 종가 데이터 추출
            close_prices = []
            for _, row in ohlcv_data.head(20).iterrows():
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
            if current_rsi < self.signal_config['rsi_oversold'] and trend in ['uptrend', 'neutral']:
                signal = 'oversold_bullish'
            elif current_rsi > self.signal_config['rsi_overbought'] and trend in ['downtrend', 'neutral']:
                signal = 'overbought_bearish'
            elif current_rsi < self.signal_config['rsi_oversold']:
                signal = 'oversold'
            elif current_rsi > self.signal_config['rsi_overbought']:
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

    def _analyze_time_conditions(self, candidate: CandleTradeCandidate) -> Dict:
        """⏰ 시간 기반 조건 분석"""
        try:
            current_time = datetime.now()

            # 거래 시간 체크
            trading_hours = self._is_trading_time()

            # 보유 시간 분석 (진입한 종목의 경우)
            holding_duration = None
            time_pressure = 'none'

            if (hasattr(candidate, 'status') and
                candidate.status.value == 'ENTERED' and
                candidate.performance.entry_time):

                holding_duration = current_time - candidate.performance.entry_time
                max_holding = timedelta(hours=candidate.risk_management.max_holding_hours)

                if holding_duration >= max_holding * 0.8:
                    time_pressure = 'high'
                elif holding_duration >= max_holding * 0.5:
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
                'time_pressure': time_pressure
            }

        except Exception as e:
            logger.debug(f"시간 조건 분석 오류: {e}")
            return {'signal': 'normal', 'trading_hours': True, 'time_pressure': 'none'}

    def _analyze_risk_conditions(self, candidate: CandleTradeCandidate, current_price: float) -> Dict:
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
                risk_level = 'high_loss'
            elif pnl_pct >= 3.0:
                signal = 'profit_target'
                risk_level = 'secure_profit'
            elif pnl_pct <= -3.0:
                signal = 'loss_limit'
                risk_level = 'high_loss'
            elif pnl_pct >= 1.0:
                signal = 'profit_zone'
                risk_level = 'low'
            elif pnl_pct <= -1.0:
                signal = 'loss_zone'
                risk_level = 'medium'
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

    def _analyze_exit_conditions(self, candidate: CandleTradeCandidate, current_price: float,
                               ohlcv_data: Optional[Any] = None) -> Dict:
        """🎯 매도 조건 분석 (진입한 종목용)"""
        try:
            should_exit = False
            exit_reasons = []

            # 패턴별 설정 가져오기 (외부 함수 호출 필요)
            target_profit_pct, stop_loss_pct = self._get_default_targets()

            # 수익률 기반 매도
            if candidate.performance.entry_price:
                pnl_pct = ((current_price - candidate.performance.entry_price) / candidate.performance.entry_price) * 100

                if pnl_pct >= target_profit_pct:
                    should_exit = True
                    exit_reasons.append(f'{target_profit_pct}% 수익 달성')
                elif pnl_pct <= -stop_loss_pct:
                    should_exit = True
                    exit_reasons.append(f'{stop_loss_pct}% 손절')

            # 목표가/손절가 도달
            if current_price >= candidate.risk_management.target_price:
                should_exit = True
                exit_reasons.append('목표가 도달')
            elif current_price <= candidate.risk_management.stop_loss_price:
                should_exit = True
                exit_reasons.append('손절가 도달')

            signal = 'strong_sell' if should_exit else 'hold'

            return {
                'signal': signal,
                'should_exit': should_exit,
                'exit_reasons': exit_reasons
            }

        except Exception as e:
            logger.debug(f"매도 조건 분석 오류: {e}")
            return {'signal': 'hold', 'should_exit': False, 'exit_reasons': []}

    def _analyze_entry_conditions(self, candidate: CandleTradeCandidate, current_price: float) -> Dict:
        """🎯 진입 조건 분석 (관찰 중인 종목용)"""
        try:
            can_enter = True
            entry_reasons = []

            # 기본 진입 조건들
            min_price = 1000
            max_price = 500000

            # 가격대 체크
            if not (min_price <= current_price <= max_price):
                can_enter = False
            else:
                entry_reasons.append('가격대 적정')

            # 거래 시간 체크
            if self._is_trading_time():
                entry_reasons.append('거래 시간')
            else:
                can_enter = False

            signal = 'buy_ready' if can_enter else 'wait'

            return {
                'signal': signal,
                'can_enter': can_enter,
                'entry_reasons': entry_reasons
            }

        except Exception as e:
            logger.debug(f"진입 조건 분석 오류: {e}")
            return {'signal': 'wait', 'can_enter': False, 'entry_reasons': []}

    # ==========================================
    # 🧮 종합 계산 메서드들
    # ==========================================

    def _calculate_comprehensive_signal(self, pattern_signals: Dict, technical_signals: Dict,
                                      time_signals: Dict, risk_signals: Dict, position_signals: Dict,
                                      focus_on_exit: bool = False) -> Tuple[TradeSignal, int]:
        """🧮 종합 신호 계산"""
        try:
            # 가중치 설정
            if focus_on_exit:
                weights = {
                    'risk': 0.4, 'position': 0.3, 'time': 0.2, 'technical': 0.1, 'pattern': 0.0
                }
            else:
                weights = {
                    'pattern': 0.4, 'technical': 0.3, 'position': 0.2, 'risk': 0.1, 'time': 0.0
                }

            # 각 신호의 점수 계산 (0~100)
            pattern_score = self._get_signal_score(pattern_signals.get('signal', 'neutral'), 'pattern')
            technical_score = self._get_signal_score(technical_signals.get('signal', 'neutral'), 'technical')
            time_score = self._get_signal_score(time_signals.get('signal', 'normal'), 'time')
            risk_score = self._get_signal_score(risk_signals.get('signal', 'neutral'), 'risk')
            position_score = self._get_signal_score(position_signals.get('signal', 'wait'), 'position')

            # 가중 평균 계산
            total_score = (
                pattern_score * weights['pattern'] +
                technical_score * weights['technical'] +
                time_score * weights['time'] +
                risk_score * weights['risk'] +
                position_score * weights['position']
            )

            # 신호 결정
            if focus_on_exit:
                if total_score >= 80:
                    return TradeSignal.STRONG_SELL, int(total_score)
                elif total_score >= 60:
                    return TradeSignal.SELL, int(total_score)
                else:
                    return TradeSignal.HOLD, int(total_score)
            else:
                if total_score >= 80:
                    return TradeSignal.STRONG_BUY, int(total_score)
                elif total_score >= 60:
                    return TradeSignal.BUY, int(total_score)
                else:
                    return TradeSignal.HOLD, int(total_score)

        except Exception as e:
            logger.debug(f"종합 신호 계산 오류: {e}")
            return TradeSignal.HOLD, 50

    def _get_signal_score(self, signal: str, signal_type: str) -> float:
        """신호를 점수로 변환 (0~100)"""
        try:
            if signal_type == 'pattern':
                scores = {'bullish': 80, 'bearish': 20, 'neutral': 50}
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
                    'loss_limit': 85, 'profit_zone': 60, 'loss_zone': 40, 'neutral': 50
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

    # ==========================================
    # 🔧 유틸리티 메서드들
    # ==========================================

    def _is_trading_time(self) -> bool:
        """거래 시간 체크"""
        try:
            current_time = datetime.now().time()
            trading_start = datetime.strptime('09:00', '%H:%M').time()
            trading_end = datetime.strptime('15:20', '%H:%M').time()
            return trading_start <= current_time <= trading_end
        except Exception as e:
            logger.error(f"거래 시간 체크 오류: {e}")
            return False

    def _get_default_targets(self) -> Tuple[float, float]:
        """기본 목표/손절 설정 반환"""
        return 3.0, 2.0  # 3% 목표, 2% 손절

    def should_update_signal(self, candidate: CandleTradeCandidate, analysis_result: Dict) -> bool:
        """신호 업데이트 필요 여부 판단"""
        try:
            new_signal = analysis_result['new_signal']
            new_strength = analysis_result['signal_strength']

            # 신호가 변경되었거나 강도가 크게 변했을 때만 업데이트
            signal_changed = new_signal != candidate.trade_signal
            strength_changed = abs(new_strength - candidate.signal_strength) >= 20

            return signal_changed or strength_changed

        except Exception as e:
            logger.debug(f"신호 업데이트 판단 오류: {e}")
            return False

    def should_update_exit_signal(self, candidate: CandleTradeCandidate, analysis_result: Dict) -> bool:
        """매도 신호 업데이트 필요 여부 판단"""
        try:
            new_signal = analysis_result['new_signal']
            return new_signal in [TradeSignal.SELL, TradeSignal.STRONG_SELL] and candidate.trade_signal != new_signal
        except Exception as e:
            logger.debug(f"매도 신호 업데이트 판단 오류: {e}")
            return False

    # ==========================================
    # 🔄 주기적 신호 재평가 시스템
    # ==========================================

    async def periodic_evaluation(self, all_stocks: Dict[str, CandleTradeCandidate]) -> int:
        """🔄 모든 종목에 대한 주기적 신호 재평가"""
        try:
            if not all_stocks:
                return 0

            logger.debug(f"🔄 주기적 신호 재평가 시작: {len(all_stocks)}개 종목")

            # 종목들을 상태별로 분류하여 처리
            watching_stocks = []
            entered_stocks = []

            for stock_code, candidate in all_stocks.items():
                if hasattr(candidate.status, 'name'):
                    status_name = candidate.status.name
                else:
                    status_name = str(candidate.status)

                if status_name in ['WATCHING', 'BUY_READY']:
                    watching_stocks.append(candidate)
                elif status_name in ['ENTERED', 'SELL_READY']:
                    entered_stocks.append(candidate)

            # 배치 처리로 API 호출 최적화
            updated_count = 0

            # 1. 관찰 중인 종목들 재평가 (우선순위 높음)
            if watching_stocks:
                updated_count += await self._batch_evaluate_watching_stocks(watching_stocks)

            # 2. 진입한 종목들 재평가 (매도 신호 중심)
            if entered_stocks:
                updated_count += await self._batch_evaluate_entered_stocks(entered_stocks)

            if updated_count > 0:
                logger.info(f"🔄 신호 재평가 완료: {updated_count}개 종목 업데이트")

            return updated_count

        except Exception as e:
            logger.error(f"주기적 신호 재평가 오류: {e}")
            return 0

    async def _batch_evaluate_watching_stocks(self, candidates: List[CandleTradeCandidate]) -> int:
        """관찰 중인 종목들 배치 재평가"""
        try:
            updated_count = 0

            # API 호출 제한을 위해 5개씩 배치 처리
            batch_size = 5
            for i in range(0, len(candidates), batch_size):
                batch = candidates[i:i + batch_size]

                for candidate in batch:
                    try:
                        # 다각도 종합 분석 수행
                        analysis_result = await self.comprehensive_signal_analysis(candidate)

                        if analysis_result and self.should_update_signal(candidate, analysis_result):
                            # 신호 업데이트
                            old_signal = candidate.trade_signal
                            candidate.trade_signal = analysis_result['new_signal']
                            candidate.signal_strength = analysis_result['signal_strength']
                            candidate.signal_updated_at = datetime.now(self.korea_tz)

                            # 우선순위 재계산 (간단한 버전)
                            candidate.entry_priority = min(100, max(0, analysis_result['signal_strength']))

                            logger.info(f"🔄 {candidate.stock_code} 신호 업데이트: "
                                       f"{old_signal.value} → {candidate.trade_signal.value} "
                                       f"(강도:{candidate.signal_strength})")
                            updated_count += 1

                    except Exception as e:
                        logger.debug(f"종목 재평가 오류 ({candidate.stock_code}): {e}")
                        continue

                # API 호출 간격 조절
                if i + batch_size < len(candidates):
                    await asyncio.sleep(0.5)  # 0.5초 대기

            return updated_count

        except Exception as e:
            logger.error(f"관찰 종목 배치 재평가 오류: {e}")
            return 0

    async def _batch_evaluate_entered_stocks(self, candidates: List[CandleTradeCandidate]) -> int:
        """진입한 종목들 배치 재평가 (매도 신호 중심)"""
        try:
            updated_count = 0

            for candidate in candidates:
                try:
                    # 기존 보유 종목도 매도 신호 재평가
                    analysis_result = await self.comprehensive_signal_analysis(candidate, focus_on_exit=True)

                    if analysis_result and self.should_update_exit_signal(candidate, analysis_result):
                        # 매도 신호 업데이트
                        old_signal = candidate.trade_signal
                        candidate.trade_signal = analysis_result['new_signal']
                        candidate.signal_strength = analysis_result['signal_strength']
                        candidate.signal_updated_at = datetime.now(self.korea_tz)

                        logger.info(f"🔄 {candidate.stock_code} 매도신호 업데이트: "
                                   f"{old_signal.value} → {candidate.trade_signal.value} "
                                   f"(강도:{candidate.signal_strength})")
                        updated_count += 1

                        # 강한 매도 신호시 즉시 매도 검토 로그
                        if candidate.trade_signal in [TradeSignal.STRONG_SELL, TradeSignal.SELL]:
                            logger.info(f"🎯 {candidate.stock_code} 강한 매도 신호 - 즉시 매도 검토 필요")

                except Exception as e:
                    logger.debug(f"진입 종목 재평가 오류 ({candidate.stock_code}): {e}")
                    continue

            return updated_count

        except Exception as e:
            logger.error(f"진입 종목 배치 재평가 오류: {e}")
            return 0
