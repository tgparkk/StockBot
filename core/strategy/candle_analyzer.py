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
        """⏰ 시간 기반 조건 분석"""
        try:
            current_time = datetime.now()

            # 거래 시간 체크
            trading_hours = self._is_trading_time()

            # 보유 시간 분석 (진입한 종목의 경우)
            holding_duration = None
            time_pressure = 'none'

            if candidate.status == CandleStatus.ENTERED and candidate.performance.entry_time:
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
                'time_pressure': time_pressure
            }

        except Exception as e:
            logger.debug(f"시간 조건 분석 오류: {e}")
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

            # 🆕 캐시된 OHLCV 데이터 우선 사용 (API 호출 제거)
            ohlcv_data = candidate.get_ohlcv_data()
            if ohlcv_data is None:
                logger.debug(f"📄 {stock_code} 캐시된 일봉 데이터 없음 - 분석 제한")
                # 캐시된 데이터가 없어도 기본 분석은 진행
                ohlcv_data = None

            # 2. 📊 최신 캔들 패턴 재분석 (OHLCV 데이터 전달)
            pattern_signals = await self.analyze_current_patterns(stock_code, current_price, ohlcv_data)

            # 3. 📈 기술적 지표 분석 (OHLCV 데이터 전달)
            technical_signals = await self.analyze_technical_indicators(stock_code, current_price, ohlcv_data)

            # 4. ⏰ 시간 기반 조건 분석
            time_signals = self.analyze_time_conditions(candidate)

            # 5. 💰 리스크 조건 분석
            risk_signals = self.analyze_risk_conditions(candidate, current_price)

            # 6. 🎯 포지션 상태별 특화 분석
            if focus_on_exit:
                position_signals = self._analyze_exit_conditions(candidate, current_price, ohlcv_data)
            else:
                position_signals = self._analyze_entry_conditions_simple(candidate, current_price)

            # 7. 종합 신호 계산
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

    def _analyze_exit_conditions(self, candidate: CandleTradeCandidate, current_price: float, ohlcv_data: Optional[Any] = None) -> Dict:
        """🎯 매도 조건 분석 (진입한 종목용)"""
        try:
            # 기본 매도 조건들
            should_exit = False
            exit_reasons = []

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

            signal = 'strong_sell' if should_exit else 'hold'

            return {
                'signal': signal,
                'should_exit': should_exit,
                'exit_reasons': exit_reasons
            }

        except Exception as e:
            logger.debug(f"매도 조건 분석 오류: {e}")
            return {'signal': 'hold', 'should_exit': False, 'exit_reasons': []}

    def _analyze_entry_conditions_simple(self, candidate: CandleTradeCandidate, current_price: float) -> Dict:
        """🎯 진입 조건 간단 분석 (관찰 중인 종목용)"""
        try:
            # 기본 진입 조건 체크
            can_enter = True
            entry_reasons = []

            # 가격대 체크
            if not (self.config['min_price'] <= current_price <= self.config['max_price']):
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

    def _calculate_comprehensive_signal(self, pattern_signals: Dict, technical_signals: Dict,
                                      time_signals: Dict, risk_signals: Dict, position_signals: Dict,
                                      focus_on_exit: bool = False) -> Tuple[TradeSignal, int]:
        """🧮 종합 신호 계산"""
        try:
            # 가중치 설정
            if focus_on_exit:
                # 매도 신호 중심
                weights = {
                    'risk': 0.4,        # 리스크 조건이 가장 중요
                    'position': 0.3,    # 포지션 분석
                    'time': 0.2,        # 시간 조건
                    'technical': 0.1,   # 기술적 지표
                    'pattern': 0.0      # 패턴은 매도시 덜 중요
                }
            else:
                # 매수 신호 중심
                weights = {
                    'pattern': 0.4,     # 패턴이 가장 중요
                    'technical': 0.3,   # 기술적 지표
                    'position': 0.2,    # 진입 조건
                    'risk': 0.1,        # 리스크 조건
                    'time': 0.0         # 시간은 덜 중요
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
                # 매도 신호
                if total_score >= 80:
                    return TradeSignal.STRONG_SELL, int(total_score)
                elif total_score >= 60:
                    return TradeSignal.SELL, int(total_score)
                else:
                    return TradeSignal.HOLD, int(total_score)
            else:
                # 매수 신호
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
            time_rules = self.config.get('time_exit_rules', {})

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
