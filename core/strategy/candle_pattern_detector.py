"""
캔들 패턴 감지 및 분석 시스템
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from utils.logger import setup_logger

from .candle_trade_candidate import (
    CandlePatternInfo, PatternType, TradeSignal
)

logger = setup_logger(__name__)


class CandlePatternDetector:
    """캔들 패턴 감지 및 분석 시스템"""

    def __init__(self):
        # 패턴별 신뢰도 가중치 설정 (실전 검증 기반)
        self.pattern_weights = {
            PatternType.HAMMER: 0.85,
            PatternType.INVERTED_HAMMER: 0.75,
            PatternType.BULLISH_ENGULFING: 0.90,
            PatternType.BEARISH_ENGULFING: 0.90,
            PatternType.MORNING_STAR: 0.95,
            PatternType.EVENING_STAR: 0.95,
            PatternType.DOJI: 0.70,
            PatternType.RISING_THREE_METHODS: 0.80,
            PatternType.FALLING_THREE_METHODS: 0.80
        }

        # 패턴 감지 임계값 설정 (🔧 더 유연하게 조정)
        self.thresholds = {
            'body_shadow_ratio': 0.35,      # 몸통/그림자 비율 (0.3 → 0.35로 완화)
            'engulfing_threshold': 1.05,    # 장악형 최소 비율 (1.1 → 1.05로 완화)
            'doji_body_ratio': 0.08,        # 도지 몸통 비율 (0.05 → 0.08로 완화)
            'star_gap_threshold': 0.001,    # 별형 갭 임계값 (0.002 → 0.001로 완화)
            'trend_min_days': 2,            # 추세 확인 최소 일수 (3 → 2로 완화)
            'volume_confirmation': 1.1,     # 거래량 확인 배율 (1.2 → 1.1로 완화)
            'min_confidence': 0.4,          # 🆕 최소 신뢰도 기준 추가 (유연한 필터링)
            'trend_strength_min': 0.3       # 🆕 최소 추세 강도 (기존 0.5에서 완화)
        }

    def analyze_stock_patterns(self, stock_code: str, ohlcv_data: pd.DataFrame,
                             volume_data: Optional[pd.DataFrame] = None) -> List[CandlePatternInfo]:
        """종목의 캔들 패턴 종합 분석"""
        try:
            if ohlcv_data is None or ohlcv_data.empty:
                logger.warning(f"종목 {stock_code}: OHLCV 데이터 없음")
                return []

            # 🔧 데이터 부족 조건 완화 (5일 → 3일)
            if len(ohlcv_data) < 3:
                logger.warning(f"종목 {stock_code}: 데이터 부족 ({len(ohlcv_data)}일)")
                return []

            # 데이터 전처리
            df = self._prepare_data(ohlcv_data)

            # 🔧 전처리 실패 시에도 기본 분석 시도
            if df.empty:
                logger.warning(f"종목 {stock_code}: 데이터 전처리 실패, 기본 분석 시도")
                df = self._prepare_basic_data(ohlcv_data)
                if df.empty:
                    return []

            detected_patterns = []

            # 🔥 1. 망치형 패턴 감지
            hammer_patterns = self._detect_hammer_patterns(df, stock_code)
            detected_patterns.extend(hammer_patterns)

            # 🔥 2. 장악형 패턴 감지
            engulfing_patterns = self._detect_engulfing_patterns(df, stock_code)
            detected_patterns.extend(engulfing_patterns)

            # 🔥 3. 샛별형 패턴 감지 (데이터 부족 시 스킵)
            if len(df) >= 3:
                star_patterns = self._detect_star_patterns(df, stock_code)
                detected_patterns.extend(star_patterns)

            # 🔥 4. 도지 패턴 감지
            doji_patterns = self._detect_doji_patterns(df, stock_code)
            detected_patterns.extend(doji_patterns)

            # 🔥 5. 삼법형 패턴 감지 (데이터 부족 시 스킵)
            if len(df) >= 5:
                three_methods_patterns = self._detect_three_methods_patterns(df, stock_code)
                detected_patterns.extend(three_methods_patterns)

            # 🆕 6. 기본 추세 패턴 감지 (패턴이 없는 경우를 위한 대안)
            if not detected_patterns:
                basic_patterns = self._detect_basic_trend_patterns(df, stock_code)
                detected_patterns.extend(basic_patterns)

            # 패턴 품질 필터링 및 정렬
            filtered_patterns = self._filter_and_rank_patterns(detected_patterns, df)

            if filtered_patterns:
                pattern_names = [p.pattern_type.value for p in filtered_patterns]
                logger.info(f"🎯 {stock_code} 패턴 감지: {', '.join(pattern_names)}")
            else:
                logger.debug(f"❌ {stock_code} 패턴 감지 실패 - 조건을 만족하는 패턴 없음")

            return filtered_patterns

        except Exception as e:
            logger.error(f"패턴 분석 오류 ({stock_code}): {e}")
            return []

    def _prepare_data(self, ohlcv_data: pd.DataFrame) -> pd.DataFrame:
        """데이터 전처리 및 지표 계산"""
        try:
            df = ohlcv_data.copy()

            # 필수 컬럼 확인 및 변환
            required_cols = ['open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                # 컬럼명 변환 시도 (KIS API 형식)
                col_mapping = {
                    'stck_oprc': 'open',
                    'stck_hgpr': 'high',
                    'stck_lwpr': 'low',
                    'stck_clpr': 'close',
                    'acml_vol': 'volume'
                }

                for old_col, new_col in col_mapping.items():
                    if old_col in df.columns:
                        df[new_col] = pd.to_numeric(df[old_col], errors='coerce')

            # 데이터 타입 변환
            for col in ['open', 'high', 'low', 'close']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            if 'volume' in df.columns:
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce')

            # 🔥 기본 캔들 지표 계산
            df['body'] = abs(df['close'] - df['open'])  # 실체 크기
            df['upper_shadow'] = df['high'] - df[['open', 'close']].max(axis=1)  # 윗꼬리
            df['lower_shadow'] = df[['open', 'close']].min(axis=1) - df['low']   # 아래꼬리
            df['total_range'] = df['high'] - df['low']  # 전체 범위

            # 몸통 비율
            df['body_ratio'] = df['body'] / df['total_range']
            df['body_ratio'] = df['body_ratio'].fillna(0)

            # 상승/하락 구분
            df['is_bullish'] = df['close'] > df['open']
            df['is_bearish'] = df['close'] < df['open']

            # 🔥 1. 거래량 관련 지표
            df = self._calculate_volume_indicators(df)

            # 🔥 2. 변동성 지표
            df = self._calculate_volatility_indicators(df)

            # 🔥 3. 모멘텀 지표
            df = self._calculate_momentum_indicators(df)

            # 🔥 4. 기존 이동평균
            df['ma_5'] = df['close'].rolling(window=5).mean()
            df['ma_20'] = df['close'].rolling(window=20).mean()

            # 최신 데이터가 첫 번째 행이 되도록 정렬
            df = df.sort_index(ascending=False).reset_index(drop=True)

            return df

        except Exception as e:
            logger.error(f"데이터 전처리 오류: {e}")
            return pd.DataFrame()

    def _prepare_basic_data(self, ohlcv_data: pd.DataFrame) -> pd.DataFrame:
        """🆕 기본 데이터 전처리 (오류 시 대안)"""
        try:
            df = ohlcv_data.copy()

            # 기본 컬럼명 변환 시도
            col_mapping = {
                'stck_oprc': 'open', 'stck_hgpr': 'high', 'stck_lwpr': 'low',
                'stck_clpr': 'close', 'acml_vol': 'volume'
            }

            for old_col, new_col in col_mapping.items():
                if old_col in df.columns:
                    df[new_col] = pd.to_numeric(df[old_col], errors='coerce')

            # 필수 컬럼 확인
            required_cols = ['open', 'high', 'low', 'close']
            if not all(col in df.columns for col in required_cols):
                return pd.DataFrame()

            # 기본 지표만 계산
            df['body'] = abs(df['close'] - df['open'])
            df['total_range'] = df['high'] - df['low']
            df['body_ratio'] = df['body'] / df['total_range'].replace(0, 1)
            df['is_bullish'] = df['close'] > df['open']
            df['is_bearish'] = df['close'] < df['open']

            # 정렬
            df = df.sort_index(ascending=False).reset_index(drop=True)
            return df

        except Exception as e:
            logger.error(f"기본 데이터 전처리 오류: {e}")
            return pd.DataFrame()

    def _detect_hammer_patterns(self, df: pd.DataFrame, stock_code: str) -> List[CandlePatternInfo]:
        """망치형/역망치형 패턴 감지"""
        patterns = []

        try:
            for i in range(min(3, len(df))):  # 최근 3일만 체크
                current = df.iloc[i]

                # 기본 조건 체크
                if current['total_range'] <= 0:
                    continue

                body_ratio = current['body_ratio']
                upper_shadow_ratio = current['upper_shadow'] / current['total_range']
                lower_shadow_ratio = current['lower_shadow'] / current['total_range']

                # 🔨 망치형 패턴 (Hammer)
                if (body_ratio <= self.thresholds['body_shadow_ratio'] and
                    lower_shadow_ratio >= 0.6 and  # 아래꼬리가 전체의 60% 이상
                    upper_shadow_ratio <= 0.1):    # 윗꼬리는 10% 이하

                    # 하락 추세 확인 (🔧 조건 완화)
                    trend_strength = self._check_downtrend(df, i)
                    if trend_strength > self.thresholds['trend_strength_min']:

                        # 🆕 기술적 지표 종합 확인
                        volume_confirmation = self._check_volume_confirmation(df, i)
                        momentum_confirmation = self._check_momentum_confirmation(df, i, 'hammer')
                        volatility_confirmation = self._check_volatility_confirmation(df, i)

                        # 🆕 신뢰도 계산 개선
                        base_confidence = 0.7 + (lower_shadow_ratio * 0.2) + (trend_strength * 0.1)
                        technical_bonus = (volume_confirmation * 0.15) + (momentum_confirmation * 0.15) + (volatility_confirmation * 0.1)

                        confidence = min(0.98, base_confidence + technical_bonus)
                        strength = int(85 + (confidence - 0.7) * 75)

                        pattern_info = CandlePatternInfo(
                            pattern_type=PatternType.HAMMER,
                            confidence=confidence,
                            strength=strength,
                            formation_bars=1,
                            detected_at=datetime.now(),
                            description=f"망치형 패턴 (하락추세 반전신호, 신뢰도 {confidence:.1%}, V:{volume_confirmation:.1f})"
                        )
                        patterns.append(pattern_info)
                        logger.debug(f"🔨 {stock_code} 망치형 감지: 신뢰도 {confidence:.1%}, 강도 {strength}")

                # 🔨 역망치형 패턴 (Inverted Hammer)
                elif (body_ratio <= self.thresholds['body_shadow_ratio'] and
                      upper_shadow_ratio >= 0.6 and   # 윗꼬리가 전체의 60% 이상
                      lower_shadow_ratio <= 0.1):     # 아래꼬리는 10% 이하

                    # 하락 추세 확인 (🔧 조건 완화)
                    trend_strength = self._check_downtrend(df, i)
                    if trend_strength > (self.thresholds['trend_strength_min'] * 0.8):  # 역망치형은 더 완화

                        # 🆕 기술적 지표 종합 확인
                        volume_confirmation = self._check_volume_confirmation(df, i)
                        momentum_confirmation = self._check_momentum_confirmation(df, i, 'hammer')
                        volatility_confirmation = self._check_volatility_confirmation(df, i)

                        # 🆕 신뢰도 계산 개선 (역망치형은 더 보수적)
                        base_confidence = 0.6 + (upper_shadow_ratio * 0.2) + (trend_strength * 0.1)
                        technical_bonus = (volume_confirmation * 0.1) + (momentum_confirmation * 0.1) + (volatility_confirmation * 0.05)

                        confidence = min(0.90, base_confidence + technical_bonus)
                        strength = int(75 + (confidence - 0.6) * 50)

                        pattern_info = CandlePatternInfo(
                            pattern_type=PatternType.INVERTED_HAMMER,
                            confidence=confidence,
                            strength=strength,
                            formation_bars=1,
                            detected_at=datetime.now(),
                            description=f"역망치형 패턴 (하락추세 반전 예고, 신뢰도 {confidence:.1%}, V:{volume_confirmation:.1f})"
                        )
                        patterns.append(pattern_info)
                        logger.debug(f"🔨 {stock_code} 역망치형 감지: 신뢰도 {confidence:.1%}, 강도 {strength}")

        except Exception as e:
            logger.error(f"망치형 패턴 감지 오류 ({stock_code}): {e}")

        return patterns

    def _detect_engulfing_patterns(self, df: pd.DataFrame, stock_code: str) -> List[CandlePatternInfo]:
        """장악형 패턴 감지"""
        patterns = []

        try:
            for i in range(min(2, len(df) - 1)):  # 최근 2일, 최소 2일 데이터 필요
                current = df.iloc[i]
                previous = df.iloc[i + 1]

                # 기본 조건: 현재 몸통이 이전 몸통보다 커야 함
                if current['body'] <= previous['body'] * self.thresholds['engulfing_threshold']:
                    continue

                # 🟢 상승 장악형 (Bullish Engulfing)
                if (previous['is_bearish'] and current['is_bullish'] and
                    current['open'] <= previous['close'] and  # 갭 하락 또는 동일
                    current['close'] >= previous['open']):    # 완전 장악

                    # 하락 추세 확인
                    trend_strength = self._check_downtrend(df, i + 1)
                    if trend_strength > 0.4:

                        # 🆕 기술적 지표 종합 확인
                        volume_confirmation = self._check_volume_confirmation(df, i)
                        momentum_confirmation = self._check_momentum_confirmation(df, i, 'bullish')
                        volatility_confirmation = self._check_volatility_confirmation(df, i)

                        engulfing_ratio = current['body'] / previous['body']

                        # 🆕 신뢰도 계산 개선
                        base_confidence = 0.75 + min(0.1, (engulfing_ratio - 1.1) * 0.2) + (trend_strength * 0.05)
                        technical_bonus = (volume_confirmation * 0.2) + (momentum_confirmation * 0.2) + (volatility_confirmation * 0.1)

                        confidence = min(0.98, base_confidence + technical_bonus)
                        strength = int(88 + (confidence - 0.75) * 80)

                        pattern_info = CandlePatternInfo(
                            pattern_type=PatternType.BULLISH_ENGULFING,
                            confidence=confidence,
                            strength=strength,
                            formation_bars=2,
                            detected_at=datetime.now(),
                            description=f"상승장악형 패턴 (강력한 반전신호, 장악률 {engulfing_ratio:.1f}배, V:{volume_confirmation:.1f})"
                        )
                        patterns.append(pattern_info)
                        logger.debug(f"🟢 {stock_code} 상승장악형 감지: 신뢰도 {confidence:.1%}, 장악률 {engulfing_ratio:.1f}배")

                # 🔴 하락 장악형 (Bearish Engulfing)
                elif (previous['is_bullish'] and current['is_bearish'] and
                      current['open'] >= previous['close'] and  # 갭 상승 또는 동일
                      current['close'] <= previous['open']):    # 완전 장악

                    # 상승 추세 확인
                    trend_strength = self._check_uptrend(df, i + 1)
                    if trend_strength > 0.4:

                        # 🆕 기술적 지표 종합 확인
                        volume_confirmation = self._check_volume_confirmation(df, i)
                        momentum_confirmation = self._check_momentum_confirmation(df, i, 'bearish')
                        volatility_confirmation = self._check_volatility_confirmation(df, i)

                        engulfing_ratio = current['body'] / previous['body']

                        # 🆕 신뢰도 계산 개선
                        base_confidence = 0.75 + min(0.1, (engulfing_ratio - 1.1) * 0.2) + (trend_strength * 0.05)
                        technical_bonus = (volume_confirmation * 0.2) + (momentum_confirmation * 0.2) + (volatility_confirmation * 0.1)

                        confidence = min(0.98, base_confidence + technical_bonus)
                        strength = int(88 + (confidence - 0.75) * 80)

                        pattern_info = CandlePatternInfo(
                            pattern_type=PatternType.BEARISH_ENGULFING,
                            confidence=confidence,
                            strength=strength,
                            formation_bars=2,
                            detected_at=datetime.now(),
                            description=f"하락장악형 패턴 (강력한 반전신호, 장악률 {engulfing_ratio:.1f}배, V:{volume_confirmation:.1f})"
                        )
                        patterns.append(pattern_info)
                        logger.debug(f"🔴 {stock_code} 하락장악형 감지: 신뢰도 {confidence:.1%}, 장악률 {engulfing_ratio:.1f}배")

        except Exception as e:
            logger.error(f"장악형 패턴 감지 오류 ({stock_code}): {e}")

        return patterns

    def _detect_star_patterns(self, df: pd.DataFrame, stock_code: str) -> List[CandlePatternInfo]:
        """샛별형 패턴 감지 (3일 패턴)"""
        patterns = []

        try:
            for i in range(min(1, len(df) - 2)):  # 최소 3일 데이터 필요
                first = df.iloc[i + 2]   # 첫째 날 (과거)
                second = df.iloc[i + 1]  # 둘째 날 (중간, 별)
                third = df.iloc[i]       # 셋째 날 (현재)

                # 기본 조건: 중간 봉이 작은 몸통 (별 형태)
                if second['body_ratio'] > 0.3:  # 별은 작은 몸통이어야 함
                    continue

                # 🌅 샛별형 (Morning Star) - 상승 반전
                if (first['is_bearish'] and third['is_bullish'] and
                    first['body'] > first['total_range'] * 0.6 and  # 첫 번째 봉이 충분히 큼
                    third['body'] > third['total_range'] * 0.6):    # 세 번째 봉이 충분히 큼

                    # 갭 확인
                    gap1 = abs(second['high'] - first['close']) / first['close']
                    gap2 = abs(third['open'] - second['low']) / second['low']

                    if gap1 >= self.thresholds['star_gap_threshold'] or gap2 >= self.thresholds['star_gap_threshold']:

                        # 하락 추세 확인
                        trend_strength = self._check_downtrend(df, i + 2)
                        if trend_strength > 0.6:

                            # 세 번째 봉이 첫 번째 봉 중간점 이상 회복
                            recovery_ratio = (third['close'] - first['low']) / (first['open'] - first['low'])

                            if recovery_ratio >= 0.5:
                                confidence = min(0.98, 0.80 + (trend_strength * 0.1) +
                                               (recovery_ratio * 0.1) + (max(gap1, gap2) * 20))
                                strength = int(93 + (confidence - 0.80) * 35)

                                pattern_info = CandlePatternInfo(
                                    pattern_type=PatternType.MORNING_STAR,
                                    confidence=confidence,
                                    strength=strength,
                                    formation_bars=3,
                                    detected_at=datetime.now(),
                                    description=f"샛별형 패턴 (강력한 상승반전, 회복률 {recovery_ratio:.1%})"
                                )
                                patterns.append(pattern_info)
                                logger.debug(f"🌅 {stock_code} 샛별형 감지: 신뢰도 {confidence:.1%}, 회복률 {recovery_ratio:.1%}")

                # 🌆 저녁별형 (Evening Star) - 하락 반전
                elif (first['is_bullish'] and third['is_bearish'] and
                      first['body'] > first['total_range'] * 0.6 and
                      third['body'] > third['total_range'] * 0.6):

                    gap1 = abs(second['low'] - first['close']) / first['close']
                    gap2 = abs(third['open'] - second['high']) / second['high']

                    if gap1 >= self.thresholds['star_gap_threshold'] or gap2 >= self.thresholds['star_gap_threshold']:

                        # 상승 추세 확인
                        trend_strength = self._check_uptrend(df, i + 2)
                        if trend_strength > 0.6:

                            # 세 번째 봉이 첫 번째 봉 중간점 이하로 하락
                            decline_ratio = (first['high'] - third['close']) / (first['high'] - first['open'])

                            if decline_ratio >= 0.5:
                                confidence = min(0.98, 0.80 + (trend_strength * 0.1) +
                                               (decline_ratio * 0.1) + (max(gap1, gap2) * 20))
                                strength = int(93 + (confidence - 0.80) * 35)

                                pattern_info = CandlePatternInfo(
                                    pattern_type=PatternType.EVENING_STAR,
                                    confidence=confidence,
                                    strength=strength,
                                    formation_bars=3,
                                    detected_at=datetime.now(),
                                    description=f"저녁별형 패턴 (강력한 하락반전, 하락률 {decline_ratio:.1%})"
                                )
                                patterns.append(pattern_info)
                                logger.debug(f"🌆 {stock_code} 저녁별형 감지: 신뢰도 {confidence:.1%}, 하락률 {decline_ratio:.1%}")

        except Exception as e:
            logger.error(f"별형 패턴 감지 오류 ({stock_code}): {e}")

        return patterns

    def _detect_doji_patterns(self, df: pd.DataFrame, stock_code: str) -> List[CandlePatternInfo]:
        """도지 패턴 감지"""
        patterns = []

        try:
            for i in range(min(2, len(df))):  # 최근 2일만 체크
                current = df.iloc[i]

                # 도지 조건: 매우 작은 몸통
                if current['body_ratio'] <= self.thresholds['doji_body_ratio']:

                    # 긴 그림자가 있어야 함 (변동성 존재)
                    # 🆕 안전장치: total_range가 0이면 스킵
                    if current['total_range'] <= 0:
                        continue

                    shadow_ratio = (current['upper_shadow'] + current['lower_shadow']) / current['total_range']

                    if shadow_ratio >= 0.7:  # 그림자가 전체의 70% 이상

                        # 이전 추세 확인
                        trend_strength = max(
                            self._check_uptrend(df, i),
                            self._check_downtrend(df, i)
                        )

                        if trend_strength > 0.4:  # 명확한 추세 존재

                            confidence = min(0.85, 0.60 + (shadow_ratio * 0.2) + (trend_strength * 0.15))
                            strength = int(70 + (confidence - 0.60) * 60)

                            pattern_info = CandlePatternInfo(
                                pattern_type=PatternType.DOJI,
                                confidence=confidence,
                                strength=strength,
                                formation_bars=1,
                                detected_at=datetime.now(),
                                description=f"도지 패턴 (추세 전환 신호, 그림자비율 {shadow_ratio:.1%})"
                            )
                            patterns.append(pattern_info)
                            logger.debug(f"✨ {stock_code} 도지 감지: 신뢰도 {confidence:.1%}, 그림자비율 {shadow_ratio:.1%}")

        except Exception as e:
            logger.error(f"도지 패턴 감지 오류 ({stock_code}): {e}")

        return patterns

    def _detect_three_methods_patterns(self, df: pd.DataFrame, stock_code: str) -> List[CandlePatternInfo]:
        """삼법형 패턴 감지 (5일 패턴)"""
        patterns = []

        try:
            if len(df) < 5:
                return patterns

            # 최근 5일 데이터로 패턴 체크
            recent_5 = df.head(5)

            first = recent_5.iloc[4]  # 첫째 날 (가장 과거)
            middle_3 = recent_5.iloc[1:4]  # 중간 3일
            last = recent_5.iloc[0]   # 마지막 날 (가장 최근)

            # 🟢 상승삼법형 (Rising Three Methods)
            if first['is_bullish'] and last['is_bullish']:

                # 첫째와 마지막 봉이 충분한 크기
                if (first['body'] > first['total_range'] * 0.6 and
                    last['body'] > last['total_range'] * 0.6):

                    # 중간 3일이 모두 작은 봉이고 첫째 봉 범위 내
                    middle_valid = True
                    for _, middle_bar in middle_3.iterrows():
                        if (middle_bar['body'] > middle_bar['total_range'] * 0.4 or  # 너무 큰 몸통
                            middle_bar['high'] > first['high'] or                     # 첫째 봉 고점 돌파
                            middle_bar['low'] < first['low']):                       # 첫째 봉 저점 이탈
                            middle_valid = False
                            break

                    if middle_valid and last['close'] > first['close']:

                        # 상승 추세 확인
                        trend_strength = self._check_uptrend(df, 5)
                        if trend_strength > 0.5:

                            progress_ratio = (last['close'] - first['open']) / (first['high'] - first['low'])
                            confidence = min(0.90, 0.70 + (trend_strength * 0.15) + (progress_ratio * 0.1))
                            strength = int(78 + (confidence - 0.70) * 60)

                            pattern_info = CandlePatternInfo(
                                pattern_type=PatternType.RISING_THREE_METHODS,
                                confidence=confidence,
                                strength=strength,
                                formation_bars=5,
                                detected_at=datetime.now(),
                                description=f"상승삼법형 패턴 (추세지속, 진행률 {progress_ratio:.1%})"
                            )
                            patterns.append(pattern_info)
                            logger.debug(f"📈 {stock_code} 상승삼법형 감지: 신뢰도 {confidence:.1%}")

            # 🔴 하락삼법형 (Falling Three Methods)
            elif first['is_bearish'] and last['is_bearish']:

                if (first['body'] > first['total_range'] * 0.6 and
                    last['body'] > last['total_range'] * 0.6):

                    middle_valid = True
                    for _, middle_bar in middle_3.iterrows():
                        if (middle_bar['body'] > middle_bar['total_range'] * 0.4 or
                            middle_bar['high'] > first['high'] or
                            middle_bar['low'] < first['low']):
                            middle_valid = False
                            break

                    if middle_valid and last['close'] < first['close']:

                        trend_strength = self._check_downtrend(df, 5)
                        if trend_strength > 0.5:

                            decline_ratio = (first['open'] - last['close']) / (first['high'] - first['low'])
                            confidence = min(0.90, 0.70 + (trend_strength * 0.15) + (decline_ratio * 0.1))
                            strength = int(78 + (confidence - 0.70) * 60)

                            pattern_info = CandlePatternInfo(
                                pattern_type=PatternType.FALLING_THREE_METHODS,
                                confidence=confidence,
                                strength=strength,
                                formation_bars=5,
                                detected_at=datetime.now(),
                                description=f"하락삼법형 패턴 (추세지속, 하락률 {decline_ratio:.1%})"
                            )
                            patterns.append(pattern_info)
                            logger.debug(f"📉 {stock_code} 하락삼법형 감지: 신뢰도 {confidence:.1%}")

        except Exception as e:
            logger.error(f"삼법형 패턴 감지 오류 ({stock_code}): {e}")

        return patterns

    def _detect_basic_trend_patterns(self, df: pd.DataFrame, stock_code: str) -> List[CandlePatternInfo]:
        """🆕 기본 추세 패턴 감지 (대안 패턴)"""
        patterns = []

        try:
            if len(df) < 2:
                return patterns

            current = df.iloc[0]
            previous = df.iloc[1] if len(df) > 1 else current

            # 🔥 단순 상승 패턴
            if current['is_bullish'] and current['body_ratio'] > 0.5:
                # 🆕 기술적 지표 확인
                volume_confirmation = self._check_volume_confirmation(df, 0)
                momentum_confirmation = self._check_momentum_confirmation(df, 0, 'bullish')
                volatility_confirmation = self._check_volatility_confirmation(df, 0)

                # 🆕 신뢰도 계산 개선
                base_confidence = 0.4 + (current['body_ratio'] * 0.2)
                technical_bonus = (volume_confirmation * 0.2) + (momentum_confirmation * 0.15) + (volatility_confirmation * 0.1)

                confidence = min(0.75, base_confidence + technical_bonus)
                strength = int(60 + (confidence - 0.4) * 80)

                pattern_info = CandlePatternInfo(
                    pattern_type=PatternType.BULLISH_ENGULFING,  # 기존 타입 재사용
                    confidence=confidence,
                    strength=strength,
                    formation_bars=1,
                    detected_at=datetime.now(),
                    description=f"기본 상승 패턴 (큰 양봉, 몸통비율 {current['body_ratio']:.1%}, V:{volume_confirmation:.1f})"
                )
                patterns.append(pattern_info)
                logger.debug(f"📈 {stock_code} 기본 상승패턴 감지: 신뢰도 {confidence:.1%}")

            # 🔥 연속 상승 패턴
            elif (len(df) >= 2 and current['is_bullish'] and previous['is_bullish'] and
                  current['close'] > previous['close']):

                # 🆕 기술적 지표 확인
                volume_confirmation = self._check_volume_confirmation(df, 0)
                momentum_confirmation = self._check_momentum_confirmation(df, 0, 'bullish')
                volatility_confirmation = self._check_volatility_confirmation(df, 0)

                price_increase = (current['close'] - previous['close']) / previous['close']

                # 🆕 신뢰도 계산 개선
                base_confidence = 0.45 + (price_increase * 2)
                technical_bonus = (volume_confirmation * 0.25) + (momentum_confirmation * 0.2) + (volatility_confirmation * 0.1)

                confidence = min(0.80, base_confidence + technical_bonus)
                strength = int(65 + (confidence - 0.45) * 85)

                pattern_info = CandlePatternInfo(
                    pattern_type=PatternType.RISING_THREE_METHODS,  # 기존 타입 재사용
                    confidence=confidence,
                    strength=strength,
                    formation_bars=2,
                    detected_at=datetime.now(),
                    description=f"연속 상승 패턴 (2일 연속 상승, 상승률 {price_increase:.1%}, V:{volume_confirmation:.1f})"
                )
                patterns.append(pattern_info)
                logger.debug(f"📈 {stock_code} 연속상승패턴 감지: 상승률 {price_increase:.1%}")

            # 🆕 추가: 하락 후 반등 패턴
            elif (len(df) >= 3 and current['is_bullish'] and
                  previous['is_bearish'] and current['close'] > previous['open']):

                # 🆕 기술적 지표 확인
                volume_confirmation = self._check_volume_confirmation(df, 0)
                momentum_confirmation = self._check_momentum_confirmation(df, 0, 'bullish')
                volatility_confirmation = self._check_volatility_confirmation(df, 0)

                recovery_ratio = (current['close'] - previous['low']) / (previous['high'] - previous['low'])

                # 🆕 신뢰도 계산
                base_confidence = 0.5 + (recovery_ratio * 0.2)
                technical_bonus = (volume_confirmation * 0.2) + (momentum_confirmation * 0.15) + (volatility_confirmation * 0.1)

                confidence = min(0.85, base_confidence + technical_bonus)
                strength = int(70 + (confidence - 0.5) * 60)

                pattern_info = CandlePatternInfo(
                    pattern_type=PatternType.HAMMER,  # 기존 타입 재사용
                    confidence=confidence,
                    strength=strength,
                    formation_bars=2,
                    detected_at=datetime.now(),
                    description=f"하락 후 반등 패턴 (회복률 {recovery_ratio:.1%}, V:{volume_confirmation:.1f})"
                )
                patterns.append(pattern_info)
                logger.debug(f"📈 {stock_code} 하락후반등패턴 감지: 회복률 {recovery_ratio:.1%}")

        except Exception as e:
            logger.error(f"기본 패턴 감지 오류 ({stock_code}): {e}")

        return patterns

    # ========== 보조 함수들 ==========

    def _check_uptrend(self, df: pd.DataFrame, start_idx: int) -> float:
        """상승 추세 강도 확인 (0.0~1.0)"""
        try:
            if start_idx >= len(df) - 2:
                return 0.0

            # 최근 며칠간의 추세 확인
            trend_period = min(self.thresholds['trend_min_days'], len(df) - start_idx)
            prices = [df.iloc[start_idx + i]['close'] for i in range(trend_period)]

            if len(prices) < 2:
                return 0.0

            # 상승 일수 비율
            up_days = sum(1 for i in range(1, len(prices)) if prices[i-1] < prices[i])
            up_ratio = up_days / (len(prices) - 1)

            # 전체 상승폭
            total_change = (prices[-1] - prices[0]) / prices[0] if prices[0] > 0 else 0

            return min(1.0, up_ratio * 0.7 + min(1.0, total_change * 10) * 0.3)

        except Exception as e:
            logger.error(f"상승추세 확인 오류: {e}")
            return 0.0

    def _check_downtrend(self, df: pd.DataFrame, start_idx: int) -> float:
        """하락 추세 강도 확인 (0.0~1.0)"""
        try:
            if start_idx >= len(df) - 2:
                return 0.0

            trend_period = min(self.thresholds['trend_min_days'], len(df) - start_idx)
            prices = [df.iloc[start_idx + i]['close'] for i in range(trend_period)]

            if len(prices) < 2:
                return 0.0

            # 하락 일수 비율
            down_days = sum(1 for i in range(1, len(prices)) if prices[i-1] > prices[i])
            down_ratio = down_days / (len(prices) - 1)

            # 전체 하락폭
            total_change = (prices[0] - prices[-1]) / prices[0] if prices[0] > 0 else 0

            return min(1.0, down_ratio * 0.7 + min(1.0, total_change * 10) * 0.3)

        except Exception as e:
            logger.error(f"하락추세 확인 오류: {e}")
            return 0.0

    def _check_volume_confirmation(self, df: pd.DataFrame, idx: int) -> float:
        """🔥 강화된 거래량 확인 (0.0~1.0)"""
        try:
            if idx >= len(df):
                return 0.0

            current = df.iloc[idx]

            # 🆕 1. 기본 거래량 비율 확인
            volume_ratio = current.get('volume_ratio', 1.0)
            base_score = 0.0

            if volume_ratio >= 2.0:  # 평균의 2배 이상
                base_score = 1.0
            elif volume_ratio >= 1.5:  # 평균의 1.5배 이상
                base_score = 0.8
            elif volume_ratio >= 1.2:  # 평균의 1.2배 이상
                base_score = 0.6
            elif volume_ratio >= 1.0:  # 평균 이상
                base_score = 0.4
            else:
                base_score = 0.2

            # 🆕 2. 거래대금 추가 확인
            value_ratio = current.get('value_ratio', 1.0)
            if value_ratio >= 1.5:
                base_score += 0.2
            elif value_ratio >= 1.2:
                base_score += 0.1

            # 🆕 3. VWAP 대비 가격 위치
            price_vs_vwap = current.get('price_vs_vwap', 0.0)
            if abs(price_vs_vwap) > 0.02:  # VWAP에서 2% 이상 벗어남
                base_score += 0.1

            return min(1.0, base_score)

        except Exception as e:
            logger.error(f"거래량 확인 오류: {e}")
            return 0.0

    def _check_momentum_confirmation(self, df: pd.DataFrame, idx: int, pattern_type: str) -> float:
        """🆕 모멘텀 지표 확인 (0.0~1.0)"""
        try:
            if idx >= len(df):
                return 0.0

            current = df.iloc[idx]
            score = 0.0

            # RSI 확인 (임시로 기본값 사용)
            rsi = current.get('rsi_14', 50.0)

            if pattern_type.lower() in ['bullish', 'hammer', 'morning_star']:
                # 상승 패턴의 경우
                if rsi < 30:  # 과매도 구간
                    score += 0.4
                elif rsi < 40:
                    score += 0.2
                elif rsi > 70:  # 과매수 구간 (반전 주의)
                    score -= 0.2

            elif pattern_type.lower() in ['bearish', 'evening_star']:
                # 하락 패턴의 경우
                if rsi > 70:  # 과매수 구간
                    score += 0.4
                elif rsi > 60:
                    score += 0.2
                elif rsi < 30:  # 과매도 구간 (반전 주의)
                    score -= 0.2

            # MACD 확인
            macd = current.get('macd', 0.0)
            macd_signal = current.get('macd_signal', 0.0)
            macd_histogram = current.get('macd_histogram', 0.0)

            if pattern_type.lower() in ['bullish', 'hammer', 'morning_star']:
                if macd > macd_signal and macd_histogram > 0:
                    score += 0.3
                elif macd_histogram > 0:
                    score += 0.1

            elif pattern_type.lower() in ['bearish', 'evening_star']:
                if macd < macd_signal and macd_histogram < 0:
                    score += 0.3
                elif macd_histogram < 0:
                    score += 0.1

            # 가격 모멘텀 확인
            momentum_5 = current.get('momentum_5', 0.0)
            if pattern_type.lower() in ['bullish', 'hammer', 'morning_star']:
                if momentum_5 > 0.02:  # 5일간 2% 이상 상승
                    score += 0.2
                elif momentum_5 < -0.05:  # 5일간 5% 이상 하락 (반전 기회)
                    score += 0.1

            elif pattern_type.lower() in ['bearish', 'evening_star']:
                if momentum_5 < -0.02:  # 5일간 2% 이상 하락
                    score += 0.2
                elif momentum_5 > 0.05:  # 5일간 5% 이상 상승 (반전 기회)
                    score += 0.1

            return min(1.0, max(0.0, score))

        except Exception as e:
            logger.error(f"모멘텀 확인 오류: {e}")
            return 0.0

    def _check_volatility_confirmation(self, df: pd.DataFrame, idx: int) -> float:
        """🆕 변동성 지표 확인 (0.0~1.0)"""
        try:
            if idx >= len(df):
                return 0.0

            current = df.iloc[idx]
            score = 0.0

            # 1. ATR 기반 변동성 확인
            volatility_level = current.get('volatility_level', 0.02)
            if 0.015 <= volatility_level <= 0.05:  # 적정 변동성 (1.5%~5%)
                score += 0.3
            elif volatility_level > 0.05:  # 높은 변동성
                score += 0.1

            # 2. 볼린저 밴드 위치
            bb_position = current.get('bb_position', 0.5)
            if bb_position <= 0.2:  # 하단 근처 (과매도)
                score += 0.3
            elif bb_position >= 0.8:  # 상단 근처 (과매수)
                score += 0.3
            elif 0.3 <= bb_position <= 0.7:  # 중간 영역
                score += 0.2

            # 3. 일중 변동률
            intraday_vol = current.get('intraday_volatility', 0.0)
            if 0.02 <= intraday_vol <= 0.08:  # 적정 일중 변동률
                score += 0.2

            return min(1.0, score)

        except Exception as e:
            logger.error(f"변동성 확인 오류: {e}")
            return 0.0

    def _filter_and_rank_patterns(self, patterns: List[CandlePatternInfo], df: pd.DataFrame) -> List[CandlePatternInfo]:
        """패턴 필터링 및 순위 정렬"""
        try:
            if not patterns:
                return []

            # 중복 패턴 제거 (같은 타입에서 가장 좋은 것만)
            best_patterns = {}
            for pattern in patterns:
                pattern_type = pattern.pattern_type
                if (pattern_type not in best_patterns or
                    pattern.confidence > best_patterns[pattern_type].confidence):
                    best_patterns[pattern_type] = pattern

            # 최소 신뢰도 필터링
            min_confidence = self.thresholds['min_confidence']
            filtered_patterns = [p for p in best_patterns.values() if p.confidence >= min_confidence]

            # 신뢰도 순으로 정렬
            filtered_patterns.sort(key=lambda x: x.confidence, reverse=True)

            return filtered_patterns

        except Exception as e:
            logger.error(f"패턴 필터링 오류: {e}")
            return patterns

    def _calculate_volume_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """🆕 거래량 관련 지표 계산"""
        try:
            # 1. 거래대금 (Value) - 거래량 × 가격
            df['value'] = df['volume'] * df['close']

            # 2. VWAP (Volume Weighted Average Price) - 14일 기준
            if len(df) >= 14:
                df['vwap_14'] = (df['value'].rolling(window=14).sum() /
                               df['volume'].rolling(window=14).sum())
            else:
                df['vwap_14'] = df['close']  # 데이터 부족 시 종가 사용

            # 3. 거래량 이동평균
            df['volume_ma_5'] = df['volume'].rolling(window=5).mean()
            df['volume_ma_20'] = df['volume'].rolling(window=20).mean()

            # 4. 거래량 비율 (현재 vs 평균)
            df['volume_ratio'] = df['volume'] / df['volume_ma_20']
            df['volume_ratio'] = df['volume_ratio'].fillna(1.0)

            # 5. 거래대금 비율
            df['value_ma_5'] = df['value'].rolling(window=5).mean()
            df['value_ratio'] = df['value'] / df['value_ma_5']
            df['value_ratio'] = df['value_ratio'].fillna(1.0)

            # 6. 가격대별 거래량 프로파일 (간단 버전)
            df['price_vs_vwap'] = (df['close'] - df['vwap_14']) / df['vwap_14']
            df['price_vs_vwap'] = df['price_vs_vwap'].fillna(0.0)

            return df

        except Exception as e:
            logger.error(f"거래량 지표 계산 오류: {e}")
            return df

    def _calculate_volatility_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """🆕 변동성 지표 계산"""
        try:
            # 1. ATR (Average True Range) - 14일 기준
            df['tr'] = df[['high', 'low']].apply(
                lambda x: max(
                    x['high'] - x['low'],  # 당일 고저
                    abs(x['high'] - df['close'].shift(1).iloc[x.name]) if x.name < len(df)-1 else x['high'] - x['low'],  # 전날 종가와 당일 고가
                    abs(x['low'] - df['close'].shift(1).iloc[x.name]) if x.name < len(df)-1 else x['high'] - x['low']   # 전날 종가와 당일 저가
                ), axis=1
            )

            df['atr_14'] = df['tr'].rolling(window=14).mean()
            df['atr_14'] = df['atr_14'].fillna(df['total_range'])  # 데이터 부족 시 당일 범위 사용

            # 2. 일중 변동률 - (High - Low) / Close
            df['intraday_volatility'] = df['total_range'] / df['close']
            df['intraday_volatility'] = df['intraday_volatility'].fillna(0.0)

            # 3. 볼린저 밴드용 표준편차 (20일)
            df['price_std_20'] = df['close'].rolling(window=20).std()
            df['price_std_20'] = df['price_std_20'].fillna(df['close'].std())

            # 4. 볼린저 밴드 계산
            df['bb_middle'] = df['close'].rolling(window=20).mean()
            df['bb_upper'] = df['bb_middle'] + (df['price_std_20'] * 2)
            df['bb_lower'] = df['bb_middle'] - (df['price_std_20'] * 2)

            # 5. 볼린저 밴드 위치 (0~1, 0.5가 중간)
            df['bb_position'] = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'])
            df['bb_position'] = df['bb_position'].fillna(0.5)

            # 6. ATR 기반 변동성 레벨
            df['volatility_level'] = df['atr_14'] / df['close']
            df['volatility_level'] = df['volatility_level'].fillna(0.02)  # 기본 2%

            return df

        except Exception as e:
            logger.error(f"변동성 지표 계산 오류: {e}")
            return df

    def _calculate_momentum_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """🆕 모멘텀 지표 계산"""
        try:
                        # 1. RSI (Relative Strength Index) - 14일 기준
            delta = df['close'].diff()
            gain = delta.copy()
            loss = delta.copy()

            gain[gain < 0] = 0
            loss[loss > 0] = 0
            loss = loss.abs()

            avg_gain = gain.rolling(window=14).mean()
            avg_loss = loss.rolling(window=14).mean()

            rs = avg_gain / avg_loss.replace(0, 1)  # 0으로 나누기 방지
            df['rsi_14'] = 100 - (100 / (1 + rs))
            df['rsi_14'] = df['rsi_14'].fillna(50.0)  # 데이터 부족 시 중간값

            # 2. MACD (Moving Average Convergence Divergence)
            exp12 = df['close'].ewm(span=12).mean()
            exp26 = df['close'].ewm(span=26).mean()
            df['macd'] = exp12 - exp26
            df['macd_signal'] = df['macd'].ewm(span=9).mean()
            df['macd_histogram'] = df['macd'] - df['macd_signal']

            # MACD 데이터 부족 시 기본값
            df['macd'] = df['macd'].fillna(0.0)
            df['macd_signal'] = df['macd_signal'].fillna(0.0)
            df['macd_histogram'] = df['macd_histogram'].fillna(0.0)

            # 3. 가격 모멘텀 (N일 전 대비 변화율)
            df['momentum_5'] = (df['close'] - df['close'].shift(5)) / df['close'].shift(5)
            df['momentum_10'] = (df['close'] - df['close'].shift(10)) / df['close'].shift(10)
            df['momentum_5'] = df['momentum_5'].fillna(0.0)
            df['momentum_10'] = df['momentum_10'].fillna(0.0)

            # 4. 스토캐스틱 %K, %D (14일)
            low_14 = df['low'].rolling(window=14).min()
            high_14 = df['high'].rolling(window=14).max()
            df['stoch_k'] = 100 * (df['close'] - low_14) / (high_14 - low_14)
            df['stoch_d'] = df['stoch_k'].rolling(window=3).mean()
            df['stoch_k'] = df['stoch_k'].fillna(50.0)
            df['stoch_d'] = df['stoch_d'].fillna(50.0)

            return df

        except Exception as e:
            logger.error(f"모멘텀 지표 계산 오류: {e}")
            return df
