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

        # 패턴 감지 임계값 설정
        self.thresholds = {
            'body_shadow_ratio': 0.3,      # 몸통/그림자 비율 (망치형)
            'engulfing_threshold': 1.1,     # 장악형 최소 비율
            'doji_body_ratio': 0.05,       # 도지 몸통 비율
            'star_gap_threshold': 0.002,   # 별형 갭 임계값 (0.2%)
            'trend_min_days': 3,           # 추세 확인 최소 일수
            'volume_confirmation': 1.2     # 거래량 확인 배율
        }

    def analyze_stock_patterns(self, stock_code: str, ohlcv_data: pd.DataFrame,
                             volume_data: Optional[pd.DataFrame] = None) -> List[CandlePatternInfo]:
        """종목의 캔들 패턴 종합 분석"""
        try:
            if ohlcv_data is None or ohlcv_data.empty:
                logger.warning(f"종목 {stock_code}: OHLCV 데이터 없음")
                return []

            if len(ohlcv_data) < 5:
                logger.warning(f"종목 {stock_code}: 데이터 부족 ({len(ohlcv_data)}일)")
                return []

            # 데이터 전처리
            df = self._prepare_data(ohlcv_data)

            detected_patterns = []

            # 🔥 1. 망치형 패턴 감지
            hammer_patterns = self._detect_hammer_patterns(df, stock_code)
            detected_patterns.extend(hammer_patterns)

            # 🔥 2. 장악형 패턴 감지
            engulfing_patterns = self._detect_engulfing_patterns(df, stock_code)
            detected_patterns.extend(engulfing_patterns)

            # 🔥 3. 샛별형 패턴 감지
            star_patterns = self._detect_star_patterns(df, stock_code)
            detected_patterns.extend(star_patterns)

            # 🔥 4. 도지 패턴 감지
            doji_patterns = self._detect_doji_patterns(df, stock_code)
            detected_patterns.extend(doji_patterns)

            # 🔥 5. 삼법형 패턴 감지
            three_methods_patterns = self._detect_three_methods_patterns(df, stock_code)
            detected_patterns.extend(three_methods_patterns)

            # 패턴 품질 필터링 및 정렬
            filtered_patterns = self._filter_and_rank_patterns(detected_patterns, df)

            if filtered_patterns:
                pattern_names = [p.pattern_type.value for p in filtered_patterns]
                logger.info(f"🎯 {stock_code} 패턴 감지: {', '.join(pattern_names)}")

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

            # 추가 지표 계산
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

            # 이동평균 (추세 확인용)
            df['ma_5'] = df['close'].rolling(window=5).mean()
            df['ma_20'] = df['close'].rolling(window=20).mean()

            # 거래량 평균
            df['volume_ma'] = df['volume'].rolling(window=5).mean()

            # 최신 데이터가 첫 번째 행이 되도록 정렬
            df = df.sort_index(ascending=False).reset_index(drop=True)

            return df

        except Exception as e:
            logger.error(f"데이터 전처리 오류: {e}")
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

                    # 하락 추세 확인
                    trend_strength = self._check_downtrend(df, i)
                    if trend_strength > 0.5:

                        confidence = min(0.95, 0.7 + (lower_shadow_ratio * 0.3) + (trend_strength * 0.2))
                        strength = int(85 + (confidence - 0.7) * 50)

                        pattern_info = CandlePatternInfo(
                            pattern_type=PatternType.HAMMER,
                            confidence=confidence,
                            strength=strength,
                            formation_bars=1,
                            detected_at=datetime.now(),
                            description=f"망치형 패턴 (하락추세 반전신호, 신뢰도 {confidence:.1%})"
                        )
                        patterns.append(pattern_info)
                        logger.debug(f"🔨 {stock_code} 망치형 감지: 신뢰도 {confidence:.1%}, 강도 {strength}")

                # 🔨 역망치형 패턴 (Inverted Hammer)
                elif (body_ratio <= self.thresholds['body_shadow_ratio'] and
                      upper_shadow_ratio >= 0.6 and   # 윗꼬리가 전체의 60% 이상
                      lower_shadow_ratio <= 0.1):     # 아래꼬리는 10% 이하

                    # 하락 추세 확인
                    trend_strength = self._check_downtrend(df, i)
                    if trend_strength > 0.3:  # 역망치형은 조건 완화

                        confidence = min(0.85, 0.6 + (upper_shadow_ratio * 0.25) + (trend_strength * 0.15))
                        strength = int(75 + (confidence - 0.6) * 40)

                        pattern_info = CandlePatternInfo(
                            pattern_type=PatternType.INVERTED_HAMMER,
                            confidence=confidence,
                            strength=strength,
                            formation_bars=1,
                            detected_at=datetime.now(),
                            description=f"역망치형 패턴 (하락추세 반전 예고, 신뢰도 {confidence:.1%})"
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

                        # 거래량 확인
                        volume_confirmation = self._check_volume_confirmation(df, i)

                        engulfing_ratio = current['body'] / previous['body']
                        confidence = min(0.95, 0.75 + min(0.15, (engulfing_ratio - 1.1) * 0.3) +
                                       (trend_strength * 0.1) + (volume_confirmation * 0.05))
                        strength = int(88 + (confidence - 0.75) * 60)

                        pattern_info = CandlePatternInfo(
                            pattern_type=PatternType.BULLISH_ENGULFING,
                            confidence=confidence,
                            strength=strength,
                            formation_bars=2,
                            detected_at=datetime.now(),
                            description=f"상승장악형 패턴 (강력한 반전신호, 장악률 {engulfing_ratio:.1f}배)"
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

                        volume_confirmation = self._check_volume_confirmation(df, i)

                        engulfing_ratio = current['body'] / previous['body']
                        confidence = min(0.95, 0.75 + min(0.15, (engulfing_ratio - 1.1) * 0.3) +
                                       (trend_strength * 0.1) + (volume_confirmation * 0.05))
                        strength = int(88 + (confidence - 0.75) * 60)

                        pattern_info = CandlePatternInfo(
                            pattern_type=PatternType.BEARISH_ENGULFING,
                            confidence=confidence,
                            strength=strength,
                            formation_bars=2,
                            detected_at=datetime.now(),
                            description=f"하락장악형 패턴 (강력한 반전신호, 장악률 {engulfing_ratio:.1f}배)"
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
        """거래량 확인 (0.0~1.0)"""
        try:
            if idx >= len(df) or 'volume' not in df.columns:
                return 0.0

            current_volume = df.iloc[idx]['volume']

            # 평균 거래량 계산
            volume_period = min(5, len(df) - idx)
            if volume_period < 2:
                return 0.0

            volume_data = [df.iloc[idx + i]['volume'] for i in range(1, volume_period + 1)]
            avg_volume = sum(volume_data) / len(volume_data) if volume_data else 1

            if avg_volume <= 0:
                return 0.0

            volume_ratio = current_volume / avg_volume

            if volume_ratio >= self.thresholds['volume_confirmation']:
                return min(1.0, (volume_ratio - 1.0) * 0.5)
            else:
                return 0.0

        except Exception as e:
            logger.error(f"거래량 확인 오류: {e}")
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
            min_confidence = 0.6
            filtered_patterns = [p for p in best_patterns.values() if p.confidence >= min_confidence]

            # 신뢰도 순으로 정렬
            filtered_patterns.sort(key=lambda x: x.confidence, reverse=True)

            return filtered_patterns

        except Exception as e:
            logger.error(f"패턴 필터링 오류: {e}")
            return patterns
