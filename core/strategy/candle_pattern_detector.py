"""
캔들 패턴 감지 및 분석 시스템
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from utils.logger import setup_logger

from .candle_trade_candidate import (
    CandlePatternInfo, PatternType, TradeSignal
)

logger = setup_logger(__name__)


class CandlePatternDetector:
    """캔들 패턴 감지 및 분석 시스템"""

    def __init__(self):
        # 🎯 새로운 4가지 패턴 전용 가중치 설정
        self.pattern_weights = {
            PatternType.HAMMER: 0.80,                    # 망치형 - 2% 목표
            PatternType.BULLISH_ENGULFING: 0.90,         # 상승장악형 - 3% 목표 (강한 패턴)
            PatternType.PIERCING_LINE: 0.75,             # 관통형 - 2% 목표
            PatternType.MORNING_STAR: 0.95,              # 아침샛별 - 4% 목표 (가장 강한 패턴)
        }

        # 🎯 시가 매수 전략 임계값
        self.thresholds = {
            'hammer_lower_shadow_min': 0.6,      # 망치형: 아래꼬리 최소 60%
            'hammer_body_max': 0.3,              # 망치형: 몸통 최대 30%
            'hammer_upper_shadow_max': 0.1,      # 망치형: 윗꼬리 최대 10%
            
            'engulfing_ratio_min': 1.1,          # 장악형: 최소 110% 크기
            
            'piercing_body_min': 0.6,            # 관통형: 전날 음봉 몸통 최소 60%
            'piercing_penetration_min': 0.5,     # 관통형: 최소 50% 관통
            
            'morning_star_doji_max': 0.3,        # 아침샛별: 중간일 몸통 최대 30%
            'morning_star_bullish_min': 0.6,     # 아침샛별: 마지막일 양봉 몸통 최소 60%
            
            'downtrend_strength_min': 0.3,       # 하락추세 최소 강도
            'min_confidence': 0.6,               # 최소 신뢰도
        }

    def analyze_stock_patterns(self, stock_code: str, ohlcv_data: pd.DataFrame,
                             volume_data: Optional[pd.DataFrame] = None) -> List[CandlePatternInfo]:
        """🎯 새로운 4가지 패턴 분석 - 전날까지 데이터로 다음날 시가 매수 전략"""
        try:
            if ohlcv_data is None or ohlcv_data.empty:
                logger.warning(f"종목 {stock_code}: OHLCV 데이터 없음")
                return []

            # 데이터 전처리
            df = self._prepare_data(ohlcv_data)
            if df.empty:
                logger.warning(f"종목 {stock_code}: 데이터 전처리 실패")
                return []

            detected_patterns = []

            # 🎯 1. 망치형 (Hammer) - 5일 데이터 필요
            if len(df) >= 5:
                hammer_patterns = self._detect_hammer_pattern_new(df, stock_code)
                detected_patterns.extend(hammer_patterns)

            # 🎯 2. 상승장악형 (Bullish Engulfing) - 4일 데이터 필요
            if len(df) >= 4:
                engulfing_patterns = self._detect_bullish_engulfing_pattern_new(df, stock_code)
                detected_patterns.extend(engulfing_patterns)

            # 🎯 3. 관통형 (Piercing Line) - 3일 데이터 필요
            if len(df) >= 3:
                piercing_patterns = self._detect_piercing_line_pattern_new(df, stock_code)
                detected_patterns.extend(piercing_patterns)

            # 🎯 4. 아침샛별 (Morning Star) - 4일 데이터 필요
            if len(df) >= 4:
                morning_star_patterns = self._detect_morning_star_pattern_new(df, stock_code)
                detected_patterns.extend(morning_star_patterns)

            # 패턴 필터링 및 정렬
            filtered_patterns = self._filter_patterns_for_next_day_buy(detected_patterns, df)

            if filtered_patterns:
                pattern_names = [f"{p.pattern_type.value}(신뢰도:{p.confidence:.2f})" for p in filtered_patterns]
                logger.info(f"🎯 {stock_code} 다음날 시가 매수 패턴: {', '.join(pattern_names)}")
            else:
                logger.debug(f"❌ {stock_code} 매수 조건 패턴 없음")

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
            df['upper_shadow'] = df['high'] - df[['open', 'close']].max(axis=1)  # 윗꼬리
            df['lower_shadow'] = df[['open', 'close']].min(axis=1) - df['low']   # 아래꼬리
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

    def _detect_hammer_pattern_new(self, df: pd.DataFrame, stock_code: str) -> List[CandlePatternInfo]:
        """🔨 망치형 패턴 감지 - 5일 데이터 기준"""
        patterns = []
        
        try:
            # 어제 캔들(index=1)에서 패턴 확인 (오늘은 index=0)
            yesterday_idx = 1
            if yesterday_idx >= len(df):
                return patterns
                
            yesterday = df.iloc[yesterday_idx]
            
            # 망치형 기본 조건
            body_size = abs(yesterday['close'] - yesterday['open'])
            total_range = yesterday['high'] - yesterday['low']
            lower_shadow = min(yesterday['open'], yesterday['close']) - yesterday['low']
            upper_shadow = yesterday['high'] - max(yesterday['open'], yesterday['close'])
            
            if total_range <= 0:
                return patterns
                
            lower_shadow_ratio = lower_shadow / total_range
            upper_shadow_ratio = upper_shadow / total_range
            body_ratio = body_size / total_range
            
            # 망치형 조건: 긴 아래꼬리 + 작은 몸통 + 짧은 윗꼬리
            if (lower_shadow_ratio >= 0.6 and  # 아래꼬리가 전체의 60% 이상
                body_ratio <= 0.3 and          # 몸통이 전체의 30% 이하
                upper_shadow_ratio <= 0.1):    # 윗꼬리가 전체의 10% 이하
                
                # 하락추세 확인 (5일간)
                downtrend_strength = self._check_downtrend_simple(df, yesterday_idx, 5)
                
                if downtrend_strength > 0.3:  # 30% 이상 하락추세
                    # 종가가 상단부에 있는지 확인
                    close_position = (yesterday['close'] - yesterday['low']) / total_range
                    
                    if close_position >= 0.5:  # 종가가 상단부 50% 이상
                        confidence = 0.6 + (lower_shadow_ratio * 0.2) + (downtrend_strength * 0.2)
                        strength = int(70 + (lower_shadow_ratio * 20) + (downtrend_strength * 10))
                        
                        pattern = CandlePatternInfo(
                            pattern_type=PatternType.HAMMER,
                            confidence=min(confidence, 0.95),
                            strength=min(strength, 100),
                            detected_at=yesterday_idx,
                            trade_signal=TradeSignal.BUY,
                            target_price_ratio=1.02,  # 2% 목표
                            stop_loss_ratio=0.985,    # 1.5% 손절
                            expected_duration_hours=48  # 1-2일 보유
                        )
                        patterns.append(pattern)
                        logger.debug(f"🔨 {stock_code} 망치형 패턴 감지 (신뢰도: {confidence:.2f})")
                        
        except Exception as e:
            logger.debug(f"망치형 패턴 감지 오류 ({stock_code}): {e}")
            
        return patterns

    def _detect_bullish_engulfing_pattern_new(self, df: pd.DataFrame, stock_code: str) -> List[CandlePatternInfo]:
        """📈 상승장악형 패턴 감지 - 4일 데이터 기준"""
        patterns = []
        
        try:
            # 어제 캔들(index=1)과 그 전날(index=2) 비교
            if len(df) < 2:
                return patterns
                
            yesterday = df.iloc[1]  # 어제 (장악하는 양봉)
            day_before = df.iloc[2]  # 그 전날 (장악당하는 음봉)
            
            # 전날이 음봉, 어제가 양봉
            if (day_before['close'] < day_before['open'] and  # 전날 음봉
                yesterday['close'] > yesterday['open']):      # 어제 양봉
                
                # 완전포함 조건: 어제 양봉이 전날 음봉을 완전히 포함
                if (yesterday['open'] < day_before['close'] and  # 어제 시가 < 전날 종가
                    yesterday['close'] > day_before['open']):    # 어제 종가 > 전날 시가
                    
                    # 장악 강도 계산
                    day_before_body = abs(day_before['open'] - day_before['close'])
                    yesterday_body = abs(yesterday['open'] - yesterday['close'])
                    engulfing_ratio = yesterday_body / day_before_body if day_before_body > 0 else 1
                    
                    if engulfing_ratio >= 1.1:  # 최소 10% 이상 큰 몸통
                        confidence = 0.7 + min((engulfing_ratio - 1) * 0.2, 0.25)
                        strength = int(75 + min((engulfing_ratio - 1) * 15, 25))
                        
                        pattern = CandlePatternInfo(
                            pattern_type=PatternType.BULLISH_ENGULFING,
                            confidence=min(confidence, 0.95),
                            strength=min(strength, 100),
                            detected_at=1,
                            trade_signal=TradeSignal.STRONG_BUY,
                            target_price_ratio=1.03,  # 3% 목표 (강한 패턴)
                            stop_loss_ratio=0.98,     # 2% 손절
                            expected_duration_hours=24  # 당일-1일 보유
                        )
                        patterns.append(pattern)
                        logger.debug(f"📈 {stock_code} 상승장악형 패턴 감지 (장악비율: {engulfing_ratio:.2f})")
                        
        except Exception as e:
            logger.debug(f"상승장악형 패턴 감지 오류 ({stock_code}): {e}")
            
        return patterns

    def _detect_piercing_line_pattern_new(self, df: pd.DataFrame, stock_code: str) -> List[CandlePatternInfo]:
        """⚡ 관통형 패턴 감지 - 3일 데이터 기준"""
        patterns = []
        
        try:
            if len(df) < 2:
                return patterns
                
            yesterday = df.iloc[1]  # 어제 (관통하는 양봉)
            day_before = df.iloc[2]  # 그 전날 (강한 음봉)
            
            # 전날이 강한 음봉
            day_before_body = abs(day_before['open'] - day_before['close'])
            day_before_range = day_before['high'] - day_before['low']
            
            if (day_before['close'] < day_before['open'] and  # 전날 음봉
                day_before_body / day_before_range >= 0.6):   # 몸통이 전체의 60% 이상 (강한 음봉)
                
                # 어제가 양봉이고 갭하락 후 반등
                if (yesterday['close'] > yesterday['open'] and    # 어제 양봉
                    yesterday['open'] < day_before['close']):     # 갭하락 시작
                    
                    # 관통 깊이 확인 (전날 몸통의 50% 이상 관통)
                    penetration = yesterday['close'] - day_before['close']
                    day_before_body_size = day_before['open'] - day_before['close']
                    penetration_ratio = penetration / day_before_body_size if day_before_body_size > 0 else 0
                    
                    if penetration_ratio >= 0.5:  # 50% 이상 관통
                        confidence = 0.65 + min(penetration_ratio * 0.2, 0.25)
                        strength = int(70 + penetration_ratio * 20)
                        
                        pattern = CandlePatternInfo(
                            pattern_type=PatternType.PIERCING_LINE,
                            confidence=min(confidence, 0.9),
                            strength=min(strength, 95),
                            detected_at=1,
                            trade_signal=TradeSignal.BUY,
                            target_price_ratio=1.02,  # 2% 목표
                            stop_loss_ratio=0.985,    # 1.5% 손절
                            expected_duration_hours=24  # 당일-1일 보유
                        )
                        patterns.append(pattern)
                        logger.debug(f"⚡ {stock_code} 관통형 패턴 감지 (관통률: {penetration_ratio:.2f})")
                        
        except Exception as e:
            logger.debug(f"관통형 패턴 감지 오류 ({stock_code}): {e}")
            
        return patterns

    def _detect_morning_star_pattern_new(self, df: pd.DataFrame, stock_code: str) -> List[CandlePatternInfo]:
        """⭐ 아침샛별 패턴 감지 - 4일 데이터 기준"""
        patterns = []
        
        try:
            if len(df) < 3:
                return patterns
                
            yesterday = df.iloc[1]      # 어제 (3일차 - 강한 양봉)
            middle_day = df.iloc[2]     # 그 전날 (2일차 - 도지/팽이)
            first_day = df.iloc[3]      # 3일 전 (1일차 - 음봉)
            
            # 1일차: 음봉
            first_is_bearish = first_day['close'] < first_day['open']
            
            # 2일차: 도지 또는 작은 몸통 (팽이)
            middle_body = abs(middle_day['close'] - middle_day['open'])
            middle_range = middle_day['high'] - middle_day['low']
            middle_body_ratio = middle_body / middle_range if middle_range > 0 else 0
            is_doji_or_spinning = middle_body_ratio <= 0.3  # 몸통이 30% 이하
            
            # 3일차: 강한 양봉
            yesterday_is_bullish = yesterday['close'] > yesterday['open']
            yesterday_body = abs(yesterday['close'] - yesterday['open'])
            yesterday_range = yesterday['high'] - yesterday['low']
            yesterday_body_ratio = yesterday_body / yesterday_range if yesterday_range > 0 else 0
            is_strong_bullish = yesterday_body_ratio >= 0.6  # 몸통이 60% 이상
            
            if (first_is_bearish and is_doji_or_spinning and 
                yesterday_is_bullish and is_strong_bullish):
                
                # 갭 확인 (2일차가 1일차보다 낮게 시작, 3일차가 2일차보다 높게 마감)
                gap1 = middle_day['high'] < first_day['low']  # 하방 갭
                gap2 = yesterday['close'] > middle_day['high']  # 상방 돌파
                
                if gap1 or gap2:  # 갭 중 하나라도 있으면
                    # 3일차 양봉이 1일차 몸통 중간 이상 관통하는지 확인
                    first_body_mid = (first_day['open'] + first_day['close']) / 2
                    penetration_strength = yesterday['close'] > first_body_mid
                    
                    if penetration_strength:
                        confidence = 0.8 + (yesterday_body_ratio * 0.15)
                        strength = int(85 + (yesterday_body_ratio * 15))
                        
                        pattern = CandlePatternInfo(
                            pattern_type=PatternType.MORNING_STAR,
                            confidence=min(confidence, 0.95),
                            strength=min(strength, 100),
                            detected_at=1,
                            trade_signal=TradeSignal.STRONG_BUY,
                            target_price_ratio=1.04,  # 4% 목표 (가장 강한 패턴)
                            stop_loss_ratio=0.975,    # 2.5% 손절
                            expected_duration_hours=24  # 1일 보유
                        )
                        patterns.append(pattern)
                        logger.debug(f"⭐ {stock_code} 아침샛별 패턴 감지 (신뢰도: {confidence:.2f})")
                        
        except Exception as e:
            logger.debug(f"아침샛별 패턴 감지 오류 ({stock_code}): {e}")
            
        return patterns

    def _check_downtrend_simple(self, df: pd.DataFrame, start_idx: int, days: int) -> float:
        """간단한 하락추세 확인"""
        try:
            if start_idx + days >= len(df):
                return 0.0
                
            prices = []
            for i in range(start_idx, start_idx + days):
                if i < len(df):
                    prices.append(df.iloc[i]['close'])
                    
            if len(prices) < 2:
                return 0.0
                
            # 선형 추세 계산
            x = list(range(len(prices)))
            slope = np.polyfit(x, prices, 1)[0]
            
            # 음의 기울기를 0-1 범위로 정규화
            return min(abs(slope) / prices[0], 1.0) if slope < 0 else 0.0
            
        except Exception:
            return 0.0

    def _filter_patterns_for_next_day_buy(self, patterns: List[CandlePatternInfo], df: pd.DataFrame) -> List[CandlePatternInfo]:
        """다음날 시가 매수를 위한 패턴 필터링"""
        if not patterns or df.empty:
            return []
            
        try:
            # 오늘 시가 (index=0의 open)
            today_open = df.iloc[0]['open'] if len(df) > 0 else 0
            
            filtered = []
            for pattern in patterns:
                # 시가 매수 가능 여부 확인 (시가 대비 -1.5% ~ +1.5% 범위)
                if today_open > 0:
                    buy_range_low = today_open * 0.985   # -1.5%
                    buy_range_high = today_open * 1.015  # +1.5%
                    
                    # 현재가가 매수 범위 내에 있는지 확인 (실제로는 실시간 체크)
                    # 여기서는 패턴이 유효하다고 가정하고 모두 통과
                    pattern.metadata = {
                        'buy_range_low': buy_range_low,
                        'buy_range_high': buy_range_high,
                        'target_open_price': today_open
                    }
                    filtered.append(pattern)
                    
            # 신뢰도 순으로 정렬
            filtered.sort(key=lambda x: (x.confidence, x.strength), reverse=True)
            
            return filtered[:3]  # 최대 3개 패턴만 반환
            
        except Exception as e:
            logger.error(f"패턴 필터링 오류: {e}")
            return patterns[:3]

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

    def _check_uptrend(self, df: pd.DataFrame, start_idx: int) -> float:
        """상승 추세 강도 확인 (0.0~1.0) - 강화된 분석"""
        try:
            if start_idx >= len(df) - 2:
                return 0.0

            # 🔧 추세 확인 기간 확대 (최소 5일)
            trend_period = min(max(self.thresholds['trend_min_days'], 5), len(df) - start_idx)
            prices = [df.iloc[start_idx + i]['close'] for i in range(trend_period)]

            if len(prices) < 3:  # 최소 3일 데이터 필요
                return 0.0

            # 1. 상승 일수 비율 계산
            up_days = sum(1 for i in range(1, len(prices)) if prices[i-1] < prices[i])
            up_ratio = up_days / (len(prices) - 1)

            # 2. 전체 상승폭 계산
            total_change = (prices[-1] - prices[0]) / prices[0] if prices[0] > 0 else 0

            # 3. 🆕 추세의 일관성 확인 (연속성)
            consecutive_up = 0
            max_consecutive_up = 0
            for i in range(1, len(prices)):
                if prices[i] > prices[i-1]:
                    consecutive_up += 1
                    max_consecutive_up = max(max_consecutive_up, consecutive_up)
                else:
                    consecutive_up = 0
            
            consistency_score = max_consecutive_up / (len(prices) - 1)

            # 4. 🆕 평균 상승률 계산
            daily_changes = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices)) if prices[i-1] > 0]
            avg_daily_change = sum(daily_changes) / len(daily_changes) if daily_changes else 0

            # 5. 종합 점수 계산 (더 엄격한 기준)
            trend_score = (
                up_ratio * 0.4 +                           # 상승 일수 비율
                min(1.0, total_change * 8) * 0.3 +         # 전체 상승폭 (8배 가중치로 조정)
                consistency_score * 0.2 +                  # 추세 일관성
                min(1.0, avg_daily_change * 50) * 0.1      # 평균 일일 상승률
            )

            return min(1.0, trend_score)

        except Exception as e:
            logger.error(f"상승추세 확인 오류: {e}")
            return 0.0

    def _check_downtrend(self, df: pd.DataFrame, start_idx: int) -> float:
        """하락 추세 강도 확인 (0.0~1.0) - 강화된 분석"""
        try:
            if start_idx >= len(df) - 2:
                return 0.0

            # 🔧 추세 확인 기간 확대 (최소 5일)
            trend_period = min(max(self.thresholds['trend_min_days'], 5), len(df) - start_idx)
            prices = [df.iloc[start_idx + i]['close'] for i in range(trend_period)]

            if len(prices) < 3:  # 최소 3일 데이터 필요
                return 0.0

            # 1. 하락 일수 비율 계산
            down_days = sum(1 for i in range(1, len(prices)) if prices[i-1] > prices[i])
            down_ratio = down_days / (len(prices) - 1)

            # 2. 전체 하락폭 계산
            total_change = (prices[0] - prices[-1]) / prices[0] if prices[0] > 0 else 0

            # 3. 🆕 추세의 일관성 확인 (연속성)
            consecutive_down = 0
            max_consecutive_down = 0
            for i in range(1, len(prices)):
                if prices[i] < prices[i-1]:
                    consecutive_down += 1
                    max_consecutive_down = max(max_consecutive_down, consecutive_down)
                else:
                    consecutive_down = 0
            
            consistency_score = max_consecutive_down / (len(prices) - 1)

            # 4. 🆕 평균 하락률 계산
            daily_changes = [(prices[i-1] - prices[i]) / prices[i-1] for i in range(1, len(prices)) if prices[i-1] > 0]
            avg_daily_change = sum(daily_changes) / len(daily_changes) if daily_changes else 0

            # 5. 종합 점수 계산 (더 엄격한 기준)
            trend_score = (
                down_ratio * 0.4 +                         # 하락 일수 비율
                min(1.0, total_change * 8) * 0.3 +         # 전체 하락폭 (8배 가중치로 조정)
                consistency_score * 0.2 +                  # 추세 일관성
                min(1.0, avg_daily_change * 50) * 0.1      # 평균 일일 하락률
            )

            return min(1.0, trend_score)

        except Exception as e:
            logger.error(f"하락추세 확인 오류: {e}")
            return 0.0

    def _check_volume_confirmation(self, df: pd.DataFrame, idx: int) -> float:
        """🔥 강화된 거래량 확인 (0.0~1.0) - 더 엄격한 기준"""
        try:
            if idx >= len(df):
                return 0.0

            current = df.iloc[idx]

            # 🆕 1. 기본 거래량 비율 확인 (더 엄격한 기준)
            volume_ratio = current.get('volume_ratio', 1.0)
            base_score = 0.0

            if volume_ratio >= 2.5:  # 평균의 2.5배 이상 (강화)
                base_score = 1.0
            elif volume_ratio >= 2.0:  # 평균의 2배 이상 (강화)
                base_score = 0.8
            elif volume_ratio >= 1.5:  # 평균의 1.5배 이상
                base_score = 0.6
            elif volume_ratio >= self.thresholds['volume_confirmation']:  # 설정된 임계값 이상
                base_score = 0.4
            else:
                base_score = 0.1  # 거래량 부족 시 매우 낮은 점수

            # 🆕 2. 거래대금 추가 확인 (더 엄격)
            value_ratio = current.get('value_ratio', 1.0)
            if value_ratio >= 2.0:  # 거래대금 2배 이상 (강화)
                base_score += 0.2
            elif value_ratio >= 1.5:  # 거래대금 1.5배 이상
                base_score += 0.1

            # 🆕 3. VWAP 대비 가격 위치 (더 엄격)
            price_vs_vwap = current.get('price_vs_vwap', 0.0)
            if abs(price_vs_vwap) > 0.03:  # VWAP에서 3% 이상 벗어남 (강화)
                base_score += 0.1
            elif abs(price_vs_vwap) > 0.02:  # VWAP에서 2% 이상 벗어남
                base_score += 0.05

            # 🆕 4. 최근 거래량 추세 확인
            if idx < len(df) - 3:  # 최소 3일 데이터 확인
                recent_volumes = [df.iloc[idx + i].get('volume', 0) for i in range(min(3, len(df) - idx))]
                if len(recent_volumes) >= 2:
                    volume_trend = (recent_volumes[0] - recent_volumes[-1]) / recent_volumes[-1] if recent_volumes[-1] > 0 else 0
                    if volume_trend > 0.2:  # 최근 거래량 20% 이상 증가
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
        """변동성 확인 지표"""
        try:
            current = df.iloc[idx]
            
            # 1. ATR 대비 변동성 체크
            atr_ratio = current.get('total_range', 0) / current.get('atr_14', current.get('total_range', 1))
            volatility_score = min(1.0, atr_ratio / 1.5)  # ATR의 1.5배 이상이면 만점
            
            # 2. 볼린저 밴드 위치 체크
            bb_position = current.get('bb_position', 0.5)
            if bb_position < 0.2 or bb_position > 0.8:  # 밴드 끝쪽에 있으면 변동성 높음
                volatility_score += 0.3
            
            # 3. 일중 변동률 체크
            intraday_vol = current.get('intraday_volatility', 0.02)
            if intraday_vol > 0.03:  # 3% 이상 일중 변동
                volatility_score += 0.2
                
            return min(1.0, volatility_score)
            
        except Exception as e:
            logger.error(f"변동성 확인 오류: {e}")
            return 0.5

    def _validate_pattern_still_valid(self, df: pd.DataFrame, pattern_idx: int, pattern_type: str) -> bool:
        """패턴이 여전히 유효한지 검증"""
        try:
            # 패턴 무효화 설정 로드
            import json
            import os
            
            config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                                     'config', 'candle_strategy_config.json')
            
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            invalidation_config = config.get('quality_management', {}).get('pattern_invalidation', {})
            
            if not invalidation_config.get('enabled', False):
                return True
            
            consecutive_days = invalidation_config.get('consecutive_opposite_days', 2)
            price_threshold = invalidation_config.get('price_decline_threshold', -3.0)
            max_age_days = invalidation_config.get('max_pattern_age_days', 3)
            
            # 1. 패턴 나이 체크
            if pattern_idx >= max_age_days:
                logger.debug(f"패턴 무효화: 너무 오래됨 ({pattern_idx}일 전)")
                return False
            
            # 2. 연속 반대 움직임 체크
            if pattern_type in ['bullish_engulfing', 'hammer', 'inverted_hammer']:
                # 상승 패턴의 경우 연속 하락 체크
                consecutive_declines = 0
                total_decline = 0
                
                for i in range(pattern_idx):
                    current = df.iloc[i]
                    if current['close'] < current['open']:  # 하락 캔들
                        consecutive_declines += 1
                        total_decline += (current['close'] - current['open']) / current['open'] * 100
                    else:
                        break
                
                # 연속 하락일 수 또는 총 하락률 체크
                if (consecutive_declines >= consecutive_days or 
                    total_decline <= price_threshold):
                    logger.debug(f"패턴 무효화: 연속 {consecutive_declines}일 하락, 총 {total_decline:.1f}% 하락")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"패턴 검증 오류: {e}")
            return True  # 오류 시 보수적으로 유효하다고 판단
