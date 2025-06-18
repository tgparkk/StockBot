"""
캔들 패턴 감지 및 분석 시스템
"""
import pandas as pd
import numpy as np
import json
import os
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

        # 🎯 시가 매수 전략 임계값 - 현실적으로 강화
        self.thresholds = {
            'hammer_lower_shadow_min': 0.45,     # 망치형: 아래꼬리 최소 45% (기존 60%에서 약간 완화)
            'hammer_body_max': 0.40,             # 망치형: 몸통 최대 40% (기존 30%에서 약간 완화)
            'hammer_upper_shadow_max': 0.15,     # 망치형: 윗꼬리 최대 15% (기존 10%에서 약간 완화)
            
            'engulfing_ratio_min': 0.85,         # 장악형: 최소 85% 크기 (기존 110%에서 완화)
            
            'piercing_body_min': 0.6,            # 관통형: 전날 음봉 몸통 최소 60%
            'piercing_penetration_min': 0.35,    # 관통형: 최소 35% 관통 (기존 50%에서 완화)
            
            'morning_star_doji_max': 0.3,        # 아침샛별: 중간일 몸통 최대 30%
            'morning_star_bullish_min': 0.6,     # 아침샛별: 마지막일 양봉 몸통 최소 60%
            
            'downtrend_strength_min': 0.015,     # 하락추세 최소 강도 1.5% (기존 0.5%에서 강화)
            'min_confidence': 0.65,              # 최소 신뢰도 65% (기존 60%에서 강화)
        }
        
        # 🆕 Config 파일 경로 및 캐시
        self.config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
            'config', 'candle_strategy_config.json'
        )
        self._config_cache = None
        self._config_last_loaded = None

    def _load_config(self) -> Dict:
        """🆕 Config 파일 로드 (캐싱 적용)"""
        try:
            # 캐시된 설정이 있고 5분 이내라면 재사용
            if (self._config_cache and self._config_last_loaded and 
                (datetime.now() - self._config_last_loaded).seconds < 300):
                return self._config_cache
                
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                
            self._config_cache = config
            self._config_last_loaded = datetime.now()
            
            logger.debug("📄 Config 파일 로드 완료")
            return config
            
        except Exception as e:
            logger.error(f"Config 파일 로드 실패: {e}")
            # 기본값 반환
            return {
                "pattern_targets": {
                    "hammer": {"target": 1.8, "stop": 1.5},
                    "bullish_engulfing": {"target": 2.3, "stop": 2.0},
                    "piercing_line": {"target": 1.8, "stop": 1.5},
                    "morning_star": {"target": 2.5, "stop": 2.5}
                }
            }

    def _get_pattern_config(self, pattern_type: PatternType) -> Dict:
        """🆕 패턴별 Config 값 조회"""
        config = self._load_config()
        pattern_targets = config.get('pattern_targets', {})
        
        # PatternType enum을 문자열로 변환
        pattern_key = pattern_type.value
        
        if pattern_key in pattern_targets:
            target_config = pattern_targets[pattern_key]
            return {
                'target_price_ratio': 1.0 + (target_config.get('target', 2.0) / 100.0),
                'stop_loss_ratio': 1.0 - (target_config.get('stop', 2.0) / 100.0),
                'max_hours': target_config.get('max_hours', 24),
                'min_minutes': target_config.get('min_minutes', 720)
            }
        else:
            # 기본값
            logger.warning(f"⚠️ {pattern_key} 패턴 Config 없음, 기본값 사용")
            return {
                'target_price_ratio': 1.02,  # 2% 목표
                'stop_loss_ratio': 0.98,     # 2% 손절
                'max_hours': 24,
                'min_minutes': 720
            }

    def analyze_stock_patterns(self, stock_code: str, ohlcv_data: pd.DataFrame,
                             volume_data: Optional[pd.DataFrame] = None) -> List[CandlePatternInfo]:
        """
        🎯 캔들 패턴 분석 - Config 파일 기반 목표/손절 설정
        
        📈 매수 타이밍 설명:
        - 망치형/상승장악형/관통형/아침샛별 패턴이 감지되면
        - **반전 신호로 판단하여 다음날 시가에 매수 시도**
        - 즉, 하락추세 중 반전 패턴 출현 → 즉시 매수 (반전 전 매수)
        - detected_at=1 (어제 캔들에서 패턴 감지) → 오늘 시가 매수
        
        🎯 Config 연동:
        - target_price_ratio, stop_loss_ratio는 candle_strategy_config.json에서 로드
        - 패턴별로 다른 목표/손절 비율 적용 가능
        """
        try:
            if ohlcv_data is None or ohlcv_data.empty:
                logger.warning(f"🔍 {stock_code}: OHLCV 데이터 없음")
                return []

            # 🆕 간단한 데이터 전처리 우선 시도
            df = self._prepare_basic_data_safe(ohlcv_data)
            if df.empty:
                logger.warning(f"🔍 {stock_code}: 기본 데이터 전처리 실패")
                return []

            logger.debug(f"🔍 {stock_code} 분석 시작: {len(df)}일 데이터")
            
            detected_patterns = []

            # 🎯 완화된 조건으로 패턴 감지
            
            # 1. 개선된 망치형 패턴 (3일 데이터면 충분)
            if len(df) >= 3:
                hammer_patterns = self._detect_hammer_pattern_relaxed(df, stock_code)
                detected_patterns.extend(hammer_patterns)
                
            # 2. 개선된 상승장악형 패턴 (2일 데이터면 충분)
            if len(df) >= 2:
                engulfing_patterns = self._detect_bullish_engulfing_relaxed(df, stock_code)
                detected_patterns.extend(engulfing_patterns)
                
            # 3. 개선된 관통형 패턴
            if len(df) >= 2:
                piercing_patterns = self._detect_piercing_line_relaxed(df, stock_code)
                detected_patterns.extend(piercing_patterns)
                
            # 4. 개선된 아침샛별 패턴
            if len(df) >= 3:
                morning_star_patterns = self._detect_morning_star_relaxed(df, stock_code)
                detected_patterns.extend(morning_star_patterns)

            # 📊 결과 로깅
            if detected_patterns:
                pattern_summary = [f"{p.pattern_type.value}({p.confidence:.2f})" 
                                 for p in detected_patterns]
                logger.debug(f"🎯 {stock_code} 감지된 패턴: {', '.join(pattern_summary)}")
            else:
                logger.debug(f"❌ {stock_code} 감지된 패턴 없음")

            # 패턴 필터링 및 정렬
            filtered_patterns = self._filter_and_sort_patterns(detected_patterns, df)
            
            if filtered_patterns:
                final_summary = [f"{p.pattern_type.value}(신뢰도:{p.confidence:.2f})" 
                               for p in filtered_patterns]
                logger.debug(f"✅ {stock_code} 최종 선택: {', '.join(final_summary)}")

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

    def _prepare_basic_data_safe(self, ohlcv_data: pd.DataFrame) -> pd.DataFrame:
        """🆕 안전한 기본 데이터 전처리 - 실패 가능성 최소화"""
        try:
            df = ohlcv_data.copy()
            
            # 🔧 컬럼명 정규화 (KIS API 대응)
            column_mapping = {
                'stck_oprc': 'open', 'stck_hgpr': 'high', 'stck_lwpr': 'low',
                'stck_clpr': 'close', 'acml_vol': 'volume',
                # 추가 가능한 컬럼명들
                'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'
            }
            
            for old_name, new_name in column_mapping.items():
                if old_name in df.columns and new_name not in df.columns:
                    df[new_name] = df[old_name]
            
            # 필수 컬럼 확인
            required_cols = ['open', 'high', 'low', 'close']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                logger.error(f"필수 컬럼 누락: {missing_cols}, 사용 가능한 컬럼: {list(df.columns)}")
                return pd.DataFrame()
            
            # 🔧 데이터 타입 변환 (안전하게)
            for col in required_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
            if 'volume' in df.columns:
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
            else:
                df['volume'] = 0  # 거래량 데이터가 없으면 0으로 설정
                
            # 🔧 기본 캔들 정보 계산 (안전하게)
            df['body'] = abs(df['close'] - df['open'])
            df['upper_shadow'] = df['high'] - df[['open', 'close']].max(axis=1)
            df['lower_shadow'] = df[['open', 'close']].min(axis=1) - df['low']
            df['total_range'] = df['high'] - df['low']
            
            # 0으로 나누기 방지
            df['total_range'] = df['total_range'].replace(0, 0.01)
            
            df['body_ratio'] = df['body'] / df['total_range']
            df['upper_shadow_ratio'] = df['upper_shadow'] / df['total_range']
            df['lower_shadow_ratio'] = df['lower_shadow'] / df['total_range']
            
            # 상승/하락 구분
            df['is_bullish'] = df['close'] > df['open']
            df['is_bearish'] = df['close'] < df['open']
            
            # 🔧 정렬 (최신 데이터가 첫 번째 행)
            if hasattr(df.index, 'sort_values'):
                df = df.sort_index(ascending=False)
            df = df.reset_index(drop=True)
            
            # 🔧 데이터 유효성 검증
            df = df.dropna(subset=['open', 'high', 'low', 'close'])
            
            if len(df) == 0:
                logger.error("유효한 OHLC 데이터가 없습니다")
                return pd.DataFrame()
                
            logger.debug(f"전처리 완료: {len(df)}일 데이터, 최근가: {df.iloc[0]['close']:.0f}")
            return df
            
        except Exception as e:
            logger.error(f"기본 데이터 전처리 오류: {e}")
            return pd.DataFrame()

# 🗑️ 기존 _detect_hammer_pattern_new 함수 제거 (사용하지 않음)

# 🗑️ 기존 _detect_bullish_engulfing_pattern_new 함수 제거 (사용하지 않음)

# 🗑️ 기존 _detect_piercing_line_pattern_new 함수 제거 (사용하지 않음)

# 🗑️ 기존 _detect_morning_star_pattern_new 함수 제거 (사용하지 않음)

    def _check_downtrend_simple(self, df: pd.DataFrame, start_idx: int, days: int) -> float:
        """🆕 개선된 하락추세 확인 - 더 관대한 조건"""
        try:
            if start_idx + days >= len(df):
                # 🆕 데이터 부족시 최소한의 데이터로 체크
                available_days = len(df) - start_idx - 1
                if available_days < 2:
                    return 0.0
                days = available_days
                
            prices = []
            dates_info = []  # 🆕 디버깅용
            
            for i in range(start_idx, start_idx + days):
                if i < len(df):
                    price = df.iloc[i]['close']
                    prices.append(price)
                    dates_info.append(f"[{i}]={price:.0f}")
                    
            if len(prices) < 2:
                return 0.0
            
            # 🆕 디버깅 정보
            # logger.debug(f"📉 하락추세 체크: {' → '.join(dates_info)}")
                
            # 🆕 다양한 하락추세 측정 방식
            
            # 1. 선형 추세 (기존 방식)
            x = list(range(len(prices)))
            slope = np.polyfit(x, prices, 1)[0]
            linear_trend = min(abs(slope) / prices[0], 1.0) if slope < 0 else 0.0
            
            # 2. 🆕 단순 비교 (시작 vs 끝)
            start_price = prices[0]  # 최신 (어제)
            end_price = prices[-1]   # 가장 오래된
            simple_trend = (end_price - start_price) / end_price if end_price > 0 else 0.0
            simple_trend = max(simple_trend, 0.0)  # 양수만 (하락시)
            
            # 3. 🆕 연속 하락일 체크
            down_days = 0
            for i in range(1, len(prices)):
                if prices[i-1] < prices[i]:  # 어제가 그제보다 낮음 (하락)
                    down_days += 1
            
            consecutive_down_ratio = down_days / (len(prices) - 1) if len(prices) > 1 else 0.0
            
            # 4. 🆕 종합 하락추세 점수 (3가지 방식의 가중평균)
            final_score = (
                linear_trend * 0.4 +
                simple_trend * 0.4 +
                consecutive_down_ratio * 0.2
            )
            
            # 🆕 디버깅 정보 (상세)
            # logger.debug(f"📉 하락추세 분석: 선형={linear_trend:.2f}, 단순={simple_trend:.2f}, 연속={consecutive_down_ratio:.2f} → 최종={final_score:.2f}")
            
            return min(final_score, 1.0)
            
        except Exception as e:
            # logger.debug(f"하락추세 체크 오류: {e}")
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



    def _detect_hammer_pattern_relaxed(self, df: pd.DataFrame, stock_code: str) -> List[CandlePatternInfo]:
        """🔨 개선된 망치형 패턴 감지 - 실용적인 조건들"""
        patterns = []
        
        try:
            if len(df) < 2:
                return patterns
                
            yesterday = df.iloc[1] if len(df) > 1 else df.iloc[0]
            
            # 🔧 직접 계산 (전처리 데이터 의존성 제거)
            body_size = abs(yesterday['close'] - yesterday['open'])
            total_range = yesterday['high'] - yesterday['low']
            
            if total_range <= 0:
                return patterns
                
            lower_shadow = min(yesterday['open'], yesterday['close']) - yesterday['low']
            upper_shadow = yesterday['high'] - max(yesterday['open'], yesterday['close'])
            
            lower_shadow_ratio = lower_shadow / total_range
            upper_shadow_ratio = upper_shadow / total_range
            body_ratio = body_size / total_range
            
            # 🆕 현실적인 망치형 조건 (기존 relaxed에서 강화)
            conditions = {
                'long_lower_shadow': lower_shadow_ratio >= 0.45,  # 15% → 45%로 강화
                'small_body': body_ratio <= 0.40,               # 75% → 40%로 강화
                'short_upper_shadow': upper_shadow_ratio <= 0.15 # 50% → 15%로 강화
            }
            
            if all(conditions.values()):
                # 🔧 하락 추세 조건 강화 (1.5% 이상)
                simple_downtrend = self._check_simple_downtrend(df, 1, 3)
                
                # 🔧 종가 위치 확인 (강화된 조건)
                close_position = (yesterday['close'] - yesterday['low']) / total_range
                
                # 🔧 강화된 조건: 하락추세 1.5% 이상 AND 종가위치 30% 이상
                if simple_downtrend >= 0.015 and close_position >= 0.30:
                    confidence = 0.6 + (lower_shadow_ratio * 0.3) + (simple_downtrend * 0.1)
                    strength = int(60 + (lower_shadow_ratio * 25) + (simple_downtrend * 15))
                    
                    # 🆕 Config에서 목표/손절 비율 로드
                    pattern_config = self._get_pattern_config(PatternType.HAMMER)
                    
                    pattern = CandlePatternInfo(
                        pattern_type=PatternType.HAMMER,
                        confidence=min(confidence, 0.9),
                        strength=min(strength, 95),
                        description=f"망치형 - 아래꼬리:{lower_shadow_ratio:.1%}, 하락추세:{simple_downtrend:.1%}",
                        detected_at=1,
                        target_price_ratio=pattern_config['target_price_ratio'],  # Config 값 사용
                        stop_loss_ratio=pattern_config['stop_loss_ratio'],        # Config 값 사용
                        expected_duration_hours=pattern_config['max_hours'],      # Config 값 사용
                        metadata={
                            'lower_shadow_ratio': lower_shadow_ratio,
                            'body_ratio': body_ratio,
                            'simple_downtrend': simple_downtrend,
                            'support_price': yesterday['low'],
                            'config_target_pct': (pattern_config['target_price_ratio'] - 1.0) * 100,
                            'config_stop_pct': (1.0 - pattern_config['stop_loss_ratio']) * 100
                        }
                    )
                    
                    patterns.append(pattern)
                    logger.info(f"🔨 {stock_code} 망치형 패턴 발견! (완화된 조건)")
                    
        except Exception as e:
            logger.error(f"망치형 패턴 감지 오류 ({stock_code}): {e}")
            
        return patterns

    def _detect_bullish_engulfing_relaxed(self, df: pd.DataFrame, stock_code: str) -> List[CandlePatternInfo]:
        """📈 개선된 상승장악형 패턴 감지 - 실용적인 조건들"""
        patterns = []
        
        try:
            if len(df) < 2:
                return patterns
                
            yesterday = df.iloc[1]  # 어제 (장악하는 양봉)
            day_before = df.iloc[2] if len(df) > 2 else df.iloc[1]  # 그 전날 (장악당하는 음봉)
            
            # 🔧 직접 계산 (전처리 데이터 의존성 제거)
            
            # 1. 전날이 음봉이어야 함 (완화)
            day_before_bearish = day_before['close'] < day_before['open']
            if not day_before_bearish:
                return patterns
                
            # 2. 어제가 양봉이어야 함 (완화)
            yesterday_bullish = yesterday['close'] > yesterday['open']
            if not yesterday_bullish:
                return patterns
                
            # 🆕 3. 매우 완화된 장악 조건
            yesterday_body_size = abs(yesterday['close'] - yesterday['open'])
            day_before_body_size = abs(day_before['open'] - day_before['close'])
            
            # 🔧 크기 비교 (0.5배 → 0.85배로 강화)
            size_ratio = yesterday_body_size / day_before_body_size if day_before_body_size > 0 else 1.0
            size_condition = size_ratio >= 0.85  # 85% 크기 필요
            
            # 🔧 포함 조건 (1% 여유로 강화)
            engulfs_open = yesterday['open'] <= day_before['open'] * 1.01   # 1% 여유
            engulfs_close = yesterday['close'] >= day_before['close'] * 0.99  # 1% 여유
            
            if size_condition and engulfs_open and engulfs_close:
                # 🔧 하락 추세 조건 강화
                simple_downtrend = self._check_simple_downtrend(df, 2, 3)
                
                # 🔧 하락 추세 1.5% 이상 필요 (기존 0.5%)
                if simple_downtrend >= 0.015:
                    confidence = 0.65 + (size_ratio * 0.15) + (simple_downtrend * 0.1)
                    strength = int(65 + (size_ratio * 20) + (simple_downtrend * 15))
                    
                    # 🆕 Config에서 목표/손절 비율 로드
                    pattern_config = self._get_pattern_config(PatternType.BULLISH_ENGULFING)
                    
                    pattern = CandlePatternInfo(
                        pattern_type=PatternType.BULLISH_ENGULFING,
                        confidence=min(confidence, 0.9),
                        strength=min(strength, 95),
                        description=f"상승장악형 - 크기비율:{size_ratio:.2f}, 하락추세:{simple_downtrend:.1%}",
                        detected_at=1,
                        target_price_ratio=pattern_config['target_price_ratio'],  # Config 값 사용
                        stop_loss_ratio=pattern_config['stop_loss_ratio'],        # Config 값 사용
                        expected_duration_hours=pattern_config['max_hours'],      # Config 값 사용
                        metadata={
                            'size_ratio': size_ratio,
                            'engulfs_range': (engulfs_open, engulfs_close),
                            'simple_downtrend': simple_downtrend,
                            'support_price': yesterday['low'],
                            'config_target_pct': (pattern_config['target_price_ratio'] - 1.0) * 100,
                            'config_stop_pct': (1.0 - pattern_config['stop_loss_ratio']) * 100
                        }
                    )
                    
                    patterns.append(pattern)
                    logger.info(f"📈 {stock_code} 상승장악형 패턴 발견! (완화된 조건)")
                    
        except Exception as e:
            logger.error(f"상승장악형 패턴 감지 오류 ({stock_code}): {e}")
            
        return patterns

    def _detect_piercing_line_relaxed(self, df: pd.DataFrame, stock_code: str) -> List[CandlePatternInfo]:
        """🎯 개선된 관통형 패턴 감지 - 실용적인 조건들"""
        patterns = []
        
        try:
            if len(df) < 2:
                return patterns
                
            yesterday = df.iloc[1]  # 어제 (관통하는 양봉)
            day_before = df.iloc[2] if len(df) > 2 else df.iloc[1]  # 그 전날 (관통당하는 음봉)
            
            # 🔧 직접 계산 (전처리 데이터 의존성 제거)
            
            # 1. 전날이 음봉이어야 함 (완화)
            day_before_bearish = day_before['close'] < day_before['open']
            if not day_before_bearish:
                return patterns
                
            # 2. 어제가 양봉이어야 함 (완화)
            yesterday_bullish = yesterday['close'] > yesterday['open']
            if not yesterday_bullish:
                return patterns
                
            # 🆕 3. 매우 완화된 관통 조건
            day_before_body = day_before['open'] - day_before['close']  # 음봉 몸통
            
            # 🔧 관통 정도 (15% → 35%로 강화)
            if day_before_body > 0:
                penetration_ratio = (yesterday['close'] - day_before['close']) / day_before_body
                penetration_condition = penetration_ratio >= 0.35  # 35% 이상 관통
            else:
                penetration_condition = False
            
            # 🔧 시가 조건 (갭다운 조건 강화)
            gap_down = yesterday['open'] <= day_before['close']  # 갭다운 또는 동일 레벨
            
            if penetration_condition and gap_down:
                # 🔧 하락 추세 조건 강화
                simple_downtrend = self._check_simple_downtrend(df, 2, 3)
                
                # 🔧 하락 추세 1.5% 이상 필요 (기존 0.5%)
                if simple_downtrend >= 0.015:
                    confidence = 0.65 + (penetration_ratio * 0.2) + (simple_downtrend * 0.1)
                    strength = int(65 + (penetration_ratio * 25) + (simple_downtrend * 10))
                    
                    # 🆕 Config에서 목표/손절 비율 로드
                    pattern_config = self._get_pattern_config(PatternType.PIERCING_LINE)
                    
                    pattern = CandlePatternInfo(
                        pattern_type=PatternType.PIERCING_LINE,
                        confidence=min(confidence, 0.9),
                        strength=min(strength, 95),
                        description=f"관통형 - 관통비율:{penetration_ratio:.1%}, 하락추세:{simple_downtrend:.1%}",
                        detected_at=1,
                        target_price_ratio=pattern_config['target_price_ratio'],  # Config 값 사용
                        stop_loss_ratio=pattern_config['stop_loss_ratio'],        # Config 값 사용
                        expected_duration_hours=pattern_config['max_hours'],      # Config 값 사용
                        metadata={
                            'penetration_ratio': penetration_ratio,
                            'gap_down': gap_down,
                            'simple_downtrend': simple_downtrend,
                            'support_price': yesterday['low'],
                            'config_target_pct': (pattern_config['target_price_ratio'] - 1.0) * 100,
                            'config_stop_pct': (1.0 - pattern_config['stop_loss_ratio']) * 100
                        }
                    )
                    
                    patterns.append(pattern)
                    logger.info(f"🎯 {stock_code} 관통형 패턴 발견! (완화된 조건)")
                    
        except Exception as e:
            logger.error(f"관통형 패턴 감지 오류 ({stock_code}): {e}")
            
        return patterns

    def _detect_morning_star_relaxed(self, df: pd.DataFrame, stock_code: str) -> List[CandlePatternInfo]:
        """⭐ 개선된 아침샛별 패턴 감지 - 실용적인 조건들"""
        patterns = []
        
        try:
            if len(df) < 3:
                return patterns
                
            yesterday = df.iloc[1]      # 어제 (세 번째 캔들 - 양봉)
            middle_day = df.iloc[2]     # 중간일 (두 번째 캔들 - 작은 몸통)
            day_before = df.iloc[3] if len(df) > 3 else df.iloc[2]  # 그 전전날 (첫 번째 캔들 - 음봉)
            
            # 🔧 직접 계산 (전처리 데이터 의존성 제거)
            
            # 1. 첫 번째 캔들이 음봉 (완화)
            day_before_bearish = day_before['close'] < day_before['open']
            if not day_before_bearish:
                return patterns
                
            # 2. 세 번째 캔들이 양봉 (완화)
            yesterday_bullish = yesterday['close'] > yesterday['open']
            if not yesterday_bullish:
                return patterns
                
            # 🆕 3. 매우 완화된 중간일 조건 (작은 몸통)
            middle_body = abs(middle_day['close'] - middle_day['open'])
            middle_range = middle_day['high'] - middle_day['low']
            middle_body_ratio = middle_body / middle_range if middle_range > 0 else 1.0
            small_body_condition = middle_body_ratio <= 0.6  # 40% → 60%로 대폭 완화
            
            # 🆕 4. 갭 조건 거의 제거 (한국 시장 특성 반영)
            gap_condition = True  # 갭 조건 거의 제거
            
            if small_body_condition and gap_condition:
                # 🔧 하락 추세 조건 대폭 완화
                simple_downtrend = self._check_simple_downtrend(df, 3, 5)
                
                # 🔧 하락 추세 0.5% 이상이면 OK (기존 10%)
                if simple_downtrend >= 0.005:
                    # 🔧 양봉 강도 확인 (대폭 완화)
                    yesterday_body = abs(yesterday['close'] - yesterday['open'])
                    yesterday_range = yesterday['high'] - yesterday['low']
                    bullish_strength = yesterday_body / yesterday_range if yesterday_range > 0 else 0
                    if bullish_strength >= 0.15:  # 30% → 15%로 대폭 완화
                        confidence = 0.7 + (bullish_strength * 0.15) + (simple_downtrend * 0.1)
                        strength = int(70 + (bullish_strength * 20) + (simple_downtrend * 10))
                        
                        pattern = CandlePatternInfo(
                            pattern_type=PatternType.MORNING_STAR,
                            confidence=min(confidence, 0.95),
                            strength=min(strength, 95),
                            description=f"아침샛별 - 중간몸통:{middle_body_ratio:.1%}, 양봉강도:{bullish_strength:.1%}",
                            detected_at=1,
                            target_price_ratio=1.06,  # 6% 목표
                            stop_loss_ratio=0.95,     # 5% 손절
                            metadata={
                                'middle_body_ratio': middle_body_ratio,
                                'bullish_strength': bullish_strength,
                                'gap_condition': gap_condition,
                                'simple_downtrend': simple_downtrend,
                                'support_price': middle_day['low']
                            }
                        )
                        
                        patterns.append(pattern)
                        logger.info(f"⭐ {stock_code} 아침샛별 패턴 발견! (완화된 조건)")
                    
        except Exception as e:
            logger.error(f"아침샛별 패턴 감지 오류 ({stock_code}): {e}")
            
        return patterns

    def _check_simple_downtrend(self, df: pd.DataFrame, start_idx: int, days: int) -> float:
        """🔧 간단한 하락추세 체크 - 복잡한 계산 제거"""
        try:
            if start_idx + days >= len(df):
                available_days = len(df) - start_idx - 1
                if available_days < 2:
                    return 0.0
                days = available_days
                
            # 시작점과 끝점 가격만 비교 (간단하게)
            start_price = df.iloc[start_idx + days - 1]['close']  # 과거 가격
            end_price = df.iloc[start_idx]['close']              # 최근 가격
            
            if start_price <= 0:
                return 0.0
                
            # 하락률 계산
            decline_pct = (start_price - end_price) / start_price
            
            # 0.0 ~ 1.0 범위로 정규화
            return max(0.0, min(1.0, decline_pct))
            
        except Exception as e:
            logger.debug(f"간단한 하락추세 체크 오류: {e}")
            return 0.0

    def _filter_and_sort_patterns(self, patterns: List[CandlePatternInfo], df: pd.DataFrame) -> List[CandlePatternInfo]:
        """🔧 패턴 필터링 및 정렬 - 실용적인 접근"""
        try:
            if not patterns:
                return []
                
            # 🔧 최소 신뢰도 필터링 (완화된 조건)
            min_confidence = 0.55  # 55% (기존 60%)
            filtered = [p for p in patterns if p.confidence >= min_confidence]
            
            if not filtered:
                # 기준 미달시에도 최고 신뢰도 1개는 선택
                best_pattern = max(patterns, key=lambda p: p.confidence)
                if best_pattern.confidence >= 0.5:  # 최소 50%
                    filtered = [best_pattern]
                    logger.info(f"📊 기준 미달이지만 최고 신뢰도 패턴 선택: {best_pattern.pattern_type.value} ({best_pattern.confidence:.2f})")
                else:
                    return []
            
            # 🔧 신뢰도 및 강도순 정렬
            filtered.sort(key=lambda p: (p.confidence, p.strength), reverse=True)
            
            # 🔧 최대 2개만 반환 (혼란 방지)
            return filtered[:2]
            
        except Exception as e:
            logger.error(f"패턴 필터링 오류: {e}")
            return patterns[:1] if patterns else []
