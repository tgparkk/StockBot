"""
실시간 캔들 패턴 감지 및 분석 시스템 (장중 전용)
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from utils.logger import setup_logger

from .candle_trade_candidate import (
    CandlePatternInfo, PatternType, TradeSignal
)

logger = setup_logger(__name__)


class RealtimePatternDetector:
    """🚀 실시간 캔들 패턴 감지 및 분석 시스템 (장중 전용)"""

    def __init__(self):
        logger.info("🚀 RealtimePatternDetector 초기화 완료")
        
        # 🎯 실시간 패턴 전용 가중치 설정
        self.pattern_weights = {
            PatternType.HAMMER: 0.75,
            PatternType.BULLISH_ENGULFING: 0.85,
            PatternType.PIERCING_LINE: 0.70,
            PatternType.MORNING_STAR: 0.90,
            PatternType.DOJI: 0.60,
        }

        # 🎯 실시간 매매 임계값
        self.thresholds = {
            'realtime_confidence_min': 0.55,
            'intraday_volume_spike': 1.5,
            'price_movement_threshold': 0.02,
            'pattern_formation_minutes': 15,
            'confirmation_candles': 2,
        }

        # 🎯 분봉 데이터 저장소
        self._minute_data_cache = {}
        self._last_analysis_time = {}

    def analyze_realtime_patterns(self, stock_code: str, 
                                current_price: float,
                                daily_ohlcv: pd.DataFrame,
                                minute_data: Optional[pd.DataFrame] = None) -> List[CandlePatternInfo]:
        """🕐 실시간 패턴 분석 메인 함수"""
        try:
            patterns = []
            
            if daily_ohlcv is None or daily_ohlcv.empty:
                logger.warning(f"🔍 {stock_code}: 일봉 데이터 없음")
                return patterns

            # 1. 진행 중인 오늘 캔들 분석
            today_patterns = self._analyze_forming_candle(stock_code, daily_ohlcv, current_price)
            patterns.extend(today_patterns)

            # 2. 분봉 데이터 기반 단기 패턴 (선택적)
            if minute_data is not None and not minute_data.empty:
                minute_patterns = self._analyze_minute_patterns(stock_code, minute_data, current_price)
                patterns.extend(minute_patterns)

            # 3. 실시간 필터링
            filtered_patterns = self._filter_realtime_patterns(patterns, current_price)
            
            if filtered_patterns:
                pattern_summary = [f"{p.pattern_type.value}({p.confidence:.2f})" 
                                 for p in filtered_patterns]
                logger.info(f"🎯 {stock_code} 실시간 패턴: {', '.join(pattern_summary)}")

            return filtered_patterns

        except Exception as e:
            logger.error(f"실시간 패턴 분석 오류 ({stock_code}): {e}")
            return []

    def _prepare_realtime_data(self, ohlcv_data: pd.DataFrame) -> pd.DataFrame:
        """🔧 실시간 분석용 데이터 전처리 (KIS API 컬럼명 정규화)"""
        try:
            df = ohlcv_data.copy()
            
            # 🔧 KIS API 컬럼명 매핑 (분봉 데이터 대응)
            column_mapping = {
                'stck_oprc': 'open',      # 시가
                'stck_hgpr': 'high',      # 고가
                'stck_lwpr': 'low',       # 저가
                'stck_prpr': 'close',     # 🆕 현재가 (종가 역할)
                'stck_clpr': 'close',     # 종가 (일봉용)
                'cntg_vol': 'volume',     # 🆕 체결거래량
                'acml_vol': 'volume',     # 누적거래량 (일봉용)
                # 추가 가능한 컬럼명들
                'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'
            }
            
            # 컬럼명 변환
            for old_name, new_name in column_mapping.items():
                if old_name in df.columns and new_name not in df.columns:
                    df[new_name] = df[old_name]
            
            # 필수 컬럼 확인
            required_cols = ['open', 'high', 'low', 'close']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                logger.error(f"실시간 분석용 필수 컬럼 누락: {missing_cols}, 사용 가능한 컬럼: {list(df.columns)}")
                logger.debug(f"컬럼 매핑 시도: {[(k, v) for k, v in column_mapping.items() if k in df.columns]}")
                return pd.DataFrame()
            
            # 데이터 타입 변환 (안전하게)
            for col in required_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
            if 'volume' in df.columns:
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
            else:
                df['volume'] = 0
                
            # NaN 값 제거
            df = df.dropna(subset=required_cols)
            
            if len(df) == 0:
                logger.error("실시간 분석용 유효한 OHLC 데이터가 없습니다")
                return pd.DataFrame()
                
            logger.debug(f"실시간 데이터 전처리 완료: {len(df)}일 데이터")
            return df
            
        except Exception as e:
            logger.error(f"실시간 데이터 전처리 오류: {e}")
            return pd.DataFrame()

    def _analyze_forming_candle(self, stock_code: str, daily_ohlcv: pd.DataFrame, 
                               current_price: float) -> List[CandlePatternInfo]:
        """🕯️ 진행 중인 오늘 캔들 분석"""
        patterns = []
        
        try:
            if len(daily_ohlcv) < 2:
                return patterns

            # 🔧 데이터 전처리 (KIS API 컬럼명 정규화)
            df = self._prepare_realtime_data(daily_ohlcv)
            if df.empty or len(df) < 2:
                return patterns

            # 오늘 캔들 정보 (진행 중)
            today = df.iloc[0].copy()
            yesterday = df.iloc[1]
            
            # 현재가로 업데이트
            today['close'] = current_price
            today['high'] = max(today.get('high', current_price), current_price)
            today['low'] = min(today.get('low', current_price), current_price)
            
            # 실시간 망치형 패턴 감지
            hammer_patterns = self._detect_realtime_hammer(stock_code, today, yesterday)
            patterns.extend(hammer_patterns)

            # 실시간 장악형 패턴 감지
            engulfing_patterns = self._detect_realtime_engulfing(stock_code, today, yesterday)
            patterns.extend(engulfing_patterns)

            return patterns

        except Exception as e:
            logger.error(f"진행중 캔들 분석 오류 ({stock_code}): {e}")
            return []

    def _detect_realtime_hammer(self, stock_code: str, today: pd.Series, 
                               yesterday: pd.Series) -> List[CandlePatternInfo]:
        """🔨 실시간 망치형 패턴 감지"""
        patterns = []
        
        try:
            # 🔧 안전한 데이터 접근
            today_open = float(today.get('open', 0))
            today_close = float(today.get('close', 0))
            today_high = float(today.get('high', 0))
            today_low = float(today.get('low', 0))
            yesterday_open = float(yesterday.get('open', 0))
            yesterday_close = float(yesterday.get('close', 0))
            
            if any(val <= 0 for val in [today_open, today_close, today_high, today_low, yesterday_open, yesterday_close]):
                logger.debug(f"🔨 {stock_code} 실시간 망치형: 유효하지 않은 가격 데이터")
                return patterns
            
            body_size = abs(today_close - today_open)
            total_range = today_high - today_low
            
            if total_range <= 0:
                return patterns
                
            lower_shadow = min(today_open, today_close) - today_low
            lower_shadow_ratio = lower_shadow / total_range
            body_ratio = body_size / total_range
            
            # 실시간 망치형 조건
            if (lower_shadow_ratio >= 0.4 and 
                body_ratio <= 0.6 and 
                yesterday_close < yesterday_open):
                
                confidence = 0.6 + (lower_shadow_ratio * 0.25)
                strength = int(65 + (lower_shadow_ratio * 20))
                
                pattern = CandlePatternInfo(
                    pattern_type=PatternType.HAMMER,
                    confidence=min(confidence, 0.85),
                    strength=min(strength, 85),
                    description=f"실시간 망치형 ({lower_shadow_ratio:.1%})",
                    detected_at=0,
                    target_price_ratio=1.025,
                    stop_loss_ratio=0.98,
                    metadata={
                        'realtime': True,
                        'lower_shadow_ratio': lower_shadow_ratio,
                        'analysis_time': datetime.now().isoformat()
                    }
                )
                
                patterns.append(pattern)
                logger.info(f"🔨 {stock_code} 실시간 망치형 감지!")

            return patterns

        except Exception as e:
            logger.error(f"실시간 망치형 감지 오류 ({stock_code}): {e}")
            return []

    def _detect_realtime_engulfing(self, stock_code: str, today: pd.Series, 
                                  yesterday: pd.Series) -> List[CandlePatternInfo]:
        """📈 실시간 상승장악형 패턴 감지"""
        patterns = []
        
        try:
            # 🔧 안전한 데이터 접근
            today_open = float(today.get('open', 0))
            today_close = float(today.get('close', 0))
            yesterday_open = float(yesterday.get('open', 0))
            yesterday_close = float(yesterday.get('close', 0))
            
            if any(val <= 0 for val in [today_open, today_close, yesterday_open, yesterday_close]):
                logger.debug(f"📈 {stock_code} 실시간 장악형: 유효하지 않은 가격 데이터")
                return patterns
            
            # 기본 조건
            yesterday_bearish = yesterday_close < yesterday_open
            today_bullish = today_close > today_open
            
            if not (yesterday_bearish and today_bullish):
                return patterns
            
            # 크기 비교
            today_body = abs(today_close - today_open)
            yesterday_body = abs(yesterday_open - yesterday_close)
            size_ratio = today_body / yesterday_body if yesterday_body > 0 else 1.0
            
            # 장악 조건
            if (today_open <= yesterday_close * 1.005 and 
                today_close >= yesterday_open * 0.995 and 
                size_ratio >= 0.8):
                
                confidence = 0.65 + (size_ratio * 0.15)
                strength = int(70 + (size_ratio * 15))
                
                pattern = CandlePatternInfo(
                    pattern_type=PatternType.BULLISH_ENGULFING,
                    confidence=min(confidence, 0.9),
                    strength=min(strength, 90),
                    description=f"실시간 상승장악형 ({size_ratio:.2f})",
                    detected_at=0,
                    target_price_ratio=1.03,
                    stop_loss_ratio=0.975,
                    metadata={
                        'realtime': True,
                        'size_ratio': size_ratio,
                        'analysis_time': datetime.now().isoformat()
                    }
                )
                
                patterns.append(pattern)
                logger.info(f"📈 {stock_code} 실시간 상승장악형 감지!")

            return patterns

        except Exception as e:
            logger.error(f"실시간 장악형 감지 오류 ({stock_code}): {e}")
            return []

    def _analyze_minute_patterns(self, stock_code: str, minute_data: pd.DataFrame, 
                               current_price: float) -> List[CandlePatternInfo]:
        """📊 분봉 데이터 기반 패턴 분석"""
        patterns = []
        
        try:
            if minute_data is None or minute_data.empty or len(minute_data) < 5:
                return patterns

            # 거래량 급증 패턴
            volume_patterns = self._detect_volume_surge(stock_code, minute_data)
            patterns.extend(volume_patterns)

            return patterns

        except Exception as e:
            logger.error(f"분봉 패턴 분석 오류 ({stock_code}): {e}")
            return []

    def _detect_volume_surge(self, stock_code: str, minute_data: pd.DataFrame) -> List[CandlePatternInfo]:
        """📊 거래량 급증 패턴"""
        patterns = []
        
        try:
            if len(minute_data) < 5:
                return patterns

            # 🔧 분봉 데이터 전처리
            df = self._prepare_realtime_data(minute_data)
            if df.empty or len(df) < 5:
                return patterns

            # 🔧 안전한 데이터 접근
            recent_volume = float(df.iloc[0].get('volume', 0))
            avg_volume = df['volume'].tail(10).mean()
            volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0
            
            if volume_ratio >= 1.5:  # 1.5배 이상 급증
                recent_price = float(df.iloc[0].get('close', 0))
                prev_price = float(df.iloc[1].get('close', recent_price) if len(df) > 1 else recent_price)
                price_change = (recent_price - prev_price) / prev_price if prev_price > 0 else 0
                
                if price_change > 0.005:  # 0.5% 이상 상승
                    confidence = min(0.7, 0.5 + (volume_ratio - 1.5) * 0.1)
                    strength = int(min(85, 65 + (volume_ratio - 1.5) * 8))
                    
                    pattern = CandlePatternInfo(
                        pattern_type=PatternType.BULLISH_ENGULFING,
                        confidence=confidence,
                        strength=strength,
                        description=f"거래량급증 ({volume_ratio:.1f}배)",
                        detected_at=0,
                        target_price_ratio=1.025,
                        stop_loss_ratio=0.98,
                        metadata={
                            'volume_surge': True,
                            'volume_ratio': volume_ratio,
                            'price_change': price_change,
                            'analysis_time': datetime.now().isoformat()
                        }
                    )
                    
                    patterns.append(pattern)
                    logger.info(f"📊 {stock_code} 거래량 급증 감지! ({volume_ratio:.1f}배)")

            return patterns

        except Exception as e:
            logger.error(f"거래량 급증 감지 오류 ({stock_code}): {e}")
            return []

    def _filter_realtime_patterns(self, patterns: List[CandlePatternInfo], 
                                 current_price: float) -> List[CandlePatternInfo]:
        """🎯 실시간 패턴 필터링"""
        try:
            if not patterns:
                return []

            # 최소 신뢰도 필터링
            min_confidence = self.thresholds['realtime_confidence_min']
            filtered = [p for p in patterns if p.confidence >= min_confidence]
            
            # 신뢰도 순 정렬
            filtered.sort(key=lambda p: p.confidence, reverse=True)
            
            return filtered[:2]  # 최대 2개

        except Exception as e:
            logger.error(f"실시간 패턴 필터링 오류: {e}")
            return patterns[:1] if patterns else []

    def get_realtime_trading_signal(self, patterns: List[CandlePatternInfo]) -> Tuple[TradeSignal, int]:
        """🎯 실시간 매매 신호 생성"""
        try:
            if not patterns:
                return TradeSignal.HOLD, 0

            primary_pattern = max(patterns, key=lambda p: p.strength)
            
            bullish_patterns = {
                PatternType.HAMMER,
                PatternType.BULLISH_ENGULFING,
                PatternType.PIERCING_LINE,
                PatternType.MORNING_STAR
            }
            
            if primary_pattern.pattern_type in bullish_patterns:
                signal_strength = min(primary_pattern.strength, 80)
                return TradeSignal.BUY, signal_strength
            
            return TradeSignal.HOLD, 0

        except Exception as e:
            logger.error(f"실시간 매매 신호 생성 오류: {e}")
            return TradeSignal.HOLD, 0

    def is_trading_time(self) -> bool:
        """거래시간 확인"""
        try:
            current_time = datetime.now().time()
            return (current_time >= datetime.strptime("09:00", "%H:%M").time() and 
                   current_time <= datetime.strptime("15:30", "%H:%M").time())
        except:
            return False 