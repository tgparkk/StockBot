"""
기술적 지표 계산 모듈
RSI, MACD, 볼린저 밴드, 스토캐스틱 등 단기거래에 필요한 지표들
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from utils.logger import setup_logger

logger = setup_logger(__name__)

class TechnicalIndicators:
    """기술적 지표 계산기"""

    @staticmethod
    def calculate_rsi(prices: List[float], period: int = 14) -> List[float]:
        """RSI(상대강도지수) 계산"""
        try:
            if len(prices) < period + 1:
                return [50.0] * len(prices)  # 기본값

            df = pd.DataFrame({'price': prices})
            df['change'] = df['price'].diff()
            df['gain'] = df['change'].where(df['change'] > 0, 0)
            df['loss'] = -df['change'].where(df['change'] < 0, 0)

            # 초기 평균
            df['avg_gain'] = df['gain'].rolling(window=period, min_periods=period).mean()
            df['avg_loss'] = df['loss'].rolling(window=period, min_periods=period).mean()

            # 이후 평균 (지수 이동 평균 방식)
            for i in range(period, len(df)):
                if i == period:
                    continue
                df.loc[i, 'avg_gain'] = (df.loc[i-1, 'avg_gain'] * (period-1) + df.loc[i, 'gain']) / period
                df.loc[i, 'avg_loss'] = (df.loc[i-1, 'avg_loss'] * (period-1) + df.loc[i, 'loss']) / period

            # RSI 계산
            df['rs'] = df['avg_gain'] / df['avg_loss']
            df['rsi'] = 100 - (100 / (1 + df['rs']))

            # NaN 값을 50으로 대체
            rsi_values = df['rsi'].fillna(50.0).tolist()
            return rsi_values

        except Exception as e:
            logger.error(f"RSI 계산 오류: {e}")
            return [50.0] * len(prices)

    @staticmethod
    def calculate_macd(prices: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, List[float]]:
        """MACD 계산"""
        try:
            if len(prices) < slow:
                null_values = [0.0] * len(prices)
                return {
                    'macd': null_values,
                    'signal': null_values,
                    'histogram': null_values
                }

            df = pd.DataFrame({'price': prices})

            # EMA 계산
            df['ema_fast'] = df['price'].ewm(span=fast).mean()
            df['ema_slow'] = df['price'].ewm(span=slow).mean()

            # MACD 라인
            df['macd'] = df['ema_fast'] - df['ema_slow']

            # 시그널 라인
            df['signal'] = df['macd'].ewm(span=signal).mean()

            # 히스토그램
            df['histogram'] = df['macd'] - df['signal']

            return {
                'macd': df['macd'].fillna(0.0).tolist(),
                'signal': df['signal'].fillna(0.0).tolist(),
                'histogram': df['histogram'].fillna(0.0).tolist()
            }

        except Exception as e:
            logger.error(f"MACD 계산 오류: {e}")
            null_values = [0.0] * len(prices)
            return {
                'macd': null_values,
                'signal': null_values,
                'histogram': null_values
            }

    @staticmethod
    def calculate_bollinger_bands(prices: List[float], period: int = 20, std_dev: float = 2.0) -> Dict[str, List[float]]:
        """볼린저 밴드 계산"""
        try:
            if len(prices) < period:
                return {
                    'upper': prices.copy(),
                    'middle': prices.copy(),
                    'lower': prices.copy(),
                    'bandwidth': [0.0] * len(prices)
                }

            df = pd.DataFrame({'price': prices})

            # 중간선 (이동평균)
            df['middle'] = df['price'].rolling(window=period).mean()

            # 표준편차
            df['std'] = df['price'].rolling(window=period).std()

            # 상/하한선
            df['upper'] = df['middle'] + (df['std'] * std_dev)
            df['lower'] = df['middle'] - (df['std'] * std_dev)

            # 밴드폭 (%B)
            df['bandwidth'] = (df['price'] - df['lower']) / (df['upper'] - df['lower']) * 100

            return {
                'upper': df['upper'].bfill().tolist(),
                'middle': df['middle'].bfill().tolist(),
                'lower': df['lower'].bfill().tolist(),
                'bandwidth': df['bandwidth'].fillna(50.0).tolist()
            }

        except Exception as e:
            logger.error(f"볼린저 밴드 계산 오류: {e}")
            return {
                'upper': prices.copy(),
                'middle': prices.copy(),
                'lower': prices.copy(),
                'bandwidth': [50.0] * len(prices)
            }

    @staticmethod
    def calculate_stochastic(highs: List[float], lows: List[float], closes: List[float],
                           k_period: int = 14, d_period: int = 3) -> Dict[str, List[float]]:
        """스토캐스틱 계산"""
        try:
            if len(closes) < k_period:
                null_values = [50.0] * len(closes)
                return {
                    'k': null_values,
                    'd': null_values
                }

            df = pd.DataFrame({
                'high': highs,
                'low': lows,
                'close': closes
            })

            # 최고/최저가 (K 기간)
            df['highest_high'] = df['high'].rolling(window=k_period).max()
            df['lowest_low'] = df['low'].rolling(window=k_period).min()

            # %K 계산
            df['k'] = ((df['close'] - df['lowest_low']) /
                      (df['highest_high'] - df['lowest_low'])) * 100

            # %D 계산 (%K의 이동평균)
            df['d'] = df['k'].rolling(window=d_period).mean()

            return {
                'k': df['k'].fillna(50.0).tolist(),
                'd': df['d'].fillna(50.0).tolist()
            }

        except Exception as e:
            logger.error(f"스토캐스틱 계산 오류: {e}")
            null_values = [50.0] * len(closes)
            return {
                'k': null_values,
                'd': null_values
            }

    @staticmethod
    def calculate_moving_averages(prices: List[float], periods: List[int] = [5, 20, 60]) -> Dict[str, List[float]]:
        """이동평균선들 계산"""
        try:
            df = pd.DataFrame({'price': prices})
            result = {}

            for period in periods:
                if len(prices) >= period:
                    ma = df['price'].rolling(window=period).mean()
                    result[f'ma_{period}'] = ma.bfill().tolist()
                else:
                    result[f'ma_{period}'] = prices.copy()

            return result

        except Exception as e:
            logger.error(f"이동평균 계산 오류: {e}")
            result = {}
            for period in periods:
                result[f'ma_{period}'] = prices.copy()
            return result

    @staticmethod
    def calculate_support_resistance(prices: List[float], window: int = 5) -> Dict[str, float]:
        """지지/저항선 계산 (최근 데이터 기반)"""
        try:
            if len(prices) < window * 2:
                recent_prices = prices
            else:
                recent_prices = prices[-window*2:]

            # 지지선: 최근 최저가
            support = min(recent_prices)

            # 저항선: 최근 최고가
            resistance = max(recent_prices)

            # 현재가
            current_price = prices[-1] if prices else 0

            # 지지/저항선 대비 위치 (%)
            if resistance > support:
                position_pct = (current_price - support) / (resistance - support) * 100
            else:
                position_pct = 50.0

            return {
                'support': support,
                'resistance': resistance,
                'current_price': current_price,
                'position_pct': position_pct
            }

        except Exception as e:
            logger.error(f"지지/저항선 계산 오류: {e}")
            current_price = prices[-1] if prices else 0
            return {
                'support': current_price,
                'resistance': current_price,
                'current_price': current_price,
                'position_pct': 50.0
            }

    @classmethod
    def analyze_all_indicators(cls, price_data: List[Dict]) -> Dict:
        """모든 지표 종합 분석"""
        try:
            if not price_data or len(price_data) < 5:
                return cls._get_default_analysis()

            # 가격 데이터 추출
            closes = [float(d.get('stck_clpr', 0)) for d in price_data]
            highs = [float(d.get('stck_hgpr', 0)) for d in price_data]
            lows = [float(d.get('stck_lwpr', 0)) for d in price_data]
            volumes = [int(d.get('acml_vol', 0)) for d in price_data]

            # 각 지표 계산
            rsi_values = cls.calculate_rsi(closes)
            macd_data = cls.calculate_macd(closes)
            bb_data = cls.calculate_bollinger_bands(closes)
            stoch_data = cls.calculate_stochastic(highs, lows, closes)
            ma_data = cls.calculate_moving_averages(closes)
            sr_data = cls.calculate_support_resistance(closes)

            # 현재 값들 (최신)
            current_rsi = rsi_values[-1] if rsi_values else 50.0
            current_macd = macd_data['macd'][-1] if macd_data['macd'] else 0.0
            current_signal = macd_data['signal'][-1] if macd_data['signal'] else 0.0
            current_k = stoch_data['k'][-1] if stoch_data['k'] else 50.0
            current_d = stoch_data['d'][-1] if stoch_data['d'] else 50.0
            current_price = closes[-1] if closes else 0

            # 신호 생성
            signals = cls._generate_signals(
                current_rsi, current_macd, current_signal,
                current_k, current_d, current_price,
                bb_data, ma_data, sr_data
            )

            return {
                'indicators': {
                    'rsi': current_rsi,
                    'macd': current_macd,
                    'macd_signal': current_signal,
                    'macd_histogram': current_macd - current_signal,
                    'stoch_k': current_k,
                    'stoch_d': current_d,
                    'bb_position': bb_data['bandwidth'][-1] if bb_data['bandwidth'] else 50.0,
                    'ma_5': ma_data.get('ma_5', [current_price])[-1],
                    'ma_20': ma_data.get('ma_20', [current_price])[-1],
                    'support': sr_data['support'],
                    'resistance': sr_data['resistance']
                },
                'signals': signals,
                'overall_score': cls._calculate_overall_score(signals),
                'timestamp': price_data[-1].get('stck_bsop_date', '') if price_data else ''
            }

        except Exception as e:
            logger.error(f"종합 지표 분석 오류: {e}")
            return cls._get_default_analysis()

    @staticmethod
    def _generate_signals(rsi: float, macd: float, signal: float,
                         stoch_k: float, stoch_d: float, current_price: float,
                         bb_data: Dict, ma_data: Dict, sr_data: Dict) -> Dict[str, str]:
        """기술적 지표 기반 신호 생성"""
        signals = {}

        # RSI 신호
        if rsi < 30:
            signals['rsi'] = 'oversold'  # 과매도
        elif rsi > 70:
            signals['rsi'] = 'overbought'  # 과매수
        else:
            signals['rsi'] = 'neutral'

        # MACD 신호
        if macd > signal and macd > 0:
            signals['macd'] = 'bullish'  # 강세
        elif macd < signal and macd < 0:
            signals['macd'] = 'bearish'  # 약세
        else:
            signals['macd'] = 'neutral'

        # 스토캐스틱 신호
        if stoch_k < 20 and stoch_d < 20:
            signals['stochastic'] = 'oversold'
        elif stoch_k > 80 and stoch_d > 80:
            signals['stochastic'] = 'overbought'
        elif stoch_k > stoch_d:
            signals['stochastic'] = 'bullish_cross'
        else:
            signals['stochastic'] = 'neutral'

        # 볼린저 밴드 신호
        bb_position = bb_data['bandwidth'][-1] if bb_data['bandwidth'] else 50.0
        if bb_position < 20:
            signals['bollinger'] = 'oversold'
        elif bb_position > 80:
            signals['bollinger'] = 'overbought'
        else:
            signals['bollinger'] = 'neutral'

        # 이동평균 신호
        ma_5 = ma_data.get('ma_5', [current_price])[-1]
        ma_20 = ma_data.get('ma_20', [current_price])[-1]

        if current_price > ma_5 > ma_20:
            signals['moving_average'] = 'bullish'
        elif current_price < ma_5 < ma_20:
            signals['moving_average'] = 'bearish'
        else:
            signals['moving_average'] = 'neutral'

        # 지지/저항 신호
        support = sr_data['support']
        resistance = sr_data['resistance']
        position_pct = sr_data['position_pct']

        if position_pct < 25:
            signals['support_resistance'] = 'near_support'
        elif position_pct > 75:
            signals['support_resistance'] = 'near_resistance'
        else:
            signals['support_resistance'] = 'neutral'

        return signals

    @staticmethod
    def _calculate_overall_score(signals: Dict[str, str]) -> int:
        """종합 점수 계산 (0-100)"""
        try:
            bullish_signals = 0
            bearish_signals = 0
            total_signals = 0

            for indicator, signal in signals.items():
                total_signals += 1

                if signal in ['bullish', 'oversold', 'bullish_cross', 'near_support']:
                    bullish_signals += 1
                elif signal in ['bearish', 'overbought', 'near_resistance']:
                    bearish_signals += 1

            if total_signals == 0:
                return 50

            # 강세 비율을 점수로 변환 (0-100)
            bullish_ratio = bullish_signals / total_signals
            score = int(bullish_ratio * 100)

            return score

        except Exception as e:
            logger.error(f"종합 점수 계산 오류: {e}")
            return 50

    @staticmethod
    def _get_default_analysis() -> Dict:
        """기본 분석 결과"""
        return {
            'indicators': {
                'rsi': 50.0,
                'macd': 0.0,
                'macd_signal': 0.0,
                'macd_histogram': 0.0,
                'stoch_k': 50.0,
                'stoch_d': 50.0,
                'bb_position': 50.0,
                'ma_5': 0.0,
                'ma_20': 0.0,
                'support': 0.0,
                'resistance': 0.0
            },
            'signals': {
                'rsi': 'neutral',
                'macd': 'neutral',
                'stochastic': 'neutral',
                'bollinger': 'neutral',
                'moving_average': 'neutral',
                'support_resistance': 'neutral'
            },
            'overall_score': 50,
            'timestamp': ''
        }

    @classmethod
    def get_quick_signals(cls, price_data: List[Dict]) -> Dict[str, any]:
        """빠른 신호 분석 (단기거래용)"""
        try:
            if not price_data or len(price_data) < 3:
                return {'action': 'HOLD', 'strength': 0, 'reason': '데이터 부족'}

            # 최근 3개 데이터만 사용 (빠른 분석)
            recent_data = price_data[-3:]
            closes = [float(d.get('stck_clpr', 0)) for d in recent_data]
            volumes = [int(d.get('acml_vol', 0)) for d in recent_data]

            current_price = closes[-1]
            prev_price = closes[-2] if len(closes) > 1 else current_price
            change_rate = ((current_price - prev_price) / prev_price * 100) if prev_price > 0 else 0

            # 간단한 RSI (3기간)
            simple_rsi = cls.calculate_rsi(closes, period=min(3, len(closes)))[-1]

            # 빠른 판단
            if change_rate > 2.0 and simple_rsi < 70:
                return {
                    'action': 'BUY',
                    'strength': min(int(change_rate * 10), 100),
                    'reason': f'상승 {change_rate:.1f}%, RSI {simple_rsi:.0f}'
                }
            elif change_rate < -1.5 and simple_rsi > 30:
                return {
                    'action': 'SELL',
                    'strength': min(int(abs(change_rate) * 10), 100),
                    'reason': f'하락 {change_rate:.1f}%, RSI {simple_rsi:.0f}'
                }
            else:
                return {
                    'action': 'HOLD',
                    'strength': 0,
                    'reason': f'변화율 {change_rate:.1f}%, RSI {simple_rsi:.0f}'
                }

        except Exception as e:
            logger.error(f"빠른 신호 분석 오류: {e}")
            return {'action': 'HOLD', 'strength': 0, 'reason': '분석 오류'}


# 편의 함수들
def get_rsi(prices: List[float], period: int = 14) -> float:
    """RSI 현재값만 반환"""
    rsi_values = TechnicalIndicators.calculate_rsi(prices, period)
    return rsi_values[-1] if rsi_values else 50.0

def get_macd_signal(prices: List[float]) -> str:
    """MACD 신호만 반환"""
    macd_data = TechnicalIndicators.calculate_macd(prices)
    macd = macd_data['macd'][-1] if macd_data['macd'] else 0.0
    signal = macd_data['signal'][-1] if macd_data['signal'] else 0.0

    if macd > signal:
        return 'BUY'
    elif macd < signal:
        return 'SELL'
    else:
        return 'HOLD'

def is_oversold(prices: List[float]) -> bool:
    """과매도 상태 확인"""
    rsi = get_rsi(prices)
    return rsi < 30

def is_overbought(prices: List[float]) -> bool:
    """과매수 상태 확인"""
    rsi = get_rsi(prices)
    return rsi > 70
