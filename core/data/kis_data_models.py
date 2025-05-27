"""
KIS API 데이터 모델
한국투자증권 API 응답 데이터 구조 정의
"""
from dataclasses import dataclass
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd

@dataclass
class KISCurrentPrice:
    """현재가 정보 (FHKST01010100)"""
    stck_shrn_iscd: str      # 주식단축종목코드
    stck_prpr: int           # 주식현재가
    prdy_vrss: int           # 전일대비
    prdy_vrss_sign: str      # 전일대비부호
    prdy_ctrt: float         # 전일대비율
    stck_oprc: int           # 주식시가
    stck_hgpr: int           # 주식최고가
    stck_lwpr: int           # 주식최저가
    stck_clpr: int           # 주식종가 (전일)
    acml_vol: int            # 누적거래량
    acml_tr_pbmn: int        # 누적거래대금
    seln_cntg_qty: int       # 매도체결량
    shnu_cntg_qty: int       # 매수체결량
    ntby_cntg_qty: int       # 순매수체결량
    stck_cntg_hour: str      # 주식체결시간

    @property
    def gap_percent(self) -> float:
        """갭 비율 계산"""
        if self.stck_clpr == 0:
            return 0.0
        return ((self.stck_oprc - self.stck_clpr) / self.stck_clpr) * 100

    @property
    def change_percent(self) -> float:
        """현재가 변동률"""
        return self.prdy_ctrt

@dataclass
class KISHistoricalData:
    """기간별 시세 정보 (FHKST03010100)"""
    stck_bsop_date: str      # 주식영업일자
    stck_oprc: int           # 주식시가
    stck_hgpr: int           # 주식최고가
    stck_lwpr: int           # 주식최저가
    stck_clpr: int           # 주식종가
    acml_vol: int            # 누적거래량
    prdy_vrss_vol_rate: float # 전일대비거래량비율

@dataclass
class KISOrderBook:
    """호가 정보 (FHKST01010200)"""
    askp_rsqn: List[int]     # 매도호가 잔량 (1~10호가)
    bidp_rsqn: List[int]     # 매수호가 잔량 (1~10호가)
    askp: List[int]          # 매도호가 (1~10호가)
    bidp: List[int]          # 매수호가 (1~10호가)
    total_askp_rsqn: int     # 총 매도호가 잔량
    total_bidp_rsqn: int     # 총 매수호가 잔량

    @property
    def bid_ask_ratio(self) -> float:
        """매수/매도 잔량 비율"""
        if self.total_askp_rsqn == 0:
            return float('inf')
        return self.total_bidp_rsqn / self.total_askp_rsqn

@dataclass
class KISMinuteData:
    """분봉 데이터 (FHKST03010200)"""
    stck_bsop_date: str      # 주식영업일자
    stck_cntg_hour: str      # 주식체결시간
    stck_oprc: int           # 주식시가
    stck_hgpr: int           # 주식최고가
    stck_lwpr: int           # 주식최저가
    stck_clpr: int           # 주식종가
    cntg_vol: int            # 체결거래량

@dataclass
class GapTradingData:
    """갭 트레이딩 전략용 데이터"""
    current_price: KISCurrentPrice
    prev_close: int
    gap_size: float
    gap_direction: str       # 'UP' or 'DOWN'
    vol_ratio: float         # 현재거래량/전일거래량
    first_10min_vol: int     # 시가 후 10분간 거래량

    @classmethod
    def from_kis_data(cls, current: KISCurrentPrice, historical: List[KISHistoricalData]):
        """KIS API 데이터로부터 생성"""
        if not historical:
            return None

        prev_close = historical[-1].stck_clpr if historical else current.stck_clpr
        gap_size = ((current.stck_oprc - prev_close) / prev_close) * 100 if prev_close > 0 else 0
        gap_direction = 'UP' if gap_size > 0 else 'DOWN'

        # 전일 거래량 (가장 최근 데이터)
        prev_vol = historical[-1].acml_vol if historical else 1
        vol_ratio = current.acml_vol / prev_vol if prev_vol > 0 else 0

        return cls(
            current_price=current,
            prev_close=prev_close,
            gap_size=gap_size,
            gap_direction=gap_direction,
            vol_ratio=vol_ratio,
            first_10min_vol=current.acml_vol  # 실제로는 10분 데이터 필요
        )

@dataclass
class VolumeBreakoutData:
    """거래량 돌파 전략용 데이터"""
    current_price: KISCurrentPrice
    historical_data: List[KISHistoricalData]
    order_book: KISOrderBook
    avg_vol_20d: float
    vol_ratio: float
    resistance_level: int    # 5일 최고가
    support_level: int       # 5일 최저가
    breakout_point: Optional[int]
    buying_power: float      # 매수세 강도

    @classmethod
    def from_kis_data(cls, current: KISCurrentPrice, historical: List[KISHistoricalData],
                     order_book: KISOrderBook):
        """KIS API 데이터로부터 생성"""
        if len(historical) < 20:
            return None

        # 20일 평균 거래량
        avg_vol_20d = sum(h.acml_vol for h in historical[-20:]) / 20
        vol_ratio = current.acml_vol / avg_vol_20d if avg_vol_20d > 0 else 0

        # 5일 고저가
        recent_5d = historical[-5:] if len(historical) >= 5 else historical
        resistance_level = max(h.stck_hgpr for h in recent_5d) if recent_5d else current.stck_hgpr
        support_level = min(h.stck_lwpr for h in recent_5d) if recent_5d else current.stck_lwpr

        # 돌파 지점 확인 (2% 이상 돌파)
        breakout_point = None
        if current.stck_prpr >= resistance_level * 1.02:
            breakout_point = resistance_level
        elif current.stck_prpr <= support_level * 0.98:
            breakout_point = support_level

        # 매수세 강도 (순매수 / 총거래량)
        buying_power = current.ntby_cntg_qty / current.acml_vol if current.acml_vol > 0 else 0

        return cls(
            current_price=current,
            historical_data=historical,
            order_book=order_book,
            avg_vol_20d=avg_vol_20d,
            vol_ratio=vol_ratio,
            resistance_level=resistance_level,
            support_level=support_level,
            breakout_point=breakout_point,
            buying_power=buying_power
        )

@dataclass
class MomentumData:
    """모멘텀 전략용 데이터"""
    current_price: KISCurrentPrice
    historical_data: List[KISHistoricalData]
    minute_data: List[KISMinuteData]
    ma_5: float
    ma_20: float
    ma_60: float
    rsi_9: float
    macd_line: float
    macd_signal: float
    macd_histogram: float
    return_1d: float
    return_5d: float
    trend_strength: float

    @classmethod
    def from_kis_data(cls, current: KISCurrentPrice, historical: List[KISHistoricalData],
                     minute_data: List[KISMinuteData]):
        """KIS API 데이터로부터 생성"""
        if len(historical) < 60:
            return None

        # 이동평균 계산 (유효한 데이터만 사용)
        closes = [h.stck_clpr for h in historical if h.stck_clpr > 0]

        if not closes:
            return None

        ma_5 = sum(closes[-5:]) / min(5, len(closes)) if len(closes) >= 1 else current.stck_prpr
        ma_20 = sum(closes[-20:]) / min(20, len(closes)) if len(closes) >= 1 else current.stck_prpr
        ma_60 = sum(closes[-60:]) / min(60, len(closes)) if len(closes) >= 1 else current.stck_prpr

        # RSI 계산 (9일)
        rsi_9 = cls._calculate_rsi(closes, 9)

        # MACD 계산 (5,13,5)
        macd_line, macd_signal, macd_histogram = cls._calculate_macd(closes, 5, 13, 5)

        # 수익률 계산 (0으로 나누기 방지)
        return_1d = 0.0
        if closes and len(closes) > 0 and closes[-1] > 0:
            return_1d = ((current.stck_prpr - closes[-1]) / closes[-1]) * 100

        return_5d = 0.0
        if len(closes) >= 5 and closes[-5] > 0:
            return_5d = ((current.stck_prpr - closes[-5]) / closes[-5]) * 100

        # 추세 강도 (단순화)
        trend_strength = abs(return_5d) / 5  # 5일 평균 변동률

        return cls(
            current_price=current,
            historical_data=historical,
            minute_data=minute_data,
            ma_5=ma_5,
            ma_20=ma_20,
            ma_60=ma_60,
            rsi_9=rsi_9,
            macd_line=macd_line,
            macd_signal=macd_signal,
            macd_histogram=macd_histogram,
            return_1d=return_1d,
            return_5d=return_5d,
            trend_strength=trend_strength
        )

    @staticmethod
    def _calculate_rsi(prices: List[int], period: int) -> float:
        """RSI 계산"""
        if len(prices) < period + 1:
            return 50.0

        gains = []
        losses = []

        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(-change)

        if len(gains) < period:
            return 50.0

        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return rsi

    @staticmethod
    def _calculate_macd(prices: List[int], fast: int, slow: int, signal: int) -> tuple:
        """MACD 계산 (0으로 나누기 방지)"""
        if len(prices) < slow or not prices:
            return 0.0, 0.0, 0.0

        try:
            # 유효한 가격만 필터링
            valid_prices = [p for p in prices if p > 0]
            if len(valid_prices) < slow:
                return 0.0, 0.0, 0.0

            # 간단한 MACD 계산 (실제로는 EMA 사용)
            fast_period = min(fast, len(valid_prices))
            slow_period = min(slow, len(valid_prices))

            fast_ma = sum(valid_prices[-fast_period:]) / fast_period
            slow_ma = sum(valid_prices[-slow_period:]) / slow_period

            macd_line = fast_ma - slow_ma

            # Signal line (단순화)
            macd_signal = macd_line * 0.9  # 실제로는 EMA 계산
            macd_histogram = macd_line - macd_signal

            return macd_line, macd_signal, macd_histogram

        except (ZeroDivisionError, ValueError):
            return 0.0, 0.0, 0.0
