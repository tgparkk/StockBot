"""
전략 데이터 어댑터
KIS API 데이터를 기존 전략 인터페이스(MarketData, DataFrame)에 맞게 변환
"""
import pandas as pd
from datetime import datetime
from typing import Optional, List, Any, Dict, Tuple
from core.strategy.base import MarketData
from utils.logger import setup_logger

logger = setup_logger(__name__)

class StrategyDataAdapter:
    """전략 데이터 어댑터"""

    @staticmethod
    def kis_to_market_data(kis_current: Any, timestamp: Optional[datetime] = None) -> MarketData:
        """
        KIS 현재가 데이터를 MarketData로 변환

        Args:
            kis_current: KIS 현재가 정보
            timestamp: 타임스탬프 (없으면 현재 시간)

        Returns:
            MarketData 객체
        """
        if timestamp is None:
            timestamp = now_kst()

        return MarketData(
            stock_code=kis_current.stck_shrn_iscd,
            current_price=float(kis_current.stck_prpr),
            open_price=float(kis_current.stck_oprc),
            high_price=float(kis_current.stck_hgpr),
            low_price=float(kis_current.stck_lwpr),
            volume=kis_current.acml_vol,
            prev_close=float(kis_current.stck_clpr),
            timestamp=timestamp
        )

    @staticmethod
    def kis_historical_to_dataframe(historical_data: List[Any]) -> pd.DataFrame:
        """
        KIS 기간별 데이터를 DataFrame으로 변환

        Args:
            historical_data: KIS 기간별 데이터 리스트

        Returns:
            pandas DataFrame
        """
        if not historical_data:
            return pd.DataFrame()

        data = []
        for item in historical_data:
            data.append({
                'date': pd.to_datetime(item.stck_bsop_date, format='%Y%m%d'),
                'open': item.stck_oprc,
                'high': item.stck_hgpr,
                'low': item.stck_lwpr,
                'close': item.stck_clpr,
                'volume': item.acml_vol,
                'volume_rate': item.prdy_vrss_vol_rate
            })

        df = pd.DataFrame(data)
        df = df.sort_values('date').reset_index(drop=True)
        return df

    @staticmethod
    def gap_trading_to_market_data(gap_data: Any) -> Tuple[MarketData, pd.DataFrame]:
        """
        갭 트레이딩 데이터를 전략용 데이터로 변환

        Args:
            gap_data: 갭 트레이딩 데이터

        Returns:
            (MarketData, DataFrame) 튜플
        """
        # 현재가 정보
        market_data = StrategyDataAdapter.kis_to_market_data(gap_data.current_price)

        # 과거 데이터 (간단한 형태)
        historical_df = pd.DataFrame([{
            'date': now_kst(),
            'close': gap_data.prev_close,
            'volume': gap_data.current_price.acml_vol // gap_data.vol_ratio if gap_data.vol_ratio > 0 else gap_data.current_price.acml_vol
        }])

        return market_data, historical_df

    @staticmethod
    def volume_breakout_to_market_data(volume_data: Any) -> Tuple[MarketData, pd.DataFrame]:
        """
        거래량 돌파 데이터를 전략용 데이터로 변환

        Args:
            volume_data: 거래량 돌파 데이터

        Returns:
            (MarketData, DataFrame) 튜플
        """
        # 현재가 정보
        market_data = StrategyDataAdapter.kis_to_market_data(volume_data.current_price)

        # 과거 데이터
        historical_df = StrategyDataAdapter.kis_historical_to_dataframe(volume_data.historical_data)

        return market_data, historical_df

    @staticmethod
    def momentum_to_market_data(momentum_data: Any) -> Tuple[MarketData, pd.DataFrame]:
        """
        모멘텀 데이터를 전략용 데이터로 변환

        Args:
            momentum_data: 모멘텀 데이터

        Returns:
            (MarketData, DataFrame) 튜플
        """
        # 현재가 정보
        market_data = StrategyDataAdapter.kis_to_market_data(momentum_data.current_price)

        # 과거 데이터
        historical_df = StrategyDataAdapter.kis_historical_to_dataframe(momentum_data.historical_data)

        # 기술적 지표 추가
        if not historical_df.empty:
            historical_df['ma_5'] = momentum_data.ma_5
            historical_df['ma_20'] = momentum_data.ma_20
            historical_df['ma_60'] = momentum_data.ma_60
            historical_df['rsi'] = momentum_data.rsi_9
            historical_df['macd'] = momentum_data.macd_line
            historical_df['macd_signal'] = momentum_data.macd_signal

        return market_data, historical_df

    @staticmethod
    def create_enhanced_gap_data(gap_data: Any) -> Dict:
        """
        갭 트레이딩용 강화된 데이터 생성

        Args:
            gap_data: 갭 트레이딩 데이터

        Returns:
            강화된 데이터 딕셔너리
        """
        return {
            'gap_size': gap_data.gap_size,
            'gap_direction': gap_data.gap_direction,
            'volume_ratio': gap_data.vol_ratio,
            'first_10min_volume': gap_data.first_10min_vol,
            'current_price': gap_data.current_price.stck_prpr,
            'open_price': gap_data.current_price.stck_oprc,
            'prev_close': gap_data.prev_close,
            'is_gap_up': gap_data.gap_direction == 'UP',
            'is_volume_surge': gap_data.vol_ratio >= 2.0,  # 2배 이상
            'gap_strength': abs(gap_data.gap_size) / 10.0,  # 정규화된 갭 강도
        }

    @staticmethod
    def create_enhanced_volume_data(volume_data: Any) -> Dict:
        """
        거래량 돌파용 강화된 데이터 생성

        Args:
            volume_data: 거래량 돌파 데이터

        Returns:
            강화된 데이터 딕셔너리
        """
        return {
            'volume_ratio': volume_data.vol_ratio,
            'avg_volume_20d': volume_data.avg_vol_20d,
            'resistance_level': volume_data.resistance_level,
            'support_level': volume_data.support_level,
            'breakout_point': volume_data.breakout_point,
            'buying_power': volume_data.buying_power,
            'bid_ask_ratio': volume_data.order_book.bid_ask_ratio,
            'is_volume_breakout': volume_data.vol_ratio >= 3.0,  # 3배 이상
            'is_price_breakout': volume_data.breakout_point is not None,
            'breakout_direction': 'UP' if volume_data.breakout_point and volume_data.current_price.stck_prpr > volume_data.breakout_point else 'DOWN',
            'volume_strength': min(volume_data.vol_ratio / 10.0, 1.0),  # 정규화된 거래량 강도
        }

    @staticmethod
    def create_enhanced_momentum_data(momentum_data: Any) -> Dict:
        """
        모멘텀용 강화된 데이터 생성

        Args:
            momentum_data: 모멘텀 데이터

        Returns:
            강화된 데이터 딕셔너리
        """
        return {
            'ma_5': momentum_data.ma_5,
            'ma_20': momentum_data.ma_20,
            'ma_60': momentum_data.ma_60,
            'rsi_9': momentum_data.rsi_9,
            'macd_line': momentum_data.macd_line,
            'macd_signal': momentum_data.macd_signal,
            'macd_histogram': momentum_data.macd_histogram,
            'return_1d': momentum_data.return_1d,
            'return_5d': momentum_data.return_5d,
            'trend_strength': momentum_data.trend_strength,
            'is_bullish_ma': momentum_data.ma_5 > momentum_data.ma_20 > momentum_data.ma_60,
            'is_bearish_ma': momentum_data.ma_5 < momentum_data.ma_20 < momentum_data.ma_60,
            'is_rsi_oversold': momentum_data.rsi_9 < 30,
            'is_rsi_overbought': momentum_data.rsi_9 > 70,
            'is_macd_bullish': momentum_data.macd_line > momentum_data.macd_signal,
            'momentum_score': (momentum_data.return_5d + momentum_data.trend_strength) / 2,
        }

class KISDataValidator:
    """KIS 데이터 검증 클래스"""

    @staticmethod
    def validate_current_price(data: Any) -> bool:
        """현재가 데이터 검증"""
        if not data:
            return False

        # 기본 필수 필드 확인
        if data.stck_prpr <= 0:
            logger.warning(f"현재가가 0 이하: {data.stck_prpr}")
            return False

        if data.acml_vol < 0:
            logger.warning(f"거래량이 음수: {data.acml_vol}")
            return False

        # 가격 범위 검증 (상한가/하한가 확인)
        if data.stck_clpr > 0:
            change_rate = abs((data.stck_prpr - data.stck_clpr) / data.stck_clpr)
            if change_rate > 0.3:  # 30% 이상 변동은 비정상
                logger.warning(f"비정상적인 가격 변동: {change_rate:.2%}")
                return False

        return True

    @staticmethod
    def validate_historical_data(data: List[Any]) -> bool:
        """기간별 데이터 검증"""
        if not data:
            return False

        for item in data:
            if item.stck_clpr <= 0:
                logger.warning(f"종가가 0 이하: {item.stck_bsop_date}")
                return False

            if item.acml_vol < 0:
                logger.warning(f"거래량이 음수: {item.stck_bsop_date}")
                return False

        return True

    @staticmethod
    def validate_order_book(data: Any) -> bool:
        """호가 데이터 검증"""
        if not data:
            return False

        # 호가 개수 확인
        if len(data.askp) != 10 or len(data.bidp) != 10:
            logger.warning("호가 개수가 10개가 아님")
            return False

        # 총 잔량과 개별 잔량 합계 확인
        total_ask = sum(data.askp_rsqn)
        total_bid = sum(data.bidp_rsqn)

        if abs(total_ask - data.total_askp_rsqn) > 100:  # 100주 오차 허용
            logger.warning(f"매도 잔량 불일치: {total_ask} vs {data.total_askp_rsqn}")

        if abs(total_bid - data.total_bidp_rsqn) > 100:
            logger.warning(f"매수 잔량 불일치: {total_bid} vs {data.total_bidp_rsqn}")

        return True
