"""
모멘텀 전략
가격 추세와 기술적 지표를 활용한 추세 추종 전략
09:30 이후 시간대에서 활용
"""
from typing import Optional
import pandas as pd
import numpy as np
from datetime import datetime
from .base import BaseStrategy, Signal, MarketData

class MomentumStrategy(BaseStrategy):
    """모멘텀 전략"""
    
    def __init__(self, config: dict = None):
        """
        모멘텀 전략 초기화
        
        Args:
            config: 전략 설정
                - short_ma_period: 단기 이동평균 기간 (기본 5)
                - long_ma_period: 장기 이동평균 기간 (기본 20)
                - rsi_period: RSI 기간 (기본 14)
                - min_momentum_percent: 최소 모멘텀 (기본 1.5%)
        """
        default_config = {
            'short_ma_period': 5,           # 단기 이동평균
            'long_ma_period': 20,           # 장기 이동평균
            'rsi_period': 14,               # RSI 기간
            'min_momentum_percent': 1.5,    # 최소 모멘텀 %
            'max_rsi_buy': 70,              # 매수시 최대 RSI
            'min_rsi_sell': 30,             # 매도시 최소 RSI
            'volume_threshold': 1.5,        # 거래량 기준 (평균 대비)
            'stop_loss_percent': 3.5,       # 손절 비율
            'take_profit_percent': 8.0,     # 익절 비율
        }
        
        if config:
            default_config.update(config)
            
        super().__init__("Momentum", default_config)
    
    def generate_signal(self, market_data: MarketData, historical_data: pd.DataFrame = None) -> Optional[Signal]:
        """
        모멘텀 신호 생성
        
        Args:
            market_data: 현재 시장 데이터
            historical_data: 과거 데이터 (필수)
            
        Returns:
            Signal 객체 또는 None
        """
        if not self.is_active:
            return None
        
        if historical_data is None or len(historical_data) < self.config['long_ma_period']:
            return None
        
        # 기술적 지표 계산
        indicators = self._calculate_indicators(historical_data, market_data)
        if not indicators:
            return None
        
        # 모멘텀 확인
        momentum_valid = self._check_momentum(market_data, indicators)
        if not momentum_valid:
            return None
        
        # 거래량 확인
        volume_valid = self._check_volume(market_data, historical_data)
        if not volume_valid:
            return None
        
        # 신호 방향 결정
        signal_type = self._determine_signal_direction(market_data, indicators)
        if not signal_type:
            return None
        
        # 신호 강도 계산
        strength = self._calculate_signal_strength(market_data, indicators, historical_data)
        
        reason = f"Momentum signal (MA: {indicators['ma_signal']}, RSI: {indicators['rsi']:.1f})"
        
        signal = Signal(
            stock_code=market_data.stock_code,
            signal_type=signal_type,
            strength=strength,
            price=market_data.current_price,
            volume=market_data.volume,
            reason=reason,
            timestamp=market_data.timestamp,
            strategy_name=self.name
        )
        
        # 신호 검증
        if self.validate_signal(signal, market_data):
            self.last_signals[market_data.stock_code] = signal
            return signal
        
        return None
    
    def validate_signal(self, signal: Signal, market_data: MarketData) -> bool:
        """
        신호 검증
        
        Args:
            signal: 검증할 신호
            market_data: 현재 시장 데이터
            
        Returns:
            신호 유효성 여부
        """
        # 기본 검증
        if signal.strength < 0.4:  # 최소 신호 강도
            return False
        
        # 가격 유효성 검증
        if market_data.current_price <= 0:
            return False
        
        # 중복 신호 방지 (같은 종목에 대해 15분 내 중복 신호 제거)
        if market_data.stock_code in self.last_signals:
            last_signal = self.last_signals[market_data.stock_code]
            time_diff = (market_data.timestamp - last_signal.timestamp).total_seconds()
            if time_diff < 900:  # 15분
                return False
        
        return True
    
    def _calculate_indicators(self, historical_data: pd.DataFrame, market_data: MarketData) -> dict:
        """
        기술적 지표 계산
        
        Args:
            historical_data: 과거 데이터
            market_data: 현재 시장 데이터
            
        Returns:
            지표 딕셔너리
        """
        try:
            # 현재 가격을 포함한 데이터 생성
            data = historical_data.copy()
            current_row = pd.Series({
                'close': market_data.current_price,
                'high': market_data.high_price,
                'low': market_data.low_price,
                'volume': market_data.volume
            })
            
            # 이동평균 계산
            short_ma = data['close'].tail(self.config['short_ma_period']).mean()
            long_ma = data['close'].tail(self.config['long_ma_period']).mean()
            
            # 이동평균 신호
            ma_signal = 'bullish' if short_ma > long_ma else 'bearish'
            
            # RSI 계산
            rsi = self._calculate_rsi(data['close'], self.config['rsi_period'])
            
            # 가격 모멘텀 계산 (최근 5일 대비 변화율)
            if len(data) >= 5:
                price_momentum = ((market_data.current_price - data['close'].iloc[-5]) / 
                                data['close'].iloc[-5]) * 100
            else:
                price_momentum = market_data.change_percent
            
            return {
                'short_ma': short_ma,
                'long_ma': long_ma,
                'ma_signal': ma_signal,
                'rsi': rsi,
                'price_momentum': price_momentum
            }
        
        except Exception:
            return None
    
    def _calculate_rsi(self, prices: pd.Series, period: int) -> float:
        """RSI 계산"""
        try:
            if len(prices) < period + 1:
                return 50.0  # 기본값
            
            delta = prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            return float(rsi.iloc[-1]) if not np.isnan(rsi.iloc[-1]) else 50.0
        
        except Exception:
            return 50.0
    
    def _check_momentum(self, market_data: MarketData, indicators: dict) -> bool:
        """
        모멘텀 확인
        
        Args:
            market_data: 현재 시장 데이터
            indicators: 기술적 지표
            
        Returns:
            모멘텀 유효성 여부
        """
        # 최소 모멘텀 확인
        momentum = abs(indicators['price_momentum'])
        if momentum < self.config['min_momentum_percent']:
            return False
        
        # 이동평균 정렬 확인
        if indicators['ma_signal'] == 'bearish' and indicators['price_momentum'] > 0:
            return False  # 하락 추세에서 상승 모멘텀은 제외
        
        return True
    
    def _check_volume(self, market_data: MarketData, historical_data: pd.DataFrame) -> bool:
        """
        거래량 확인
        
        Args:
            market_data: 현재 시장 데이터
            historical_data: 과거 데이터
            
        Returns:
            거래량 유효성 여부
        """
        try:
            # 최근 10일 평균 거래량 계산
            avg_volume = historical_data['volume'].tail(10).mean()
            if avg_volume <= 0:
                return market_data.volume > 5000  # 최소 거래량
            
            volume_ratio = market_data.volume / avg_volume
            return volume_ratio >= self.config['volume_threshold']
        
        except Exception:
            return market_data.volume > 5000
    
    def _determine_signal_direction(self, market_data: MarketData, indicators: dict) -> Optional[str]:
        """
        신호 방향 결정
        
        Args:
            market_data: 현재 시장 데이터
            indicators: 기술적 지표
            
        Returns:
            신호 방향 ('BUY', 'SELL', None)
        """
        rsi = indicators['rsi']
        ma_signal = indicators['ma_signal']
        momentum = indicators['price_momentum']
        
        # 매수 신호 조건
        if (ma_signal == 'bullish' and 
            momentum > self.config['min_momentum_percent'] and
            rsi < self.config['max_rsi_buy']):
            return 'BUY'
        
        # 매도 신호 조건 (공매도는 제한적으로 사용)
        if (ma_signal == 'bearish' and 
            momentum < -self.config['min_momentum_percent'] and
            rsi > self.config['min_rsi_sell']):
            return 'SELL'
        
        return None
    
    def _calculate_signal_strength(self, market_data: MarketData, indicators: dict, 
                                 historical_data: pd.DataFrame) -> float:
        """
        신호 강도 계산
        
        Args:
            market_data: 현재 시장 데이터
            indicators: 기술적 지표
            historical_data: 과거 데이터
            
        Returns:
            신호 강도 (0.0 ~ 1.0)
        """
        try:
            # 모멘텀 강도
            momentum_abs = abs(indicators['price_momentum'])
            momentum_strength = min(momentum_abs / 5.0, 1.0)  # 5% 모멘텀이면 최대
            
            # 이동평균 간격 강도
            ma_diff = abs(indicators['short_ma'] - indicators['long_ma'])
            ma_strength = min(ma_diff / indicators['long_ma'] * 50, 1.0) if indicators['long_ma'] > 0 else 0
            
            # RSI 강도 (극값에 가까울수록 강함)
            rsi_strength = min(abs(indicators['rsi'] - 50) / 20, 1.0)
            
            # 거래량 강도
            try:
                avg_volume = historical_data['volume'].tail(10).mean()
                volume_ratio = market_data.volume / avg_volume if avg_volume > 0 else 1
                volume_strength = min(volume_ratio / 3.0, 1.0)  # 3배 이상이면 최대
            except:
                volume_strength = 0.5
            
            # 종합 강도 계산
            total_strength = (
                momentum_strength * 0.3 +   # 모멘텀 30%
                ma_strength * 0.25 +        # 이동평균 25%
                rsi_strength * 0.25 +       # RSI 25%
                volume_strength * 0.2       # 거래량 20%
            )
            
            return min(max(total_strength, 0.0), 1.0)
        
        except Exception:
            return 0.5
    
    def get_stop_loss_price(self, entry_price: float, signal_type: str) -> float:
        """손절가 계산"""
        if signal_type == 'BUY':
            return entry_price * (1 - self.config['stop_loss_percent'] / 100)
        else:
            return entry_price * (1 + self.config['stop_loss_percent'] / 100)
    
    def get_take_profit_price(self, entry_price: float, signal_type: str) -> float:
        """익절가 계산"""
        if signal_type == 'BUY':
            return entry_price * (1 + self.config['take_profit_percent'] / 100)
        else:
            return entry_price * (1 - self.config['take_profit_percent'] / 100)
