"""
거래량 돌파 전략
거래량 급증과 함께 가격 돌파가 발생할 때 신호를 생성하는 전략
모든 시간대에서 활용 가능
"""
from typing import Optional
import pandas as pd
import numpy as np
from datetime import datetime
from .base import BaseStrategy, Signal, MarketData

class VolumeBreakoutStrategy(BaseStrategy):
    """거래량 돌파 전략"""
    
    def __init__(self, config: dict = None):
        """
        거래량 돌파 전략 초기화
        
        Args:
            config: 전략 설정
                - min_volume_ratio: 최소 거래량 비율 (기본 3.0배)
                - price_breakout_percent: 가격 돌파 기준 (기본 2.0%)
                - lookback_period: 돌파 기준 기간 (기본 20일)
                - min_price: 최소 주가 (기본 1000원)
        """
        default_config = {
            'min_volume_ratio': 3.0,        # 평균 거래량 대비 최소 비율
            'price_breakout_percent': 2.0,   # 가격 돌파 기준 %
            'lookback_period': 20,           # 돌파 기준 기간
            'min_price': 1000,               # 최소 주가 (저가주 제외)
            'volume_ma_period': 10,          # 거래량 이동평균 기간
            'stop_loss_percent': 4.0,        # 손절 비율
            'take_profit_percent': 10.0,     # 익절 비율
        }
        
        if config:
            default_config.update(config)
            
        super().__init__("VolumeBreakout", default_config)
    
    def generate_signal(self, market_data: MarketData, historical_data: pd.DataFrame = None) -> Optional[Signal]:
        """
        거래량 돌파 신호 생성
        
        Args:
            market_data: 현재 시장 데이터
            historical_data: 과거 데이터 (필수)
            
        Returns:
            Signal 객체 또는 None
        """
        if not self.is_active:
            return None
        
        if historical_data is None or len(historical_data) < self.config['lookback_period']:
            return None
        
        # 최소 주가 확인
        if market_data.current_price < self.config['min_price']:
            return None
        
        # 거래량 돌파 확인
        volume_breakout = self._check_volume_breakout(market_data, historical_data)
        if not volume_breakout:
            return None
        
        # 가격 돌파 확인
        price_breakout = self._check_price_breakout(market_data, historical_data)
        if not price_breakout:
            return None
        
        # 신호 강도 계산
        strength = self._calculate_signal_strength(market_data, historical_data)
        
        # 신호 방향 결정 (상승 돌파 or 하락 돌파)
        signal_type = self._determine_signal_direction(market_data, historical_data)
        
        reason = f"Volume breakout ({self._get_volume_ratio(market_data, historical_data):.1f}x) with price breakout"
        
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
        
        # 중복 신호 방지 (같은 종목에 대해 10분 내 중복 신호 제거)
        if market_data.stock_code in self.last_signals:
            last_signal = self.last_signals[market_data.stock_code]
            time_diff = (market_data.timestamp - last_signal.timestamp).total_seconds()
            if time_diff < 600:  # 10분
                return False
        
        return True
    
    def _check_volume_breakout(self, market_data: MarketData, historical_data: pd.DataFrame) -> bool:
        """
        거래량 돌파 확인
        
        Args:
            market_data: 현재 시장 데이터
            historical_data: 과거 데이터
            
        Returns:
            거래량 돌파 여부
        """
        try:
            # 거래량 이동평균 계산
            volume_ma = historical_data['volume'].tail(self.config['volume_ma_period']).mean()
            
            if volume_ma <= 0:
                return False
            
            volume_ratio = market_data.volume / volume_ma
            return volume_ratio >= self.config['min_volume_ratio']
        
        except Exception:
            return False
    
    def _check_price_breakout(self, market_data: MarketData, historical_data: pd.DataFrame) -> bool:
        """
        가격 돌파 확인
        
        Args:
            market_data: 현재 시장 데이터
            historical_data: 과거 데이터
            
        Returns:
            가격 돌파 여부
        """
        try:
            # 최근 N일간의 고가/저가 계산
            lookback_data = historical_data.tail(self.config['lookback_period'])
            recent_high = lookback_data['high'].max()
            recent_low = lookback_data['low'].min()
            
            # 상승 돌파 확인
            upward_breakout_price = recent_high * (1 + self.config['price_breakout_percent'] / 100)
            upward_breakout = market_data.current_price >= upward_breakout_price
            
            # 하락 돌파 확인
            downward_breakout_price = recent_low * (1 - self.config['price_breakout_percent'] / 100)
            downward_breakout = market_data.current_price <= downward_breakout_price
            
            return upward_breakout or downward_breakout
        
        except Exception:
            return False
    
    def _determine_signal_direction(self, market_data: MarketData, historical_data: pd.DataFrame) -> str:
        """
        신호 방향 결정
        
        Args:
            market_data: 현재 시장 데이터
            historical_data: 과거 데이터
            
        Returns:
            신호 방향 ('BUY' or 'SELL')
        """
        try:
            # 최근 N일간의 고가/저가 계산
            lookback_data = historical_data.tail(self.config['lookback_period'])
            recent_high = lookback_data['high'].max()
            recent_low = lookback_data['low'].min()
            
            # 상승 돌파인지 확인
            upward_breakout_price = recent_high * (1 + self.config['price_breakout_percent'] / 100)
            if market_data.current_price >= upward_breakout_price:
                return 'BUY'
            
            # 하락 돌파인지 확인
            downward_breakout_price = recent_low * (1 - self.config['price_breakout_percent'] / 100)
            if market_data.current_price <= downward_breakout_price:
                return 'SELL'
            
            # 기본값은 상승 (추세를 따라가는 방향)
            return 'BUY'
        
        except Exception:
            return 'BUY'
    
    def _get_volume_ratio(self, market_data: MarketData, historical_data: pd.DataFrame) -> float:
        """거래량 비율 계산"""
        try:
            volume_ma = historical_data['volume'].tail(self.config['volume_ma_period']).mean()
            if volume_ma <= 0:
                return 0
            return market_data.volume / volume_ma
        except:
            return 0
    
    def _calculate_signal_strength(self, market_data: MarketData, historical_data: pd.DataFrame) -> float:
        """
        신호 강도 계산
        
        Args:
            market_data: 현재 시장 데이터
            historical_data: 과거 데이터
            
        Returns:
            신호 강도 (0.0 ~ 1.0)
        """
        try:
            # 거래량 강도
            volume_ratio = self._get_volume_ratio(market_data, historical_data)
            volume_strength = min(volume_ratio / 10.0, 1.0)  # 10배 이상이면 최대
            
            # 가격 돌파 강도
            lookback_data = historical_data.tail(self.config['lookback_period'])
            recent_high = lookback_data['high'].max()
            recent_low = lookback_data['low'].min()
            price_range = recent_high - recent_low
            
            if price_range > 0:
                # 현재 가격이 범위의 어디에 위치하는지
                price_position = (market_data.current_price - recent_low) / price_range
                # 극단값에 가까울수록 강도 증가
                price_strength = max(price_position, 1 - price_position) * 2
                price_strength = min(price_strength, 1.0)
            else:
                price_strength = 0.5
            
            # 변동률 강도
            change_strength = min(abs(market_data.change_percent) / 5.0, 1.0)  # 5% 변동시 최대
            
            # 종합 강도 계산
            total_strength = (
                volume_strength * 0.4 +     # 거래량 40%
                price_strength * 0.4 +      # 가격 위치 40%
                change_strength * 0.2       # 변동률 20%
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
