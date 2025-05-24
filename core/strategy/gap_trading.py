"""
갭 트레이딩 전략
장 시작시 갭(Gap) 발생 종목을 대상으로 하는 전략
주로 09:00-09:30 시간대에 활용
"""
from typing import Optional
import pandas as pd
from datetime import datetime
from .base import BaseStrategy, Signal, MarketData

class GapTradingStrategy(BaseStrategy):
    """갭 트레이딩 전략"""
    
    def __init__(self, config: dict = None):
        """
        갭 트레이딩 전략 초기화
        
        Args:
            config: 전략 설정
                - min_gap_percent: 최소 갭 비율 (기본 3.0%)
                - max_gap_percent: 최대 갭 비율 (기본 15.0%)
                - min_volume_ratio: 최소 거래량 비율 (기본 2.0배)
                - gap_direction: 갭 방향 ('up', 'down', 'both')
        """
        default_config = {
            'min_gap_percent': 3.0,      # 최소 갭 비율
            'max_gap_percent': 15.0,     # 최대 갭 비율 (과도한 갭 제외)
            'min_volume_ratio': 2.0,     # 평균 거래량 대비 최소 비율
            'gap_direction': 'up',       # 상향 갭만 거래
            'stop_loss_percent': 3.0,    # 손절 비율
            'take_profit_percent': 8.0,  # 익절 비율
        }
        
        if config:
            default_config.update(config)
            
        super().__init__("GapTrading", default_config)
    
    def generate_signal(self, market_data: MarketData, historical_data: pd.DataFrame = None) -> Optional[Signal]:
        """
        갭 트레이딩 신호 생성
        
        Args:
            market_data: 현재 시장 데이터
            historical_data: 과거 데이터 (거래량 비교용)
            
        Returns:
            Signal 객체 또는 None
        """
        if not self.is_active:
            return None
        
        gap_percent = market_data.gap_percent
        
        # 갭 방향 확인
        if self.config['gap_direction'] == 'up' and gap_percent <= 0:
            return None
        elif self.config['gap_direction'] == 'down' and gap_percent >= 0:
            return None
        
        gap_abs = abs(gap_percent)
        
        # 갭 크기 확인
        if gap_abs < self.config['min_gap_percent']:
            return None
        
        if gap_abs > self.config['max_gap_percent']:
            return None  # 과도한 갭은 위험
        
        # 거래량 확인
        volume_valid = self._validate_volume(market_data, historical_data)
        if not volume_valid:
            return None
        
        # 신호 강도 계산
        strength = self._calculate_signal_strength(market_data, historical_data)
        
        # 신호 생성
        signal_type = 'BUY' if gap_percent > 0 else 'SELL'
        reason = f"Gap {gap_percent:.2f}% detected with volume surge"
        
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
        if signal.strength < 0.3:  # 최소 신호 강도
            return False
        
        # 가격 유효성 검증
        if market_data.current_price <= 0:
            return False
        
        # 중복 신호 방지 (같은 종목에 대해 5분 내 중복 신호 제거)
        if market_data.stock_code in self.last_signals:
            last_signal = self.last_signals[market_data.stock_code]
            time_diff = (market_data.timestamp - last_signal.timestamp).total_seconds()
            if time_diff < 300:  # 5분
                return False
        
        return True
    
    def _validate_volume(self, market_data: MarketData, historical_data: pd.DataFrame = None) -> bool:
        """
        거래량 검증
        
        Args:
            market_data: 현재 시장 데이터
            historical_data: 과거 데이터
            
        Returns:
            거래량 유효성 여부
        """
        if historical_data is None or len(historical_data) < 5:
            # 과거 데이터가 없으면 기본 거래량 기준 적용
            return market_data.volume > 10000  # 최소 거래량
        
        try:
            # 최근 5일 평균 거래량 계산
            avg_volume = historical_data['volume'].tail(5).mean()
            volume_ratio = market_data.volume / avg_volume if avg_volume > 0 else 0
            
            return volume_ratio >= self.config['min_volume_ratio']
        except:
            return market_data.volume > 10000
    
    def _calculate_signal_strength(self, market_data: MarketData, historical_data: pd.DataFrame = None) -> float:
        """
        신호 강도 계산
        
        Args:
            market_data: 현재 시장 데이터
            historical_data: 과거 데이터
            
        Returns:
            신호 강도 (0.0 ~ 1.0)
        """
        gap_abs = abs(market_data.gap_percent)
        
        # 갭 크기에 따른 기본 강도
        gap_strength = min(gap_abs / 10.0, 1.0)  # 10% 갭이면 최대 강도
        
        # 거래량에 따른 추가 강도
        volume_strength = 0.5  # 기본값
        if historical_data is not None and len(historical_data) >= 5:
            try:
                avg_volume = historical_data['volume'].tail(5).mean()
                if avg_volume > 0:
                    volume_ratio = market_data.volume / avg_volume
                    volume_strength = min(volume_ratio / 5.0, 1.0)  # 5배 이상이면 최대
            except:
                pass
        
        # 가격 위치에 따른 강도 (고가 근처면 강도 증가)
        price_position = 0.5
        if market_data.high_price > market_data.low_price:
            price_position = (market_data.current_price - market_data.low_price) / \
                           (market_data.high_price - market_data.low_price)
        
        # 종합 강도 계산 (가중평균)
        total_strength = (
            gap_strength * 0.5 +        # 갭 크기 50%
            volume_strength * 0.3 +     # 거래량 30%
            price_position * 0.2        # 가격 위치 20%
        )
        
        return min(max(total_strength, 0.0), 1.0)
    
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