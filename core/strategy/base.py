"""
전략 기본 클래스
모든 매매 전략의 베이스가 되는 추상 클래스
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import pandas as pd
from dataclasses import dataclass

@dataclass
class Signal:
    """매매 신호 클래스"""
    stock_code: str
    signal_type: str  # 'BUY', 'SELL', 'HOLD'
    strength: float   # 신호 강도 (0.0 ~ 1.0)
    price: float      # 신호 발생 가격
    volume: int       # 거래량
    reason: str       # 신호 발생 이유
    timestamp: datetime
    strategy_name: str
    
    def __post_init__(self):
        """신호 강도 범위 검증"""
        if not 0.0 <= self.strength <= 1.0:
            raise ValueError(f"Signal strength must be between 0.0 and 1.0, got {self.strength}")

@dataclass
class MarketData:
    """시장 데이터 클래스"""
    stock_code: str
    current_price: float
    open_price: float
    high_price: float
    low_price: float
    volume: int
    prev_close: float
    timestamp: datetime
    
    @property
    def gap_percent(self) -> float:
        """갭 비율 계산"""
        if self.prev_close == 0:
            return 0.0
        return ((self.open_price - self.prev_close) / self.prev_close) * 100
    
    @property
    def change_percent(self) -> float:
        """변동률 계산"""
        if self.prev_close == 0:
            return 0.0
        return ((self.current_price - self.prev_close) / self.prev_close) * 100

class BaseStrategy(ABC):
    """
    전략 기본 클래스
    모든 매매 전략이 상속받아야 하는 추상 클래스
    """
    
    def __init__(self, name: str, config: Dict = None):
        """
        전략 초기화
        
        Args:
            name: 전략 이름
            config: 전략 설정 딕셔너리
        """
        self.name = name
        self.config = config or {}
        self.is_active = True
        self.last_signals = {}  # 종목별 마지막 신호 저장
        
    @abstractmethod
    def generate_signal(self, market_data: MarketData, historical_data: pd.DataFrame = None) -> Optional[Signal]:
        """
        매매 신호 생성
        
        Args:
            market_data: 현재 시장 데이터
            historical_data: 과거 데이터 (옵션)
            
        Returns:
            Signal 객체 또는 None
        """
        pass
    
    @abstractmethod
    def validate_signal(self, signal: Signal, market_data: MarketData) -> bool:
        """
        신호 검증
        
        Args:
            signal: 검증할 신호
            market_data: 현재 시장 데이터
            
        Returns:
            신호 유효성 여부
        """
        pass
    
    def get_signal_strength(self, market_data: MarketData) -> float:
        """
        신호 강도 계산 (기본 구현)
        
        Args:
            market_data: 시장 데이터
            
        Returns:
            신호 강도 (0.0 ~ 1.0)
        """
        return 0.5  # 기본값
    
    def update_config(self, config: Dict):
        """설정 업데이트"""
        self.config.update(config)
    
    def activate(self):
        """전략 활성화"""
        self.is_active = True
    
    def deactivate(self):
        """전략 비활성화"""
        self.is_active = False
    
    def get_status(self) -> Dict:
        """전략 상태 반환"""
        return {
            "name": self.name,
            "is_active": self.is_active,
            "config": self.config,
            "last_signals_count": len(self.last_signals)
        }
