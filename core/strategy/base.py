"""
기본 전략 클래스 및 데이터 모델 (최소 버전)
캔들 기반 시스템으로 전환하면서 레거시 호환성을 위한 최소 구현
"""
from typing import Optional, Any, Dict, List
from datetime import datetime
from dataclasses import dataclass
from enum import Enum


class SignalType(Enum):
    """신호 타입"""
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass
class Signal:
    """매매 신호"""
    signal_type: SignalType
    stock_code: str
    price: float
    confidence: float = 0.0
    timestamp: Optional[datetime] = None
    reason: str = ""

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class MarketData:
    """시장 데이터"""
    stock_code: str
    current_price: float
    open_price: float = 0.0
    high_price: float = 0.0
    low_price: float = 0.0
    prev_close: float = 0.0
    volume: int = 0
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class BaseStrategy:
    """
    기본 전략 클래스 (레거시 호환성)

    🚨 캔들 기반 시스템으로 전환됨에 따라 이 클래스는 더 이상 사용되지 않습니다.
    새로운 구현은 CandleTradeManager를 사용하세요.
    """

    def __init__(self, name: str = "BaseStrategy"):
        self.name = name
        self.is_active = False

    def analyze(self, market_data: MarketData, historical_data: Any = None) -> Signal:
        """
        시장 분석 및 신호 생성 (더미 구현)

        ⚠️ 이 메서드는 레거시 호환성을 위해서만 존재합니다.
        실제 전략 로직은 CandleTradeManager에서 처리됩니다.
        """
        return Signal(
            signal_type=SignalType.HOLD,
            stock_code=market_data.stock_code,
            price=market_data.current_price,
            reason="레거시 호환성 더미 신호"
        )

    def get_signal_strength(self, signal: Signal) -> float:
        """신호 강도 반환 (더미 구현)"""
        return 0.0

    def is_ready(self) -> bool:
        """전략 준비 상태 (더미 구현)"""
        return False
