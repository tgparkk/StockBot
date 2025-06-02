"""
전략 모듈 (캔들 기반 시스템으로 전환됨)
레거시 호환성을 위한 기본 클래스들만 제공
"""

# 🎯 기본 클래스들 (레거시 호환성)
from .base import BaseStrategy, Signal, MarketData, SignalType

# 🆕 캔들 기반 시스템 (메인)
from .candle_trade_manager import CandleTradeManager
from .candle_pattern_detector import CandlePatternDetector
from .candle_stock_manager import CandleStockManager
from .candle_trade_candidate import CandleTradeCandidate

__all__ = [
    # 레거시 호환성 클래스들
    'BaseStrategy',
    'Signal',
    'SignalType',
    'MarketData',

    # 🆕 캔들 기반 시스템 (실제 사용)
    'CandleTradeManager',
    'CandlePatternDetector',
    'CandleStockManager',
    'CandleTradeCandidate'
]
