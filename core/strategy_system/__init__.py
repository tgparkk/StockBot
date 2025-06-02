"""
전략 시스템 관련 모듈들 (DEPRECATED)
⚠️ 이 모듈은 새로운 캔들 트레이딩 시스템으로 대체되었습니다.
새로운 시스템: core.strategy.candle_trade_manager
"""

import warnings

# 기존 호환성을 위한 deprecation 경고
def _deprecated_import_warning(old_module: str, new_module: str):
    warnings.warn(
        f"{old_module}은 더 이상 지원되지 않습니다. "
        f"새로운 캔들 트레이딩 시스템 {new_module}을 사용하세요.",
        DeprecationWarning,
        stacklevel=3
    )

# Deprecated 클래스들 (호환성 유지용)
class StrategyScheduler:
    """DEPRECATED: CandleTradeManager를 사용하세요"""
    def __init__(self, *args, **kwargs):
        _deprecated_import_warning("StrategyScheduler", "CandleTradeManager")
        raise ImportError("StrategyScheduler는 더 이상 지원되지 않습니다. CandleTradeManager를 사용하세요.")

class SignalProcessor:
    """DEPRECATED: CandlePatternDetector를 사용하세요"""
    def __init__(self, *args, **kwargs):
        _deprecated_import_warning("SignalProcessor", "CandlePatternDetector")
        raise ImportError("SignalProcessor는 더 이상 지원되지 않습니다. CandlePatternDetector를 사용하세요.")

class TimeSlotManager:
    """DEPRECATED: CandleTradeManager 내장 시간 관리를 사용하세요"""
    def __init__(self, *args, **kwargs):
        _deprecated_import_warning("TimeSlotManager", "CandleTradeManager")
        raise ImportError("TimeSlotManager는 더 이상 지원되지 않습니다. CandleTradeManager를 사용하세요.")

__all__ = [
    'StrategyScheduler',  # Deprecated
    'SignalProcessor',    # Deprecated
    'TimeSlotManager'     # Deprecated
]
