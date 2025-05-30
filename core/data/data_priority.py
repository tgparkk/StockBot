"""
데이터 우선순위 정의
"""
from enum import Enum

class DataPriority(Enum):
    """데이터 우선순위"""
    CRITICAL = "critical"       # 실시간 (체결가+호가)
    HIGH = "high"               # 준실시간 (체결가만)
    MEDIUM = "medium"           # 30초 간격
    LOW = "low"                 # 1분 간격
    BACKGROUND = "background"   # 5분 간격 