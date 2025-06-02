# Core package

"""
StockBot Core 모듈들

기존 import 호환성을 100% 유지하면서 새로운 폴더 구조도 지원합니다.
"""

# 🔄 새로운 폴더 구조에서 직접 import
from .websocket.kis_websocket_manager import KISWebSocketManager
from .trading.trading_manager import TradingManager
from .trading.position_manager import PositionManager
from .trading.trade_executor import TradeExecutor, TradeConfig
from .trading.trade_database import TradeDatabase
from .strategy.candle_trade_manager import CandleTradeManager
from .strategy.candle_stock_manager import CandleStockManager
from .strategy.candle_pattern_detector import CandlePatternDetector
from .api.rest_api_manager import KISRestAPIManager
from .data.kis_data_collector import KISDataCollector
from .data.hybrid_data_manager import SimpleHybridDataManager
from .data.data_priority import DataPriority
from .system.worker_manager import WorkerManager

# 🔄 기존 호환성을 위한 직접 export (main.py 등에서 사용)
kis_websocket_manager = KISWebSocketManager
trading_manager = TradingManager
position_manager = PositionManager
trade_executor = TradeExecutor
trade_database = TradeDatabase
candle_trade_manager = CandleTradeManager
candle_stock_manager = CandleStockManager
candle_pattern_detector = CandlePatternDetector
rest_api_manager = KISRestAPIManager
kis_data_collector = KISDataCollector
hybrid_data_manager = SimpleHybridDataManager
data_priority = DataPriority
worker_manager = WorkerManager

# 🆕 새로운 폴더 구조 지원 (기존 클래스들을 재사용)
class WebSocketModule:
    """WebSocket 관련 모듈 접근자"""
    KISWebSocketManager = KISWebSocketManager
    # 다른 웹소켓 관련 클래스들도 여기에 추가 가능

class APIModule:
    """API 관련 모듈 접근자"""
    KISRestAPIManager = KISRestAPIManager
    # 다른 API 관련 클래스들도 여기에 추가 가능

class TradingModule:
    """거래 관련 모듈 접근자"""
    TradingManager = TradingManager
    PositionManager = PositionManager
    TradeExecutor = TradeExecutor
    TradeConfig = TradeConfig
    TradeDatabase = TradeDatabase

class StrategyModule:
    """전략 시스템 관련 모듈 접근자"""
    CandleTradeManager = CandleTradeManager
    CandleStockManager = CandleStockManager
    CandlePatternDetector = CandlePatternDetector

class DataModule:
    """데이터 관련 모듈 접근자"""
    KISDataCollector = KISDataCollector
    SimpleHybridDataManager = SimpleHybridDataManager
    DataPriority = DataPriority

class SystemModule:
    """시스템 관련 모듈 접근자"""
    WorkerManager = WorkerManager

# 새로운 구조 인스턴스 생성
websocket = WebSocketModule()
api = APIModule()
trading = TradingModule()
strategy = StrategyModule()
data = DataModule()
system = SystemModule()

__all__ = [
    # 🔄 기존 클래스들 (100% 호환성)
    'KISWebSocketManager',
    'TradingManager',
    'PositionManager',
    'TradeExecutor',
    'TradeConfig',
    'TradeDatabase',
    'CandleTradeManager',
    'CandleStockManager',
    'CandlePatternDetector',
    'KISRestAPIManager',
    'KISDataCollector',
    'SimpleHybridDataManager',
    'DataPriority',
    'WorkerManager',

    # 🆕 새로운 모듈 접근자들
    'websocket',
    'api',
    'trading',
    'strategy',
    'data',
    'system',

    # 모듈 그룹
    'APIModule', 'DataModule', 'WebSocketModule', 'TradingModule', 'StrategyModule', 'SystemModule'
]
