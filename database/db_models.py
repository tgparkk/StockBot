"""
데이터베이스 모델 정의
시간대별 종목 선택, 거래 기록, 포지션 관리, 성과 분석 등을 위한 테이블들
"""
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional
import pytz
from peewee import *

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')

# 데이터베이스 설정
database = SqliteDatabase('database/stockbot.db')

class BaseModel(Model):
    """기본 모델 클래스"""
    class Meta:
        database = database
    
    created_at = DateTimeField(default=lambda: datetime.now(KST))
    updated_at = DateTimeField(default=lambda: datetime.now(KST))
    
    def save(self, *args, **kwargs):
        self.updated_at = datetime.now(KST)
        return super().save(*args, **kwargs)

# ========== 열거형 정의 ==========

class StrategyType(Enum):
    """전략 타입"""
    GAP_TRADING = "gap_trading"
    VOLUME_BREAKOUT = "volume_breakout" 
    MOMENTUM = "momentum"
    EMERGENCY_VOLUME = "emergency_volume"
    EMERGENCY_PRICE = "emergency_price"

class TimeSlot(Enum):
    """시간대"""
    GOLDEN_TIME = "golden_time"           # 09:00-09:30
    MORNING_LEADERS = "morning_leaders"   # 09:30-11:30
    LUNCH_TIME = "lunch_time"             # 11:30-14:00
    CLOSING_TREND = "closing_trend"       # 14:00-15:20

class OrderType(Enum):
    """주문 타입"""
    BUY = "buy"
    SELL = "sell"

class OrderStatus(Enum):
    """주문 상태"""
    PENDING = "pending"         # 주문 대기
    PARTIAL = "partial"         # 부분 체결
    FILLED = "filled"           # 완전 체결
    CANCELLED = "cancelled"     # 취소
    FAILED = "failed"           # 실패

class PositionStatus(Enum):
    """포지션 상태"""
    OPEN = "open"               # 보유 중
    CLOSED = "closed"           # 청산 완료

class DataPriority(Enum):
    """데이터 우선순위"""
    CRITICAL = "critical"       # 실시간 (체결가+호가)
    HIGH = "high"               # 준실시간 (체결가만)
    MEDIUM = "medium"           # 30초 간격
    LOW = "low"                 # 1분 간격
    BACKGROUND = "background"   # 5분 간격

# ========== 시간대별 종목 선택 테이블들 ==========

class GoldenTimeStock(BaseModel):
    """골든타임 선택 종목 (09:00-09:30)"""
    stock_code = CharField(max_length=10)
    stock_name = CharField(max_length=100)
    strategy_type = CharField(choices=[(s.value, s.value) for s in StrategyType])
    score = DecimalField(max_digits=10, decimal_places=4)
    reason = TextField()
    priority = CharField(choices=[(p.value, p.value) for p in DataPriority])
    
    # 선택 당시 시장 데이터
    current_price = IntegerField()
    change_rate = DecimalField(max_digits=8, decimal_places=4)
    volume = BigIntegerField()
    volume_ratio = DecimalField(max_digits=8, decimal_places=2, null=True)
    gap_rate = DecimalField(max_digits=8, decimal_places=4, null=True)
    
    # 메타 정보
    selection_date = DateField()
    selection_time = TimeField()
    is_active = BooleanField(default=True)
    activation_success = BooleanField(default=False)
    
    class Meta:
        table_name = 'golden_time_stocks'
        indexes = (
            (('selection_date', 'strategy_type'), False),
            (('stock_code', 'selection_date'), False),
        )

class MorningLeadersStock(BaseModel):
    """주도주 시간 선택 종목 (09:30-11:30)"""
    stock_code = CharField(max_length=10)
    stock_name = CharField(max_length=100)
    strategy_type = CharField(choices=[(s.value, s.value) for s in StrategyType])
    score = DecimalField(max_digits=10, decimal_places=4)
    reason = TextField()
    priority = CharField(choices=[(p.value, p.value) for p in DataPriority])
    
    # 선택 당시 시장 데이터
    current_price = IntegerField()
    change_rate = DecimalField(max_digits=8, decimal_places=4)
    volume = BigIntegerField()
    volume_ratio = DecimalField(max_digits=8, decimal_places=2, null=True)
    breakout_direction = CharField(max_length=10, null=True)
    buying_pressure = CharField(max_length=20, null=True)
    
    # 메타 정보
    selection_date = DateField()
    selection_time = TimeField()
    is_active = BooleanField(default=True)
    activation_success = BooleanField(default=False)
    
    class Meta:
        table_name = 'morning_leaders_stocks'
        indexes = (
            (('selection_date', 'strategy_type'), False),
            (('stock_code', 'selection_date'), False),
        )

class LunchTimeStock(BaseModel):
    """점심시간 선택 종목 (11:30-14:00)"""
    stock_code = CharField(max_length=10)
    stock_name = CharField(max_length=100)
    strategy_type = CharField(choices=[(s.value, s.value) for s in StrategyType])
    score = DecimalField(max_digits=10, decimal_places=4)
    reason = TextField()
    priority = CharField(choices=[(p.value, p.value) for p in DataPriority])
    
    # 선택 당시 시장 데이터
    current_price = IntegerField()
    change_rate = DecimalField(max_digits=8, decimal_places=4)
    volume = BigIntegerField()
    momentum_strength = DecimalField(max_digits=8, decimal_places=4, null=True)
    trend_quality = CharField(max_length=20, null=True)
    consecutive_up_days = IntegerField(null=True)
    
    # 메타 정보
    selection_date = DateField()
    selection_time = TimeField()
    is_active = BooleanField(default=True)
    activation_success = BooleanField(default=False)
    
    class Meta:
        table_name = 'lunch_time_stocks'
        indexes = (
            (('selection_date', 'strategy_type'), False),
            (('stock_code', 'selection_date'), False),
        )

class ClosingTrendStock(BaseModel):
    """마감 추세 선택 종목 (14:00-15:20)"""
    stock_code = CharField(max_length=10)
    stock_name = CharField(max_length=100)
    strategy_type = CharField(choices=[(s.value, s.value) for s in StrategyType])
    score = DecimalField(max_digits=10, decimal_places=4)
    reason = TextField()
    priority = CharField(choices=[(p.value, p.value) for p in DataPriority])
    
    # 선택 당시 시장 데이터
    current_price = IntegerField()
    change_rate = DecimalField(max_digits=8, decimal_places=4)
    volume = BigIntegerField()
    momentum_strength = DecimalField(max_digits=8, decimal_places=4, null=True)
    trend_quality = CharField(max_length=20, null=True)
    ma_position = CharField(max_length=10, null=True)
    
    # 메타 정보
    selection_date = DateField()
    selection_time = TimeField()
    is_active = BooleanField(default=True)
    activation_success = BooleanField(default=False)
    
    class Meta:
        table_name = 'closing_trend_stocks'
        indexes = (
            (('selection_date', 'strategy_type'), False),
            (('stock_code', 'selection_date'), False),
        )

# ========== 거래 기록 테이블 ==========

class TradeRecord(BaseModel):
    """거래 기록"""
    # 기본 정보
    order_id = CharField(max_length=50, unique=True)
    stock_code = CharField(max_length=10)
    stock_name = CharField(max_length=100)
    
    # 주문 정보
    order_type = CharField(choices=[(o.value, o.value) for o in OrderType])
    order_status = CharField(choices=[(s.value, s.value) for s in OrderStatus])
    
    # 수량 및 가격
    order_quantity = IntegerField()
    executed_quantity = IntegerField(default=0)
    order_price = IntegerField()
    executed_price = IntegerField(null=True)
    
    # 전략 정보
    time_slot = CharField(choices=[(t.value, t.value) for t in TimeSlot])
    strategy_type = CharField(choices=[(s.value, s.value) for s in StrategyType])
    
    # 시간 정보
    order_time = DateTimeField()
    executed_time = DateTimeField(null=True)
    
    # 거래 결과
    trade_amount = BigIntegerField(null=True)  # 거래대금
    fee = IntegerField(null=True)              # 수수료
    tax = IntegerField(null=True)              # 세금
    net_amount = BigIntegerField(null=True)    # 실제 거래금액
    
    # 메타 정보
    error_message = TextField(null=True)
    retry_count = IntegerField(default=0)
    
    class Meta:
        table_name = 'trade_records'
        indexes = (
            (('stock_code', 'order_time'), False),
            (('time_slot', 'strategy_type', 'order_time'), False),
            (('order_status',), False),
        )

# ========== 포지션 관리 테이블 ==========

class Position(BaseModel):
    """포지션 관리"""
    stock_code = CharField(max_length=10)
    stock_name = CharField(max_length=100)
    
    # 포지션 정보
    status = CharField(choices=[(s.value, s.value) for s in PositionStatus])
    quantity = IntegerField()
    avg_buy_price = IntegerField()
    total_buy_amount = BigIntegerField()
    
    # 현재 평가
    current_price = IntegerField(null=True)
    current_value = BigIntegerField(null=True)
    unrealized_pnl = BigIntegerField(null=True)
    unrealized_pnl_rate = DecimalField(max_digits=8, decimal_places=4, null=True)
    
    # 실현 손익 (매도 완료시)
    sell_price = IntegerField(null=True)
    sell_amount = BigIntegerField(null=True)
    realized_pnl = BigIntegerField(null=True)
    realized_pnl_rate = DecimalField(max_digits=8, decimal_places=4, null=True)
    
    # 전략 정보
    entry_time_slot = CharField(choices=[(t.value, t.value) for t in TimeSlot])
    entry_strategy = CharField(choices=[(s.value, s.value) for s in StrategyType])
    exit_strategy = CharField(max_length=50, null=True)
    
    # 시간 정보
    entry_time = DateTimeField()
    exit_time = DateTimeField(null=True)
    holding_duration = IntegerField(null=True)  # 보유 시간(분)
    
    class Meta:
        table_name = 'positions'
        indexes = (
            (('stock_code', 'status'), False),
            (('status', 'entry_time'), False),
            (('entry_time_slot', 'entry_strategy'), False),
        )

# ========== 전략 성과 테이블 ==========

class StrategyPerformance(BaseModel):
    """전략별 성과"""
    # 전략 정보
    time_slot = CharField(choices=[(t.value, t.value) for t in TimeSlot])
    strategy_type = CharField(choices=[(s.value, s.value) for s in StrategyType])
    date = DateField()
    
    # 선택 종목 통계
    total_candidates = IntegerField()
    activated_stocks = IntegerField()
    traded_stocks = IntegerField()
    
    # 거래 통계
    total_trades = IntegerField()
    win_trades = IntegerField()
    lose_trades = IntegerField()
    win_rate = DecimalField(max_digits=5, decimal_places=2)
    
    # 수익률 통계
    total_pnl = BigIntegerField()
    average_pnl = BigIntegerField()
    max_profit = BigIntegerField()
    max_loss = BigIntegerField()
    total_pnl_rate = DecimalField(max_digits=8, decimal_places=4)
    
    # 기타 지표
    avg_holding_time = IntegerField()  # 평균 보유시간(분)
    sharpe_ratio = DecimalField(max_digits=8, decimal_places=4, null=True)
    max_drawdown = DecimalField(max_digits=8, decimal_places=4, null=True)
    
    class Meta:
        table_name = 'strategy_performance'
        indexes = (
            (('date', 'time_slot', 'strategy_type'), True),  # Unique constraint
            (('time_slot', 'strategy_type', 'date'), False),
        )

# ========== 시장 스크리닝 로그 테이블 ==========

class MarketScreeningLog(BaseModel):
    """시장 스크리닝 로그"""
    # 스크리닝 정보
    screening_type = CharField(max_length=50)  # volume_leaders, price_movers, bid_ask_leaders
    
    # 종목 정보
    stock_code = CharField(max_length=10)
    stock_name = CharField(max_length=100)
    rank = IntegerField()
    
    # 스크리닝 지표
    current_price = IntegerField()
    change_rate = DecimalField(max_digits=8, decimal_places=4)
    volume = BigIntegerField()
    volume_ratio = DecimalField(max_digits=8, decimal_places=2, null=True)
    
    # 추가 지표 (타입별로 다름)
    market_cap = BigIntegerField(null=True)
    amount = BigIntegerField(null=True)
    bid_quantity = BigIntegerField(null=True)
    ask_quantity = BigIntegerField(null=True)
    buying_pressure = CharField(max_length=20, null=True)
    
    # 후속 조치
    criteria = CharField(max_length=50)
    was_activated = BooleanField(default=False)
    priority_assigned = CharField(max_length=20, null=True)
    
    class Meta:
        table_name = 'market_screening_logs'
        indexes = (
            (('created_at', 'screening_type'), False),
            (('stock_code', 'created_at'), False),
            (('screening_type', 'rank', 'created_at'), False),
        )

# ========== 시스템 상태 로그 테이블 ==========

class SystemLog(BaseModel):
    """시스템 상태 로그"""
    # 로그 정보
    log_level = CharField(max_length=10)  # INFO, WARNING, ERROR, CRITICAL
    component = CharField(max_length=50)  # scheduler, data_manager, trading_api 등
    message = TextField()
    
    # 상세 정보
    details = TextField(null=True)  # JSON 형태의 상세 정보
    error_traceback = TextField(null=True)
    
    # 성능 지표
    memory_usage = IntegerField(null=True)  # MB
    cpu_usage = DecimalField(max_digits=5, decimal_places=2, null=True)  # %
    api_call_count = IntegerField(null=True)
    websocket_connections = IntegerField(null=True)
    
    class Meta:
        table_name = 'system_logs'
        indexes = (
            (('log_level', 'created_at'), False),
            (('component', 'created_at'), False),
            (('created_at',), False),
        )

# ========== API 호출 통계 테이블 ==========

class APICallStats(BaseModel):
    """API 호출 통계"""
    # API 정보
    api_type = CharField(max_length=30)  # REST, WebSocket
    endpoint = CharField(max_length=100)
    method = CharField(max_length=10, null=True)  # GET, POST
    
    # 호출 통계
    total_calls = IntegerField()
    success_calls = IntegerField()
    error_calls = IntegerField()
    success_rate = DecimalField(max_digits=5, decimal_places=2)
    
    # 성능 통계
    avg_response_time = DecimalField(max_digits=8, decimal_places=2)  # ms
    min_response_time = DecimalField(max_digits=8, decimal_places=2)
    max_response_time = DecimalField(max_digits=8, decimal_places=2)
    
    # 시간 정보
    date = DateField()
    hour = IntegerField()  # 0-23
    
    class Meta:
        table_name = 'api_call_stats'
        indexes = (
            (('date', 'hour', 'api_type'), False),
            (('api_type', 'endpoint', 'date'), False),
        )

# ========== 모든 테이블 리스트 ==========
ALL_TABLES = [
    GoldenTimeStock,
    MorningLeadersStock, 
    LunchTimeStock,
    ClosingTrendStock,
    TradeRecord,
    Position,
    StrategyPerformance,
    MarketScreeningLog,
    SystemLog,
    APICallStats,
]
