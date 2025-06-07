"""
캔들 기반 매매 종목 정보 데이터 클래스
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum
import pandas as pd


class PatternType(Enum):
    """캔들 패턴 타입"""
    HAMMER = "hammer"
    INVERTED_HAMMER = "inverted_hammer"
    BULLISH_ENGULFING = "bullish_engulfing"
    BEARISH_ENGULFING = "bearish_engulfing"
    MORNING_STAR = "morning_star"
    EVENING_STAR = "evening_star"
    DOJI = "doji"
    RISING_THREE_METHODS = "rising_three_methods"
    FALLING_THREE_METHODS = "falling_three_methods"


class TradeSignal(Enum):
    """매매 신호 타입"""
    STRONG_BUY = "strong_buy"
    BUY = "buy"
    HOLD = "hold"
    SELL = "sell"
    STRONG_SELL = "strong_sell"
    AVOID = "avoid"


class CandleStatus(Enum):
    """종목 상태"""
    SCANNING = "scanning"        # 스캐닝 중
    WATCHING = "watching"        # 관찰 중
    BUY_READY = "buy_ready"     # 매수 준비
    SELL_READY = "sell_ready"   # 매도 준비
    ENTERED = "entered"          # 진입 완료
    EXITED = "exited"           # 청산 완료
    STOPPED = "stopped"          # 손절 완료


@dataclass
class CandlePatternInfo:
    """캔들 패턴 정보"""
    pattern_type: PatternType
    confidence: float           # 0.0~1.0 신뢰도
    strength: int              # 0~100 강도 점수
    formation_bars: int        # 패턴 형성에 사용된 봉 수
    detected_at: datetime      # 패턴 감지 시간
    description: str           # 패턴 설명


@dataclass
class EntryConditions:
    """진입 조건 체크 결과"""
    volume_check: bool = False          # 거래량 조건
    rsi_check: bool = False            # RSI 조건
    time_check: bool = False           # 시간대 조건
    price_check: bool = False          # 가격대 조건
    market_cap_check: bool = False     # 시가총액 조건
    daily_volume_check: bool = False   # 일 거래대금 조건
    overall_passed: bool = False       # 전체 통과 여부
    fail_reasons: List[str] = field(default_factory=list)  # 실패 이유
    technical_indicators: Dict[str, Any] = field(default_factory=dict)  # 기술적 지표 값들


@dataclass
class RiskManagement:
    """리스크 관리 정보"""
    position_size_pct: float       # 포지션 크기 (%)
    position_amount: int           # 실제 투자 금액
    stop_loss_price: float         # 손절가
    target_price: float            # 목표가
    trailing_stop_pct: float       # 추적 손절 비율
    max_holding_hours: int         # 최대 보유 시간
    risk_score: int               # 0~100 위험도 점수

    # 동적 조정값
    current_trailing_stop: Optional[float] = None
    last_trailing_update: Optional[datetime] = None


@dataclass
class PerformanceTracking:
    """성과 추적 정보"""
    entry_time: Optional[datetime] = None
    entry_price: Optional[float] = None
    entry_quantity: Optional[int] = None

    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None

    # 계산된 성과
    unrealized_pnl: Optional[float] = None
    realized_pnl: Optional[float] = None
    pnl_pct: Optional[float] = None
    holding_time_hours: Optional[float] = None

    # 실시간 추적
    max_price_seen: Optional[float] = None
    min_price_seen: Optional[float] = None
    max_unrealized_profit: Optional[float] = None
    max_unrealized_loss: Optional[float] = None


@dataclass
class CandleTradeCandidate:
    """캔들 기반 매매 종목 정보 - 메인 클래스"""

    # ========== 기본 정보 ==========
    stock_code: str
    stock_name: str
    current_price: float
    market_type: str                    # "KOSPI", "KOSDAQ"

    # ========== 🆕 일봉 데이터 캐싱 ==========
    ohlcv_data: Optional[pd.DataFrame] = None       # 일봉 데이터 캐시
    ohlcv_last_updated: Optional[datetime] = None   # 일봉 데이터 마지막 업데이트 시간
    ohlcv_update_date: Optional[str] = None         # 일봉 데이터 업데이트 일자 (YYYYMMDD)

    # ========== 캔들 패턴 정보 ==========
    detected_patterns: List[CandlePatternInfo] = field(default_factory=list)
    primary_pattern: Optional[CandlePatternInfo] = None
    pattern_score: int = 0              # 0~100 종합 패턴 점수

    # ========== 매매 신호 ==========
    trade_signal: TradeSignal = TradeSignal.HOLD
    signal_strength: int = 0            # 0~100 신호 강도
    signal_updated_at: datetime = field(default_factory=datetime.now)

    # ========== 진입 조건 ==========
    entry_conditions: EntryConditions = field(default_factory=EntryConditions)
    recommended_entry_price: Optional[float] = None
    entry_priority: int = 0             # 0~100 진입 우선순위

    # ========== 리스크 관리 ==========
    risk_management: RiskManagement = field(default_factory=lambda: RiskManagement(
        position_size_pct=0.0, position_amount=0, stop_loss_price=0.0,
        target_price=0.0, trailing_stop_pct=0.0, max_holding_hours=0, risk_score=100
    ))

    # ========== 상태 관리 ==========
    status: CandleStatus = CandleStatus.SCANNING
    created_at: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)

    # ========== 실시간 데이터 ==========
    last_price_update: datetime = field(default_factory=datetime.now)
    price_alerts: List[float] = field(default_factory=list)

    # ========== 성과 추적 ==========
    performance: PerformanceTracking = field(default_factory=PerformanceTracking)

    # ========== 추가 메타데이터 ==========
    metadata: Dict[str, Any] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)

    # ========== 🆕 일봉 데이터 캐싱 메서드 ==========

    def cache_ohlcv_data(self, ohlcv_data: pd.DataFrame):
        """일봉 데이터 캐싱"""
        self.ohlcv_data = ohlcv_data.copy() if ohlcv_data is not None else None
        self.ohlcv_last_updated = datetime.now()
        self.ohlcv_update_date = datetime.now().strftime('%Y%m%d')
        self.last_updated = datetime.now()

    def is_ohlcv_data_valid(self) -> bool:
        """캐시된 일봉 데이터의 유효성 확인"""
        if self.ohlcv_data is None or self.ohlcv_update_date is None:
            return False

        # 오늘 날짜와 비교
        today = datetime.now().strftime('%Y%m%d')
        return self.ohlcv_update_date == today

    def get_ohlcv_data(self) -> Optional[pd.DataFrame]:
        """캐시된 일봉 데이터 조회"""
        if self.is_ohlcv_data_valid():
            return self.ohlcv_data
        return None

    def invalidate_ohlcv_cache(self):
        """일봉 데이터 캐시 무효화"""
        self.ohlcv_data = None
        self.ohlcv_last_updated = None
        self.ohlcv_update_date = None

    def add_pattern(self, pattern_info: CandlePatternInfo):
        """패턴 정보 추가"""
        self.detected_patterns.append(pattern_info)

        # 가장 강한 패턴을 primary로 설정
        if not self.primary_pattern or pattern_info.strength > self.primary_pattern.strength:
            self.primary_pattern = pattern_info

        # 패턴 점수 재계산
        self.pattern_score = self._calculate_pattern_score()
        self.last_updated = datetime.now()

    def _calculate_pattern_score(self) -> int:
        """종합 패턴 점수 계산"""
        if not self.detected_patterns:
            return 0

        # 가중 평균 계산
        total_score = 0
        total_weight = 0

        for pattern in self.detected_patterns:
            weight = pattern.confidence * pattern.strength
            total_score += weight
            total_weight += pattern.confidence

        return int(total_score / total_weight) if total_weight > 0 else 0

    def update_price(self, new_price: float):
        """가격 업데이트 및 성과 계산"""
        self.current_price = new_price
        self.last_price_update = datetime.now()
        self.last_updated = datetime.now()

        # 성과 추적 업데이트
        if self.status == CandleStatus.ENTERED and self.performance.entry_price:
            # 미실현 손익 계산
            entry_price = self.performance.entry_price
            quantity = self.performance.entry_quantity or 0

            self.performance.unrealized_pnl = (new_price - entry_price) * quantity
            self.performance.pnl_pct = ((new_price - entry_price) / entry_price) * 100

            # 최고/최저가 업데이트
            if not self.performance.max_price_seen or new_price > self.performance.max_price_seen:
                self.performance.max_price_seen = new_price

            if not self.performance.min_price_seen or new_price < self.performance.min_price_seen:
                self.performance.min_price_seen = new_price

            # 최대 미실현 손익 업데이트
            if self.performance.unrealized_pnl > 0:
                if (not self.performance.max_unrealized_profit or
                    self.performance.unrealized_pnl > self.performance.max_unrealized_profit):
                    self.performance.max_unrealized_profit = self.performance.unrealized_pnl
            else:
                if (not self.performance.max_unrealized_loss or
                    self.performance.unrealized_pnl < self.performance.max_unrealized_loss):
                    self.performance.max_unrealized_loss = self.performance.unrealized_pnl

    def enter_position(self, entry_price: float, quantity: int):
        """포지션 진입 기록"""
        self.status = CandleStatus.ENTERED
        self.performance.entry_time = datetime.now()
        self.performance.entry_price = entry_price
        self.performance.entry_quantity = quantity
        self.last_updated = datetime.now()

        # 초기 추적값 설정
        self.performance.max_price_seen = entry_price
        self.performance.min_price_seen = entry_price

    def exit_position(self, exit_price: float, reason: str):
        """포지션 청산 기록"""
        self.status = CandleStatus.EXITED
        self.performance.exit_time = datetime.now()
        self.performance.exit_price = exit_price
        self.performance.exit_reason = reason
        self.last_updated = datetime.now()

        # 실현 손익 계산
        if self.performance.entry_price and self.performance.entry_quantity:
            entry_price = self.performance.entry_price
            quantity = self.performance.entry_quantity

            self.performance.realized_pnl = (exit_price - entry_price) * quantity
            self.performance.pnl_pct = ((exit_price - entry_price) / entry_price) * 100

            # 보유 시간 계산
            if self.performance.entry_time:
                holding_time = self.performance.exit_time - self.performance.entry_time
                self.performance.holding_time_hours = holding_time.total_seconds() / 3600

    def get_signal_summary(self) -> str:
        """신호 요약 문자열"""
        patterns = [p.pattern_type.value for p in self.detected_patterns]
        return f"{self.trade_signal.value.upper()} ({self.signal_strength}점) - {', '.join(patterns)}"

    def is_ready_for_entry(self) -> bool:
        """진입 준비 상태 확인"""
        return (self.entry_conditions.overall_passed and
                self.trade_signal in [TradeSignal.STRONG_BUY, TradeSignal.BUY] and
                self.status in [CandleStatus.WATCHING, CandleStatus.BUY_READY])

    def get_risk_level(self) -> str:
        """위험도 레벨 문자열"""
        score = self.risk_management.risk_score
        if score >= 80:
            return "HIGH"
        elif score >= 60:
            return "MEDIUM"
        elif score >= 40:
            return "LOW"
        else:
            return "VERY_LOW"

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리 변환 (API 응답용)"""
        return {
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'current_price': self.current_price,
            'trade_signal': self.trade_signal.value,
            'signal_strength': self.signal_strength,
            'pattern_score': self.pattern_score,
            'primary_pattern': self.primary_pattern.pattern_type.value if self.primary_pattern else None,
            'status': self.status.value,
            'entry_ready': self.is_ready_for_entry(),
            'risk_level': self.get_risk_level(),
            'position_size_pct': self.risk_management.position_size_pct,
            'target_price': self.risk_management.target_price,
            'stop_loss_price': self.risk_management.stop_loss_price,
            'last_updated': self.last_updated.strftime('%Y-%m-%d %H:%M:%S')
        }
