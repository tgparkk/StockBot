"""
KIS WebSocket 구독 관리자
"""
from typing import Dict, Set, List, Optional
from dataclasses import dataclass
from datetime import datetime
from utils.logger import setup_logger

logger = setup_logger(__name__)


@dataclass
class Subscription:
    """구독 정보"""
    tr_id: str          # H0STCNT0, H0STASP0, H0STCNI0, H0STCNI9
    tr_key: str         # 종목코드 또는 HTS ID
    strategy_name: str  # 전략명
    priority: int       # 우선순위 (높을수록 중요)
    created_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()

    @property
    def subscription_key(self) -> str:
        """구독 키"""
        return f"{self.tr_id}|{self.tr_key}"


class KISSubscriptionManager:
    """구독 관리자 (41건 제한 관리)"""

    MAX_SUBSCRIPTIONS = 41  # KIS API 제한

    def __init__(self):
        """초기화"""
        self.subscriptions: Dict[str, Subscription] = {}
        self.pending_adds: Set[str] = set()  # 추가 대기 중
        self.pending_removes: Set[str] = set()  # 제거 대기 중

    def can_add_subscription(self) -> bool:
        """구독 추가 가능 여부"""
        current_count = len(self.subscriptions) + len(self.pending_adds) - len(self.pending_removes)
        return current_count < self.MAX_SUBSCRIPTIONS

    def add_subscription(self, tr_id: str, tr_key: str, strategy_name: str = "", priority: int = 50) -> bool:
        """구독 추가"""
        subscription_key = f"{tr_id}|{tr_key}"

        # 이미 구독 중인지 확인
        if subscription_key in self.subscriptions:
            logger.debug(f"이미 구독 중: {subscription_key}")
            return True

        # 구독 가능 여부 확인
        if not self.can_add_subscription():
            logger.warning(f"구독 제한 초과 (최대 {self.MAX_SUBSCRIPTIONS}건)")
            return False

        # 구독 정보 생성
        subscription = Subscription(
            tr_id=tr_id,
            tr_key=tr_key,
            strategy_name=strategy_name,
            priority=priority
        )

        self.subscriptions[subscription_key] = subscription
        self.pending_adds.add(subscription_key)

        logger.info(f"구독 추가: {subscription_key} (전략: {strategy_name})")
        return True

    def remove_subscription(self, subscription_key: str) -> bool:
        """구독 제거"""
        if subscription_key not in self.subscriptions:
            logger.debug(f"구독되지 않은 키: {subscription_key}")
            return False

        self.pending_removes.add(subscription_key)
        logger.info(f"구독 제거: {subscription_key}")
        return True

    def remove_by_strategy(self, strategy_name: str) -> int:
        """전략별 구독 제거"""
        removed_count = 0

        for key, subscription in list(self.subscriptions.items()):
            if subscription.strategy_name == strategy_name:
                if self.remove_subscription(key):
                    removed_count += 1

        logger.info(f"전략 '{strategy_name}' 구독 {removed_count}건 제거")
        return removed_count

    def get_pending_additions(self) -> List[Subscription]:
        """추가 대기 중인 구독 목록"""
        return [self.subscriptions[key] for key in self.pending_adds if key in self.subscriptions]

    def get_pending_removals(self) -> List[str]:
        """제거 대기 중인 구독 키 목록"""
        return list(self.pending_removes)

    def confirm_addition(self, subscription_key: str):
        """구독 추가 확인"""
        self.pending_adds.discard(subscription_key)

    def confirm_removal(self, subscription_key: str):
        """구독 제거 확인"""
        self.pending_removes.discard(subscription_key)
        self.subscriptions.pop(subscription_key, None)

    def get_subscription_status(self) -> Dict:
        """구독 상태 정보"""
        strategies = {}
        for subscription in self.subscriptions.values():
            strategy = subscription.strategy_name or "기본"
            strategies[strategy] = strategies.get(strategy, 0) + 1

        return {
            'total_subscriptions': len(self.subscriptions),
            'max_subscriptions': self.MAX_SUBSCRIPTIONS,
            'available_slots': self.MAX_SUBSCRIPTIONS - len(self.subscriptions),
            'pending_adds': len(self.pending_adds),
            'pending_removes': len(self.pending_removes),
            'strategies': strategies
        }

    def get_subscriptions_by_type(self) -> Dict[str, List[str]]:
        """타입별 구독 목록"""
        by_type = {}

        for subscription in self.subscriptions.values():
            tr_id = subscription.tr_id
            if tr_id not in by_type:
                by_type[tr_id] = []
            by_type[tr_id].append(subscription.tr_key)

        return by_type

    def clear_all(self):
        """모든 구독 제거"""
        for key in list(self.subscriptions.keys()):
            self.remove_subscription(key)

        logger.info("모든 구독 제거 요청")
