"""
통합 WebSocket 관리자
KIS API WebSocket 41건 제한 최적화 + 실시간 데이터 수신
"""
import os
import json
import asyncio
import threading
import websockets
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Callable, Any
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from base64 import b64decode
from dotenv import load_dotenv
from utils.logger import setup_logger
from .rest_api_manager import KISRestAPIManager
import pytz

# 환경변수 로드
load_dotenv('config/.env')

logger = setup_logger(__name__)

class SubscriptionType(Enum):
    """구독 타입"""
    STOCK_PRICE = "H0STCNT0"      # 주식체결가
    STOCK_ORDERBOOK = "H0STASP0"  # 주식호가
    STOCK_EXECUTION = "H0STCNI9"  # 주식체결통보
    MARKET_INDEX = "H0UPCNT0"     # 지수

class TradingTimeSlot(Enum):
    """거래 시간대"""
    PRE_MARKET = "pre_market"       # 08:30-09:00
    GOLDEN_TIME = "golden_time"     # 09:00-09:30 (Gap Trading 집중)
    MORNING_TREND = "morning_trend" # 09:30-11:30 (주도주 시간)
    LUNCH_TIME = "lunch_time"       # 11:30-14:00 (점심 시간)
    CLOSING_TREND = "closing_trend" # 14:00-15:20 (마감 추세)
    AFTER_MARKET = "after_market"   # 15:20-15:30

@dataclass
class SubscriptionSlot:
    """구독 슬롯"""
    stock_code: str
    subscription_type: SubscriptionType
    priority: int = 0              # 우선순위 (높을수록 중요)
    strategy_name: str = ""        # 전략명
    performance_score: float = 0.0 # 성과 점수
    created_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)

    @property
    def subscription_key(self) -> str:
        """구독 키 생성"""
        return f"{self.subscription_type.value}|{self.stock_code}"

@dataclass
class AllocationStrategy:
    """할당 전략"""
    name: str
    max_realtime_stocks: int      # 실시간 종목 수 (체결가+호가)
    max_semi_realtime_stocks: int # 준실시간 종목 수 (체결가만)
    market_indices_count: int     # 시장 지수 수
    reserve_slots: int            # 예비 슬롯
    strategy_weights: Dict[str, float] = field(default_factory=dict)

    @property
    def total_slots(self) -> int:
        """총 사용 슬롯 수"""
        return (self.max_realtime_stocks * 2 +  # 체결가 + 호가
                self.max_semi_realtime_stocks +   # 체결가만
                self.market_indices_count +       # 지수
                self.reserve_slots)               # 예비

class KISWebSocketManager:
    """통합 WebSocket 관리자 (연결 + 최적화)"""

    # 웹소켓 엔드포인트
    WS_ENDPOINTS = {
        "real_time": "ws://ops.koreainvestment.com:21000",  # 실시간 데이터
    }

    # 최대 구독 제한
    MAX_SUBSCRIPTIONS = 41  # KIS API 제한

    def __init__(self):
        """초기화"""
        # API 인증 정보
        self.app_key = os.getenv('KIS_APP_KEY')
        self.app_secret = os.getenv('KIS_APP_SECRET')
        self.hts_id = os.getenv('KIS_HTS_ID')

        if not all([self.app_key, self.app_secret]):
            raise ValueError("웹소켓 인증 정보가 설정되지 않았습니다. .env 파일을 확인하세요.")

        # REST API 관리자 (접속키 발급용)
        self.rest_api = KISRestAPIManager()
        self.approval_key = None  # 웹소켓 접속키

        # 웹소켓 연결 정보
        self.websocket = None
        self.is_connected = False
        self.encryption_keys = {}  # 암호화 키 저장

        # 최적화 구독 관리
        self.current_subscriptions: Dict[str, SubscriptionSlot] = {}
        self.subscription_lock = threading.RLock()

        # 시간대별 할당 전략
        self.allocation_strategies = self._init_allocation_strategies()
        self.current_strategy: Optional[AllocationStrategy] = None

        # 성과 추적
        self.performance_tracker = PerformanceTracker()
        self.rebalance_scheduler = RebalanceScheduler(self)

        # 콜백 함수들
        self.callbacks = {
            "stock_price": [],      # 주식체결가 콜백
            "stock_orderbook": [],  # 주식호가 콜백
            "stock_execution": []   # 주식체결통보 콜백
        }

        # 통계
        self.stats = {
            'total_subscriptions': 0,
            'subscription_changes': 0,
            'performance_upgrades': 0,
            'emergency_additions': 0
        }

    def _init_allocation_strategies(self) -> Dict[TradingTimeSlot, AllocationStrategy]:
        """시간대별 할당 전략 초기화"""
        strategies = {
            TradingTimeSlot.GOLDEN_TIME: AllocationStrategy(
                name="Gap Trading 집중",
                max_realtime_stocks=8,      # 16슬롯 (체결가+호가)
                max_semi_realtime_stocks=15, # 15슬롯 (체결가만)
                market_indices_count=5,      # 5슬롯 (KOSPI, KOSDAQ 등)
                reserve_slots=5,             # 5슬롯 (긴급용)
                strategy_weights={
                    'gap_trading': 1.0,
                    'volume_breakout': 0.5,
                    'momentum': 0.3
                }
            ),

            TradingTimeSlot.MORNING_TREND: AllocationStrategy(
                name="주도주 포착",
                max_realtime_stocks=6,       # 12슬롯
                max_semi_realtime_stocks=18, # 18슬롯
                market_indices_count=5,      # 5슬롯
                reserve_slots=6,             # 6슬롯
                strategy_weights={
                    'volume_breakout': 0.7,
                    'momentum': 0.6,
                    'gap_trading': 0.3
                }
            ),

            TradingTimeSlot.LUNCH_TIME: AllocationStrategy(
                name="거래량 모니터링",
                max_realtime_stocks=5,       # 10슬롯
                max_semi_realtime_stocks=20, # 20슬롯
                market_indices_count=5,      # 5슬롯
                reserve_slots=6,             # 6슬롯
                strategy_weights={
                    'volume_breakout': 0.8,
                    'momentum': 0.4
                }
            ),

            TradingTimeSlot.CLOSING_TREND: AllocationStrategy(
                name="마감 추세",
                max_realtime_stocks=7,       # 14슬롯
                max_semi_realtime_stocks=16, # 16슬롯
                market_indices_count=5,      # 5슬롯
                reserve_slots=6,             # 6슬롯
                strategy_weights={
                    'momentum': 0.8,
                    'volume_breakout': 0.6
                }
            )
        }

        # 모든 전략이 41건 이하인지 확인
        for slot, strategy in strategies.items():
            if strategy.total_slots > self.MAX_SUBSCRIPTIONS:
                logger.warning(f"{slot.value} 전략이 {strategy.total_slots}건으로 제한 초과")

        return strategies

    # ===== WebSocket 연결 관리 =====

    async def connect(self) -> bool:
        """웹소켓 연결"""
        try:
            # 웹소켓 접속키 발급
            logger.info("웹소켓 접속키 발급 중...")
            self.approval_key = self.rest_api.get_websocket_approval_key()

            logger.info("웹소켓 연결 시작...")
            self.websocket = await websockets.connect(
                self.WS_ENDPOINTS["real_time"],
                ping_interval=None,
                ping_timeout=None
            )
            self.is_connected = True
            logger.info("웹소켓 연결 성공")
            return True

        except Exception as e:
            logger.error(f"웹소켓 연결 실패: {e}")
            self.is_connected = False
            return False

    async def disconnect(self):
        """웹소켓 연결 해제"""
        if self.websocket:
            try:
                is_closed = getattr(self.websocket, 'closed', False)
                if not is_closed:
                    await self.websocket.close()
            except Exception as e:
                logger.warning(f"웹소켓 연결 해제 중 오류: {e}")
            finally:
                self.is_connected = False
                logger.info("웹소켓 연결 해제")

    # ===== 메시지 생성 =====

    def _create_subscribe_message(self, tr_id: str, tr_key: str) -> str:
        """구독 메시지 생성"""
        message = {
            "header": {
                "approval_key": self.approval_key,
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8"
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": tr_key
                }
            }
        }
        return json.dumps(message, ensure_ascii=False)

    def _create_unsubscribe_message(self, tr_id: str, tr_key: str) -> str:
        """구독 해제 메시지 생성"""
        message = {
            "header": {
                "approval_key": self.approval_key,
                "custtype": "P",
                "tr_type": "2",
                "content-type": "utf-8"
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": tr_key
                }
            }
        }
        return json.dumps(message, ensure_ascii=False)

    # ===== 구독 기본 기능 =====

    async def subscribe(self, subscription_key: str) -> bool:
        """구독 키로 구독 (예: "H0STCNT0|005930")"""
        if not self.is_connected:
            logger.error("웹소켓이 연결되지 않았습니다.")
            return False

        try:
            tr_id, stock_code = subscription_key.split("|")
            message = self._create_subscribe_message(tr_id, stock_code)

            await self.websocket.send(message)
            logger.debug(f"구독 완료: {subscription_key}")
            return True

        except Exception as e:
            logger.error(f"구독 실패: {subscription_key} - {e}")
            return False

    async def unsubscribe(self, subscription_key: str) -> bool:
        """구독 해제"""
        if not self.is_connected:
            return False

        try:
            tr_id, stock_code = subscription_key.split("|")
            message = self._create_unsubscribe_message(tr_id, stock_code)

            await self.websocket.send(message)
            logger.debug(f"구독 해제: {subscription_key}")
            return True

        except Exception as e:
            logger.error(f"구독 해제 실패: {subscription_key} - {e}")
            return False

    # ===== 시간대별 최적화 전략 =====

    def get_current_time_slot(self) -> TradingTimeSlot:
        """현재 시간대 확인"""
        now = datetime.now()
        current_time = now.time()

        if current_time < datetime.strptime("09:00", "%H:%M").time():
            return TradingTimeSlot.PRE_MARKET
        elif current_time < datetime.strptime("09:30", "%H:%M").time():
            return TradingTimeSlot.GOLDEN_TIME
        elif current_time < datetime.strptime("11:30", "%H:%M").time():
            return TradingTimeSlot.MORNING_TREND
        elif current_time < datetime.strptime("14:00", "%H:%M").time():
            return TradingTimeSlot.LUNCH_TIME
        elif current_time < datetime.strptime("15:20", "%H:%M").time():
            return TradingTimeSlot.CLOSING_TREND
        else:
            return TradingTimeSlot.AFTER_MARKET

    def switch_strategy(self, time_slot: TradingTimeSlot = None) -> bool:
        """전략 전환 (시간대별 동적 구독 변경)"""
        if time_slot is None:
            time_slot = self.get_current_time_slot()

        if time_slot not in self.allocation_strategies:
            logger.warning(f"지원하지 않는 시간대: {time_slot}")
            return False

        new_strategy = self.allocation_strategies[time_slot]

        with self.subscription_lock:
            logger.info(f"전략 전환: {new_strategy.name} ({time_slot.value})")

            # 기존 구독 해제
            asyncio.create_task(self._unsubscribe_all())

            # 새 전략 적용
            self.current_strategy = new_strategy

            # 새로운 구독 설정
            success = self._apply_strategy(new_strategy)

            if success:
                self.stats['subscription_changes'] += 1
                logger.info(f"전략 전환 완료: {len(self.current_subscriptions)}건 구독")

            return success

    def _apply_strategy(self, strategy: AllocationStrategy) -> bool:
        """전략 적용"""
        try:
            new_subscriptions = []

            # 1. 시장 지수 구독 (최우선)
            market_indices = self._get_market_indices()
            for index_code in market_indices[:strategy.market_indices_count]:
                slot = SubscriptionSlot(
                    stock_code=index_code,
                    subscription_type=SubscriptionType.MARKET_INDEX,
                    priority=100,
                    strategy_name="market_index"
                )
                new_subscriptions.append(slot)

            # 2. 실시간 종목 (체결가 + 호가)
            realtime_candidates = self._select_realtime_candidates(strategy)
            for stock_code in realtime_candidates[:strategy.max_realtime_stocks]:
                # 체결가 구독
                price_slot = SubscriptionSlot(
                    stock_code=stock_code,
                    subscription_type=SubscriptionType.STOCK_PRICE,
                    priority=90,
                    strategy_name="realtime_core"
                )
                new_subscriptions.append(price_slot)

                # 호가 구독
                orderbook_slot = SubscriptionSlot(
                    stock_code=stock_code,
                    subscription_type=SubscriptionType.STOCK_ORDERBOOK,
                    priority=85,
                    strategy_name="realtime_core"
                )
                new_subscriptions.append(orderbook_slot)

            # 3. 준실시간 종목 (체결가만)
            semi_realtime_candidates = self._select_semi_realtime_candidates(strategy)
            for stock_code in semi_realtime_candidates[:strategy.max_semi_realtime_stocks]:
                slot = SubscriptionSlot(
                    stock_code=stock_code,
                    subscription_type=SubscriptionType.STOCK_PRICE,
                    priority=70,
                    strategy_name="semi_realtime"
                )
                new_subscriptions.append(slot)

            # 4. 구독 적용
            if len(new_subscriptions) <= self.MAX_SUBSCRIPTIONS:
                self.current_subscriptions.clear()
                for slot in new_subscriptions:
                    self.current_subscriptions[slot.subscription_key] = slot

                # WebSocket 구독 실행
                asyncio.create_task(self._subscribe_slots(new_subscriptions))

                return True
            else:
                logger.error(f"구독 수 초과: {len(new_subscriptions)} > {self.MAX_SUBSCRIPTIONS}")
                return False

        except Exception as e:
            logger.error(f"전략 적용 실패: {e}")
            return False

    def _get_market_indices(self) -> List[str]:
        """시장 지수 목록"""
        return [
            "KOSPI",      # 코스피 지수
            "KOSDAQ",     # 코스닥 지수
            "KRX100",     # KRX 100
            "KOSPI200",   # 코스피 200
            "USD"         # 달러 환율
        ]

    def _select_realtime_candidates(self, strategy: AllocationStrategy) -> List[str]:
        """실시간 후보 선택 (성과 기반)"""
        # 성과 점수 기반 정렬
        scored_stocks = self.performance_tracker.get_top_performers(
            strategy_weights=strategy.strategy_weights,
            count=strategy.max_realtime_stocks * 2
        )

        return [stock for stock, score in scored_stocks]

    def _select_semi_realtime_candidates(self, strategy: AllocationStrategy) -> List[str]:
        """준실시간 후보 선택"""
        # 실시간에서 제외된 중상위 종목들
        all_candidates = self.performance_tracker.get_all_candidates()
        realtime_stocks = set(self._select_realtime_candidates(strategy))

        semi_candidates = [
            stock for stock in all_candidates
            if stock not in realtime_stocks
        ]

        return semi_candidates[:strategy.max_semi_realtime_stocks]

    async def _subscribe_slots(self, slots: List[SubscriptionSlot]):
        """슬롯 목록 구독"""
        subscription_keys = [slot.subscription_key for slot in slots]

        try:
            for key in subscription_keys:
                await self.subscribe(key)

            logger.info(f"{len(subscription_keys)}건 WebSocket 구독 완료")
            self.stats['total_subscriptions'] = len(self.current_subscriptions)

        except Exception as e:
            logger.error(f"WebSocket 구독 실패: {e}")

    async def _unsubscribe_all(self):
        """모든 구독 해제"""
        try:
            for key in list(self.current_subscriptions.keys()):
                await self.unsubscribe(key)

            logger.info("모든 WebSocket 구독 해제 완료")

        except Exception as e:
            logger.error(f"구독 해제 실패: {e}")

    # ===== 긴급 구독 관리 =====

    def add_emergency_subscription(self, stock_code: str, strategy_name: str, priority: int = 95) -> bool:
        """긴급 구독 추가"""
        with self.subscription_lock:
            current_count = len(self.current_subscriptions)

            if current_count >= self.MAX_SUBSCRIPTIONS:
                # 가장 낮은 우선순위 종목 교체
                lowest_priority_slot = min(
                    self.current_subscriptions.values(),
                    key=lambda x: x.priority
                )

                if lowest_priority_slot.priority < priority:
                    # 교체 실행
                    self.remove_subscription(lowest_priority_slot.subscription_key)
                    logger.info(f"낮은 우선순위 종목 교체: {lowest_priority_slot.stock_code} → {stock_code}")
                else:
                    logger.warning(f"긴급 추가 실패: 우선순위 부족 ({priority} <= {lowest_priority_slot.priority})")
                    return False

            # 새 슬롯 추가
            slot = SubscriptionSlot(
                stock_code=stock_code,
                subscription_type=SubscriptionType.STOCK_PRICE,
                priority=priority,
                strategy_name=strategy_name
            )

            self.current_subscriptions[slot.subscription_key] = slot

            # WebSocket 구독
            asyncio.create_task(self.subscribe(slot.subscription_key))

            self.stats['emergency_additions'] += 1
            logger.info(f"긴급 구독 추가: {stock_code} (우선순위: {priority})")

            return True

    def remove_subscription(self, subscription_key: str) -> bool:
        """구독 제거"""
        with self.subscription_lock:
            if subscription_key in self.current_subscriptions:
                del self.current_subscriptions[subscription_key]

                # WebSocket 구독 해제
                asyncio.create_task(self.unsubscribe(subscription_key))

                return True

            return False

    # ===== 성과 관리 =====

    def update_performance_score(self, stock_code: str, score: float):
        """성과 점수 업데이트"""
        self.performance_tracker.update_score(stock_code, score)

        # 일정 주기마다 리밸런싱 트리거
        if self.rebalance_scheduler.should_rebalance():
            asyncio.create_task(self.rebalance_subscriptions())

    async def rebalance_subscriptions(self):
        """성과 기반 구독 리밸런싱"""
        if not self.current_strategy:
            return

        with self.subscription_lock:
            logger.info("성과 기반 구독 리밸런싱 시작")

            # 현재 성과 평가
            current_performance = {}
            for key, slot in self.current_subscriptions.items():
                score = self.performance_tracker.get_score(slot.stock_code)
                current_performance[key] = score

            # 하위 성과 종목 식별 (시장 지수 제외)
            non_index_slots = {
                k: v for k, v in current_performance.items()
                if self.current_subscriptions[k].subscription_type != SubscriptionType.MARKET_INDEX
            }

            bottom_performers = sorted(
                non_index_slots.items(),
                key=lambda x: x[1]
            )[:3]  # 하위 3개

            # 새로운 기회 종목 검색
            new_opportunities = self.performance_tracker.get_new_opportunities(count=5)

            # 교체 실행
            replacements = 0
            for (old_key, old_score), new_stock in zip(bottom_performers, new_opportunities):
                if new_stock[1] > old_score * 1.2:  # 20% 이상 성과 개선시에만
                    old_slot = self.current_subscriptions[old_key]

                    # 새 슬롯 생성
                    new_slot = SubscriptionSlot(
                        stock_code=new_stock[0],
                        subscription_type=old_slot.subscription_type,
                        priority=old_slot.priority + 5,
                        strategy_name="performance_upgrade"
                    )

                    # 교체
                    del self.current_subscriptions[old_key]
                    self.current_subscriptions[new_slot.subscription_key] = new_slot

                    # WebSocket 교체
                    await self.unsubscribe(old_key)
                    await self.subscribe(new_slot.subscription_key)

                    replacements += 1
                    logger.info(f"성과 기반 교체: {old_slot.stock_code} → {new_stock[0]}")

            self.stats['performance_upgrades'] += replacements
            logger.info(f"리밸런싱 완료: {replacements}건 교체")

    # ===== 데이터 처리 =====

    def _decrypt_data(self, encrypted_data: str, tr_id: str) -> str:
        """데이터 복호화"""
        try:
            # 암호화 키가 없으면 원본 그대로 반환
            if tr_id not in self.encryption_keys:
                return encrypted_data

            # AES 복호화
            key = self.encryption_keys[tr_id].encode('utf-8')
            cipher = AES.new(key, AES.MODE_ECB)

            # Base64 디코딩 후 복호화
            encrypted_bytes = b64decode(encrypted_data)
            decrypted_bytes = cipher.decrypt(encrypted_bytes)

            # 패딩 제거
            decrypted_data = unpad(decrypted_bytes, AES.block_size)

            return decrypted_data.decode('utf-8')

        except Exception as e:
            logger.warning(f"데이터 복호화 실패: {e}")
            return encrypted_data

    def _parse_real_time_data(self, message: str) -> Dict:
        """실시간 데이터 파싱"""
        try:
            data = json.loads(message)

            # 헤더 정보 추출
            header = data.get('header', {})
            tr_id = header.get('tr_id', '')

            # 바디 데이터 추출
            body = data.get('body', {})

            return {
                'tr_id': tr_id,
                'header': header,
                'body': body,
                'timestamp': datetime.now()
            }

        except Exception as e:
            logger.error(f"실시간 데이터 파싱 실패: {e}")
            return {}

    def add_callback(self, data_type: str, callback: Callable):
        """콜백 함수 추가"""
        if data_type in self.callbacks:
            self.callbacks[data_type].append(callback)

    def remove_callback(self, data_type: str, callback: Callable):
        """콜백 함수 제거"""
        if data_type in self.callbacks and callback in self.callbacks[data_type]:
            self.callbacks[data_type].remove(callback)

    async def _handle_message(self, message: str):
        """메시지 처리"""
        try:
            parsed_data = self._parse_real_time_data(message)

            if not parsed_data:
                return

            tr_id = parsed_data['tr_id']

            # TR ID별 처리
            if tr_id == SubscriptionType.STOCK_PRICE.value:
                await self._process_stock_price_data(parsed_data)
            elif tr_id == SubscriptionType.STOCK_ORDERBOOK.value:
                await self._process_stock_orderbook_data(parsed_data)
            elif tr_id == SubscriptionType.STOCK_EXECUTION.value:
                await self._process_stock_execution_data(parsed_data)

        except Exception as e:
            logger.error(f"메시지 처리 실패: {e}")

    async def _process_stock_price_data(self, data: Dict):
        """주식체결가 데이터 처리"""
        try:
            # 콜백 함수들 실행
            for callback in self.callbacks["stock_price"]:
                await callback(data)

        except Exception as e:
            logger.error(f"주식체결가 데이터 처리 실패: {e}")

    async def _process_stock_orderbook_data(self, data: Dict):
        """주식호가 데이터 처리"""
        try:
            # 콜백 함수들 실행
            for callback in self.callbacks["stock_orderbook"]:
                await callback(data)

        except Exception as e:
            logger.error(f"주식호가 데이터 처리 실패: {e}")

    async def _process_stock_execution_data(self, data: Dict):
        """주식체결통보 데이터 처리"""
        try:
            # 콜백 함수들 실행
            for callback in self.callbacks["stock_execution"]:
                await callback(data)

        except Exception as e:
            logger.error(f"주식체결통보 데이터 처리 실패: {e}")

    # ===== 메인 루프 =====

    async def start_listening(self):
        """실시간 데이터 수신 시작 (재연결 로직 포함)"""
        if not self.is_connected:
            logger.error("웹소켓이 연결되지 않았습니다.")
            return

        logger.info("실시간 데이터 수신 시작")

        # 장외시간 체크
        kst = pytz.timezone('Asia/Seoul')
        now = datetime.now(kst)

        if not KISRestAPIManager.is_market_open(now):
            logger.warning(f"🕐 장외시간 ({now.strftime('%Y-%m-%d %H:%M:%S')}): 웹소켓 연결 유지만 합니다")

        try:
            async for message in self.websocket:
                await self._handle_message(message)

        except websockets.exceptions.ConnectionClosed:
            logger.warning("웹소켓 연결이 끊어졌습니다.")
            self.is_connected = False

            # 장중이면 재연결 시도
            if KISRestAPIManager.is_market_open(datetime.now(kst)):
                logger.info("장중 재연결 시도...")
                await self._reconnect()
        except Exception as e:
            logger.error(f"데이터 수신 중 오류: {e}")

    async def _reconnect(self, max_attempts: int = 3):
        """웹소켓 재연결 시도"""
        for attempt in range(max_attempts):
            try:
                logger.info(f"재연결 시도 {attempt + 1}/{max_attempts}")
                await asyncio.sleep(5)  # 5초 대기

                if await self.connect():
                    logger.info("재연결 성공")
                    return True

            except Exception as e:
                logger.warning(f"재연결 시도 {attempt + 1} 실패: {e}")

        logger.error(f"재연결 실패 - {max_attempts}회 시도 모두 실패")
        return False

    # ===== 상태 조회 =====

    def get_subscription_status(self) -> Dict:
        """구독 상태 조회"""
        with self.subscription_lock:
            status = {
                'total_subscriptions': len(self.current_subscriptions),
                'available_slots': self.MAX_SUBSCRIPTIONS - len(self.current_subscriptions),
                'current_strategy': self.current_strategy.name if self.current_strategy else None,
                'time_slot': self.get_current_time_slot().value,
                'statistics': self.stats.copy(),
                'is_connected': self.is_connected
            }

            # 타입별 분류
            type_counts = defaultdict(int)
            for slot in self.current_subscriptions.values():
                type_counts[slot.subscription_type.value] += 1

            status['subscription_breakdown'] = dict(type_counts)

            return status


class PerformanceTracker:
    """성과 추적기"""

    def __init__(self):
        self.stock_scores: Dict[str, float] = {}
        self.score_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=20))
        self.last_update: Dict[str, datetime] = {}

    def update_score(self, stock_code: str, score: float):
        """점수 업데이트"""
        self.stock_scores[stock_code] = score
        self.score_history[stock_code].append((datetime.now(), score))
        self.last_update[stock_code] = datetime.now()

    def get_score(self, stock_code: str) -> float:
        """점수 조회"""
        return self.stock_scores.get(stock_code, 0.0)

    def get_top_performers(self, strategy_weights: Dict[str, float], count: int = 10) -> List[Tuple[str, float]]:
        """상위 성과 종목 조회"""
        # 실제로는 각 전략별 점수를 가중평균하여 계산
        # 여기서는 단순화된 버전
        sorted_stocks = sorted(
            self.stock_scores.items(),
            key=lambda x: x[1],
            reverse=True
        )

        return sorted_stocks[:count]

    def get_all_candidates(self) -> List[str]:
        """모든 후보 종목 조회"""
        return list(self.stock_scores.keys())

    def get_new_opportunities(self, count: int = 5) -> List[Tuple[str, float]]:
        """새로운 기회 종목 조회"""
        # 최근에 점수가 급상승한 종목들
        recent_gainers = []

        for stock_code, history in self.score_history.items():
            if len(history) >= 2:
                recent_score = history[-1][1]
                old_score = history[-2][1]

                if recent_score > old_score * 1.1:  # 10% 이상 증가
                    recent_gainers.append((stock_code, recent_score))

        return sorted(recent_gainers, key=lambda x: x[1], reverse=True)[:count]


class RebalanceScheduler:
    """리밸런싱 스케줄러"""

    def __init__(self, manager: KISWebSocketManager):
        self.manager = manager
        self.last_rebalance = datetime.now()
        self.rebalance_interval = timedelta(minutes=5)  # 5분마다
        self.rebalance_count = 0

    def should_rebalance(self) -> bool:
        """리밸런싱 필요 여부"""
        now = datetime.now()

        if now - self.last_rebalance >= self.rebalance_interval:
            self.last_rebalance = now
            self.rebalance_count += 1
            return True

        return False
