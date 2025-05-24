"""
시간대별 전략 스케줄러
각 시간대 이전에 종목 탐색을 완료하고 전략을 전환하는 시스템
별도 스레드를 사용하여 메인 스레드 차단 방지
하드코딩된 종목 제거하고 REST API 동적 발굴 적용
"""
import asyncio
import configparser
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING
from dataclasses import dataclass, field
from enum import Enum
import pytz
from utils.logger import setup_logger
from core.rest_api_manager import KISRestAPIManager
from core.hybrid_data_manager import HybridDataManager, DataPriority

# 순환 import 방지를 위한 TYPE_CHECKING
if TYPE_CHECKING:
    from main import StockBotMain

# 데이터베이스 관리
from database.db_manager import db_manager
from database.db_models import TimeSlot as DBTimeSlot

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')

logger = setup_logger(__name__)

class StrategyPhase(Enum):
    """전략 단계"""
    PREPARATION = "preparation"    # 준비 단계 (종목 탐색)
    EXECUTION = "execution"        # 실행 단계 (실제 거래)
    TRANSITION = "transition"      # 전환 단계 (다음 전략 준비)

@dataclass
class TimeSlotConfig:
    """시간대별 설정"""
    name: str
    start_time: time
    end_time: time
    description: str
    primary_strategies: Dict[str, float]
    secondary_strategies: Dict[str, float]
    preparation_time: int = 15  # 사전 준비 시간(분)

@dataclass
class StockCandidate:
    """종목 후보"""
    stock_code: str
    strategy_type: str
    score: float
    reason: str
    discovered_at: datetime
    data: Dict = field(default_factory=dict)

class StrategyScheduler:
    """시간대별 전략 스케줄러 - 동적 종목 발굴 버전"""

    def __init__(self, trading_api: KISRestAPIManager, data_manager: HybridDataManager):
        self.trading_api = trading_api
        self.data_manager = data_manager

        # 설정 로드
        self.config = configparser.ConfigParser()
        self.config.read('config/settings.ini', encoding='utf-8')

        # 스케줄러 상태
        self.scheduler_running = False
        self.screening_active = False
        self.current_slot: Optional[TimeSlotConfig] = None
        self.current_phase = StrategyPhase.PREPARATION
        self.preparation_completed = False

        # 시간대별 설정
        self.time_slots = self._load_time_slot_configs()

        # 종목 후보 관리 (스레드 안전)
        self.candidates: Dict[str, List[StockCandidate]] = {}
        self.active_stocks: Dict[str, List[str]] = {}
        self.discovery_lock = threading.RLock()

        # 스레드 풀 - 종목 탐색용 (메인 스레드 차단 방지)
        self.discovery_executor = ThreadPoolExecutor(
            max_workers=3,
            thread_name_prefix="discovery"
        )
        # 백그라운드 스크리닝용 스레드 풀
        self.screening_executor = ThreadPoolExecutor(
            max_workers=2,
            thread_name_prefix="screening"
        )

        logger.info("🕐 전략 스케줄러 초기화 완료")

    def _load_time_slot_configs(self) -> Dict[str, TimeSlotConfig]:
        """시간대별 설정 로드"""
        slots = {}

        # 골든타임 (09:00-09:30)
        slots['golden_time'] = TimeSlotConfig(
            name='golden_time',
            start_time=time(9, 0),
            end_time=time(9, 30),
            description='골든타임 - 갭 트레이딩 집중',
            primary_strategies={'gap_trading': 0.7},
            secondary_strategies={'volume_breakout': 0.3}
        )

        # 주도주 시간 (09:30-11:30)
        slots['morning_leaders'] = TimeSlotConfig(
            name='morning_leaders',
            start_time=time(9, 30),
            end_time=time(11, 30),
            description='주도주 시간 - 거래량 돌파',
            primary_strategies={'volume_breakout': 0.6},
            secondary_strategies={'momentum': 0.4}
        )

        # 점심시간 (11:30-14:00)
        slots['lunch_time'] = TimeSlotConfig(
            name='lunch_time',
            start_time=time(11, 30),
            end_time=time(14, 0),
            description='점심시간 - 안정적 모멘텀',
            primary_strategies={'momentum': 0.5},
            secondary_strategies={'gap_trading': 0.3, 'volume_breakout': 0.2}
        )

        # 마감 추세 (14:00-15:20)
        slots['closing_trend'] = TimeSlotConfig(
            name='closing_trend',
            start_time=time(14, 0),
            end_time=time(15, 20),
            description='마감 추세 - 모멘텀 강화',
            primary_strategies={'momentum': 0.8},
            secondary_strategies={'volume_breakout': 0.2}
        )

        return slots

    async def start_scheduler(self):
        """스케줄러 시작 - 초기화 + 메인 루프"""
        try:
            # 1. 초기화 단계
            await self._initialize_scheduler()

            # 2. 메인 스케줄링 루프
            await self._run_main_loop()

        except Exception as e:
            logger.error(f"❌ 스케줄러 시작 실패: {e}")
            raise
        finally:
            logger.info("🛑 스케줄러 종료")

    async def _initialize_scheduler(self):
        """스케줄러 초기화"""
        self.scheduler_running = True
        logger.info("🕐 전략 스케줄러 시작")

        # 1. 백그라운드 스크리닝 시작
        await self._start_background_screening()
        logger.info("📊 백그라운드 스크리닝 활성화")

        # 2. 즉시 첫 번째 스케줄링 실행 (30초 대기 없이)
        await self._schedule_loop()
        logger.info("⚡ 초기 전략 활성화 완료")

    async def _run_main_loop(self):
        """메인 스케줄링 루프 - 하루 4번 전략 전환"""
        logger.info("🔄 메인 스케줄링 루프 시작 (시간대별 전환)")

        consecutive_errors = 0
        max_consecutive_errors = 3

        while self.scheduler_running:
            try:
                # 다음 전략 준비 시간까지 대기
                next_preparation_time = self._get_next_preparation_time()

                if next_preparation_time:
                    sleep_seconds = self._calculate_sleep_time(next_preparation_time)

                    if sleep_seconds > 60:  # 1분 이상 남은 경우
                        logger.info(f"⏰ 다음 전략 준비까지 {sleep_seconds//60}분 {sleep_seconds%60}초 대기")

                        # 긴 대기 시간은 1분 단위로 나누어 체크 (중간에 중단 가능)
                        while sleep_seconds > 0 and self.scheduler_running:
                            wait_time = min(60, sleep_seconds)  # 최대 1분씩 대기
                            await asyncio.sleep(wait_time)
                            sleep_seconds -= wait_time
                    else:
                        await asyncio.sleep(sleep_seconds)

                    # 전략 준비 시간 도달 - 스케줄링 실행
                    if self.scheduler_running:
                        await self._schedule_loop()
                else:
                    # 장외 시간 - 더 긴 간격으로 대기
                    logger.info("💤 장외 시간 - 30분 대기")
                    await asyncio.sleep(1800)  # 30분 대기

                # 성공 시 오류 카운터 리셋
                consecutive_errors = 0

            except Exception as e:
                consecutive_errors += 1
                logger.error(f"스케줄러 오류 ({consecutive_errors}/{max_consecutive_errors}): {e}")

                # 연속 오류 시 점진적 대기 시간 증가
                if consecutive_errors >= max_consecutive_errors:
                    logger.warning(f"⚠️ 연속 오류 {max_consecutive_errors}회 - 시스템 안정화 대기")
                    await asyncio.sleep(300)  # 5분 대기
                    consecutive_errors = 0
                else:
                    backoff_time = min(30 * consecutive_errors, 120)
                    await asyncio.sleep(backoff_time)

    def _get_next_preparation_time(self) -> Optional[datetime]:
        """다음 전략 준비 시간 계산"""
        now = datetime.now(KST)
        today = now.date()

        # 시간대별 준비 시간 (시작 15분 전)
        preparation_times = [
            datetime.combine(today, time(8, 45)),   # 골든타임 준비 (08:45)
            datetime.combine(today, time(9, 15)),   # 주도주 시간 준비 (09:15)
            datetime.combine(today, time(11, 15)),  # 점심시간 준비 (11:15)
            datetime.combine(today, time(13, 45)),  # 마감 추세 준비 (13:45)
        ]

        # 한국 시간대 적용
        preparation_times = [KST.localize(dt) for dt in preparation_times]

        # 현재 시간 이후의 가장 가까운 준비 시간 찾기
        for prep_time in preparation_times:
            if prep_time > now:
                return prep_time

        # 오늘의 모든 준비 시간이 지났으면 다음 날 첫 번째 시간
        tomorrow = today + timedelta(days=1)
        next_day_first = datetime.combine(tomorrow, time(8, 45))
        return KST.localize(next_day_first)

    def _calculate_sleep_time(self, target_time: datetime) -> int:
        """대상 시간까지의 대기 시간 계산 (초)"""
        now = datetime.now(KST)
        time_diff = target_time - now
        return max(0, int(time_diff.total_seconds()))

    async def _start_background_screening(self):
        """백그라운드 스크리닝 시작 - 동적 종목 발굴"""
        self.screening_active = True

        # 전체 시장 스크리닝을 별도 스레드에서 실행
        loop = asyncio.get_event_loop()
        screening_future = loop.run_in_executor(
            self.screening_executor,
            self._background_screening_sync
        )

        logger.info("📊 백그라운드 시장 스크리닝 시작 (동적 발굴)")

        # 주기적 스크리닝 (5분마다)
        asyncio.create_task(self._periodic_screening())

    def _background_screening_sync(self):
        """백그라운드 스크리닝 (동기 버전)"""
        try:
            while self.screening_active:
                # 전체 시장 스크리닝
                screening_results = self.trading_api.get_market_screening_candidates("all")

                # 백그라운드 종목들을 데이터 매니저에 추가
                background_data = screening_results.get('background', [])
                if isinstance(background_data, list):
                    background_dict = {
                        'volume_leaders': background_data[:15],
                        'price_movers': background_data[15:30] if len(background_data) > 15 else [],
                        'bid_ask_leaders': background_data[30:40] if len(background_data) > 30 else []
                    }
                    background_data = background_dict

                # 거래량 급증 종목들
                volume_leaders = background_data.get('volume_leaders', [])
                for stock_data in volume_leaders[:15]:  # 상위 15개
                    stock_code = stock_data['stock_code']
                    if stock_data['volume_ratio'] >= 200:  # 2배 이상
                        self.data_manager.add_stock_request(
                            stock_code=stock_code,
                            priority=DataPriority.BACKGROUND,
                            strategy_name="background_screening",
                            callback=self.background_screening_callback
                        )

                # 등락률 상위 종목들
                price_movers = background_data.get('price_movers', [])
                for stock_data in price_movers[:15]:  # 상위 15개
                    stock_code = stock_data['stock_code']
                    if abs(stock_data['change_rate']) >= 3.0:  # 3% 이상
                        self.data_manager.add_stock_request(
                            stock_code=stock_code,
                            priority=DataPriority.BACKGROUND,
                            strategy_name="background_screening",
                            callback=self.background_screening_callback
                        )

                # 호가 잔량 주요 종목들
                bid_ask_leaders = background_data.get('bid_ask_leaders', [])
                for stock_data in bid_ask_leaders[:10]:  # 상위 10개
                    stock_code = stock_data['stock_code']
                    if stock_data['buying_pressure'] == 'STRONG':
                        self.data_manager.add_stock_request(
                            stock_code=stock_code,
                            priority=DataPriority.BACKGROUND,
                            strategy_name="background_screening",
                            callback=self.background_screening_callback
                        )

                logger.info(f"📊 백그라운드 스크리닝 완료 - 거래량:{len(volume_leaders)}, 등락률:{len(price_movers)}, 호가:{len(bid_ask_leaders)}")

                # 5분 대기
                import time
                time.sleep(300)

        except Exception as e:
            logger.error(f"백그라운드 스크리닝 오류: {e}")

    async def _periodic_screening(self):
        """주기적 스크리닝 (비동기)"""
        while self.screening_active:
            try:
                await asyncio.sleep(300)  # 5분 대기

                # 새로운 스크리닝 실행
                loop = asyncio.get_event_loop()
                screening_future = loop.run_in_executor(
                    self.screening_executor,
                    self._background_screening_sync
                )

            except Exception as e:
                logger.error(f"주기적 스크리닝 오류: {e}")

    def background_screening_callback(self, stock_code: str, data: Dict, source: str):
        """백그라운드 스크리닝 콜백"""
        try:
            # 주목할 만한 움직임 감지시 우선순위 상향
            if 'current_price' not in data:
                return

            price_data = data['current_price']
            change_rate = price_data.get('change_rate', 0)
            volume = price_data.get('volume', 0)

            # 거래량 급증 (3배 이상) 감지
            avg_volume = volume / 3  # 임시 평균
            volume_ratio = volume / avg_volume if avg_volume > 0 else 0

            if volume_ratio >= 5.0:  # 5배 이상 급증
                candidate = StockCandidate(
                    stock_code=stock_code,
                    strategy_type='emergency_volume',
                    score=volume_ratio / 3.0,
                    reason=f"거래량{volume_ratio:.1f}배 폭증",
                    discovered_at=datetime.now(),
                    data=data
                )
                self._add_discovered_candidate(candidate)

                # MEDIUM 우선순위로 승격
                self.data_manager.upgrade_priority(stock_code, DataPriority.MEDIUM)
                logger.info(f"🚨 긴급 거래량 급증: {stock_code} {volume_ratio:.1f}배 → MEDIUM 우선순위")

            # 급격한 가격 변동 (5% 이상) 감지
            if abs(change_rate) >= 5.0:
                candidate = StockCandidate(
                    stock_code=stock_code,
                    strategy_type='emergency_price',
                    score=abs(change_rate) / 2.0,
                    reason=f"급격한 변동{change_rate:+.1f}%",
                    discovered_at=datetime.now(),
                    data=data
                )
                self._add_discovered_candidate(candidate)

                if abs(change_rate) >= 8.0:  # 8% 이상은 HIGH 우선순위
                    self.data_manager.upgrade_priority(stock_code, DataPriority.HIGH)
                    logger.info(f"🚨 급격한 가격 변동: {stock_code} {change_rate:+.1f}% → HIGH 우선순위")

        except Exception as e:
            logger.error(f"백그라운드 스크리닝 오류: {e}")

    def _add_discovered_candidate(self, candidate: StockCandidate):
        """발견된 후보 추가 (스레드 안전)"""
        with self.discovery_lock:
            strategy_type = candidate.strategy_type
            if strategy_type not in self.candidates:
                self.candidates[strategy_type] = []

            # 중복 제거 및 점수순 정렬
            existing_codes = {c.stock_code for c in self.candidates[strategy_type]}
            if candidate.stock_code not in existing_codes:
                self.candidates[strategy_type].append(candidate)
                self.candidates[strategy_type].sort(key=lambda x: x.score, reverse=True)

                # 최대 20개까지만 유지
                self.candidates[strategy_type] = self.candidates[strategy_type][:20]

    def stop_scheduler(self):
        """스케줄러 중지"""
        self.scheduler_running = False
        self.screening_active = False

        # 스레드 풀 종료
        self.discovery_executor.shutdown(wait=True)
        self.screening_executor.shutdown(wait=True)

        logger.info("🛑 전략 스케줄러 중지")

    async def _schedule_loop(self):
        """스케줄링 메인 루프 - 시간대 전환 시에만 실행"""
        now = datetime.now(KST)
        current_time = now.time()

        logger.info(f"🔄 전략 스케줄링 실행: {current_time.strftime('%H:%M:%S')}")

        # 시간대 변경 체크
        new_slot = self._get_time_slot_for_time(current_time)

        if new_slot != self.current_slot:
            logger.info(f"🔄 시간대 변경: {self.current_slot.name if self.current_slot else 'None'} → {new_slot.name if new_slot else 'None'}")

            if new_slot:
                # 새 시간대 시작
                await self._start_new_time_slot(new_slot)
            else:
                # 장외 시간
                await self._handle_after_hours()

        # 현재 시간대가 있고 아직 준비가 완료되지 않았다면 준비 상태 체크
        elif self.current_slot and not self.preparation_completed:
            await self._check_preparation_status()

        # 이미 준비가 완료된 상태라면 로그만 출력
        elif self.current_slot and self.preparation_completed:
            logger.info(f"✅ {self.current_slot.name} 전략 실행 중 (준비 완료)")
        else:
            logger.info("⏸️ 대기 상태 - 해당 시간대 없음")

    def _get_time_slot_for_time(self, current_time: time) -> Optional[TimeSlotConfig]:
        """현재 시간에 해당하는 시간대 반환"""
        for slot in self.time_slots.values():
            if slot.start_time <= current_time <= slot.end_time:
                return slot
        return None

    async def _start_new_time_slot(self, slot: TimeSlotConfig):
        """새 시간대 시작"""
        self.current_slot = slot
        self.current_phase = StrategyPhase.PREPARATION
        self.preparation_completed = False

        logger.info(f"📅 새 시간대 시작: {slot.description}")

        # 이전 전략 정리
        await self._cleanup_previous_strategy()

        # 새 전략을 위한 종목 탐색 시작 (별도 스레드에서!)
        await self._start_stock_discovery_threaded()

    async def _cleanup_previous_strategy(self):
        """이전 전략 정리"""
        # 기존 활성 종목들을 백그라운드로 전환
        for strategy_type, stocks in self.active_stocks.items():
            for stock_code in stocks:
                self.data_manager.upgrade_priority(stock_code, DataPriority.BACKGROUND)

        # 활성 종목 초기화
        self.active_stocks.clear()
        logger.info("🧹 이전 전략 정리 완료")

    async def _start_stock_discovery_threaded(self):
        """종목 탐색 시작 (별도 스레드에서 실행)"""
        if not self.current_slot:
            return

        logger.info(f"🔍 종목 탐색 시작: {self.current_slot.description}")

        # 스레드에서 실행할 태스크들 준비
        loop = asyncio.get_event_loop()
        discovery_futures = []

        # 주요 전략 탐색
        for strategy_name, weight in self.current_slot.primary_strategies.items():
            future = loop.run_in_executor(
                self.discovery_executor,
                self._discover_stocks_sync,
                strategy_name, weight, True
            )
            discovery_futures.append(future)

        # 보조 전략 탐색
        for strategy_name, weight in self.current_slot.secondary_strategies.items():
            future = loop.run_in_executor(
                self.discovery_executor,
                self._discover_stocks_sync,
                strategy_name, weight, False
            )
            discovery_futures.append(future)

        # 모든 탐색 완료 대기
        results = await asyncio.gather(*discovery_futures, return_exceptions=True)

        # 결과 처리
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"종목 탐색 오류: {result}")

        # 탐색 완료 후 전략 활성화
        await self._activate_discovered_strategies()

    def _discover_stocks_sync(self, strategy_name: str, weight: float, is_primary: bool) -> List[StockCandidate]:
        """동기 방식 종목 탐색 (스레드에서 실행) - 동적 발굴 적용"""
        try:
            if strategy_name == 'gap_trading':
                candidates = self._discover_gap_candidates_sync()
            elif strategy_name == 'volume_breakout':
                candidates = self._discover_volume_candidates_sync()
            elif strategy_name == 'momentum':
                candidates = self._discover_momentum_candidates_sync()
            else:
                logger.warning(f"알 수 없는 전략: {strategy_name}")
                return []

            # 스레드 안전하게 후보 저장
            with self.discovery_lock:
                self.candidates[strategy_name] = candidates

            logger.info(f"✅ {strategy_name} 탐색 완료: {len(candidates)}개 후보")
            return candidates

        except Exception as e:
            logger.error(f"{strategy_name} 탐색 중 오류: {e}")
            return []

    def _discover_gap_candidates_sync(self) -> List[StockCandidate]:
        """갭 트레이딩 후보 탐색 (동기) - REST API 동적 발굴"""
        candidates = []

        try:
            # 갭 조건 설정 (config에서 로드)
            gap_config = self.config['gap_trading_config']
            min_gap = float(gap_config.get('min_gap_percent', '3.0'))
            max_gap = float(gap_config.get('max_gap_percent', '15.0'))
            min_volume_ratio = float(gap_config.get('min_volume_ratio', '2.0'))

            # REST API를 통한 동적 갭 트레이딩 후보 발굴
            gap_candidates = self.trading_api.discover_gap_trading_candidates(
                gap_min=min_gap,
                gap_max=max_gap,
                volume_ratio_min=min_volume_ratio
            )

            # StockCandidate 객체로 변환
            for gap_data in gap_candidates:
                candidate = StockCandidate(
                    stock_code=gap_data['stock_code'],
                    strategy_type='gap_trading',
                    score=gap_data['score'],
                    reason=f"갭{gap_data['gap_rate']:+.1f}% 거래량{gap_data['volume_ratio']:.1f}배",
                    discovered_at=datetime.now(),
                    data=gap_data
                )
                candidates.append(candidate)

            logger.info(f"🔍 갭 트레이딩 후보 발굴: {len(candidates)}개")

        except Exception as e:
            logger.error(f"갭 트레이딩 후보 탐색 중 오류: {e}")

        return candidates

    def _discover_volume_candidates_sync(self) -> List[StockCandidate]:
        """거래량 돌파 후보 탐색 (동기) - REST API 동적 발굴"""
        candidates = []

        try:
            # 거래량 조건 설정 (config에서 로드)
            volume_config = self.config['volume_breakout_config']
            min_volume_ratio = float(volume_config.get('min_volume_ratio', '3.0'))
            min_price_change = float(volume_config.get('min_price_change', '1.0'))

            # REST API를 통한 동적 거래량 돌파 후보 발굴
            volume_candidates = self.trading_api.discover_volume_breakout_candidates(
                volume_ratio_min=min_volume_ratio,
                price_change_min=min_price_change
            )

            # StockCandidate 객체로 변환
            for volume_data in volume_candidates:
                candidate = StockCandidate(
                    stock_code=volume_data['stock_code'],
                    strategy_type='volume_breakout',
                    score=volume_data['score'],
                    reason=f"거래량{volume_data['volume_ratio']:.1f}배 변동{volume_data['change_rate']:+.1f}%",
                    discovered_at=datetime.now(),
                    data=volume_data
                )
                candidates.append(candidate)

            logger.info(f"🚀 거래량 돌파 후보 발굴: {len(candidates)}개")

        except Exception as e:
            logger.error(f"거래량 돌파 후보 탐색 중 오류: {e}")

        return candidates

    def _discover_momentum_candidates_sync(self) -> List[StockCandidate]:
        """모멘텀 후보 탐색 (동기) - REST API 동적 발굴"""
        candidates = []

        try:
            # 모멘텀 조건 설정 (config에서 로드)
            momentum_config = self.config['momentum_config']
            min_change_rate = float(momentum_config.get('min_momentum_percent', '1.5'))
            min_volume_ratio = float(momentum_config.get('min_volume_ratio', '1.5'))

            # REST API를 통한 동적 모멘텀 후보 발굴
            momentum_candidates = self.trading_api.discover_momentum_candidates(
                min_change_rate=min_change_rate,
                min_volume_ratio=min_volume_ratio
            )

            # StockCandidate 객체로 변환
            for momentum_data in momentum_candidates:
                candidate = StockCandidate(
                    stock_code=momentum_data['stock_code'],
                    strategy_type='momentum',
                    score=momentum_data['score'],
                    reason=f"모멘텀{momentum_data['change_rate']:+.1f}% {momentum_data['trend_quality']}",
                    discovered_at=datetime.now(),
                    data=momentum_data
                )
                candidates.append(candidate)

            logger.info(f"📈 모멘텀 후보 발굴: {len(candidates)}개")

        except Exception as e:
            logger.error(f"모멘텀 후보 탐색 중 오류: {e}")

        return candidates

    async def _activate_discovered_strategies(self):
        """탐색된 전략 활성화"""
        if not self.current_slot:
            return

        logger.info("🚀 전략 활성화 시작")

        # 주요 전략 활성화
        for strategy_name, weight in self.current_slot.primary_strategies.items():
            candidates = self.candidates.get(strategy_name, [])
            if candidates:
                await self._activate_strategy_candidates(strategy_name, candidates, DataPriority.CRITICAL, weight)

        # 보조 전략 활성화
        for strategy_name, weight in self.current_slot.secondary_strategies.items():
            candidates = self.candidates.get(strategy_name, [])
            if candidates:
                await self._activate_strategy_candidates(strategy_name, candidates, DataPriority.HIGH, weight)

        self.preparation_completed = True
        self.current_phase = StrategyPhase.EXECUTION

        logger.info("✅ 전략 활성화 완료")

    async def _activate_strategy_candidates(self, strategy_name: str, candidates: List[StockCandidate],
                                         priority: DataPriority, weight: float):
        """전략 후보들을 활성화"""
        activated_stocks = []

        # 우선순위에 따른 종목 수 제한
        max_stocks = 8 if priority == DataPriority.CRITICAL else 15

        # 현재 시간대를 DB 시간대 enum으로 변환
        current_db_slot = self._convert_to_db_timeslot(self.current_slot.name if self.current_slot else 'golden_time')

        for candidate in candidates[:max_stocks]:
            success = self.data_manager.add_stock_request(
                stock_code=candidate.stock_code,
                priority=priority,
                strategy_name=strategy_name,
                callback=self._create_strategy_callback(strategy_name)
            )

            if success:
                activated_stocks.append(candidate.stock_code)

                # 데이터베이스에 선택된 종목 기록
                self._record_selected_stock(candidate, current_db_slot, priority, success)

                logger.info(f"📊 {strategy_name} 활성화: {candidate.stock_code} ({candidate.reason})")

        self.active_stocks[strategy_name] = activated_stocks
        logger.info(f"✅ {strategy_name} 전략: {len(activated_stocks)}개 종목 활성화")

    def _convert_to_db_timeslot(self, slot_name: str) -> DBTimeSlot:
        """시간대 이름을 DB TimeSlot enum으로 변환"""
        mapping = {
            'golden_time': DBTimeSlot.GOLDEN_TIME,
            'morning_leaders': DBTimeSlot.MORNING_LEADERS,
            'lunch_time': DBTimeSlot.LUNCH_TIME,
            'closing_trend': DBTimeSlot.CLOSING_TREND,
        }
        return mapping.get(slot_name, DBTimeSlot.GOLDEN_TIME)

    def _record_selected_stock(self, candidate: StockCandidate, time_slot: DBTimeSlot,
                             priority: DataPriority, activation_success: bool):
        """선택된 종목을 데이터베이스에 기록"""
        try:
            stock_data = {
                'stock_code': candidate.stock_code,
                'stock_name': candidate.data.get('stock_name', ''),
                'strategy_type': candidate.strategy_type,
                'score': candidate.score,
                'reason': candidate.reason,
                'priority': priority.value,
                'current_price': candidate.data.get('current_price', 0),
                'change_rate': candidate.data.get('change_rate', 0),
                'volume': candidate.data.get('volume', 0),
                'activation_success': activation_success,
            }

            # 전략별 특화 데이터 추가
            if candidate.strategy_type == 'gap_trading':
                stock_data.update({
                    'volume_ratio': candidate.data.get('volume_ratio'),
                    'gap_rate': candidate.data.get('gap_rate'),
                })
            elif candidate.strategy_type == 'volume_breakout':
                stock_data.update({
                    'volume_ratio': candidate.data.get('volume_ratio'),
                    'breakout_direction': candidate.data.get('breakout_direction'),
                    'buying_pressure': candidate.data.get('buying_pressure'),
                })
            elif candidate.strategy_type == 'momentum':
                stock_data.update({
                    'momentum_strength': candidate.data.get('momentum_strength'),
                    'trend_quality': candidate.data.get('trend_quality'),
                    'consecutive_up_days': candidate.data.get('consecutive_up_days'),
                    'ma_position': candidate.data.get('ma_position'),
                })

            # DB에 기록
            success = db_manager.record_selected_stock(time_slot, stock_data)
            if success:
                logger.debug(f"💾 종목 선택 기록: {candidate.stock_code} ({time_slot.value})")
            else:
                logger.warning(f"⚠️ 종목 선택 기록 실패: {candidate.stock_code}")

        except Exception as e:
            logger.error(f"종목 선택 기록 중 오류: {e}")

    def _create_strategy_callback(self, strategy_name: str):
        """전략별 콜백 함수 생성 - 실제 거래 신호 처리"""
        def callback(stock_code: str, data: Dict, source: str):
            try:
                # 현재 포지션이 있는지 확인
                bot_instance = getattr(self, '_bot_instance', None)
                if not bot_instance:
                    return

                # 실시간 데이터에서 필요한 정보 추출
                current_price_data = data.get('current_price', {})
                if not current_price_data:
                    return

                current_price = current_price_data.get('current_price', 0)
                if current_price <= 0:
                    return

                # 포지션 확인
                has_position = stock_code in bot_instance.positions

                # 전략별 신호 생성 및 처리
                signal = self._generate_trading_signal(
                    strategy_name, stock_code, data, source, has_position
                )

                if signal:
                    # 신호를 main의 거래 큐에 전송 (주문 실행은 monitor_positions에서)
                    signal['stock_code'] = stock_code
                    signal['strategy_type'] = f"signal_{strategy_name}"
                    asyncio.create_task(
                        bot_instance.add_trading_signal(signal)
                    )

            except Exception as e:
                logger.error(f"{strategy_name} 콜백 오류: {e}")

        return callback

    def _generate_trading_signal(self, strategy_name: str, stock_code: str,
                               data: Dict, source: str, has_position: bool) -> Optional[Dict]:
        """실시간 데이터 기반 거래 신호 생성"""
        try:
            current_price_data = data.get('current_price', {})
            current_price = current_price_data.get('current_price', 0)
            change_rate = current_price_data.get('change_rate', 0)
            volume = current_price_data.get('volume', 0)

            # 기본 필터링
            if current_price <= 0:
                return None

            # 이미 포지션이 있는 경우 매도 신호만 고려
            if has_position:
                return self._check_sell_signal(strategy_name, stock_code, data)

            # 포지션이 없는 경우 매수 신호 고려
            return self._check_buy_signal(strategy_name, stock_code, data)

        except Exception as e:
            logger.error(f"거래 신호 생성 오류 ({strategy_name}, {stock_code}): {e}")
            return None

    def _check_buy_signal(self, strategy_name: str, stock_code: str, data: Dict) -> Optional[Dict]:
        """매수 신호 확인"""
        current_price_data = data.get('current_price', {})
        current_price = current_price_data.get('current_price', 0)
        change_rate = current_price_data.get('change_rate', 0)
        volume = current_price_data.get('volume', 0)

        # 전략별 매수 조건 확인
        if strategy_name == 'gap_trading':
            # 갭 상승 + 거래량 급증
            if change_rate >= 3.0 and volume > 0:  # 3% 이상 상승
                return {
                    'action': 'BUY',
                    'price': current_price,
                    'reason': f'갭 상승 {change_rate:.1f}% + 거래량 급증',
                    'strength': min(change_rate / 10.0, 1.0)
                }

        elif strategy_name == 'volume_breakout':
            # 거래량 돌파 + 가격 상승
            if change_rate >= 2.0 and volume > 0:  # 2% 이상 상승
                return {
                    'action': 'BUY',
                    'price': current_price,
                    'reason': f'거래량 돌파 + {change_rate:.1f}% 상승',
                    'strength': min(change_rate / 8.0, 1.0)
                }

        elif strategy_name == 'momentum':
            # 모멘텀 지속 + 상승세
            if change_rate >= 1.5 and volume > 0:  # 1.5% 이상 상승
                return {
                    'action': 'BUY',
                    'price': current_price,
                    'reason': f'모멘텀 지속 {change_rate:.1f}%',
                    'strength': min(change_rate / 6.0, 1.0)
                }

        return None

    def _check_sell_signal(self, strategy_name: str, stock_code: str, data: Dict) -> Optional[Dict]:
        """매도 신호 확인 (포지션 보유 시)"""
        current_price_data = data.get('current_price', {})
        current_price = current_price_data.get('current_price', 0)
        change_rate = current_price_data.get('change_rate', 0)

        # 급락 시 매도 신호
        if change_rate <= -2.0:  # 2% 이상 하락
            return {
                'action': 'SELL',
                'price': current_price,
                'reason': f'급락 신호 {change_rate:.1f}%',
                'strength': min(abs(change_rate) / 5.0, 1.0)
            }

        # 전략별 매도 조건
        if strategy_name == 'gap_trading':
            # 갭 상승 후 반전 신호
            if change_rate <= -1.0:
                return {
                    'action': 'SELL',
                    'price': current_price,
                    'reason': f'갭 반전 {change_rate:.1f}%',
                    'strength': 0.7
                }

        return None

    def set_bot_instance(self, bot_instance: 'StockBotMain'):
        """봇 인스턴스 설정 (콜백에서 사용)"""
        self._bot_instance = bot_instance

    async def _check_preparation_status(self):
        """준비 상태 체크 - 종목 탐색 진행률 및 타임아웃 관리"""
        if not self.current_slot:
            return

        # 현재 시간과 시간대 시작 시간 비교
        now = datetime.now()
        current_time = now.time()

        # 시간대 시작 후 경과 시간 계산
        slot_start = datetime.combine(now.date(), self.current_slot.start_time)
        if now < slot_start:
            # 아직 시간대가 시작되지 않음
            return

        elapsed_minutes = (now - slot_start).total_seconds() / 60

        # 준비 시간 초과 체크 (기본 15분)
        max_preparation_time = self.current_slot.preparation_time

        if elapsed_minutes > max_preparation_time:
            logger.warning(f"⚠️ 준비 시간 초과: {elapsed_minutes:.1f}분 > {max_preparation_time}분")

            # 강제로 기본 전략 활성화
            await self._activate_emergency_strategies()
            self.preparation_completed = True
            self.current_phase = StrategyPhase.EXECUTION

        elif elapsed_minutes > max_preparation_time * 0.7:  # 70% 경과 시 경고
            progress_rate = self._calculate_discovery_progress()
            logger.info(f"📊 종목 탐색 진행률: {progress_rate:.1f}% (경과: {elapsed_minutes:.1f}분)")

            if progress_rate < 50:  # 진행률이 낮으면 경고
                logger.warning(f"⚠️ 종목 탐색 지연 중 - 진행률: {progress_rate:.1f}%")

        # 정상 진행 상황 로깅 (5분마다)
        elif int(elapsed_minutes) % 5 == 0 and int(elapsed_minutes) > 0:
            progress_rate = self._calculate_discovery_progress()
            logger.debug(f"🔍 종목 탐색 중: {progress_rate:.1f}% 완료 (경과: {elapsed_minutes:.1f}분)")

    def _calculate_discovery_progress(self) -> float:
        """종목 탐색 진행률 계산"""
        if not self.current_slot:
            return 0.0

        total_strategies = len(self.current_slot.primary_strategies) + len(self.current_slot.secondary_strategies)
        if total_strategies == 0:
            return 100.0

        completed_strategies = 0

        # 주요 전략 완료 체크
        for strategy_name in self.current_slot.primary_strategies.keys():
            if strategy_name in self.candidates and len(self.candidates[strategy_name]) > 0:
                completed_strategies += 1

        # 보조 전략 완료 체크
        for strategy_name in self.current_slot.secondary_strategies.keys():
            if strategy_name in self.candidates and len(self.candidates[strategy_name]) > 0:
                completed_strategies += 1

        return (completed_strategies / total_strategies) * 100.0

    async def _activate_emergency_strategies(self):
        """비상 시 기본 전략 활성화"""
        logger.warning("🚨 비상 전략 활성화 - 기본 종목으로 대체")

        # 백그라운드에서 발견된 종목들 중 상위 종목 사용
        emergency_stocks = []

        # 긴급 거래량 급증 종목들
        if 'emergency_volume' in self.candidates:
            emergency_stocks.extend(self.candidates['emergency_volume'][:5])

        # 긴급 가격 변동 종목들
        if 'emergency_price' in self.candidates:
            emergency_stocks.extend(self.candidates['emergency_price'][:5])

        # 종목이 부족하면 기본 대형주로 보완
        if len(emergency_stocks) < 5:
            default_stocks = [
                '005930',  # 삼성전자
                '000660',  # SK하이닉스
                '035420',  # NAVER
                '051910',  # LG화학
                '006400',  # 삼성SDI
            ]

            for stock_code in default_stocks:
                if len(emergency_stocks) >= 10:
                    break

                # 중복 제거
                if not any(c.stock_code == stock_code for c in emergency_stocks):
                    emergency_candidate = StockCandidate(
                        stock_code=stock_code,
                        strategy_type='emergency_default',
                        score=50.0,
                        reason="비상 기본 전략",
                        discovered_at=datetime.now()
                    )
                    emergency_stocks.append(emergency_candidate)

        # 비상 전략 활성화
        if emergency_stocks:
            await self._activate_strategy_candidates(
                'emergency',
                emergency_stocks,
                DataPriority.HIGH,
                1.0
            )
            logger.info(f"✅ 비상 전략 활성화: {len(emergency_stocks)}개 종목")

    async def _check_current_time_slot(self):
        """현재 시간대 확인"""
        current_time = datetime.now().time()
        slot = self._get_time_slot_for_time(current_time)

        if slot:
            await self._start_new_time_slot(slot)
        else:
            await self._handle_after_hours()

    async def _handle_after_hours(self):
        """장외 시간 처리"""
        if self.current_slot:
            logger.info("📴 장외 시간 - 모든 전략 비활성화")
            await self._cleanup_previous_strategy()
            self.current_slot = None
            self.current_phase = StrategyPhase.PREPARATION

    def get_status(self) -> Dict:
        """현재 스케줄러 상태 반환"""
        return {
            'scheduler_running': self.scheduler_running,
            'current_slot': self.current_slot.name if self.current_slot else None,
            'current_phase': self.current_phase.value,
            'preparation_completed': self.preparation_completed,
            'active_strategies': list(self.active_stocks.keys()),
            'total_active_stocks': sum(len(stocks) for stocks in self.active_stocks.values()),
            'candidates_count': {strategy: len(candidates) for strategy, candidates in self.candidates.items()},
            'screening_active': self.screening_active
        }
