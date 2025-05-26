"""
전략 스케줄러 (리팩토링 간소화 버전)
기존 1365줄을 300줄 이하로 간소화
"""
import asyncio
import threading
from datetime import datetime, time
from typing import Dict, List, Optional, TYPE_CHECKING
from enum import Enum
from utils.logger import setup_logger
from .time_slot_manager import TimeSlotManager, TimeSlotConfig
from .stock_discovery import StockDiscovery, StockCandidate
from core.rest_api_manager import KISRestAPIManager
from core.hybrid_data_manager import SimpleHybridDataManager

# 순환 import 방지
if TYPE_CHECKING:
    from main import StockBot

logger = setup_logger(__name__)

class StrategyPhase(Enum):
    """전략 단계"""
    PREPARATION = "preparation"
    EXECUTION = "execution"
    TRANSITION = "transition"

class StrategyScheduler:
    """간소화된 전략 스케줄러"""

    def __init__(self, trading_api: KISRestAPIManager, data_manager: SimpleHybridDataManager):
        """초기화"""
        self.trading_api = trading_api
        self.data_manager = data_manager

        # 관리자들
        self.time_manager = TimeSlotManager()
        self.stock_discovery = StockDiscovery(trading_api)

        # 스케줄러 상태
        self.scheduler_running = False
        self.current_slot: Optional[TimeSlotConfig] = None
        self.current_phase = StrategyPhase.PREPARATION
        self.preparation_completed = False

        # 봇 인스턴스 (나중에 설정)
        self.bot_instance: Optional['StockBot'] = None

        # 활성 종목 저장
        self.active_stocks: Dict[str, List[str]] = {}

        logger.info("📅 간소화된 전략 스케줄러 초기화 완료")

    async def start_scheduler(self):
        """스케줄러 시작"""
        try:
            logger.info("🚀 전략 스케줄러 시작")

            # 백그라운드 스크리닝 시작
            self.stock_discovery.start_background_screening()

            # 메인 스케줄링 루프 시작
            self.scheduler_running = True
            await self._main_scheduling_loop()

        except Exception as e:
            logger.error(f"스케줄러 시작 실패: {e}")
            raise
        finally:
            self.stop_scheduler()

    async def _main_scheduling_loop(self):
        """메인 스케줄링 루프"""
        logger.info("🔄 메인 스케줄링 루프 시작")

        while self.scheduler_running:
            try:
                # 다음 준비 시간 계산
                next_prep_time = self.time_manager.get_next_preparation_time()

                if next_prep_time:
                    # 준비 시간까지 대기
                    sleep_seconds = self.time_manager.calculate_sleep_time(next_prep_time)

                    if sleep_seconds > 60:
                        logger.info(f"⏰ 다음 전략 준비까지 {sleep_seconds//60}분 대기")

                        # 1분씩 나누어 대기 (중간 중단 가능)
                        while sleep_seconds > 0 and self.scheduler_running:
                            wait_time = min(60, sleep_seconds)
                            await asyncio.sleep(wait_time)
                            sleep_seconds -= wait_time
                    else:
                        await asyncio.sleep(sleep_seconds)

                    # 전략 실행
                    if self.scheduler_running:
                        await self._execute_time_slot_strategy()
                else:
                    # 장외 시간 - 30분 대기
                    logger.info("💤 장외 시간 - 30분 대기")
                    await asyncio.sleep(1800)

            except Exception as e:
                logger.error(f"스케줄링 루프 오류: {e}")
                await asyncio.sleep(300)  # 5분 대기 후 재시도

    async def _execute_time_slot_strategy(self):
        """시간대별 전략 실행"""
        try:
            # 현재 시간대 확인
            current_slot = self.time_manager.get_current_time_slot()

            if not current_slot:
                logger.info("📅 활성 시간대가 없음")
                return

            # 새로운 시간대 시작
            if not self.current_slot or self.current_slot.name != current_slot.name:
                logger.info(f"🔄 새 시간대 시작: {current_slot.name} ({current_slot.description})")
                self.current_slot = current_slot

                # 이전 전략 정리
                await self._cleanup_previous_strategy()

                # 새 전략 준비 및 활성화
                await self._prepare_and_activate_strategy(current_slot)

        except Exception as e:
            logger.error(f"시간대 전략 실행 오류: {e}")

    async def _cleanup_previous_strategy(self):
        """이전 전략 정리"""
        try:
            logger.info("🧹 이전 전략 정리 중...")

            # 활성 종목 정리
            if hasattr(self, 'active_stocks'):
                for strategy_name, stock_codes in self.active_stocks.items():
                    for stock_code in stock_codes:
                        self.data_manager.remove_stock(stock_code)
                self.active_stocks.clear()

            logger.info("✅ 이전 전략 정리 완료")

        except Exception as e:
            logger.error(f"이전 전략 정리 오류: {e}")

    async def _prepare_and_activate_strategy(self, slot: TimeSlotConfig):
        """전략 준비 및 활성화"""
        try:
            logger.info(f"🎯 전략 준비 시작: {slot.name}")

            # 1단계: 종목 탐색
            await self._discover_strategy_stocks(slot)

            # 2단계: 전략 활성화
            await self._activate_strategies(slot)

            logger.info(f"✅ 전략 활성화 완료: {slot.name}")

        except Exception as e:
            logger.error(f"전략 준비/활성화 오류: {e}")

    async def _discover_strategy_stocks(self, slot: TimeSlotConfig):
        """전략별 종목 탐색"""
        try:
            logger.info(f"🔍 종목 탐색 시작: {slot.name}")

            # 기본 전략들 탐색
            all_strategies = {**slot.primary_strategies, **slot.secondary_strategies}

            discovery_tasks = []
            for strategy_name, weight in all_strategies.items():
                task = asyncio.create_task(
                    self._discover_single_strategy(strategy_name, weight)
                )
                discovery_tasks.append(task)

            # 모든 탐색 완료 대기 (최대 60초)
            await asyncio.wait_for(
                asyncio.gather(*discovery_tasks, return_exceptions=True),
                timeout=60
            )

            logger.info("✅ 종목 탐색 완료")

        except asyncio.TimeoutError:
            logger.warning("⚠️ 종목 탐색 시간 초과 (60초)")
        except Exception as e:
            logger.error(f"종목 탐색 오류: {e}")

    async def _discover_single_strategy(self, strategy_name: str, weight: float):
        """단일 전략 종목 탐색"""
        try:
            # 별도 스레드에서 탐색 실행
            loop = asyncio.get_event_loop()
            candidates = await loop.run_in_executor(
                None,
                self.stock_discovery.discover_strategy_stocks,
                strategy_name, weight, True
            )

            if candidates:
                stock_codes = [c.stock_code for c in candidates]
                self.active_stocks[strategy_name] = stock_codes
                logger.info(f"✅ {strategy_name} 전략: {len(stock_codes)}개 종목 발견")

        except Exception as e:
            logger.error(f"단일 전략 탐색 오류 ({strategy_name}): {e}")

    async def _activate_strategies(self, slot: TimeSlotConfig):
        """전략 활성화"""
        try:
            all_strategies = {**slot.primary_strategies, **slot.secondary_strategies}

            for strategy_name, weight in all_strategies.items():
                if strategy_name in self.active_stocks:
                    await self._activate_single_strategy(strategy_name, weight)

        except Exception as e:
            logger.error(f"전략 활성화 오류: {e}")

    async def _activate_single_strategy(self, strategy_name: str, weight: float):
        """단일 전략 활성화"""
        try:
            stock_codes = self.active_stocks.get(strategy_name, [])

            for i, stock_code in enumerate(stock_codes):
                # 데이터 관리자에 종목 추가 (우선순위와 실시간 여부 설정)
                callback = self._create_strategy_callback(strategy_name)

                # 상위 13개는 실시간 시도, 나머지는 폴링
                use_realtime = i < 13
                priority = self._get_strategy_priority(strategy_name, i)

                self.data_manager.add_stock(
                    stock_code=stock_code,
                    strategy_name=strategy_name,
                    use_realtime=use_realtime,
                    callback=callback,
                    priority=priority
                )

            logger.info(f"🎯 {strategy_name} 전략 활성화: {len(stock_codes)}개 종목")

        except Exception as e:
            logger.error(f"단일 전략 활성화 오류 ({strategy_name}): {e}")

    def _get_strategy_priority(self, strategy_name: str, stock_index: int) -> int:
        """전략별 우선순위 결정"""
        # 기본 전략 우선순위
        strategy_base_priority = {
            'gap_trading': 1,      # 갭 트레이딩이 가장 높음
            'momentum': 2,         # 모멘텀이 두번째
            'volume_breakout': 3   # 거래량 돌파가 세번째
        }

        base = strategy_base_priority.get(strategy_name, 3)

        # 같은 전략 내에서도 순위별 우선순위 (상위 5개는 우선순위 상승)
        if stock_index < 5:
            return base
        elif stock_index < 10:
            return base + 1
        else:
            return base + 2

    def _create_strategy_callback(self, strategy_name: str):
        """전략별 콜백 생성"""
        def callback(stock_code: str, data: Dict, source: str):
            try:
                # 간단한 신호 생성
                signal = self._generate_simple_signal(strategy_name, stock_code, data)
                if signal:
                    # 봇에게 신호 전달
                    asyncio.create_task(self._send_signal_to_bot(signal))
            except Exception as e:
                logger.error(f"콜백 오류: {strategy_name} {stock_code} - {e}")

        return callback

    def _generate_simple_signal(self, strategy_name: str, stock_code: str, data: Dict) -> Optional[Dict]:
        """간단한 신호 생성"""
        try:
            # 임시 신호 생성 로직 (실제로는 더 복잡한 분석 필요)
            current_price = data.get('current_price', 0)
            if current_price <= 0:
                return None

            # 가격 변화율 기반 간단한 신호
            change_rate = data.get('change_rate', 0)

            signal = None
            if strategy_name == 'gap_trading' and change_rate > 3.0:
                signal = {
                    'stock_code': stock_code,
                    'signal_type': 'BUY',
                    'strategy': strategy_name,
                    'price': current_price,
                    'strength': min(change_rate / 10.0, 1.0),
                    'reason': f'갭 상승 {change_rate:.1f}%'
                }
            elif strategy_name == 'volume_breakout' and change_rate > 2.0:
                volume = data.get('volume', 0)
                if volume > 0:  # 거래량 체크는 추후 개선
                    signal = {
                        'stock_code': stock_code,
                        'signal_type': 'BUY',
                        'strategy': strategy_name,
                        'price': current_price,
                        'strength': min(change_rate / 8.0, 1.0),
                        'reason': f'거래량 돌파 {change_rate:.1f}%'
                    }
            elif strategy_name == 'momentum' and change_rate > 1.0:
                signal = {
                    'stock_code': stock_code,
                    'signal_type': 'BUY',
                    'strategy': strategy_name,
                    'price': current_price,
                    'strength': min(change_rate / 5.0, 1.0),
                    'reason': f'모멘텀 {change_rate:.1f}%'
                }

            return signal

        except Exception as e:
            logger.error(f"신호 생성 오류: {strategy_name} {stock_code} - {e}")
            return None

    async def _send_signal_to_bot(self, signal: Dict):
        """봇에게 신호 전달"""
        try:
            if self.bot_instance and hasattr(self.bot_instance, 'handle_trading_signal'):
                self.bot_instance.handle_trading_signal(signal)
        except Exception as e:
            logger.error(f"신호 전달 오류: {e}")

    def set_bot_instance(self, bot_instance: 'StockBot'):
        """봇 인스턴스 설정"""
        self.bot_instance = bot_instance
        logger.info("봇 인스턴스 설정 완료")

    def stop_scheduler(self):
        """스케줄러 중지"""
        try:
            logger.info("🛑 전략 스케줄러 중지 중...")

            self.scheduler_running = False

            # 백그라운드 스크리닝 중지
            if hasattr(self.stock_discovery, 'stop_background_screening'):
                self.stock_discovery.stop_background_screening()

            # 모든 구독 정리
            for strategy_name, stock_codes in self.active_stocks.items():
                for stock_code in stock_codes:
                    self.data_manager.remove_stock(stock_code)

            self.active_stocks.clear()

            logger.info("✅ 전략 스케줄러 중지 완료")

        except Exception as e:
            logger.error(f"스케줄러 중지 오류: {e}")

    def get_status(self) -> Dict:
        """현재 상태 조회"""
        try:
            return {
                'is_running': self.scheduler_running,
                'current_slot': self.current_slot.name if self.current_slot else None,
                'current_phase': self.current_phase.value,
                'active_strategies': dict(self.active_stocks),
                'total_stocks': sum(len(stocks) for stocks in self.active_stocks.values()),
                'data_manager_status': self.data_manager.get_status() if self.data_manager else {}
            }
        except Exception as e:
            logger.error(f"상태 조회 오류: {e}")
            return {
                'is_running': False,
                'error': str(e)
            }

    def cleanup(self):
        """정리"""
        self.stop_scheduler()
