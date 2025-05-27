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
from core.technical_indicators import TechnicalIndicators
from database.db_models import DataPriority
import time as time_module  # time 모듈과 구분

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

        # 🆕 신호 중복 방지를 위한 히스토리 관리
        self.signal_history: Dict[str, Dict] = {}  # {stock_code: {last_signal_time, last_signal_type, cooldown_until}}
        self.signal_cooldown = 300  # 5분 쿨다운
        self.signal_lock = threading.Lock()

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

                # 우선순위 결정 (DataPriority 사용)
                priority = self._get_data_priority(strategy_name, i)

                # add_stock_request 사용 (DataPriority 기반)
                self.data_manager.add_stock_request(
                    stock_code=stock_code,
                    priority=priority,
                    strategy_name=strategy_name,
                    callback=callback
                )

            logger.info(f"🎯 {strategy_name} 전략 활성화: {len(stock_codes)}개 종목")

            # 활성화 직후 즉시 신호 체크 시작
            asyncio.create_task(self._monitor_strategy_signals(strategy_name, stock_codes))

        except Exception as e:
            logger.error(f"단일 전략 활성화 오류 ({strategy_name}): {e}")

    def _get_data_priority(self, strategy_name: str, stock_index: int) -> DataPriority:
        """전략별 데이터 우선순위 결정"""
        # 전략별 기본 우선순위
        strategy_base_priority = {
            'gap_trading': DataPriority.CRITICAL,      # 갭 트레이딩이 가장 높음
            'momentum': DataPriority.HIGH,             # 모멘텀이 두번째
            'volume_breakout': DataPriority.HIGH       # 거래량 돌파가 세번째
        }

        base_priority = strategy_base_priority.get(strategy_name, DataPriority.MEDIUM)

        # 같은 전략 내에서도 순위별 우선순위 조정
        if stock_index < 5:
            return base_priority  # 상위 5개는 그대로
        elif stock_index < 10:
            # 중간 5개는 한 단계 낮춤
            if base_priority == DataPriority.CRITICAL:
                return DataPriority.HIGH
            elif base_priority == DataPriority.HIGH:
                return DataPriority.MEDIUM
            else:
                return DataPriority.LOW
        else:
            # 나머지는 두 단계 낮춤
            if base_priority == DataPriority.CRITICAL:
                return DataPriority.MEDIUM
            elif base_priority == DataPriority.HIGH:
                return DataPriority.LOW
            else:
                return DataPriority.BACKGROUND

    async def _monitor_strategy_signals(self, strategy_name: str, stock_codes: list):
        """전략 신호 모니터링 (주기적 체크)"""
        try:
            logger.info(f"🔍 {strategy_name} 신호 모니터링 시작: {len(stock_codes)}개 종목")

            # 30초 간격으로 신호 체크 (총 30분간)
            for cycle in range(60):  # 30초 * 60 = 30분
                await asyncio.sleep(30)  # 30초 대기

                logger.debug(f"🔄 {strategy_name} 신호 체크 사이클 {cycle + 1}/60")

                for stock_code in stock_codes:
                    try:
                        # 최신 데이터 조회
                        latest_data = self.data_manager.get_latest_data(stock_code)
                        if latest_data and latest_data.get('status') == 'success':
                            # 신호 생성 시도
                                                    signal = self._generate_simple_signal(strategy_name, stock_code, latest_data)
                        if signal:
                            logger.info(f"✅ 주기적 체크에서 신호 발견: {stock_code}")
                            self.send_signal_to_main_bot(signal, source="periodic_check")

                    except Exception as e:
                        logger.error(f"신호 체크 오류 ({stock_code}): {e}")

                # 10개 종목마다 잠시 대기 (API 부하 방지)
                if len(stock_codes) > 10:
                    await asyncio.sleep(5)

        except Exception as e:
            logger.error(f"{strategy_name} 신호 모니터링 오류: {e}")

    def _create_strategy_callback(self, strategy_name: str):
        """전략별 콜백 생성"""
        def callback(stock_code: str, data: Dict, source: str):
            try:
                # 간단한 신호 생성
                signal = self._generate_simple_signal(strategy_name, stock_code, data)
                if signal:
                    # 봇에게 신호 전달 (통합 버전 사용)
                    self.send_signal_to_main_bot(signal, source="realtime_callback")
            except Exception as e:
                logger.error(f"콜백 오류: {strategy_name} {stock_code} - {e}")

        return callback

    def _generate_simple_signal(self, strategy_name: str, stock_code: str, data: Dict) -> Optional[Dict]:
        """간단한 신호 생성 (기술적 지표 통합 버전)"""
        try:
            # 현재가 확인
            current_price = data.get('current_price', 0)
            if current_price <= 0:
                return None

            # 가격 변화율 확인
            change_rate = data.get('change_rate', 0)
            volume = data.get('volume', 0)

            logger.debug(f"신호 생성 체크: {stock_code} 전략={strategy_name}, 현재가={current_price:,}, 변화율={change_rate:.2f}%, 거래량={volume:,}")

            # 기술적 지표 확인을 위한 일봉 데이터 조회 (캐시 활용)
            try:
                daily_data = self.data_manager.collector.get_daily_prices(stock_code, "D", use_cache=True)
                if daily_data and len(daily_data) >= 3:
                    # 빠른 기술적 신호 분석
                    tech_signal = TechnicalIndicators.get_quick_signals(daily_data)
                    tech_score = tech_signal.get('strength', 0)
                    tech_action = tech_signal.get('action', 'HOLD')

                    logger.debug(f"기술적 지표: {stock_code} - {tech_action} (강도: {tech_score}) [캐시활용]")
                else:
                    tech_score = 0
                    tech_action = 'HOLD'
            except Exception as e:
                logger.debug(f"기술적 지표 조회 실패: {stock_code} - {e}")
                tech_score = 0
                tech_action = 'HOLD'

            signal = None

            # 전략별 신호 생성 (기술적 지표 고려)
            if strategy_name == 'gap_trading' and change_rate > 1.8:  # 2.0에서 1.8로 추가 완화
                # 기술적 지표가 매수 신호이거나 중립일 때만
                if tech_action in ['BUY', 'HOLD']:
                    # 기술적 지표 점수에 따라 신호 강도 조정
                    base_strength = min(change_rate / 8.0, 1.0)
                    tech_bonus = tech_score / 200  # 최대 0.5 보너스
                    final_strength = min(base_strength + tech_bonus, 1.0)

                    signal = {
                        'stock_code': stock_code,
                        'signal_type': 'BUY',
                        'strategy': strategy_name,
                        'price': current_price,
                        'strength': final_strength,
                        'reason': f'갭 상승 {change_rate:.1f}% (기술: {tech_action})',
                        'tech_score': tech_score
                    }
                    logger.info(f"🎯 갭 트레이딩 신호 생성: {stock_code} {change_rate:.1f}% (기술점수: {tech_score})")

            elif strategy_name == 'volume_breakout' and change_rate > 1.2:  # 1.5에서 1.2로 추가 완화
                if volume > 0 and tech_action in ['BUY', 'HOLD']:
                    base_strength = min(change_rate / 6.0, 1.0)
                    tech_bonus = tech_score / 200
                    final_strength = min(base_strength + tech_bonus, 1.0)

                    signal = {
                        'stock_code': stock_code,
                        'signal_type': 'BUY',
                        'strategy': strategy_name,
                        'price': current_price,
                        'strength': final_strength,
                        'reason': f'거래량 돌파 {change_rate:.1f}% (기술: {tech_action})',
                        'tech_score': tech_score
                    }
                    logger.info(f"🎯 볼륨 브레이크아웃 신호 생성: {stock_code} {change_rate:.1f}% (기술점수: {tech_score})")

            elif strategy_name == 'momentum' and change_rate > 0.6:  # 0.8에서 0.6으로 추가 완화
                if tech_action in ['BUY', 'HOLD']:
                    base_strength = min(change_rate / 4.0, 1.0)
                    tech_bonus = tech_score / 200
                    final_strength = min(base_strength + tech_bonus, 1.0)

                    signal = {
                        'stock_code': stock_code,
                        'signal_type': 'BUY',
                        'strategy': strategy_name,
                        'price': current_price,
                        'strength': final_strength,
                        'reason': f'모멘텀 {change_rate:.1f}% (기술: {tech_action})',
                        'tech_score': tech_score
                    }
                    logger.info(f"🎯 모멘텀 신호 생성: {stock_code} {change_rate:.1f}% (기술점수: {tech_score})")

            # 기술적 지표가 강력한 매수 신호일 때 추가 신호 생성
            elif tech_action == 'BUY' and tech_score > 70 and change_rate > 0.5:
                signal = {
                    'stock_code': stock_code,
                    'signal_type': 'BUY',
                    'strategy': f'{strategy_name}_tech',
                    'price': current_price,
                    'strength': min(tech_score / 100, 1.0),
                    'reason': f'기술적 강세 신호 (점수: {tech_score})',
                    'tech_score': tech_score
                }
                logger.info(f"🎯 기술적 신호 생성: {stock_code} 점수={tech_score}")

            if signal:
                logger.info(f"✅ 신호 생성 완료: {signal}")

            return signal

        except Exception as e:
            logger.error(f"신호 생성 오류: {strategy_name} {stock_code} - {e}")
            return None

    def send_signal_to_main_bot(self, signal: Dict, source: str = "unknown"):
        """메인 봇에게 거래 신호 전달 (중복 방지 버전)"""
        try:
            stock_code = signal.get('stock_code')
            signal_type = signal.get('signal_type')

            if not stock_code or not signal_type:
                logger.error("❌ 유효하지 않은 신호 데이터")
                return False

            # 중복 신호 체크
            if not self._is_signal_allowed(stock_code, signal_type, source):
                logger.debug(f"⏰ 신호 쿨다운 중: {stock_code} ({source})")
                return False

            # 메인 봇에 신호 전달
            if self.bot_instance and hasattr(self.bot_instance, 'handle_trading_signal'):
                logger.info(f"📤 거래신호 전달: {stock_code} {signal_type} ({source})")

                # 신호 히스토리 업데이트
                self._update_signal_history(stock_code, signal_type, source)

                # 실제 신호 전달
                self.bot_instance.handle_trading_signal(signal)
                logger.info(f"✅ 거래신호 전달 완료: {stock_code}")
                return True
            else:
                logger.error("❌ 메인 봇 인스턴스가 설정되지 않음")
                return False

        except Exception as e:
            logger.error(f"거래신호 전달 오류: {e}")
            return False

    def _is_signal_allowed(self, stock_code: str, signal_type: str, source: str) -> bool:
        """신호 허용 여부 체크 (중복 방지)"""
        try:
            with self.signal_lock:
                current_time = time_module.time()

                # 기존 히스토리 확인
                if stock_code in self.signal_history:
                    history = self.signal_history[stock_code]

                    # 쿨다운 시간 체크
                    cooldown_until = history.get('cooldown_until', 0)
                    if current_time < cooldown_until:
                        logger.debug(f"⏰ {stock_code} 쿨다운 중 (남은시간: {int(cooldown_until - current_time)}초)")
                        return False

                    # 같은 타입 신호 중복 체크 (1분 이내)
                    last_signal_time = history.get('last_signal_time', 0)
                    last_signal_type = history.get('last_signal_type', '')

                    if (signal_type == last_signal_type and
                        current_time - last_signal_time < 60):  # 1분 이내 같은 신호 차단
                        logger.debug(f"⚠️ {stock_code} 1분 이내 중복 신호 차단: {signal_type}")
                        return False

                return True

        except Exception as e:
            logger.error(f"신호 허용 체크 오류: {e}")
            return True  # 오류시 허용

    def _update_signal_history(self, stock_code: str, signal_type: str, source: str):
        """신호 히스토리 업데이트"""
        try:
            with self.signal_lock:
                current_time = time_module.time()

                # 매수 신호인 경우 쿨다운 설정
                cooldown_until = 0
                if signal_type == 'BUY':
                    cooldown_until = current_time + self.signal_cooldown  # 5분 쿨다운

                self.signal_history[stock_code] = {
                    'last_signal_time': current_time,
                    'last_signal_type': signal_type,
                    'cooldown_until': cooldown_until,
                    'source': source,
                    'count': self.signal_history.get(stock_code, {}).get('count', 0) + 1
                }

                logger.debug(f"📝 신호 히스토리 업데이트: {stock_code} {signal_type} ({source})")

        except Exception as e:
            logger.error(f"신호 히스토리 업데이트 오류: {e}")

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
