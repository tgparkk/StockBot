"""
전략 스케줄러 - 시간대별 전략 실행 관리
"""
import time
import asyncio
import threading
from typing import Dict, List, Optional, Any, TYPE_CHECKING, Callable
from datetime import datetime, timedelta
from enum import Enum
from utils.logger import setup_logger
from .time_slot_manager import TimeSlotManager, TimeSlotConfig
from .stock_discovery import StockDiscovery, StockCandidate
from ..api.rest_api_manager import KISRestAPIManager
from ..data.hybrid_data_manager import SimpleHybridDataManager
from ..data.data_priority import DataPriority
from ..analysis.technical_indicators import TechnicalIndicators
from ..trading.trade_database import TradeDatabase
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

    def __init__(self, trading_api: KISRestAPIManager, data_manager: SimpleHybridDataManager, trade_db: TradeDatabase):
        """초기화"""
        self.trading_api = trading_api
        self.data_manager = data_manager

        # 관리자들
        self.time_manager = TimeSlotManager()
        self.stock_discovery = StockDiscovery(trading_api)
        self.stock_discovery.set_data_manager(data_manager)  # 데이터 매니저 연결

        # 🆕 거래 데이터베이스 (종목 선정 기록용)
        self.trade_db = trade_db

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

        # 시작 시 현재 활성 시간대 확인 및 즉시 실행
        current_slot = self.time_manager.get_current_time_slot()
        if current_slot:
            logger.info(f"🚀 시작 시 활성 시간대 발견: {current_slot.name} - 즉시 전략 실행")
            await self._execute_time_slot_strategy()

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
                        logger.info(f"⏰ 다음 전략 준비까지 {sleep_seconds}초 대기")
                        await asyncio.sleep(sleep_seconds)

                    # 전략 실행
                    if self.scheduler_running:
                        await self._execute_time_slot_strategy()
                else:
                    # 장외 시간 - 10분 대기 후 재확인
                    logger.info("💤 장외 시간 - 10분 대기")
                    await asyncio.sleep(600)

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
            logger.info(f"🔍 {strategy_name} 전략 후보 탐색 시작 (가중치: {weight})")

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

                # 후보 종목 상세 로그
                for i, candidate in enumerate(candidates[:5]):  # 상위 5개만 로그
                    logger.info(f"   {i+1}. {candidate.stock_code} - {candidate.reason} (점수: {candidate.score:.1f})")

                if len(candidates) > 5:
                    logger.info(f"   ... 외 {len(candidates)-5}개 종목")

                # 🆕 데이터베이스에 종목 선정 기록 저장
                await self._record_selected_stocks(strategy_name, candidates, weight)
            else:
                logger.warning(f"⚠️ {strategy_name} 전략: 후보 없음")

        except Exception as e:
            logger.error(f"단일 전략 탐색 오류 ({strategy_name}): {e}")

    async def _record_selected_stocks(self, strategy_name: str, candidates: List, weight: float = 1.0):
        """선정된 종목들을 데이터베이스에 기록"""
        try:
            if not candidates or not self.current_slot:
                return

            # 시간대 정보 준비
            slot_name = self.current_slot.name
            slot_start = str(self.current_slot.start_time)
            slot_end = str(self.current_slot.end_time)

            # 후보 종목들을 딕셔너리 형태로 변환
            stock_records = []
            for candidate in candidates:
                # StockCandidate 객체에서 필요한 정보 추출
                record = {
                    'stock_code': candidate.stock_code,
                    'stock_name': getattr(candidate, 'stock_name', candidate.stock_code),
                    'strategy_type': strategy_name,
                    'score': candidate.score,
                    'reason': candidate.reason,
                    'current_price': getattr(candidate, 'current_price', 0),
                    'change_rate': getattr(candidate, 'change_rate', 0.0),
                    'volume': getattr(candidate, 'volume', 0),
                    'volume_ratio': getattr(candidate, 'volume_ratio', 0.0),
                    'market_cap': getattr(candidate, 'market_cap', 0),

                    # 전략별 특화 지표
                    'gap_rate': getattr(candidate, 'gap_rate', 0.0),
                    'momentum_strength': getattr(candidate, 'momentum_strength', 0.0),
                    'breakout_volume': getattr(candidate, 'breakout_volume', 0.0),

                    # 기술적 신호 (있다면)
                    'technical_signals': getattr(candidate, 'technical_signals', {}),

                    # 메모
                    'notes': f"가중치: {weight}, 전략: {strategy_name}"
                }
                stock_records.append(record)

            # 별도 스레드에서 데이터베이스 기록 실행 (비동기 처리)
            loop = asyncio.get_event_loop()
            recorded_ids = await loop.run_in_executor(
                None,
                self.trade_db.record_selected_stocks,
                slot_name, slot_start, slot_end, stock_records
            )

            if recorded_ids:
                logger.info(f"💾 {strategy_name} 전략 종목 선정 기록 완료: {len(recorded_ids)}개")
            else:
                logger.warning(f"⚠️ {strategy_name} 전략 종목 선정 기록 실패")

        except Exception as e:
            logger.error(f"종목 선정 기록 오류 ({strategy_name}): {e}")

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

            if not stock_codes:
                logger.warning(f"⚠️ {strategy_name} 전략: 활성화할 종목 없음")
                return

            logger.info(f"🎯 {strategy_name} 전략 활성화 시작: {len(stock_codes)}개 종목")

            successful_subscriptions = 0

            for i, stock_code in enumerate(stock_codes):
                try:
                    # 데이터 관리자에 종목 추가 (우선순위와 실시간 여부 설정)
                    callback = self._create_strategy_callback(strategy_name)

                    # 우선순위 결정 (DataPriority 사용)
                    priority = self._get_data_priority(strategy_name, i)

                    logger.info(f"   📊 {stock_code} 구독 시도 (우선순위: {priority.value})")

                    # add_stock_request 사용 (DataPriority 기반)
                    success = self.data_manager.add_stock_request(
                        stock_code=stock_code,
                        priority=priority,
                        strategy_name=strategy_name,
                        callback=callback
                    )

                    if success:
                        successful_subscriptions += 1
                        logger.info(f"   ✅ {stock_code} 구독 성공")

                        # 🆕 데이터베이스에 활성화 상태 업데이트
                        try:
                            self.trade_db.update_stock_activation(stock_code, True, True)
                        except Exception as e:
                            logger.error(f"활성화 상태 업데이트 오류 ({stock_code}): {e}")
                    else:
                        logger.warning(f"   ❌ {stock_code} 구독 실패")

                        # 🆕 데이터베이스에 활성화 실패 상태 업데이트
                        try:
                            self.trade_db.update_stock_activation(stock_code, True, False)
                        except Exception as e:
                            logger.error(f"활성화 실패 상태 업데이트 오류 ({stock_code}): {e}")

                except Exception as e:
                    logger.error(f"   ❌ {stock_code} 구독 중 오류: {e}")

            logger.info(f"🎯 {strategy_name} 전략 활성화 완료: {successful_subscriptions}/{len(stock_codes)}개 성공")

            # 전략 활성화 후 웹소켓 구독 상태 확인
            if self.data_manager:
                websocket_status = self.data_manager.get_status()
                websocket_details = websocket_status.get('websocket_details', {})

                logger.info(
                    f"📡 [{strategy_name}] 웹소켓 상태: "
                    f"연결={websocket_details.get('connected', False)}, "
                    f"구독={websocket_details.get('subscription_count', 0)}/13종목, "
                    f"사용량={websocket_details.get('usage_ratio', '0/41')}"
                )

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

    def _create_strategy_callback(self, strategy_name: str) -> Callable:
        """전략별 콜백 함수 생성"""
        def strategy_callback(stock_code: str, data: Dict, source: str = 'websocket') -> None:
            """전략별 데이터 콜백"""
            try:
                # 기본 데이터 검증
                if not data or data.get('status') != 'success':
                    return

                current_price = data.get('current_price', 0)
                if current_price <= 0:
                    return

                # 신호 중복 방지 체크
                if not self._should_process_signal(stock_code, strategy_name):
                    return

                # 🆕 시장 센티먼트 확인
                market_sentiment = self._get_market_sentiment()

                # 기본 시장 데이터 생성
                market_data = {
                    'stock_code': stock_code,
                    'current_price': current_price,
                    'volume': data.get('volume', 0),
                    'change_rate': data.get('change_rate', 0),
                    'timestamp': data.get('timestamp', time_module.time()),
                    'source': source,
                    'market_sentiment': market_sentiment  # 🆕 시장 센티먼트 추가
                }

                # 전략별 신호 생성 로직
                signal = self._generate_strategy_signal(strategy_name, market_data)

                if signal:
                    logger.info(f"🎯 {strategy_name} 신호 생성: {stock_code} {signal['signal_type']} @ {current_price:,}원")

                    # 봇 인스턴스에 신호 전달
                    if self.bot_instance:
                        self.bot_instance.handle_trading_signal(signal)

                    # 신호 히스토리 업데이트
                    with self.signal_lock:
                        self.signal_history[stock_code] = {
                            'last_signal_time': time_module.time(),
                            'last_signal_type': signal['signal_type'],
                            'cooldown_until': time_module.time() + self.signal_cooldown,
                            'strategy': strategy_name
                        }

            except Exception as e:
                logger.error(f"전략 콜백 오류 ({strategy_name}, {stock_code}): {e}")

        return strategy_callback

    def _get_market_sentiment(self) -> Dict:
        """🆕 실시간 시장 센티먼트 분석"""
        try:
            # 간단한 시장 상황 분석
            current_time = time_module.time()

            # 캐시된 센티먼트 사용 (1분간 유효)
            if hasattr(self, '_market_sentiment_cache'):
                cache_time, sentiment = self._market_sentiment_cache
                if current_time - cache_time < 60:  # 1분 캐시
                    return sentiment

            # 기본 센티먼트 (실제로는 코스피/코스닥 지수 등을 활용)
            sentiment = {
                'bullish_score': 50,  # 0-100 (강세 정도)
                'volume_surge': False,  # 거래량 급증 여부
                'sector_rotation': 'balanced',  # 섹터 로테이션 상황
                'volatility': 'normal'  # 변동성 수준
            }

            # 🆕 실제 시장 데이터로 센티먼트 업데이트 (시간이 허락하면)
            try:
                # 여기에 실제 코스피/코스닥 지수 데이터 활용 가능
                # 현재는 시간대별 기본값 사용
                from datetime import datetime
                now_hour = datetime.now().hour

                if 9 <= now_hour <= 10:  # 장초반
                    sentiment['bullish_score'] = 65
                    sentiment['volatility'] = 'high'
                elif 10 <= now_hour <= 14:  # 장중
                    sentiment['bullish_score'] = 55
                    sentiment['volatility'] = 'normal'
                elif 14 <= now_hour <= 15:  # 장마감 근처
                    sentiment['bullish_score'] = 45
                    sentiment['volatility'] = 'high'

            except Exception as e:
                logger.debug(f"센티먼트 업데이트 오류: {e}")

            # 센티먼트 캐시 저장
            self._market_sentiment_cache = (current_time, sentiment)

            return sentiment

        except Exception as e:
            logger.error(f"시장 센티먼트 분석 오류: {e}")
            return {
                'bullish_score': 50,
                'volume_surge': False,
                'sector_rotation': 'balanced',
                'volatility': 'normal'
            }

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
            sentiment_multiplier = data.get('sentiment_multiplier', 1.0)  # 🆕 센티먼트 승수

            logger.debug(f"신호 생성 체크: {stock_code} 전략={strategy_name}, 현재가={current_price:,}, 변화율={change_rate:.2f}%, 거래량={volume:,}, 센티먼트승수={sentiment_multiplier:.2f}")

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

            # 🆕 다단계 민감도 전략 - 기회 확대 (센티먼트 반영)
            if strategy_name == 'gap_trading':
                # 강력한 신호 (센티먼트 반영)
                gap_threshold = 1.8 * sentiment_multiplier
                if change_rate > gap_threshold and tech_action in ['BUY', 'HOLD']:
                    base_strength = min(change_rate / 8.0, 1.0)
                    tech_bonus = tech_score / 200
                    final_strength = min(base_strength + tech_bonus, 1.0)
                    signal = self._create_signal(stock_code, strategy_name, current_price, final_strength,
                                               f'갭 상승 {change_rate:.1f}% (기준: {gap_threshold:.1f}%, 기술: {tech_action})', tech_score)

                # 🆕 중간 신호 (센티먼트 반영)
                elif 1.0 * sentiment_multiplier <= change_rate < gap_threshold and tech_action == 'BUY' and tech_score > 60:
                    final_strength = min((change_rate / 10.0) + (tech_score / 300), 0.8)
                    signal = self._create_signal(stock_code, f"{strategy_name}_moderate", current_price, final_strength,
                                               f'갭 {change_rate:.1f}% + 기술적 강세 (점수: {tech_score})', tech_score)

                # 🆕 약한 신호 (센티먼트 반영)
                elif 0.5 * sentiment_multiplier <= change_rate < 1.0 * sentiment_multiplier and tech_action == 'BUY' and tech_score > 80:
                    final_strength = min(tech_score / 150, 0.6)
                    signal = self._create_signal(stock_code, f"{strategy_name}_weak", current_price, final_strength,
                                               f'기술적 매수 신호 우선 (갭: {change_rate:.1f}%, 기술: {tech_score})', tech_score)

            elif strategy_name == 'volume_breakout':
                # 강력한 신호 (센티먼트 반영)
                volume_threshold = 1.2 * sentiment_multiplier
                if change_rate > volume_threshold and volume > 0 and tech_action in ['BUY', 'HOLD']:
                    base_strength = min(change_rate / 6.0, 1.0)
                    tech_bonus = tech_score / 200
                    final_strength = min(base_strength + tech_bonus, 1.0)
                    signal = self._create_signal(stock_code, strategy_name, current_price, final_strength,
                                               f'거래량 돌파 {change_rate:.1f}% (기준: {volume_threshold:.1f}%, 기술: {tech_action})', tech_score)

                # 🆕 중간 신호 (거래량 + 기술적 지표, 센티먼트 반영)
                elif 0.8 * sentiment_multiplier <= change_rate < volume_threshold and volume > 0 and tech_action == 'BUY' and tech_score > 70:
                    # 거래량 정보 추가 고려
                    volume_score = min(volume / 1000000, 2.0)  # 거래량 점수화
                    final_strength = min((change_rate / 8.0) + (tech_score / 250) + (volume_score / 10), 0.8)
                    signal = self._create_signal(stock_code, f"{strategy_name}_moderate", current_price, final_strength,
                                               f'볼륨 {change_rate:.1f}% + 기술 우세 (기준: {0.8 * sentiment_multiplier:.1f}%)', tech_score)

            elif strategy_name == 'momentum':
                # 강력한 신호 (센티먼트 반영)
                momentum_threshold = 0.6 * sentiment_multiplier
                if change_rate > momentum_threshold and tech_action in ['BUY', 'HOLD']:
                    base_strength = min(change_rate / 4.0, 1.0)
                    tech_bonus = tech_score / 200
                    final_strength = min(base_strength + tech_bonus, 1.0)
                    signal = self._create_signal(stock_code, strategy_name, current_price, final_strength,
                                               f'모멘텀 {change_rate:.1f}% (기준: {momentum_threshold:.1f}%, 기술: {tech_action})', tech_score)

                # 🆕 기술적 우선 신호 (센티먼트 반영)
                elif change_rate > 0.3 * sentiment_multiplier and tech_action == 'BUY' and tech_score > 85:
                    final_strength = min(tech_score / 120, 0.7)
                    signal = self._create_signal(stock_code, f"{strategy_name}_tech", current_price, final_strength,
                                               f'기술적 강력 매수 (모멘텀: {change_rate:.1f}%, 기준: {0.3 * sentiment_multiplier:.1f}%)', tech_score)

            # 🆕 순수 기술적 신호 (기존 전략과 무관, 센티먼트 반영)
            if not signal and tech_action == 'BUY' and tech_score > 90 and change_rate > 0.2 * sentiment_multiplier:
                final_strength = min(tech_score / 110, 0.9)
                signal = self._create_signal(stock_code, "technical_priority", current_price, final_strength,
                                           f'기술적 최우선 매수 (점수: {tech_score}, 변화: {change_rate:.1f}%, 기준: {0.2 * sentiment_multiplier:.1f}%)', tech_score)

            if signal:
                logger.info(f"✅ 신호 생성 완료: {signal}")

            return signal

        except Exception as e:
            logger.error(f"신호 생성 오류: {strategy_name} {stock_code} - {e}")
            return None

    def _create_signal(self, stock_code: str, strategy: str, price: int, strength: float, reason: str, tech_score: int) -> Dict:
        """신호 생성 헬퍼 메서드"""
        return {
            'stock_code': stock_code,
            'signal_type': 'BUY',
            'strategy': strategy,
            'price': price,
            'strength': strength,
            'reason': reason,
            'tech_score': tech_score
        }

    def _should_process_signal(self, stock_code: str, strategy_name: str) -> bool:
        """신호 처리 여부 판단 (중복 방지)"""
        try:
            with self.signal_lock:
                current_time = time_module.time()

                # 기존 히스토리 확인
                if stock_code in self.signal_history:
                    history = self.signal_history[stock_code]

                    # 쿨다운 시간 체크
                    cooldown_until = history.get('cooldown_until', 0)
                    if current_time < cooldown_until:
                        return False

                    # 1분 이내 같은 전략 중복 체크
                    last_signal_time = history.get('last_signal_time', 0)
                    last_strategy = history.get('strategy', '')

                    if (strategy_name == last_strategy and
                        current_time - last_signal_time < 60):
                        return False

                return True

        except Exception as e:
            logger.error(f"신호 처리 여부 판단 오류: {e}")
            return True  # 오류시 허용

    def _generate_strategy_signal(self, strategy_name: str, market_data: Dict) -> Optional[Dict]:
        """전략별 신호 생성 (콜백용)"""
        try:
            stock_code = market_data['stock_code']
            current_price = market_data['current_price']
            change_rate = market_data['change_rate']
            market_sentiment = market_data.get('market_sentiment', {})

            # 🆕 시장 센티먼트 기반 기준 조정
            sentiment_multiplier = self._get_sentiment_multiplier(market_sentiment)

            # 기본 신호 생성 로직 사용 (센티먼트 반영)
            data_for_signal = {
                'current_price': current_price,
                'change_rate': change_rate,
                'volume': market_data.get('volume', 0),
                'timestamp': market_data.get('timestamp', time_module.time()),
                'sentiment_multiplier': sentiment_multiplier  # 🆕 센티먼트 승수
            }

            return self._generate_simple_signal(strategy_name, stock_code, data_for_signal)

        except Exception as e:
            logger.error(f"전략별 신호 생성 오류: {e}")
            return None

    def _get_sentiment_multiplier(self, sentiment: Dict) -> float:
        """🆕 시장 센티먼트에 따른 승수 계산"""
        try:
            bullish_score = sentiment.get('bullish_score', 50)
            volatility = sentiment.get('volatility', 'normal')
            volume_surge = sentiment.get('volume_surge', False)

            # 기본 승수
            multiplier = 1.0

            # 강세 시장일수록 기준 완화 (더 많은 기회)
            if bullish_score > 70:
                multiplier *= 0.8  # 20% 기준 완화
            elif bullish_score > 60:
                multiplier *= 0.9  # 10% 기준 완화
            elif bullish_score < 40:
                multiplier *= 1.2  # 20% 기준 강화 (보수적)
            elif bullish_score < 30:
                multiplier *= 1.4  # 40% 기준 강화 (매우 보수적)

            # 높은 변동성 시 기준 완화 (기회 확대)
            if volatility == 'high':
                multiplier *= 0.85
            elif volatility == 'low':
                multiplier *= 1.1

            # 거래량 급증 시 기준 완화
            if volume_surge:
                multiplier *= 0.9

            # 최종 승수 범위 제한 (0.6 ~ 1.5)
            multiplier = max(0.6, min(multiplier, 1.5))

            logger.debug(f"센티먼트 승수: {multiplier:.2f} (강세:{bullish_score}, 변동성:{volatility})")
            return multiplier

        except Exception as e:
            logger.error(f"센티먼트 승수 계산 오류: {e}")
            return 1.0

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
