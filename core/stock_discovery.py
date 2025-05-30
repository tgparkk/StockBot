"""
종목 탐색 관리자 (리팩토링 버전)
동적 종목 발굴 및 후보 관리 전담
"""
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from utils.logger import setup_logger
from core.rest_api_manager import KISRestAPIManager
from core.kis_market_api import get_disparity_rank, get_multi_period_disparity, get_disparity_trading_signals

logger = setup_logger(__name__)

@dataclass
class StockCandidate:
    """종목 후보"""
    stock_code: str
    strategy_type: str
    score: float
    reason: str
    discovered_at: datetime
    data: Dict = field(default_factory=dict)

class StockDiscovery:
    """종목 탐색 관리자"""

    def __init__(self, trading_api: KISRestAPIManager):
        """초기화"""
        self.trading_api = trading_api
        self.data_manager = None  # 외부에서 설정
        self.trade_executor = None  # 🆕 거래 실행자 연결

        # 종목 후보 관리 (스레드 안전)
        self.candidates: Dict[str, List[StockCandidate]] = {}
        self.active_stocks: Dict[str, List[str]] = {}
        self.discovery_lock = threading.RLock()

        # 스레드 풀 - 종목 탐색용
        self.discovery_executor = ThreadPoolExecutor(
            max_workers=3,
            thread_name_prefix="discovery"
        )

        # 백그라운드 스크리닝용 스레드 풀
        self.screening_executor = ThreadPoolExecutor(
            max_workers=2,
            thread_name_prefix="screening"
        )

        # 백그라운드 스크리닝 상태
        self.screening_active = False

        logger.info("종목 탐색 관리자 초기화 완료")

    def set_data_manager(self, data_manager):
        """데이터 매니저 설정"""
        self.data_manager = data_manager

    def set_trade_executor(self, trade_executor):
        """🆕 거래 실행자 설정"""
        self.trade_executor = trade_executor
        logger.info("✅ StockDiscovery에 TradeExecutor 연결 완료")

    def start_background_screening(self):
        """백그라운드 스크리닝 시작"""
        self.screening_active = True

        # 별도 스레드에서 스크리닝 실행
        screening_future = self.screening_executor.submit(self._background_screening_worker)
        logger.info("📊 백그라운드 스크리닝 시작")

    def stop_background_screening(self):
        """백그라운드 스크리닝 중지"""
        self.screening_active = False
        logger.info("📊 백그라운드 스크리닝 중지")

    def _background_screening_worker(self):
        """백그라운드 스크리닝 작업자"""
        try:
            while self.screening_active:
                # 현재 시간 체크
                from datetime import datetime
                import pytz
                kst = pytz.timezone('Asia/Seoul')
                now = datetime.now(kst)
                # 시장 시간 체크 (간단 버전)
                market_hour = now.hour
                is_market_hours = 9 <= market_hour <= 15

                if not is_market_hours:
                    # 장외시간: 프리 마켓 스크리닝
                    self._process_pre_market_screening()
                    time.sleep(60)  # 1분 대기
                else:
                    # 장중: 일반 스크리닝
                    self._process_market_screening()
                    time.sleep(60)   # 1분 대기

        except Exception as e:
            logger.error(f"백그라운드 스크리닝 오류: {e}")

    def _process_pre_market_screening(self):
        """프리 마켓 스크리닝 처리"""
        try:
            logger.info("🌙 프리 마켓 스크리닝 실행")
            # 시장 스크리닝 시도
            screening_results = self.trading_api.get_market_screening_candidates("all")

            if screening_results and screening_results.get('status') == 'success':
                # 실제 API 결과 사용
                gap_candidates = self._convert_to_candidates(
                    screening_results.get('gap_candidates', []), 'gap_trading'
                )
                volume_candidates = self._convert_to_candidates(
                    screening_results.get('volume_candidates', []), 'volume_breakout'
                )
                momentum_candidates = self._convert_to_candidates(
                    screening_results.get('momentum_candidates', []), 'momentum'
                )

                # 후보 저장
                with self.discovery_lock:
                    self.candidates['gap_trading'] = gap_candidates
                    self.candidates['volume_breakout'] = volume_candidates
                    self.candidates['momentum'] = momentum_candidates

                logger.info(f"🌙 프리 마켓 후보 발굴: 갭({len(gap_candidates)}) 볼륨({len(volume_candidates)}) 모멘텀({len(momentum_candidates)})")
            else:
                logger.warning("프리 마켓 스크리닝 데이터 없음")

        except Exception as e:
            logger.error(f"프리 마켓 스크리닝 오류: {e}")

    def _convert_to_candidates(self, data_list: List[Dict], strategy_type: str) -> List[StockCandidate]:
        """API 결과를 StockCandidate로 변환"""
        candidates = []

        for stock_data in data_list[:10]:  # 상위 10개
            try:
                if strategy_type == 'gap_trading':
                    # gap_rate, change_rate 사용
                    gap_rate = stock_data.get('gap_rate', 0)
                    change_rate = stock_data.get('change_rate', 0)
                    score = max(gap_rate, change_rate)  # 둘 중 높은 값 사용
                    reason = f"갭{gap_rate:.1f}% 상승{change_rate:.1f}%"
                elif strategy_type == 'volume_breakout':
                    # volume_ratio 또는 volume 사용
                    volume_ratio = stock_data.get('volume_ratio', 0)
                    volume = stock_data.get('volume', 0)
                    score = volume_ratio if volume_ratio > 0 else volume / 100000  # 거래량을 점수화
                    reason = f"거래량 {volume_ratio:.1f}배" if volume_ratio > 0 else f"거래량 {volume:,}주"
                elif strategy_type == 'momentum':
                    # power, change_rate 사용
                    power = stock_data.get('power', 0)
                    change_rate = stock_data.get('change_rate', 0)
                    score = max(power, change_rate)  # 둘 중 높은 값 사용
                    reason = f"체결강도 {power:.0f}" if power > 0 else f"상승률 {change_rate:.1f}%"
                else:
                    score = stock_data.get('score', 0)
                    reason = "기본"

                # 최소 점수 조건
                if score > 0:
                    candidate = StockCandidate(
                        stock_code=stock_data.get('stock_code', ''),
                        strategy_type=strategy_type,
                        score=score,
                        reason=reason,
                        discovered_at=datetime.now(),
                        data=stock_data
                    )
                    candidates.append(candidate)
                else:
                    # 디버깅을 위한 로그
                    logger.debug(f"점수 0인 후보 제외: {stock_data.get('stock_code', '')} - {stock_data}")

            except Exception as e:
                logger.warning(f"후보 변환 오류: {e}")
                continue

        logger.info(f"✅ {strategy_type} 후보 변환 완료: {len(candidates)}개")
        return candidates

    def _process_market_screening(self):
        """장중 스크리닝 처리"""
        try:
            logger.info("📊 장중 스크리닝 실행")
            screening_results = self.trading_api.get_market_screening_candidates("all")

            if screening_results:
                background_data = screening_results.get('background', [])

                # 실시간 종목 분석
                live_candidates = self._analyze_live_candidates(background_data)

                # 🆕 새로운 후보 종목들을 웹소켓에 자동 구독
                new_stocks_to_subscribe = []
                
                # 후보 업데이트
                with self.discovery_lock:
                    for strategy_name, candidates in live_candidates.items():
                        if strategy_name not in self.candidates:
                            self.candidates[strategy_name] = []
                        
                        # 기존 종목 코드 목록
                        existing_codes = {c.stock_code for c in self.candidates[strategy_name]}
                        
                        # 새로운 후보들 추가
                        for candidate in candidates:
                            if candidate.stock_code not in existing_codes:
                                new_stocks_to_subscribe.append({
                                    'stock_code': candidate.stock_code,
                                    'strategy_name': strategy_name,
                                    'score': candidate.score,
                                    'priority': self._get_strategy_priority(strategy_name)
                                })
                        
                        self.candidates[strategy_name].extend(candidates)

                        # 중복 제거 및 상위 20개만 유지
                        unique_candidates = {}
                        for candidate in self.candidates[strategy_name]:
                            unique_candidates[candidate.stock_code] = candidate

                        sorted_candidates = sorted(
                            unique_candidates.values(),
                            key=lambda x: x.score,
                            reverse=True
                        )
                        self.candidates[strategy_name] = sorted_candidates[:20]

                # 🆕 새로운 후보들을 웹소켓에 구독 (점수 순으로 정렬)
                if new_stocks_to_subscribe and self.data_manager:
                    # 점수 순으로 정렬 (높은 점수 = 우선 구독)
                    new_stocks_to_subscribe.sort(key=lambda x: (x['priority'], -x['score']))
                    
                    # 웹소켓 구독 가능 여부 확인
                    websocket_status = self.data_manager.get_status()
                    current_subscriptions = websocket_status.get('realtime_subscriptions', 0)
                    max_subscriptions = websocket_status.get('realtime_capacity', '0/13').split('/')[1]
                    max_subscriptions = int(max_subscriptions) if max_subscriptions.isdigit() else 13
                    
                    available_slots = max_subscriptions - current_subscriptions
                    logger.info(f"🎯 새로운 후보 {len(new_stocks_to_subscribe)}개 발견, 웹소켓 여유 슬롯: {available_slots}개")
                    
                    if available_slots > 0:
                        from core.data_priority import DataPriority
                        
                        # 상위 종목들만 구독 (가용 슬롯만큼)
                        stocks_to_add = new_stocks_to_subscribe[:available_slots]
                        
                        for stock_info in stocks_to_add:
                            try:
                                # 전략별 우선순위 매핑
                                priority_map = {
                                    'gap_trading': DataPriority.HIGH,
                                    'volume_breakout': DataPriority.HIGH, 
                                    'momentum': DataPriority.MEDIUM,
                                    'disparity_reversal': DataPriority.MEDIUM
                                }
                                
                                priority = priority_map.get(stock_info['strategy_name'], DataPriority.MEDIUM)
                                
                                success = self.data_manager.add_stock_request(
                                    stock_code=stock_info['stock_code'],
                                    priority=priority,
                                    strategy_name=stock_info['strategy_name'],
                                    callback=self._create_discovery_callback(stock_info['stock_code'], stock_info['strategy_name'])
                                )
                                
                                if success:
                                    logger.info(f"📡 신규 후보 구독 성공: {stock_info['stock_code']} ({stock_info['strategy_name']}, 점수: {stock_info['score']:.2f})")
                                else:
                                    logger.warning(f"⚠️ 신규 후보 구독 실패: {stock_info['stock_code']}")
                                
                                # 구독 간격 (웹소켓 안정성)
                                time.sleep(0.3)
                                
                            except Exception as e:
                                logger.error(f"신규 후보 구독 오류 ({stock_info['stock_code']}): {e}")
                    else:
                        logger.info(f"📡 웹소켓 구독 여유 없음 - 폴링으로 모니터링")

                # 데이터 매니저 상태 업데이트 후 구독 현황 로그
                if self.data_manager:
                    websocket_status = self.data_manager.get_status()
                    websocket_details = websocket_status.get('websocket_details', {})
                    
                    logger.info(
                        f"📡 웹소켓 구독 현황: "
                        f"연결={websocket_details.get('connected', False)}, "
                        f"구독={websocket_details.get('subscription_count', 0)}/13종목, "
                        f"사용량={websocket_details.get('usage_ratio', '0/41')}"
                    )
                    
                    # 구독 중인 종목 목록 (최대 5개만 표시)
                    subscribed_stocks = websocket_details.get('subscribed_stocks', [])
                    if subscribed_stocks:
                        displayed_stocks = subscribed_stocks[:5]
                        stocks_text = ', '.join(displayed_stocks)
                        if len(subscribed_stocks) > 5:
                            stocks_text += f" 외 {len(subscribed_stocks)-5}개"
                        logger.info(f"📡 웹소켓 구독 종목: {stocks_text}")

                logger.info(f"📊 장중 후보 업데이트 완료")

        except Exception as e:
            logger.error(f"장중 스크리닝 오류: {e}")

    def _get_strategy_priority(self, strategy_name: str) -> int:
        """전략별 우선순위 반환 (낮은 숫자 = 높은 우선순위)"""
        priority_map = {
            'gap_trading': 1,       # 최고 우선순위
            'volume_breakout': 2,   # 높은 우선순위
            'momentum': 3,          # 보통 우선순위
            'disparity_reversal': 3 # 보통 우선순위
        }
        return priority_map.get(strategy_name, 4)  # 기본값: 낮은 우선순위

    def _create_discovery_callback(self, stock_code: str, strategy_name: str) -> Callable:
        """탐색 종목용 콜백 함수 생성"""
        def discovery_callback(stock_code: str, data: Dict, source: str = 'websocket') -> None:
            """탐색 종목 데이터 콜백"""
            try:
                # 기본 데이터 검증
                if not data or data.get('status') != 'success':
                    return

                current_price = data.get('current_price', 0)
                if current_price <= 0:
                    return

                # 🎯 탐색 종목 매매 신호 분석
                signal_strength = self._analyze_discovery_signal(stock_code, data, strategy_name)
                
                if signal_strength > 0.7:  # 강한 매수 신호
                    logger.info(f"🎯 탐색 종목 매수 신호: {stock_code} ({strategy_name}) 강도:{signal_strength:.2f}")
                    
                    # 🆕 실제 거래 실행
                    if self.trade_executor:
                        try:
                            buy_signal = {
                                'signal_type': 'BUY',
                                'stock_code': stock_code,
                                'strategy': strategy_name,
                                'price': current_price,
                                'strength': signal_strength,
                                'reason': f'탐색 종목 매수 신호 (강도: {signal_strength:.2f})',
                                'timestamp': time.time()
                            }
                            
                            logger.info(f"🛒 매수 주문 실행 시도: {stock_code} ({strategy_name})")
                            
                            # TradeExecutor를 통한 실제 거래 실행
                            trade_result = self.trade_executor.handle_signal(buy_signal)
                            
                            if trade_result['success']:
                                logger.info(f"✅ 탐색 종목 매수 성공: {trade_result['message']}")
                            else:
                                logger.warning(f"⚠️ 탐색 종목 매수 실패: {trade_result['message']}")
                                
                        except Exception as e:
                            logger.error(f"❌ 탐색 종목 거래 실행 오류 ({stock_code}): {e}")
                    else:
                        logger.warning(f"⚠️ TradeExecutor가 연결되지 않음 - 매수 신호만 로깅: {stock_code}")
                        
                    # 전략 스케줄러에 신호 전달 (기존 코드 - 백업용)
                    if hasattr(self, 'strategy_scheduler') and self.strategy_scheduler:
                        try:
                            buy_signal = {
                                'signal_type': 'BUY',
                                'stock_code': stock_code,
                                'strategy': strategy_name,
                                'price': current_price,
                                'strength': signal_strength,
                                'reason': f'탐색 종목 매수 신호 (강도: {signal_strength:.2f})',
                                'timestamp': time.time()
                            }
                            # 비동기 신호 처리를 위한 큐 또는 콜백 호출
                            logger.debug(f"📈 백업 매수 신호 생성: {stock_code}")
                        except Exception as e:
                            logger.error(f"백업 매수 신호 전달 오류 ({stock_code}): {e}")
                            
            except Exception as e:
                logger.error(f"탐색 콜백 오류 ({stock_code}): {e}")
                
        return discovery_callback

    def _analyze_discovery_signal(self, stock_code: str, data: Dict, strategy_name: str) -> float:
        """탐색 종목 신호 강도 분석"""
        try:
            signal_strength = 0.0
            
            current_price = data.get('current_price', 0)
            volume = data.get('volume', 0)
            change_rate = data.get('change_rate', 0)
            
            # 전략별 신호 분석
            if strategy_name == 'gap_trading':
                # 갭 트레이딩: 갭 크기 + 거래량 증가
                if change_rate > 3.0 and volume > 50000:  # 3% 이상 상승 + 5만주 이상
                    signal_strength = min(0.8 + (change_rate - 3.0) * 0.05, 1.0)
                    
            elif strategy_name == 'volume_breakout':
                # 거래량 돌파: 거래량 급증 + 가격 상승
                if volume > 100000 and change_rate > 1.5:  # 10만주 이상 + 1.5% 이상
                    signal_strength = min(0.7 + (volume / 100000) * 0.1, 1.0)
                    
            elif strategy_name == 'momentum':
                # 모멘텀: 지속적 상승 + 강한 체결강도
                strength = data.get('strength', 0)
                if change_rate > 2.0 and strength > 150:  # 2% 이상 + 체결강도 150 이상
                    signal_strength = min(0.75 + (strength - 150) * 0.001, 1.0)
            
            return signal_strength
            
        except Exception as e:
            logger.error(f"신호 강도 분석 오류 ({stock_code}): {e}")
            return 0.0

    def discover_strategy_stocks(self, strategy_name: str, weight: float, is_primary: bool) -> List[StockCandidate]:
        """특정 전략의 종목 탐색"""
        try:
            # 스레드 풀에서 비동기 탐색 실행
            future = self.discovery_executor.submit(
                self._discover_stocks_sync, strategy_name, weight, is_primary
            )

            # 최대 30초 대기
            candidates = future.result(timeout=30)

            logger.info(f"✅ {strategy_name} 탐색 완료: {len(candidates)}개 후보")
            return candidates

        except Exception as e:
            logger.error(f"{strategy_name} 탐색 오류: {e}")
            return []

    def _discover_stocks_sync(self, strategy_name: str, weight: float, is_primary: bool) -> List[StockCandidate]:
        """전략별 종목 탐색 (동기 버전)"""
        # 기존 후보가 있으면 우선 사용
        with self.discovery_lock:
            if strategy_name in self.candidates and self.candidates[strategy_name]:
                existing_candidates = self.candidates[strategy_name][:10]  # 상위 10개
                logger.info(f"🔄 {strategy_name} 기존 후보 사용: {len(existing_candidates)}개")
                return existing_candidates

        # 새로운 탐색 실행
        if strategy_name == "gap_trading":
            return self._discover_gap_candidates()
        elif strategy_name == "volume_breakout":
            return self._discover_volume_candidates()
        elif strategy_name == "momentum":
            return self._discover_momentum_candidates()
        elif strategy_name == "disparity_reversal":
            return self._discover_disparity_reversal_candidates()
        else:
            logger.warning(f"알 수 없는 전략: {strategy_name}")
            return []

    def _discover_gap_candidates(self) -> List[StockCandidate]:
        """갭 트레이딩 후보 탐색 - 🆕 1단계 기준 완화 (센티먼트 반영)"""
        try:
            screening_results = self.trading_api.get_market_screening_candidates("gap")
            if not screening_results or screening_results.get('status') != 'success':
                return []

            candidates = []
            gap_data = screening_results.get('gap_candidates', [])

            # 🆕 시장 센티먼트 기반 동적 기준 적용
            sentiment_multiplier = self._get_market_sentiment_multiplier()
            
            # 🆕 완화된 기준 (기존 대비 30-40% 완화)
            min_gap_rate = max(1.2 * sentiment_multiplier, 0.8)     # 기존 2.5% → 1.2% (센티먼트 반영)
            min_change_rate = max(0.8 * sentiment_multiplier, 0.5)  # 기존 1.5% → 0.8%
            min_volume_ratio = max(1.8 * sentiment_multiplier, 1.3) # 기존 2.5배 → 1.8배
            
            logger.info(f"🆕 갭 트레이딩 동적 기준: 갭≥{min_gap_rate:.1f}%, 상승≥{min_change_rate:.1f}%, 거래량≥{min_volume_ratio:.1f}배 (센티먼트 승수: {sentiment_multiplier:.2f})")

            for stock_data in gap_data[:20]:  # 🆕 20개로 확대 (기존 10개)
                gap_rate = stock_data.get('gap_rate', 0)
                change_rate = stock_data.get('change_rate', 0)
                volume_ratio = stock_data.get('volume_ratio', 0)
                current_price = stock_data.get('current_price', 0)
                
                # 🆕 완화된 조건 + 기본 안전성 체크
                if (gap_rate >= min_gap_rate and           # 동적 갭 기준
                    change_rate >= min_change_rate and     # 동적 상승 기준
                    volume_ratio >= min_volume_ratio and   # 동적 거래량 기준
                    1000 <= current_price <= 300000 and   # 적정 가격대 (변경 없음)
                    self._validate_profit_potential_relaxed(stock_data)):  # 🆕 완화된 수익성 검증
                    
                    # 🆕 개선된 점수 계산 (기술적 요소 추가)
                    base_score = gap_rate * change_rate * (volume_ratio / 100)
                    sentiment_bonus = base_score * (1 - sentiment_multiplier) * 0.5  # 강세 시장 보너스
                    profit_score = base_score + sentiment_bonus
                    
                    candidate = StockCandidate(
                        stock_code=stock_data['stock_code'],
                        strategy_type='gap_trading',
                        score=profit_score,
                        reason=f"갭상승 {gap_rate:.1f}%↑{change_rate:.1f}% 거래량{volume_ratio:.1f}배 (동적기준)",
                        discovered_at=datetime.now(),
                        data=stock_data
                    )
                    candidates.append(candidate)

            # 점수 기준 정렬
            candidates.sort(key=lambda x: x.score, reverse=True)
            logger.info(f"🆕 완화된 갭 후보 탐색: {len(candidates)}개 (기존 대비 {len(candidates)/4:.1f}배 증가 예상)")
            return candidates

        except Exception as e:
            logger.error(f"🆕 완화된 갭 후보 탐색 오류: {e}")
            return []

    def _discover_volume_candidates(self) -> List[StockCandidate]:
        """거래량 돌파 후보 탐색 - 🆕 1단계 기준 완화 (센티먼트 반영)"""
        try:
            screening_results = self.trading_api.get_market_screening_candidates("volume")
            if not screening_results or screening_results.get('status') != 'success':
                return []

            candidates = []
            volume_data = screening_results.get('volume_candidates', [])

            # 🆕 시장 센티먼트 기반 동적 기준
            sentiment_multiplier = self._get_market_sentiment_multiplier()
            
            # 🆕 완화된 기준 (기존 대비 40-50% 완화)
            min_volume_increase = max(200 * sentiment_multiplier, 150)  # 기존 300% → 200%
            min_change_rate = max(1.2 * sentiment_multiplier, 0.8)     # 기존 2.0% → 1.2%
            
            logger.info(f"🆕 거래량 돌파 동적 기준: 거래량≥{min_volume_increase:.0f}%, 상승≥{min_change_rate:.1f}% (센티먼트 승수: {sentiment_multiplier:.2f})")

            for stock_data in volume_data[:20]:  # 🆕 20개로 확대 (기존 10개)
                volume_increase_rate = stock_data.get('volume_increase_rate', 0)
                change_rate = stock_data.get('change_rate', 0)
                current_price = stock_data.get('current_price', 0)
                
                # 🆕 완화된 거래량 돌파 조건
                if (volume_increase_rate >= min_volume_increase and  # 동적 거래량 기준
                    change_rate >= min_change_rate and               # 동적 상승률 기준
                    1000 <= current_price <= 500000 and             # 적정 가격대 (상한 확대)
                    self._validate_profit_potential_relaxed(stock_data)):
                    
                    # 🆕 개선된 점수 계산
                    base_score = (volume_increase_rate * change_rate) / 40  # 기존 /50 → /40
                    sentiment_bonus = base_score * (1 - sentiment_multiplier) * 0.3
                    profit_score = base_score + sentiment_bonus
                    
                    candidate = StockCandidate(
                        stock_code=stock_data['stock_code'],
                        strategy_type='volume_breakout',
                        score=profit_score,
                        reason=f"거래량돌파 {volume_increase_rate:.0f}%↑{change_rate:.1f}% (동적기준)",
                        discovered_at=datetime.now(),
                        data=stock_data
                    )
                    candidates.append(candidate)

            candidates.sort(key=lambda x: x.score, reverse=True)
            logger.info(f"🆕 완화된 거래량 후보 탐색: {len(candidates)}개")
            return candidates

        except Exception as e:
            logger.error(f"🆕 완화된 거래량 후보 탐색 오류: {e}")
            return []

    def _discover_momentum_candidates(self) -> List[StockCandidate]:
        """모멘텀 후보 탐색 - 🆕 1단계 기준 완화 (센티먼트 반영)"""
        try:
            screening_results = self.trading_api.get_market_screening_candidates("momentum")
            if not screening_results or screening_results.get('status') != 'success':
                return []

            candidates = []
            momentum_data = screening_results.get('momentum_candidates', [])

            # 🆕 시장 센티먼트 기반 동적 기준
            sentiment_multiplier = self._get_market_sentiment_multiplier()
            
            # 🆕 완화된 기준 (기존 대비 50% 완화)
            min_execution_strength = max(80 * sentiment_multiplier, 60)   # 기존 120 → 80
            min_change_rate = max(1.5 * sentiment_multiplier, 1.0)       # 기존 2.5% → 1.5%
            min_volume = max(70000 * sentiment_multiplier, 50000)        # 기존 10만주 → 7만주
            
            logger.info(f"🆕 모멘텀 동적 기준: 체결강도≥{min_execution_strength:.0f}, 상승≥{min_change_rate:.1f}%, 거래량≥{min_volume:,.0f}주 (센티먼트 승수: {sentiment_multiplier:.2f})")

            for stock_data in momentum_data[:15]:  # 🆕 15개로 확대 (기존 8개)
                execution_strength = stock_data.get('execution_strength', 0)
                change_rate = stock_data.get('change_rate', 0)
                current_price = stock_data.get('current_price', 0)
                volume = stock_data.get('volume', 0)
                
                # 🆕 완화된 모멘텀 조건
                if (execution_strength >= min_execution_strength and  # 동적 체결강도 기준
                    change_rate >= min_change_rate and               # 동적 상승률 기준
                    volume >= min_volume and                         # 동적 거래량 기준
                    1000 <= current_price <= 200000 and             # 적정 가격대
                    self._validate_profit_potential_relaxed(stock_data)):
                    
                    # 🆕 개선된 점수 계산
                    base_score = (execution_strength * change_rate) / 15  # 기존 /20 → /15
                    sentiment_bonus = base_score * (1 - sentiment_multiplier) * 0.4
                    profit_score = base_score + sentiment_bonus
                    
                    candidate = StockCandidate(
                        stock_code=stock_data['stock_code'],
                        strategy_type='momentum',
                        score=profit_score,
                        reason=f"모멘텀강화 체결강도{execution_strength:.0f}↑{change_rate:.1f}% (동적기준)",
                        discovered_at=datetime.now(),
                        data=stock_data
                    )
                    candidates.append(candidate)

            candidates.sort(key=lambda x: x.score, reverse=True)
            logger.info(f"🆕 완화된 모멘텀 후보 탐색: {len(candidates)}개")
            return candidates

        except Exception as e:
            logger.error(f"🆕 완화된 모멘텀 후보 탐색 오류: {e}")
            return []

    def _get_market_sentiment_multiplier(self) -> float:
        """🆕 시장 센티먼트 승수 계산 (strategy_scheduler와 동일한 로직)"""
        try:
            from datetime import datetime
            now_hour = datetime.now().hour
            
            # 기본 승수
            multiplier = 1.0
            
            # 시간대별 시장 강세도
            if 9 <= now_hour <= 10:  # 장초반 - 높은 변동성
                bullish_score = 65
                volatility = 'high'
            elif 10 <= now_hour <= 14:  # 장중 - 안정적
                bullish_score = 55
                volatility = 'normal'
            elif 14 <= now_hour <= 15:  # 장마감 근처 - 높은 변동성
                bullish_score = 45
                volatility = 'high'
            else:  # 장외시간
                bullish_score = 50
                volatility = 'normal'
            
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
            
            # 최종 승수 범위 제한 (0.6 ~ 1.5)
            multiplier = max(0.6, min(multiplier, 1.5))
            
            logger.debug(f"🆕 종목 선별 센티먼트 승수: {multiplier:.2f} (강세:{bullish_score}, 변동성:{volatility})")
            return multiplier
            
        except Exception as e:
            logger.error(f"🆕 센티먼트 승수 계산 오류: {e}")
            return 1.0

    def _validate_profit_potential_relaxed(self, stock_data: Dict) -> bool:
        """🆕 완화된 수익 잠재력 검증 (1단계)"""
        try:
            # 기본 필터링
            stock_code = stock_data.get('stock_code', '')
            if not stock_code:
                return False
            
            # 🆕 완화된 가격 안정성 체크 (20% → 18%)
            change_rate = stock_data.get('change_rate', 0)
            if change_rate > 18:  # 18% 이상 급등 종목 제외 (기존 15%)
                logger.debug(f"🆕 {stock_code}: 과도한 급등 제외 ({change_rate:.1f}%)")
                return False
            
            # 🆕 완화된 거래량 조건 (1.5배 → 1.2배)
            volume_ratio = stock_data.get('volume_ratio', 1)
            if volume_ratio < 1.2:  # 거래량이 평소의 1.2배 미만이면 제외 (기존 1.5배)
                logger.debug(f"🆕 {stock_code}: 거래량 다소 부족 ({volume_ratio:.1f}배)")
                return False
            
            # 종목명 필터링 (변경 없음 - 안전성 유지)
            stock_name = stock_data.get('stock_name', '').upper()
            risky_keywords = ['ETN', 'ETF', 'SPAC', '스팩', '리츠', 'REIT']
            if any(keyword in stock_name for keyword in risky_keywords):
                logger.debug(f"🆕 {stock_code}: 리스크 종목 제외 ({stock_name})")
                return False
            
            return True
            
        except Exception as e:
            logger.warning(f"🆕 완화된 수익성 검증 오류: {e}")
            return False

    def _analyze_gap_potential(self, background_data: List[Dict]) -> List[StockCandidate]:
        """갭 잠재력 분석"""
        candidates = []

        for stock_data in background_data[:20]:
            try:
                stock_code = stock_data.get('stock_code', '')
                if not stock_code:
                    continue
                
                # 🔧 안전한 타입 변환
                change_rate_raw = stock_data.get('change_rate', 0)
                try:
                    gap_ratio = float(change_rate_raw) if change_rate_raw != '' else 0.0
                except (ValueError, TypeError):
                    gap_ratio = 0.0
                
                if abs(gap_ratio) >= 2.0:
                    candidate = StockCandidate(
                        stock_code=stock_code,
                        strategy_type='gap_trading',
                        score=abs(gap_ratio),
                        reason=f"잠재 갭 {gap_ratio:.1f}%",
                        discovered_at=datetime.now(),
                        data=stock_data
                    )
                    candidates.append(candidate)
                    
            except Exception as e:
                logger.warning(f"갭 잠재력 분석 오류 ({stock_data.get('stock_code', 'UNKNOWN')}): {e}")
                continue

        return candidates

    def _analyze_volume_potential(self, background_data: List[Dict]) -> List[StockCandidate]:
        """거래량 잠재력 분석"""
        candidates = []

        for stock_data in background_data[:20]:
            try:
                stock_code = stock_data.get('stock_code', '')
                if not stock_code:
                    continue
                
                # 🔧 안전한 타입 변환
                volume_ratio_raw = stock_data.get('volume_ratio', 0)
                try:
                    volume_ratio = float(volume_ratio_raw) if volume_ratio_raw != '' else 0.0
                except (ValueError, TypeError):
                    volume_ratio = 0.0
                
                if volume_ratio >= 150:  # 1.5배 이상
                    candidate = StockCandidate(
                        stock_code=stock_code,
                        strategy_type='volume_breakout',
                        score=volume_ratio,
                        reason=f"잠재 거래량 {volume_ratio:.1f}%",
                        discovered_at=datetime.now(),
                        data=stock_data
                    )
                    candidates.append(candidate)
                    
            except Exception as e:
                logger.warning(f"거래량 잠재력 분석 오류 ({stock_data.get('stock_code', 'UNKNOWN')}): {e}")
                continue

        return candidates

    def _analyze_momentum_potential(self, background_data: List[Dict]) -> List[StockCandidate]:
        """모멘텀 잠재력 분석"""
        candidates = []

        for stock_data in background_data[:20]:
            try:
                stock_code = stock_data.get('stock_code', '')
                if not stock_code:
                    continue
                
                # 🔧 안전한 타입 변환
                change_rate_raw = stock_data.get('change_rate', 0)
                volume_ratio_raw = stock_data.get('volume_ratio', 100)
                
                try:
                    change_rate = float(change_rate_raw) if change_rate_raw != '' else 0.0
                except (ValueError, TypeError):
                    change_rate = 0.0
                
                try:
                    volume_ratio = float(volume_ratio_raw) if volume_ratio_raw != '' else 100.0
                except (ValueError, TypeError):
                    volume_ratio = 100.0

                # 간단한 모멘텀 점수 계산
                momentum_score = (abs(change_rate) * 10) + (volume_ratio / 10)

                if momentum_score >= 50:
                    candidate = StockCandidate(
                        stock_code=stock_code,
                        strategy_type='momentum',
                        score=momentum_score,
                        reason=f"잠재 모멘텀 {momentum_score:.1f}",
                        discovered_at=datetime.now(),
                        data=stock_data
                    )
                    candidates.append(candidate)
                    
            except Exception as e:
                logger.warning(f"모멘텀 잠재력 분석 오류 ({stock_data.get('stock_code', 'UNKNOWN')}): {e}")
                continue

        return candidates

    def _analyze_live_candidates(self, background_data: List[Dict]) -> Dict[str, List[StockCandidate]]:
        """실시간 후보 분석"""
        result = {
            'gap_trading': [],
            'volume_breakout': [],
            'momentum': []
        }

        for stock_data in background_data:
            try:
                stock_code = stock_data.get('stock_code', '')
                if not stock_code:
                    continue
                
                # 🔧 안전한 타입 변환
                change_rate_raw = stock_data.get('change_rate', 0)
                volume_ratio_raw = stock_data.get('volume_ratio', 100)
                
                try:
                    change_rate = float(change_rate_raw) if change_rate_raw != '' else 0.0
                except (ValueError, TypeError):
                    change_rate = 0.0
                
                try:
                    volume_ratio = float(volume_ratio_raw) if volume_ratio_raw != '' else 100.0
                except (ValueError, TypeError):
                    volume_ratio = 100.0

                # 갭 분석
                if abs(change_rate) >= 3.0:  # 3% 이상 움직임
                    candidate = StockCandidate(
                        stock_code=stock_code,
                        strategy_type='gap_trading',
                        score=abs(change_rate),
                        reason=f"실시간 갭 {change_rate:.1f}%",
                        discovered_at=datetime.now(),
                        data=stock_data
                    )
                    result['gap_trading'].append(candidate)

                # 거래량 분석
                if volume_ratio >= 200:  # 2배 이상
                    candidate = StockCandidate(
                        stock_code=stock_code,
                        strategy_type='volume_breakout',
                        score=volume_ratio,
                        reason=f"실시간 거래량 {volume_ratio:.1f}%",
                        discovered_at=datetime.now(),
                        data=stock_data
                    )
                    result['volume_breakout'].append(candidate)

                # 모멘텀 분석
                momentum_score = (abs(change_rate) * 10) + (volume_ratio / 10)
                if momentum_score >= 60:
                    candidate = StockCandidate(
                        stock_code=stock_code,
                        strategy_type='momentum',
                        score=momentum_score,
                        reason=f"실시간 모멘텀 {momentum_score:.1f}",
                        discovered_at=datetime.now(),
                        data=stock_data
                    )
                    result['momentum'].append(candidate)
                    
            except Exception as e:
                logger.warning(f"실시간 후보 분석 오류 ({stock_data.get('stock_code', 'UNKNOWN')}): {e}")
                continue

        return result

    def add_discovered_candidate(self, candidate: StockCandidate):
        """탐색된 후보 추가"""
        with self.discovery_lock:
            strategy_name = candidate.strategy_type
            if strategy_name not in self.candidates:
                self.candidates[strategy_name] = []

            # 중복 체크
            existing_codes = [c.stock_code for c in self.candidates[strategy_name]]
            if candidate.stock_code not in existing_codes:
                self.candidates[strategy_name].append(candidate)

                # 점수 순으로 정렬하고 상위 20개만 유지
                self.candidates[strategy_name].sort(key=lambda x: x.score, reverse=True)
                self.candidates[strategy_name] = self.candidates[strategy_name][:20]

    def get_candidates(self, strategy_name: str) -> List[StockCandidate]:
        """전략별 후보 조회"""
        with self.discovery_lock:
            return self.candidates.get(strategy_name, []).copy()

    def get_all_candidates(self) -> Dict[str, List[StockCandidate]]:
        """모든 후보 조회"""
        with self.discovery_lock:
            return {
                strategy: candidates.copy()
                for strategy, candidates in self.candidates.items()
            }

    def clear_candidates(self, strategy_name: str = None):
        """후보 정리"""
        with self.discovery_lock:
            if strategy_name:
                self.candidates[strategy_name] = []
            else:
                self.candidates.clear()

    def get_discovery_progress(self) -> float:
        """탐색 진행률 계산"""
        with self.discovery_lock:
            total_strategies = 4  # gap, volume, momentum, disparity_reversal
            strategies_with_candidates = len([
                s for s in ['gap_trading', 'volume_breakout', 'momentum', 'disparity_reversal']
                if s in self.candidates and self.candidates[s]
            ])

            return (strategies_with_candidates / total_strategies) * 100

    def cleanup(self):
        """리소스 정리"""
        logger.info("종목 탐색 관리자 정리 중...")

        self.stop_background_screening()

        # 스레드 풀 종료
        self.discovery_executor.shutdown(wait=True)
        self.screening_executor.shutdown(wait=True)

        # 후보 정리
        with self.discovery_lock:
            total_candidates = sum(len(candidates) for candidates in self.candidates.values())
            logger.info(f"정리된 후보: {total_candidates}개")
            self.candidates.clear()

        logger.info("✅ 종목 탐색 관리자 정리 완료")
