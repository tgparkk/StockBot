"""
종목 탐색 관리자 (리팩토링 버전)
동적 종목 발굴 및 후보 관리 전담
"""
import time
import threading
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from utils.logger import setup_logger
from core.rest_api_manager import KISRestAPIManager

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
                    score = stock_data.get('change_rate', 0)
                    reason = f"상승률 {score:.1f}%"
                elif strategy_type == 'volume_breakout':
                    score = stock_data.get('volume_increase_rate', 0)
                    reason = f"거래량 {score:.1f}% 증가"
                elif strategy_type == 'momentum':
                    score = stock_data.get('execution_strength', 0)
                    reason = f"체결강도 {score:.0f}"
                else:
                    score = 0
                    reason = "기본"

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

            except Exception as e:
                logger.warning(f"후보 변환 오류: {e}")
                continue

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

                # 후보 업데이트
                with self.discovery_lock:
                    for strategy_name, candidates in live_candidates.items():
                        if strategy_name not in self.candidates:
                            self.candidates[strategy_name] = []
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

                logger.info("📊 장중 후보 업데이트 완료")

        except Exception as e:
            logger.error(f"장중 스크리닝 오류: {e}")

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
        else:
            logger.warning(f"알 수 없는 전략: {strategy_name}")
            return []

    def _discover_gap_candidates(self) -> List[StockCandidate]:
        """갭 트레이딩 후보 탐색"""
        try:
            screening_results = self.trading_api.get_market_screening_candidates("gap")
            if not screening_results or screening_results.get('status') != 'success':
                return []

            candidates = []
            gap_data = screening_results.get('gap_candidates', [])

            for stock_data in gap_data[:15]:  # 상위 15개
                if stock_data.get('change_rate', 0) >= 1.5:  # 1.5% 이상 상승
                    candidate = StockCandidate(
                        stock_code=stock_data['stock_code'],
                        strategy_type='gap_trading',
                        score=stock_data.get('change_rate', 0),
                        reason=f"상승률 {stock_data.get('change_rate', 0):.1f}%",
                        discovered_at=datetime.now(),
                        data=stock_data
                    )
                    candidates.append(candidate)

            logger.info(f"갭 후보 탐색: {len(candidates)}개")
            return candidates

        except Exception as e:
            logger.error(f"갭 후보 탐색 오류: {e}")
            return []

    def _discover_volume_candidates(self) -> List[StockCandidate]:
        """거래량 돌파 후보 탐색"""
        try:
            screening_results = self.trading_api.get_market_screening_candidates("volume")
            if not screening_results or screening_results.get('status') != 'success':
                return []

            candidates = []
            volume_data = screening_results.get('volume_candidates', [])

            for stock_data in volume_data[:15]:  # 상위 15개
                volume_increase_rate = stock_data.get('volume_increase_rate', 0)
                if volume_increase_rate >= 100:  # 100% 이상 증가
                    candidate = StockCandidate(
                        stock_code=stock_data['stock_code'],
                        strategy_type='volume_breakout',
                        score=volume_increase_rate,
                        reason=f"거래량 {volume_increase_rate:.1f}% 증가",
                        discovered_at=datetime.now(),
                        data=stock_data
                    )
                    candidates.append(candidate)

            logger.info(f"거래량 후보 탐색: {len(candidates)}개")
            return candidates

        except Exception as e:
            logger.error(f"거래량 후보 탐색 오류: {e}")
            return []

    def _discover_momentum_candidates(self) -> List[StockCandidate]:
        """모멘텀 후보 탐색"""
        try:
            screening_results = self.trading_api.get_market_screening_candidates("momentum")
            if not screening_results or screening_results.get('status') != 'success':
                return []

            candidates = []
            momentum_data = screening_results.get('momentum_candidates', [])

            for stock_data in momentum_data[:15]:  # 상위 15개
                execution_strength = stock_data.get('execution_strength', 0)
                if execution_strength >= 60:  # 60 이상
                    candidate = StockCandidate(
                        stock_code=stock_data['stock_code'],
                        strategy_type='momentum',
                        score=execution_strength,
                        reason=f"체결강도 {execution_strength:.0f}",
                        discovered_at=datetime.now(),
                        data=stock_data
                    )
                    candidates.append(candidate)

            logger.info(f"모멘텀 후보 탐색: {len(candidates)}개")
            return candidates

        except Exception as e:
            logger.error(f"모멘텀 후보 탐색 오류: {e}")
            return []

    def _analyze_gap_potential(self, background_data: List[Dict]) -> List[StockCandidate]:
        """갭 잠재력 분석"""
        candidates = []

        for stock_data in background_data[:20]:
            gap_ratio = stock_data.get('change_rate', 0)  # 등락률을 갭으로 가정
            if abs(gap_ratio) >= 2.0:
                candidate = StockCandidate(
                    stock_code=stock_data['stock_code'],
                    strategy_type='gap_trading',
                    score=abs(gap_ratio),
                    reason=f"잠재 갭 {gap_ratio:.1f}%",
                    discovered_at=datetime.now(),
                    data=stock_data
                )
                candidates.append(candidate)

        return candidates

    def _analyze_volume_potential(self, background_data: List[Dict]) -> List[StockCandidate]:
        """거래량 잠재력 분석"""
        candidates = []

        for stock_data in background_data[:20]:
            volume_ratio = stock_data.get('volume_ratio', 0)
            if volume_ratio >= 150:  # 1.5배 이상
                candidate = StockCandidate(
                    stock_code=stock_data['stock_code'],
                    strategy_type='volume_breakout',
                    score=volume_ratio,
                    reason=f"잠재 거래량 {volume_ratio}%",
                    discovered_at=datetime.now(),
                    data=stock_data
                )
                candidates.append(candidate)

        return candidates

    def _analyze_momentum_potential(self, background_data: List[Dict]) -> List[StockCandidate]:
        """모멘텀 잠재력 분석"""
        candidates = []

        for stock_data in background_data[:20]:
            change_rate = abs(stock_data.get('change_rate', 0))
            volume_ratio = stock_data.get('volume_ratio', 100)

            # 간단한 모멘텀 점수 계산
            momentum_score = (change_rate * 10) + (volume_ratio / 10)

            if momentum_score >= 50:
                candidate = StockCandidate(
                    stock_code=stock_data['stock_code'],
                    strategy_type='momentum',
                    score=momentum_score,
                    reason=f"잠재 모멘텀 {momentum_score:.1f}",
                    discovered_at=datetime.now(),
                    data=stock_data
                )
                candidates.append(candidate)

        return candidates

    def _analyze_live_candidates(self, background_data: List[Dict]) -> Dict[str, List[StockCandidate]]:
        """실시간 후보 분석"""
        result = {
            'gap_trading': [],
            'volume_breakout': [],
            'momentum': []
        }

        for stock_data in background_data:
            stock_code = stock_data['stock_code']
            change_rate = stock_data.get('change_rate', 0)
            volume_ratio = stock_data.get('volume_ratio', 100)

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
                    reason=f"실시간 거래량 {volume_ratio}%",
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
            total_strategies = 3  # gap, volume, momentum
            strategies_with_candidates = len([
                s for s in ['gap_trading', 'volume_breakout', 'momentum']
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
