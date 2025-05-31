"""
종목 탐색 관리자 (리팩토링 버전)
동적 종목 발굴 및 후보 관리 전담
"""
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Callable, Any, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from utils.logger import setup_logger
from ..api.rest_api_manager import KISRestAPIManager
from ..api.kis_market_api import get_disparity_rank, get_multi_period_disparity, get_disparity_trading_signals
from ..data.data_priority import DataPriority

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

        # 🆕 백그라운드 스크리닝 관련 코드 제거 (스케줄러에서 중앙집중 처리)
        # self.screening_executor = ThreadPoolExecutor(...)  # 제거
        # self.screening_active = False  # 제거

        logger.info("종목 탐색 관리자 초기화 완료 (중앙집중 모드)")

    def set_data_manager(self, data_manager):
        """데이터 매니저 설정"""
        self.data_manager = data_manager

    def set_trade_executor(self, trade_executor):
        """🆕 거래 실행자 설정"""
        self.trade_executor = trade_executor
        logger.info("✅ StockDiscovery에 TradeExecutor 연결 완료")

    # 🆕 백그라운드 스크리닝 메서드들 제거 (스케줄러 중앙집중 방식으로 대체)
    # def start_background_screening(self): - 제거
    # def stop_background_screening(self): - 제거  
    # def _background_screening_worker(self): - 제거
    # def _process_pre_market_screening(self): - 제거
    # def _process_market_screening(self): - 제거

    def discover_strategy_stocks(self, strategy_name: str, weight: float, is_primary: bool) -> List[StockCandidate]:
        """🎯 특정 전략의 종목 탐색 (스케줄러에서 호출)"""
        try:
            logger.info(f"🔍 [{strategy_name}] 전략 후보 탐색 시작 (가중치: {weight}, 우선순위: {'주요' if is_primary else '보조'})")

            # 🆕 기존 후보 우선 활용 (중복 API 호출 방지)
            with self.discovery_lock:
                if strategy_name in self.candidates and self.candidates[strategy_name]:
                    existing_candidates = self.candidates[strategy_name][:15]  # 상위 15개
                    logger.info(f"♻️ [{strategy_name}] 기존 후보 재활용: {len(existing_candidates)}개")
                    return existing_candidates

            # 🆕 개별 전략 탐색은 최소한으로 유지 (스케줄러가 주로 처리)
            logger.info(f"🔍 [{strategy_name}] 개별 전략 탐색 실행...")
            
            # 스레드 풀에서 비동기 탐색 실행
            future = self.discovery_executor.submit(
                self._discover_stocks_sync, strategy_name, weight, is_primary
            )

            # 최대 20초 대기 (단축)
            candidates = future.result(timeout=20)

            logger.info(f"✅ [{strategy_name}] 탐색 완료: {len(candidates)}개 후보")
            return candidates

        except Exception as e:
            logger.error(f"❌ [{strategy_name}] 탐색 오류: {e}")
            return []

    def _discover_stocks_sync(self, strategy_name: str, weight: float, is_primary: bool) -> List[StockCandidate]:
        """🎯 전략별 종목 탐색 (동기 버전) - 단순화"""
        try:
            # 🆕 기존 후보가 있으면 우선 사용 (API 호출 최소화)
            with self.discovery_lock:
                if strategy_name in self.candidates and self.candidates[strategy_name]:
                    existing_candidates = self.candidates[strategy_name][:12]  # 상위 12개
                    logger.info(f"♻️ [{strategy_name}] 기존 후보 재사용: {len(existing_candidates)}개")
                    return existing_candidates

            # 🆕 새로운 탐색 실행 (단순화된 버전)
            logger.warning(f"⚠️ [{strategy_name}] 기존 후보 없음 - 스케줄러 중앙집중 탐색 권장")
            
            # 기본 탐색 로직 유지 (백업용)
            if strategy_name == "gap_trading":
                return self._discover_gap_candidates_simple()
            elif strategy_name == "volume_breakout":
                return self._discover_volume_candidates_simple()
            elif strategy_name == "momentum":
                return self._discover_momentum_candidates_simple()
            elif strategy_name == "technical_screening":
                return self._discover_technical_candidates_simple()
            else:
                logger.warning(f"❌ 알 수 없는 전략: {strategy_name}")
                return []
                
        except Exception as e:
            logger.error(f"❌ [{strategy_name}] 동기 탐색 오류: {e}")
            return []

    def _discover_gap_candidates_simple(self) -> List[StockCandidate]:
        """🎯 간단한 갭 후보 탐색 (백업용)"""
        try:
            logger.info("🔍 간단 갭 후보 탐색 (백업 모드)")
            
            # 단순한 스크리닝 결과 활용
            screening_results = self.trading_api.get_market_screening_candidates("gap")
            if not screening_results:
                return []

            gap_data = screening_results.get('gap', [])
            candidates = self._convert_to_candidates(gap_data[:8], 'gap_trading')
            
            logger.info(f"✅ 간단 갭 후보: {len(candidates)}개")
            return candidates
            
        except Exception as e:
            logger.error(f"❌ 간단 갭 후보 탐색 오류: {e}")
            return []

    def _discover_volume_candidates_simple(self) -> List[StockCandidate]:
        """🎯 간단한 거래량 후보 탐색 (백업용)"""
        try:
            logger.info("🔍 간단 거래량 후보 탐색 (백업 모드)")
            
            screening_results = self.trading_api.get_market_screening_candidates("volume")
            if not screening_results:
                return []

            volume_data = screening_results.get('volume', [])
            candidates = self._convert_to_candidates(volume_data[:8], 'volume_breakout')
            
            logger.info(f"✅ 간단 거래량 후보: {len(candidates)}개")
            return candidates
            
        except Exception as e:
            logger.error(f"❌ 간단 거래량 후보 탐색 오류: {e}")
            return []

    def _discover_momentum_candidates_simple(self) -> List[StockCandidate]:
        """🎯 간단한 모멘텀 후보 탐색 (백업용)"""
        try:
            logger.info("🔍 간단 모멘텀 후보 탐색 (백업 모드)")
            
            screening_results = self.trading_api.get_market_screening_candidates("momentum")
            if not screening_results:
                return []

            momentum_data = screening_results.get('momentum', [])
            candidates = self._convert_to_candidates(momentum_data[:8], 'momentum')
            
            logger.info(f"✅ 간단 모멘텀 후보: {len(candidates)}개")
            return candidates
            
        except Exception as e:
            logger.error(f"❌ 간단 모멘텀 후보 탐색 오류: {e}")
            return []

    def _discover_technical_candidates_simple(self) -> List[StockCandidate]:
        """🎯 간단한 기술적 지표 후보 탐색 (백업용)"""
        try:
            logger.info("🔍 간단 기술적 후보 탐색 (백업 모드)")
            
            screening_results = self.trading_api.get_market_screening_candidates("technical")
            if not screening_results:
                return []

            technical_data = screening_results.get('technical', [])
            candidates = self._convert_to_candidates(technical_data[:10], 'technical_screening')
            
            logger.info(f"✅ 간단 기술적 후보: {len(candidates)}개")
            return candidates
            
        except Exception as e:
            logger.error(f"❌ 간단 기술적 후보 탐색 오류: {e}")
            return []

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
        """🧹 리소스 정리 (단순화)"""
        logger.info("🧹 종목 탐색 관리자 정리 중...")

        # 🆕 백그라운드 스크리닝 중지 제거
        # self.stop_background_screening()  # 제거

        # 스레드 풀 종료 (탐색용만)
        self.discovery_executor.shutdown(wait=True)
        # self.screening_executor.shutdown(wait=True)  # 제거

        # 후보 정리
        with self.discovery_lock:
            total_candidates = sum(len(candidates) for candidates in self.candidates.values())
            logger.info(f"정리된 후보: {total_candidates}개")
            self.candidates.clear()

        logger.info("✅ 종목 탐색 관리자 정리 완료")

    def _subscribe_premarket_candidates(self, gap_candidates: List[StockCandidate], volume_candidates: List[StockCandidate], momentum_candidates: List[StockCandidate]):
        """프리마켓 후보들을 웹소켓에 구독하는 로직"""
        if self.data_manager:
            # 전략별 우선순위 매핑
            priority_map = {
                'gap_trading': DataPriority.HIGH,
                'volume_breakout': DataPriority.HIGH,
                'momentum': DataPriority.MEDIUM,
                'disparity_reversal': DataPriority.MEDIUM
            }

            # 각 전략별로 후보들을 웹소켓에 구독
            for strategy_name, candidates in [('gap_trading', gap_candidates), ('volume_breakout', volume_candidates), ('momentum', momentum_candidates)]:
                for candidate in candidates:
                    priority = priority_map.get(strategy_name, DataPriority.MEDIUM)
                    self.data_manager.add_stock_request(
                        stock_code=candidate.stock_code,
                        priority=priority,
                        strategy_name=strategy_name,
                        callback=self._create_discovery_callback(candidate.stock_code, strategy_name)
                    )
