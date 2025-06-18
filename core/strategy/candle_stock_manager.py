"""
캔들 전략 종목 통합 관리 시스템
"""
import heapq
from collections import defaultdict, deque
from datetime import datetime, timedelta, time
from typing import Dict, List, Optional, Tuple, Set
from utils.logger import setup_logger

from .candle_trade_candidate import (
    CandleTradeCandidate, CandleStatus, TradeSignal, PatternType,
    CandlePatternInfo, EntryConditions, RiskManagement
)

logger = setup_logger(__name__)


class CandleStockManager:
    """캔들 전략 종목 통합 관리자 - 시간대별 전략 자동 전환"""

    def __init__(self, max_watch_stocks: int = 100, max_positions: int = 15):
        self.max_watch_stocks = max_watch_stocks
        self.max_positions = max_positions

        # ========== 🎯 단일 데이터 소스 (메인 종목 저장소) ==========
        self._all_stocks: Dict[str, CandleTradeCandidate] = {}

        # ========== 🆕 시간대별 전략 관리 ==========
        self._current_strategy_mode = "auto"  # "premarket", "realtime", "auto"
        self._strategy_transition_log = deque(maxlen=50)  # 전략 전환 이력

        # ========== 성능 추적 ==========
        self._recent_updates: deque = deque(maxlen=1000)  # 최근 업데이트 이력
        self._performance_stats = {
            'total_scanned': 0,
            'total_entered': 0,
            'total_exited': 0,
            'win_rate': 0.0,
            'avg_holding_hours': 0.0,
            'best_performer': None,
            'worst_performer': None,
            # 🆕 시간대별 통계
            'premarket_scanned': 0,
            'realtime_scanned': 0,
            'premarket_success_rate': 0.0,
            'realtime_success_rate': 0.0,
        }

        # ========== 설정값 ==========
        self.config = {
            'auto_cleanup_hours': 24,      # 오래된 종목 자동 정리 시간
            'max_pattern_age_hours': 6,    # 패턴 유효 시간
            # 🆕 시간대별 전략 설정
            'premarket_start_time': '08:00',     # 장전 전략 시작
            'premarket_end_time': '09:59',       # 장전 전략 종료  
            'realtime_start_time': '10:00',      # 실시간 전략 시작
            'realtime_end_time': '15:30',        # 실시간 전략 종료
            'strategy_transition_enabled': True,  # 자동 전환 활성화
        }

        self._last_cleanup = datetime.now()

    # ========== 🆕 시간대별 전략 관리 ==========

    def get_current_strategy_mode(self) -> str:
        """현재 시간대에 적합한 전략 모드 반환"""
        try:
            if self._current_strategy_mode != "auto":
                return self._current_strategy_mode

            current_time = datetime.now().time()
            
            # 시간대별 자동 결정
            premarket_start = time.fromisoformat(self.config['premarket_start_time'])
            premarket_end = time.fromisoformat(self.config['premarket_end_time'])
            realtime_start = time.fromisoformat(self.config['realtime_start_time'])
            realtime_end = time.fromisoformat(self.config['realtime_end_time'])

            if premarket_start <= current_time <= premarket_end:
                return "premarket"
            elif realtime_start <= current_time <= realtime_end:
                return "realtime"
            else:
                return "premarket"  # 장후에는 다음날 준비용으로 장전 모드

        except Exception as e:
            logger.error(f"전략 모드 결정 오류: {e}")
            return "premarket"  # 기본값

    def set_strategy_mode(self, mode: str) -> bool:
        """전략 모드 수동 설정"""
        try:
            valid_modes = ["auto", "premarket", "realtime"]
            if mode not in valid_modes:
                logger.warning(f"잘못된 전략 모드: {mode}")
                return False

            old_mode = self._current_strategy_mode
            self._current_strategy_mode = mode

            # 전환 이력 기록
            self._strategy_transition_log.append({
                'timestamp': datetime.now(),
                'old_mode': old_mode,
                'new_mode': mode,
                'trigger': 'manual'
            })

            logger.info(f"🔄 전략 모드 변경: {old_mode} → {mode}")
            return True

        except Exception as e:
            logger.error(f"전략 모드 설정 오류: {e}")
            return False

    def is_premarket_strategy_active(self) -> bool:
        """장전 전략 활성 여부"""
        return self.get_current_strategy_mode() == "premarket"

    def is_realtime_strategy_active(self) -> bool:
        """실시간 전략 활성 여부"""
        return self.get_current_strategy_mode() == "realtime"

    # ========== 종목 추가/제거 (시간대별 최적화) ==========

    def add_candidate(self, candidate: CandleTradeCandidate, strategy_source: str = "auto") -> bool:
        """🆕 새로운 후보 종목 추가 - 시간대별 전략 적용"""
        try:
            stock_code = candidate.stock_code
            current_mode = self.get_current_strategy_mode()

            # 🆕 전략 소스 자동 결정
            if strategy_source == "auto":
                strategy_source = current_mode

            # 🆕 종목에 전략 정보 메타데이터 추가
            if not hasattr(candidate, 'metadata') or candidate.metadata is None:
                candidate.metadata = {}
            
            candidate.metadata.update({
                'strategy_source': strategy_source,
                'detected_time': datetime.now().isoformat(),
                'strategy_mode': current_mode
            })

            # 기존 중복 체크 로직
            if stock_code in self._all_stocks:
                existing = self._all_stocks[stock_code]

                # ENTERED나 PENDING_ORDER 상태는 덮어쓰기 방지
                if existing.status in [CandleStatus.ENTERED, CandleStatus.PENDING_ORDER]:
                    logger.warning(f"⚠️ {stock_code} 중요 상태 보호 ({existing.status.value}) - 새 후보 추가 거부")
                    return False

                # 🆕 전략 소스별 업데이트 정책
                existing_source = existing.metadata.get('strategy_source', 'unknown') if existing.metadata else 'unknown'
                
                # 실시간 전략이 장전 전략을 덮어쓸 수 있음 (더 정확한 정보)
                if strategy_source == "realtime" and existing_source == "premarket":
                    logger.info(f"🔄 {stock_code} 실시간 전략으로 업데이트 (장전→실시간)")
                    return self.update_candidate(candidate)
                # 같은 소스끼리는 업데이트 허용
                elif strategy_source == existing_source:
                    logger.debug(f"🔄 {stock_code} 동일 전략 업데이트 ({strategy_source})")
                    return self.update_candidate(candidate)
                else:
                    logger.debug(f"🚫 {stock_code} 전략 충돌로 업데이트 거부 ({existing_source}→{strategy_source})")
                    return False

            # 최대 관찰 종목 수 체크 및 스마트 교체
            if len(self._all_stocks) >= self.max_watch_stocks:
                # 🆕 시간대별 우선순위를 고려한 교체
                new_candidate_score = self._calculate_candidate_quality_score(candidate, strategy_source)
                
                # 기존 종목 중 가장 낮은 점수 찾기 (같은 전략 소스 우선 고려)
                lowest_existing_score = float('inf')
                lowest_existing_candidate = None
                
                for existing_candidate in self._all_stocks.values():
                    if existing_candidate.status in [CandleStatus.ENTERED, CandleStatus.PENDING_ORDER]:
                        continue  # 중요 상태는 제외
                    
                    existing_source = existing_candidate.metadata.get('strategy_source', 'unknown') if existing_candidate.metadata else 'unknown'
                    existing_score = self._calculate_candidate_quality_score(existing_candidate, existing_source)
                    
                    # 🆕 전략 소스별 교체 우선순위 적용
                    if strategy_source == "realtime" and existing_source == "premarket":
                        existing_score *= 0.8  # 장전 전략 종목의 점수를 낮춤 (교체 우선순위 높임)
                    
                    if existing_score < lowest_existing_score:
                        lowest_existing_score = existing_score
                        lowest_existing_candidate = existing_candidate
                
                # 새 종목이 기존 최저 종목보다 우수하면 교체
                if (lowest_existing_candidate and 
                    new_candidate_score > lowest_existing_score + 30):  # 실시간은 30점 차이만 있어도 교체
                    
                    removed_stock = lowest_existing_candidate.stock_code
                    removed_source = lowest_existing_candidate.metadata.get('strategy_source', 'unknown') if lowest_existing_candidate.metadata else 'unknown'
                    
                    if self.remove_stock(removed_stock):
                        logger.info(f"🔄 시간대별 스마트 교체: {removed_stock}({removed_source}, 점수:{lowest_existing_score:.1f}) → "
                                   f"{stock_code}({strategy_source}, 점수:{new_candidate_score:.1f})")
                    else:
                        logger.warning(f"관찰 한도 초과 - 새 종목 {stock_code} 추가 실패")
                        return False
                else:
                    # 새 종목이 우수하지 않으면 추가 거부
                    logger.info(f"🚫 품질 기준 미달로 추가 거부: {stock_code}({strategy_source}, 점수:{new_candidate_score:.1f}) "
                               f"vs 기존최저(점수:{lowest_existing_score:.1f})")
                    return False

            # 종목 추가
            self._all_stocks[stock_code] = candidate

            # 🆕 전략별 통계 업데이트
            self._performance_stats['total_scanned'] += 1
            if strategy_source == "premarket":
                self._performance_stats['premarket_scanned'] += 1
            elif strategy_source == "realtime":
                self._performance_stats['realtime_scanned'] += 1

            # 업데이트 이력 기록
            self._recent_updates.append({
                'action': 'add',
                'stock_code': stock_code,
                'timestamp': datetime.now(),
                'status': candidate.status.value,
                'strategy_source': strategy_source,
                'strategy_mode': current_mode
            })

            # 품질 점수 계산 및 로깅
            quality_score = self._calculate_candidate_quality_score(candidate, strategy_source)
            
            logger.info(f"✅ 새 종목 추가: {stock_code}({candidate.stock_name}) - "
                       f"전략:{strategy_source}, 품질점수:{quality_score:.1f}, {candidate.get_signal_summary()}")
            return True

        except Exception as e:
            logger.error(f"종목 추가 오류 ({candidate.stock_code}): {e}")
            return False

    def update_candidate(self, candidate: CandleTradeCandidate) -> bool:
        """기존 종목 정보 업데이트"""
        try:
            stock_code = candidate.stock_code

            if stock_code not in self._all_stocks:
                logger.warning(f"업데이트 대상 종목 {stock_code} 없음")
                return False

            old_candidate = self._all_stocks[stock_code]

            # 새 정보로 교체
            self._all_stocks[stock_code] = candidate

            # 업데이트 이력 기록
            self._recent_updates.append({
                'action': 'update',
                'stock_code': stock_code,
                'timestamp': datetime.now(),
                'old_status': old_candidate.status.value,
                'new_status': candidate.status.value
            })

            logger.debug(f"🔄 종목 업데이트: {stock_code} - {candidate.get_signal_summary()}")
            return True

        except Exception as e:
            logger.error(f"종목 업데이트 오류 ({candidate.stock_code}): {e}")
            return False

    def remove_stock(self, stock_code: str) -> bool:
        """종목 제거"""
        try:
            if stock_code not in self._all_stocks:
                return False

            candidate = self._all_stocks[stock_code]

            # 메인 저장소에서 제거
            del self._all_stocks[stock_code]

            # 업데이트 이력 기록
            self._recent_updates.append({
                'action': 'remove',
                'stock_code': stock_code,
                'timestamp': datetime.now(),
                'reason': 'manual_removal'
            })

            logger.info(f"🗑️ 종목 제거: {stock_code}")
            return True

        except Exception as e:
            logger.error(f"종목 제거 오류 ({stock_code}): {e}")
            return False

    # ========== 조회 함수들 ==========

    def get_stock(self, stock_code: str) -> Optional[CandleTradeCandidate]:
        """특정 종목 조회"""
        return self._all_stocks.get(stock_code)

    def get_stocks_by_status(self, status: CandleStatus) -> List[CandleTradeCandidate]:
        """상태별 종목 조회"""
        return [candidate for candidate in self._all_stocks.values() if candidate.status == status]

    def get_stocks_by_signal(self, signal: TradeSignal) -> List[CandleTradeCandidate]:
        """신호별 종목 조회"""
        return [candidate for candidate in self._all_stocks.values() if candidate.trade_signal == signal]

    def get_stocks_by_pattern(self, pattern: PatternType) -> List[CandleTradeCandidate]:
        """패턴별 종목 조회"""
        return [candidate for candidate in self._all_stocks.values()
                for pattern_info in candidate.detected_patterns
                if pattern_info.pattern_type == pattern]

    def get_top_buy_candidates(self, limit: int = 10) -> List[CandleTradeCandidate]:
        """상위 매수 후보 조회 (우선순위순)"""
        try:
            # 🎯 직접 _all_stocks에서 매수 후보 조회 및 정렬
            candidates = [
                candidate for candidate in self._all_stocks.values()
                if candidate.is_ready_for_entry() and candidate.trade_signal in [TradeSignal.STRONG_BUY, TradeSignal.BUY]
            ]

            # 진입 우선순위 순으로 정렬 (높은 순)
            candidates.sort(key=lambda c: c.entry_priority, reverse=True)

            return candidates[:limit]

        except Exception as e:
            logger.error(f"상위 매수 후보 조회 오류: {e}")
            return []

    def get_top_sell_candidates(self, limit: int = 10) -> List[CandleTradeCandidate]:
        """상위 매도 후보 조회"""
        try:
            # 🎯 직접 _all_stocks에서 매도 후보 조회 및 정렬
            candidates = [
                candidate for candidate in self._all_stocks.values()
                if candidate.trade_signal in [TradeSignal.SELL, TradeSignal.STRONG_SELL]
            ]

            # 신호 강도 순으로 정렬 (높은 순)
            candidates.sort(key=lambda c: c.signal_strength, reverse=True)

            return candidates[:limit]

        except Exception as e:
            logger.error(f"상위 매도 후보 조회 오류: {e}")
            return []

    def get_active_positions(self) -> List[CandleTradeCandidate]:
        """활성 포지션 조회"""
        return self.get_stocks_by_status(CandleStatus.ENTERED)

    def get_watching_stocks(self) -> List[CandleTradeCandidate]:
        """관찰 중인 종목 조회"""
        return self.get_stocks_by_status(CandleStatus.WATCHING)

    # ========== 실시간 업데이트 ==========

    def update_stock_price(self, stock_code: str, new_price: float):
        """종목 가격 실시간 업데이트"""
        try:
            if stock_code not in self._all_stocks:
                return False

            candidate = self._all_stocks[stock_code]
            old_price = candidate.current_price

            # 가격 업데이트
            candidate.update_price(new_price)

            # 중요한 가격 변동시 알림
            price_change_pct = ((new_price - old_price) / old_price) * 100 if old_price > 0 else 0

            if abs(price_change_pct) > 2.0:  # 2% 이상 변동
                logger.info(f"💰 {stock_code} 급변동: {old_price:,.0f}원 → {new_price:,.0f}원 ({price_change_pct:+.1f}%)")

            return True

        except Exception as e:
            logger.error(f"가격 업데이트 오류 ({stock_code}): {e}")
            return False

    def batch_update_prices(self, price_data: Dict[str, float]):
        """여러 종목 가격 일괄 업데이트"""
        updated_count = 0

        for stock_code, price in price_data.items():
            if self.update_stock_price(stock_code, price):
                updated_count += 1

        logger.debug(f"📊 가격 일괄 업데이트: {updated_count}/{len(price_data)}개 성공")
        return updated_count

    # ========== 자동 관리 기능 ==========

    def auto_cleanup(self):
        """오래된 종목 자동 정리"""
        try:
            now = datetime.now()

            # 정리 간격 체크
            if (now - self._last_cleanup).total_seconds() < 3600:  # 1시간 간격
                return 0

            cleanup_count = 0
            cutoff_time = now - timedelta(hours=self.config['auto_cleanup_hours'])

            # 완료된 거래나 오래된 관찰 종목 정리
            to_remove = []

            for stock_code, candidate in self._all_stocks.items():
                should_remove = False

                # 완료된 거래 (24시간 후 정리)
                if candidate.status == CandleStatus.EXITED and candidate.performance.exit_time:
                    if candidate.performance.exit_time < cutoff_time:
                        should_remove = True

                # 오래된 패턴 (6시간 후 정리)
                elif candidate.status == CandleStatus.WATCHING:
                    pattern_cutoff = now - timedelta(hours=self.config['max_pattern_age_hours'])
                    if candidate.created_at < pattern_cutoff:
                        should_remove = True

                # 신호 없는 종목 (12시간 후 정리)
                elif candidate.trade_signal == TradeSignal.HOLD:
                    hold_cutoff = now - timedelta(hours=12)
                    if candidate.last_updated < hold_cutoff:
                        should_remove = True

                if should_remove:
                    to_remove.append(stock_code)

            # 정리 실행
            for stock_code in to_remove:
                if self.remove_stock(stock_code):
                    cleanup_count += 1

            self._last_cleanup = now

            if cleanup_count > 0:
                logger.info(f"🧹 자동 정리 완료: {cleanup_count}개 종목 제거")

            return cleanup_count

        except Exception as e:
            logger.error(f"자동 정리 오류: {e}")
            return 0

    # ========== 통계 및 상태 ==========

    def get_summary_stats(self) -> Dict:
        """관리 현황 요약"""
        try:
            # 마지막 업데이트 시간 계산 (안전한 방식)
            last_update = None
            if self._all_stocks:
                update_times = [c.last_updated for c in self._all_stocks.values()]
                if update_times:
                    last_update = max(update_times)

            # 🎯 _all_stocks에서 직접 통계 계산
            status_counts = {}
            signal_counts = {}

            for candidate in self._all_stocks.values():
                # 상태별 카운트
                status_key = candidate.status.value
                status_counts[status_key] = status_counts.get(status_key, 0) + 1

                # 신호별 카운트
                signal_key = candidate.trade_signal.value
                signal_counts[signal_key] = signal_counts.get(signal_key, 0) + 1

            stats = {
                'total_stocks': len(self._all_stocks),
                'by_status': status_counts,
                'by_signal': signal_counts,
                'active_positions': len(self.get_active_positions()),
                'buy_ready': len([c for c in self._all_stocks.values() if c.is_ready_for_entry()]),
                'top_patterns': self._get_top_patterns(),
                'performance': self._calculate_performance_stats(),
                'last_update': last_update
            }

            return stats

        except Exception as e:
            logger.error(f"통계 계산 오류: {e}")
            return {}

    def _get_top_patterns(self) -> List[Tuple[str, int]]:
        """상위 패턴 통계"""
        pattern_counts = {}

        for candidate in self._all_stocks.values():
            for pattern_info in candidate.detected_patterns:
                pattern_name = pattern_info.pattern_type.value
                pattern_counts[pattern_name] = pattern_counts.get(pattern_name, 0) + 1

        return sorted(pattern_counts.items(), key=lambda x: x[1], reverse=True)[:5]

    def _calculate_performance_stats(self) -> Dict:
        """성과 통계 계산"""
        try:
            completed_trades = [c for c in self._all_stocks.values()
                              if c.status == CandleStatus.EXITED and c.performance.realized_pnl is not None]

            if not completed_trades:
                return self._performance_stats

            # 승리한 거래 계산 (None 안전 처리)
            winning_trades = [c for c in completed_trades if (c.performance.realized_pnl or 0) > 0]

            # 최고/최저 성과자 계산 (안전한 방식)
            best_performer = None
            worst_performer = None

            if completed_trades:
                pnl_values = [(c.performance.pnl_pct or 0, c.stock_code) for c in completed_trades]
                if pnl_values:
                    best_performer = max(pnl_values, key=lambda x: x[0])[1]
                    worst_performer = min(pnl_values, key=lambda x: x[0])[1]

            self._performance_stats.update({
                'total_exited': len(completed_trades),
                'win_rate': len(winning_trades) / len(completed_trades) * 100,
                'avg_holding_hours': sum(c.performance.holding_time_hours or 0 for c in completed_trades) / len(completed_trades),
                'best_performer': best_performer,
                'worst_performer': worst_performer
            })

            return self._performance_stats

        except Exception as e:
            logger.error(f"성과 통계 계산 오류: {e}")
            return self._performance_stats

    # ========== 내부 유틸리티 함수들 ==========

    def _remove_lowest_priority_stock(self) -> Optional[str]:
        """🎯 스마트 우선순위 기반 종목 제거"""
        try:
            # 1. 제거 대상 후보 수집 (중요 상태 제외)
            removal_candidates = []
            
            for candidate in self._all_stocks.values():
                # 🚨 중요 상태는 절대 제거하지 않음
                if candidate.status in [CandleStatus.ENTERED, CandleStatus.PENDING_ORDER]:
                    continue
                
                # 우선순위 점수 계산 (낮을수록 제거 우선순위 높음)
                priority_score = self._calculate_removal_priority_score(candidate)
                removal_candidates.append((priority_score, candidate.stock_code, candidate))
            
            if not removal_candidates:
                logger.warning("⚠️ 제거 가능한 종목이 없습니다 (모두 중요 상태)")
                return None
            
            # 2. 우선순위 순으로 정렬 (낮은 점수 = 높은 제거 우선순위)
            removal_candidates.sort(key=lambda x: x[0])
            
            # 3. 가장 낮은 우선순위 종목 제거
            lowest_priority_score, stock_to_remove, candidate_to_remove = removal_candidates[0]
            
            # 4. 제거 실행 및 로깅
            if self.remove_stock(stock_to_remove):
                logger.info(f"🗑️ 우선순위 기반 종목 제거: {stock_to_remove}({candidate_to_remove.stock_name}) "
                           f"- 점수:{lowest_priority_score:.1f}, 상태:{candidate_to_remove.status.value}")
                return stock_to_remove

            return None

        except Exception as e:
            logger.error(f"최저 우선순위 종목 제거 오류: {e}")
            return None

    def _calculate_removal_priority_score(self, candidate: CandleTradeCandidate) -> float:
        """🎯 제거 우선순위 점수 계산 (낮을수록 제거 우선순위 높음)"""
        try:
            score = 0.0
            
            # 1. 패턴 신뢰도 (높을수록 보존 우선순위 높음)
            if candidate.detected_patterns:
                max_confidence = max(p.confidence for p in candidate.detected_patterns)
                score += max_confidence * 100  # 0~100점
            else:
                score += 0  # 패턴 없으면 0점
            
            # 2. 패턴 강도 (높을수록 보존 우선순위 높음)
            if candidate.detected_patterns:
                max_strength = max(p.strength for p in candidate.detected_patterns)
                score += max_strength  # 0~100점
            else:
                score += 0
            
            # 3. 신호 강도 (높을수록 보존 우선순위 높음)
            score += candidate.signal_strength  # 0~100점
            
            # 4. 상태별 가중치 (중요한 상태일수록 높은 점수)
            status_weights = {
                CandleStatus.BUY_READY: 50,      # 매수 준비 완료 - 높은 우선순위
                CandleStatus.WATCHING: 20,       # 관찰 중 - 중간 우선순위
                CandleStatus.SCANNING: 15,       # 스캐닝 중 - 중간 우선순위
                CandleStatus.SELL_READY: 30,     # 매도 준비 - 높은 우선순위
                CandleStatus.ENTERED: 100,       # 진입 완료 - 최고 우선순위 (제거 안됨)
                CandleStatus.EXITED: 5,          # 청산 완료 - 낮은 우선순위
                CandleStatus.STOPPED: 5          # 손절 완료 - 낮은 우선순위
            }
            score += status_weights.get(candidate.status, 0)
            
            # 5. 시간 가중치 (최근 업데이트일수록 높은 점수)
            if candidate.last_updated:
                hours_since_update = (datetime.now() - candidate.last_updated).total_seconds() / 3600
                # 6시간 이내는 보너스, 24시간 이후는 페널티
                if hours_since_update < 6:
                    score += 20  # 최근 업데이트 보너스
                elif hours_since_update > 24:
                    score -= 30  # 오래된 데이터 페널티
            
            # 6. 특별 패턴 보너스 (높은 신뢰도 패턴)
            if candidate.detected_patterns:
                for pattern in candidate.detected_patterns:
                    # Morning Star, Bullish Engulfing 등 강력한 패턴
                    if pattern.pattern_type in [PatternType.BULLISH_ENGULFING]:
                        if pattern.confidence >= 0.7:
                            score += 30  # 강력한 패턴 보너스
                    # Hammer 패턴
                    elif pattern.pattern_type in [PatternType.HAMMER, PatternType.INVERTED_HAMMER]:
                        if pattern.confidence >= 0.7:
                            score += 20  # 망치형 패턴 보너스
            
            # 7. 최종 점수 정규화 (0~500 범위)
            final_score = max(0, min(500, score))
            
            return final_score
            
        except Exception as e:
            logger.error(f"우선순위 점수 계산 오류: {e}")
            return 0.0  # 오류시 가장 낮은 점수 (제거 우선순위 최고)

    def _calculate_candidate_quality_score(self, candidate: CandleTradeCandidate, strategy_source: str = "premarket") -> float:
        """🎯 종목 품질 점수 계산 (높을수록 좋은 종목) - 시간대별 최적화"""
        try:
            score = 0.0
            
            # 🆕 기존 보유 종목은 최고 우선순위 (제거되지 않도록)
            if strategy_source == "existing_holding":
                return 999.0  # 최고 점수로 절대 제거되지 않음
            
            # 1. 패턴 신뢰도 (가장 중요한 요소)
            if candidate.detected_patterns:
                max_confidence = max(p.confidence for p in candidate.detected_patterns)
                base_confidence_score = max_confidence * 150  # 0~150점
                
                # 🆕 전략별 신뢰도 가중치
                if strategy_source == "realtime":
                    # 실시간 전략은 신뢰도 기준을 약간 완화 (진행중인 캔들이므로)
                    score += base_confidence_score * 1.1  # 10% 보너스
                else:
                    score += base_confidence_score
            
            # 2. 패턴 강도
            if candidate.detected_patterns:
                max_strength = max(p.strength for p in candidate.detected_patterns)
                base_strength_score = max_strength * 1.2  # 0~120점
                
                # 🆕 전략별 강도 가중치
                if strategy_source == "realtime":
                    score += base_strength_score * 1.05  # 5% 보너스
                else:
                    score += base_strength_score
            
            # 3. 신호 강도
            score += candidate.signal_strength  # 0~100점
            
            # 4. 🆕 전략별 패턴 타입 보너스
            if candidate.detected_patterns:
                for pattern in candidate.detected_patterns:
                    if strategy_source == "premarket":
                        # 장전 전략: 안정적인 패턴 선호
                        pattern_bonuses = {
                            PatternType.BULLISH_ENGULFING: 50,   # 최고 신뢰도
                            PatternType.HAMMER: 45,              # 매우 강력
                            PatternType.MORNING_STAR: 55,        # 장전에서는 더 높은 점수
                            PatternType.PIERCING_LINE: 40,       # 관통형
                            PatternType.INVERTED_HAMMER: 30,     # 강력
                            PatternType.RISING_THREE_METHODS: 25, # 추세 지속
                            PatternType.DOJI: 10                 # 중립적
                        }
                    else:  # realtime
                        # 실시간 전략: 빠른 반응 패턴 선호
                        pattern_bonuses = {
                            PatternType.BULLISH_ENGULFING: 55,   # 실시간에서 더 높은 점수
                            PatternType.HAMMER: 40,              # 변동성 패턴
                            PatternType.MORNING_STAR: 35,        # 장중에서는 상대적으로 낮음
                            PatternType.PIERCING_LINE: 45,       # 실시간 돌파 패턴
                            PatternType.INVERTED_HAMMER: 35,     # 실시간 반전
                            PatternType.RISING_THREE_METHODS: 50, # 실시간 추세 추종
                            PatternType.DOJI: 25                 # 실시간 반전 신호로 더 중요
                        }
                    
                    score += pattern_bonuses.get(pattern.pattern_type, 15)
            
            # 5. 진입 우선순위 (이미 계산된 값 활용)
            priority_weight = 0.8 if strategy_source == "premarket" else 0.9  # 실시간은 우선순위 더 중시
            score += candidate.entry_priority * priority_weight
            
            # 6. 현재 상태 보너스
            status_bonuses = {
                CandleStatus.BUY_READY: 30,      # 매수 준비 완료
                CandleStatus.WATCHING: 10,       # 관찰 중
                CandleStatus.SCANNING: 5,        # 스캐닝 중
                CandleStatus.SELL_READY: 15,     # 매도 준비
                CandleStatus.ENTERED: 25,        # 진입 완료 (높은 우선순위)
                CandleStatus.EXITED: 0,          # 청산 완료 (낮은 우선순위)
                CandleStatus.STOPPED: 0          # 손절 완료 (낮은 우선순위)
            }
            score += status_bonuses.get(candidate.status, 0)
            
            # 7. 🆕 전략별 최신성 보너스
            if candidate.created_at:
                hours_since_creation = (datetime.now() - candidate.created_at).total_seconds() / 3600
                
                if strategy_source == "premarket":
                    # 장전: 하루 전 패턴도 유효
                    if hours_since_creation < 6:
                        score += 25  # 6시간 이내
                    elif hours_since_creation < 24:
                        score += 15  # 24시간 이내
                    elif hours_since_creation > 48:
                        score -= 20  # 48시간 이후 페널티
                else:  # realtime
                    # 실시간: 매우 최신 패턴 선호
                    if hours_since_creation < 0.5:  # 30분 이내
                        score += 40  # 높은 보너스
                    elif hours_since_creation < 2:  # 2시간 이내
                        score += 25
                    elif hours_since_creation < 6:  # 6시간 이내
                        score += 10
                    elif hours_since_creation > 12:  # 12시간 이후 페널티
                        score -= 30
            
            # 8. 🆕 전략별 특별 보너스
            if strategy_source == "realtime":
                # 실시간 패턴에 대한 추가 보너스
                if hasattr(candidate, 'metadata') and candidate.metadata:
                    if candidate.metadata.get('realtime', False):
                        score += 20  # 실시간 감지 보너스
                    if candidate.metadata.get('volume_surge', False):
                        score += 15  # 거래량 급증 보너스
                    if candidate.metadata.get('forming_candle', False):
                        score += 10  # 진행중인 캔들 보너스
            
            # 9. 최종 점수 정규화 (0~700 범위로 확장)
            final_score = max(0, min(700, score))
            
            return final_score
            
        except Exception as e:
            logger.error(f"종목 품질 점수 계산 오류 ({strategy_source}): {e}")
            return 0.0
