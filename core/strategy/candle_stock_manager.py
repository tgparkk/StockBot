"""
캔들 전략 종목 통합 관리 시스템
"""
import heapq
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
from utils.logger import setup_logger

from .candle_trade_candidate import (
    CandleTradeCandidate, CandleStatus, TradeSignal, PatternType,
    CandlePatternInfo, EntryConditions, RiskManagement
)

logger = setup_logger(__name__)


class CandleStockManager:
    """캔들 전략 종목 통합 관리자"""

    def __init__(self, max_watch_stocks: int = 100, max_positions: int = 15):
        self.max_watch_stocks = max_watch_stocks
        self.max_positions = max_positions

        # ========== 🎯 단일 데이터 소스 (메인 종목 저장소) ==========
        self._all_stocks: Dict[str, CandleTradeCandidate] = {}



        # ========== 성능 추적 ==========
        self._recent_updates: deque = deque(maxlen=1000)  # 최근 업데이트 이력
        self._performance_stats = {
            'total_scanned': 0,
            'total_entered': 0,
            'total_exited': 0,
            'win_rate': 0.0,
            'avg_holding_hours': 0.0,
            'best_performer': None,
            'worst_performer': None
        }

        # ========== 설정값 ==========
        self.config = {
            'auto_cleanup_hours': 24,      # 오래된 종목 자동 정리 시간
            'max_pattern_age_hours': 6,    # 패턴 유효 시간
        }

        self._last_cleanup = datetime.now()

    # ========== 종목 추가/제거 ==========

    def add_candidate(self, candidate: CandleTradeCandidate) -> bool:
        """새로운 후보 종목 추가"""
        try:
            stock_code = candidate.stock_code

            # 🔧 중복 체크 강화 (상태별 처리)
            if stock_code in self._all_stocks:
                existing = self._all_stocks[stock_code]

                # ENTERED나 PENDING_ORDER 상태는 덮어쓰기 방지
                if existing.status in [CandleStatus.ENTERED, CandleStatus.PENDING_ORDER]:
                    logger.warning(f"⚠️ {stock_code} 중요 상태 보호 ({existing.status.value}) - 새 후보 추가 거부")
                    return False

                # 다른 상태는 업데이트 허용
                logger.debug(f"🔄 {stock_code} 기존 종목 업데이트 ({existing.status.value})")
                return self.update_candidate(candidate)

            # 최대 관찰 종목 수 체크
            if len(self._all_stocks) >= self.max_watch_stocks:
                removed = self._remove_lowest_priority_stock()
                if removed:
                    logger.info(f"관찰 한도 초과 - {removed} 제거")
                else:
                    logger.warning(f"관찰 한도 초과 - 새 종목 {stock_code} 추가 실패")
                    return False

            # 종목 추가
            self._all_stocks[stock_code] = candidate

            # 통계 업데이트
            self._performance_stats['total_scanned'] += 1

            # 업데이트 이력 기록
            self._recent_updates.append({
                'action': 'add',
                'stock_code': stock_code,
                'timestamp': datetime.now(),
                'status': candidate.status.value
            })

            logger.debug(f"✅ 새 종목 추가: {stock_code}({candidate.stock_name}) - {candidate.get_signal_summary()}")
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
        """가장 낮은 우선순위 종목 제거"""
        try:
            # 관찰 중이면서 신호가 약한 종목 우선 제거
            watching_candidates = [
                (candidate.signal_strength + candidate.pattern_score, candidate.stock_code)
                for candidate in self._all_stocks.values()
                if candidate.status == CandleStatus.WATCHING
            ]

            if watching_candidates:
                watching_candidates.sort()  # 낮은 점수 순
                stock_to_remove = watching_candidates[0][1]

                if self.remove_stock(stock_to_remove):
                    return stock_to_remove

            return None

        except Exception as e:
            logger.error(f"최저 우선순위 종목 제거 오류: {e}")
            return None
