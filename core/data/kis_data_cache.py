"""
KIS 데이터 캐싱 시스템 (공식 스타일)
간단하고 효율적인 메모리 캐시
"""
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List
from collections import OrderedDict
from utils.logger import setup_logger
from utils.korean_time import now_kst

logger = setup_logger(__name__)


class KISDataCache:
    """KIS 데이터 캐시 (단순하고 효율적)"""

    def __init__(self, max_size: int = 1000, default_ttl: int = 30):
        """
        Args:
            max_size: 최대 캐시 항목 수
            default_ttl: 기본 TTL (초)
        """
        self.max_size = max_size
        self.default_ttl = default_ttl

        # 캐시 저장소
        self._cache: OrderedDict[str, Dict] = OrderedDict()
        self._lock = threading.RLock()

        # 통계
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'size': 0
        }

        logger.info(f"데이터 캐시 초기화: max_size={max_size}, ttl={default_ttl}초")

    def get(self, key: str) -> Optional[Any]:
        """캐시에서 데이터 조회"""
        with self._lock:
            if key not in self._cache:
                self.stats['misses'] += 1
                return None

            item = self._cache[key]

            # TTL 확인
            if self._is_expired(item):
                del self._cache[key]
                self.stats['misses'] += 1
                self.stats['size'] = len(self._cache)
                return None

            # LRU 업데이트 (최근 사용으로 이동)
            self._cache.move_to_end(key)
            self.stats['hits'] += 1

            return item['data']

    def set(self, key: str, data: Any, ttl: Optional[int] = None) -> None:
        """캐시에 데이터 저장"""
        with self._lock:
            ttl = ttl or self.default_ttl

            item = {
                'data': data,
                'timestamp': now_kst(),
                'ttl': ttl
            }

            # 기존 항목 업데이트 또는 새 항목 추가
            self._cache[key] = item
            self._cache.move_to_end(key)  # 최신으로 이동

            # 크기 제한 확인
            while len(self._cache) > self.max_size:
                # 가장 오래된 항목 제거 (LRU)
                oldest_key = next(iter(self._cache))
                del self._cache[oldest_key]
                self.stats['evictions'] += 1

            self.stats['size'] = len(self._cache)

    def delete(self, key: str) -> bool:
        """캐시에서 특정 키 삭제"""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self.stats['size'] = len(self._cache)
                return True
            return False

    def clear(self) -> None:
        """캐시 전체 삭제"""
        with self._lock:
            self._cache.clear()
            self.stats['size'] = 0
            logger.info("캐시 전체 삭제")

    def cleanup_expired(self) -> int:
        """만료된 항목들 정리"""
        removed_count = 0

        with self._lock:
            # 만료된 키들 수집
            expired_keys = []
            for key, item in self._cache.items():
                if self._is_expired(item):
                    expired_keys.append(key)

            # 만료된 항목들 제거
            for key in expired_keys:
                del self._cache[key]
                removed_count += 1

            self.stats['size'] = len(self._cache)

        if removed_count > 0:
            logger.debug(f"만료된 캐시 항목 {removed_count}개 정리")

        return removed_count

    def _is_expired(self, item: Dict) -> bool:
        """항목 만료 여부 확인"""
        age = (now_kst() - item['timestamp']).total_seconds()
        return age > item['ttl']

    def get_stats(self) -> Dict:
        """캐시 통계 조회"""
        with self._lock:
            total_requests = self.stats['hits'] + self.stats['misses']
            hit_rate = (self.stats['hits'] / total_requests * 100) if total_requests > 0 else 0

            return {
                **self.stats.copy(),
                'hit_rate': round(hit_rate, 2),
                'max_size': self.max_size,
                'default_ttl': self.default_ttl
            }


# 전역 캐시 인스턴스들
_price_cache = KISDataCache(max_size=500, default_ttl=10)      # 현재가 캐시
_orderbook_cache = KISDataCache(max_size=200, default_ttl=30)  # 호가 캐시
_daily_cache = KISDataCache(max_size=100, default_ttl=300)     # 일봉 캐시


def get_price_cache() -> KISDataCache:
    """현재가 캐시 인스턴스 반환"""
    return _price_cache


def get_orderbook_cache() -> KISDataCache:
    """호가 캐시 인스턴스 반환"""
    return _orderbook_cache


def get_daily_cache() -> KISDataCache:
    """일봉 캐시 인스턴스 반환"""
    return _daily_cache


# 편의 함수들
def cache_current_price(stock_code: str, price_data: Dict) -> None:
    """현재가 데이터 캐시"""
    _price_cache.set(f"price:{stock_code}", price_data, ttl=10)


def get_cached_price(stock_code: str) -> Optional[Dict]:
    """캐시된 현재가 조회"""
    return _price_cache.get(f"price:{stock_code}")


def cache_orderbook(stock_code: str, orderbook_data: Dict) -> None:
    """호가 데이터 캐시"""
    _orderbook_cache.set(f"orderbook:{stock_code}", orderbook_data, ttl=30)


def get_cached_orderbook(stock_code: str) -> Optional[Dict]:
    """캐시된 호가 조회"""
    return _orderbook_cache.get(f"orderbook:{stock_code}")


def cache_daily_data(stock_code: str, daily_data: List[Dict]) -> None:
    """일봉 데이터 캐시"""
    _daily_cache.set(f"daily:{stock_code}", daily_data, ttl=300)


def get_cached_daily_data(stock_code: str) -> Optional[Dict]:
    """캐시된 일봉 조회"""
    return _daily_cache.get(f"daily:{stock_code}")


def cleanup_all_caches() -> Dict[str, int]:
    """모든 캐시 정리"""
    return {
        'price_removed': _price_cache.cleanup_expired(),
        'orderbook_removed': _orderbook_cache.cleanup_expired(),
        'daily_removed': _daily_cache.cleanup_expired()
    }


def get_all_cache_stats() -> Dict[str, Dict]:
    """모든 캐시 통계"""
    return {
        'price_cache': _price_cache.get_stats(),
        'orderbook_cache': _orderbook_cache.get_stats(),
        'daily_cache': _daily_cache.get_stats()
    }


def clear_all_caches() -> None:
    """모든 캐시 삭제"""
    _price_cache.clear()
    _orderbook_cache.clear()
    _daily_cache.clear()
    logger.info("모든 캐시 삭제 완료")
