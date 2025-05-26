"""
KIS 데이터 수집기 (공식 스타일)
WebSocket + REST API 통합 데이터 수집
"""
import time
from typing import Dict, List, Optional, Callable
from enum import Enum
from utils.logger import setup_logger
from . import kis_data_cache as cache
from .rest_api_manager import KISRestAPIManager
from .websocket_manager import KISWebSocketManager

logger = setup_logger(__name__)


class DataSource(Enum):
    """데이터 소스"""
    WEBSOCKET = "websocket"
    REST_API = "rest_api"
    CACHE = "cache"


class KISDataCollector:
    """KIS 데이터 수집기 (간소화 버전)"""

    def __init__(self, is_demo: bool = False):
        """초기화"""
        self.is_demo = is_demo

        # API 매니저들
        self.rest_api = KISRestAPIManager(is_demo=is_demo)
        self.websocket = KISWebSocketManager()

        # 콜백 등록
        self.data_callbacks: Dict[str, List[Callable]] = {}

        # 통계
        self.stats = {
            'websocket_data': 0,
            'rest_api_calls': 0,
            'cache_hits': 0,
            'total_requests': 0
        }

        logger.info(f"데이터 수집기 초기화 완료 ({'모의투자' if is_demo else '실전투자'})")

    def get_current_price(self, stock_code: str, use_cache: bool = True) -> Dict:
        """현재가 조회 (캐시 우선)"""
        self.stats['total_requests'] += 1

        # 1. 캐시 확인
        if use_cache:
            cached_data = cache.get_cached_price(stock_code)
            if cached_data:
                self.stats['cache_hits'] += 1
                return {
                    **cached_data,
                    'source': DataSource.CACHE.value,
                    'from_cache': True
                }

        # 2. REST API 호출
        try:
            data = self.rest_api.get_current_price(stock_code)
            self.stats['rest_api_calls'] += 1

            if data.get('status') == 'success':
                # 캐시에 저장
                if use_cache:
                    cache.cache_current_price(stock_code, data)

                return {
                    **data,
                    'source': DataSource.REST_API.value,
                    'from_cache': False
                }
            else:
                logger.error(f"현재가 조회 실패: {stock_code}")
                return data

        except Exception as e:
            logger.error(f"현재가 조회 오류: {stock_code} - {e}")
            return {
                'status': 'error',
                'message': f'현재가 조회 오류: {e}',
                'source': DataSource.REST_API.value
            }

    def get_orderbook(self, stock_code: str, use_cache: bool = True) -> Dict:
        """호가 조회 (캐시 우선)"""
        self.stats['total_requests'] += 1

        # 1. 캐시 확인
        if use_cache:
            cached_data = cache.get_cached_orderbook(stock_code)
            if cached_data:
                self.stats['cache_hits'] += 1
                return {
                    **cached_data,
                    'source': DataSource.CACHE.value,
                    'from_cache': True
                }

        # 2. REST API 호출
        try:
            data = self.rest_api.get_orderbook(stock_code)
            self.stats['rest_api_calls'] += 1

            if data.get('status') == 'success':
                # 캐시에 저장
                if use_cache:
                    cache.cache_orderbook(stock_code, data)

                return {
                    **data,
                    'source': DataSource.REST_API.value,
                    'from_cache': False
                }
            else:
                logger.error(f"호가 조회 실패: {stock_code}")
                return data

        except Exception as e:
            logger.error(f"호가 조회 오류: {stock_code} - {e}")
            return {
                'status': 'error',
                'message': f'호가 조회 오류: {e}',
                'source': DataSource.REST_API.value
            }

    def get_daily_prices(self, stock_code: str, period_type: str = "D",
                        use_cache: bool = True) -> List[Dict]:
        """일봉 데이터 조회 (캐시 우선)"""
        self.stats['total_requests'] += 1
        cache_key = f"{stock_code}_{period_type}"

        # 1. 캐시 확인
        if use_cache:
            cached_data = cache.get_cached_daily_data(cache_key)
            if cached_data:
                self.stats['cache_hits'] += 1
                return cached_data

        # 2. REST API 호출
        try:
            data = self.rest_api.get_daily_prices(stock_code, period_type)
            self.stats['rest_api_calls'] += 1

            if data:
                # 캐시에 저장
                if use_cache:
                    cache.cache_daily_data(cache_key, data)

                return data
            else:
                logger.error(f"일봉 조회 실패: {stock_code}")
                return []

        except Exception as e:
            logger.error(f"일봉 조회 오류: {stock_code} - {e}")
            return []

    def get_multiple_prices(self, stock_codes: List[str], use_cache: bool = True) -> Dict[str, Dict]:
        """여러 종목 현재가 배치 조회"""
        results = {}

        for stock_code in stock_codes:
            results[stock_code] = self.get_current_price(stock_code, use_cache)
            time.sleep(0.1)  # API Rate Limiting

        return results

    def get_stock_overview(self, stock_code: str, use_cache: bool = True) -> Dict:
        """종목 개요 (현재가 + 호가 통합)"""
        current_price = self.get_current_price(stock_code, use_cache)
        orderbook = self.get_orderbook(stock_code, use_cache)

        return {
            'stock_code': stock_code,
            'current_price': current_price,
            'orderbook': orderbook,
            'timestamp': time.time()
        }

    # === WebSocket 관련 ===

    def subscribe_realtime(self, stock_code: str, callback: Optional[Callable] = None) -> bool:
        """실시간 데이터 구독"""
        try:
            # WebSocket 구독 (체결가)
            success = self.websocket.subscribe_stock_price(stock_code, "data_collector")

            # 콜백 등록
            if callback:
                self.register_callback(stock_code, callback)

            if success:
                logger.info(f"실시간 구독 성공: {stock_code}")
            else:
                logger.error(f"실시간 구독 실패: {stock_code}")

            return success

        except Exception as e:
            logger.error(f"실시간 구독 오류: {stock_code} - {e}")
            return False

    def unsubscribe_realtime(self, stock_code: str) -> bool:
        """실시간 데이터 구독 해제"""
        try:
            # WebSocket 구독 해제
            success = self.websocket.unsubscribe("H0STCNT0", stock_code)

            # 콜백 제거
            if stock_code in self.data_callbacks:
                del self.data_callbacks[stock_code]

            if success:
                logger.info(f"실시간 구독 해제: {stock_code}")

            return success

        except Exception as e:
            logger.error(f"실시간 구독 해제 오류: {stock_code} - {e}")
            return False

    def _websocket_callback(self, data: Dict) -> None:
        """WebSocket 데이터 콜백"""
        if 'stock_code' in data:
            stock_code = data['stock_code']
            self.stats['websocket_data'] += 1

            # 캐시에 저장
            cache.cache_current_price(stock_code, {
                'status': 'success',
                **data,
                'source': DataSource.WEBSOCKET.value
            })

            # 등록된 콜백 실행
            if stock_code in self.data_callbacks:
                for callback in self.data_callbacks[stock_code]:
                    try:
                        callback(stock_code, data)
                    except Exception as e:
                        logger.error(f"콜백 실행 오류: {stock_code} - {e}")

    def register_callback(self, stock_code: str, callback: Callable) -> None:
        """데이터 콜백 등록"""
        if stock_code not in self.data_callbacks:
            self.data_callbacks[stock_code] = []

        self.data_callbacks[stock_code].append(callback)
        logger.debug(f"콜백 등록: {stock_code}")

    def unregister_callback(self, stock_code: str, callback: Callable) -> None:
        """데이터 콜백 해제"""
        if stock_code in self.data_callbacks:
            try:
                self.data_callbacks[stock_code].remove(callback)
                if not self.data_callbacks[stock_code]:
                    del self.data_callbacks[stock_code]
                logger.debug(f"콜백 해제: {stock_code}")
            except ValueError:
                pass

    # === 상태 및 통계 ===

    def get_stats(self) -> Dict:
        """수집기 통계"""
        cache_stats = cache.get_all_cache_stats()

        return {
            'collector_stats': self.stats.copy(),
            'cache_stats': cache_stats,
            'websocket_status': {'connected': False, 'subscriptions': 0},  # 임시
            'subscriptions': len(self.data_callbacks)
        }

    def cleanup_cache(self) -> Dict[str, int]:
        """캐시 정리"""
        return cache.cleanup_all_caches()

    def clear_all_data(self) -> None:
        """모든 데이터 삭제"""
        cache.clear_all_caches()
        self.data_callbacks.clear()
        logger.info("모든 데이터 삭제 완료")

    def get_cache_status(self) -> Dict:
        """캐시 상태 조회"""
        return cache.get_all_cache_stats()


# 편의 함수들 (공식 스타일)
def get_current_price(stock_code: str, is_demo: bool = False) -> Dict:
    """현재가 간단 조회"""
    collector = KISDataCollector(is_demo=is_demo)
    return collector.get_current_price(stock_code)


def get_orderbook(stock_code: str, is_demo: bool = False) -> Dict:
    """호가 간단 조회"""
    collector = KISDataCollector(is_demo=is_demo)
    return collector.get_orderbook(stock_code)


def get_stock_overview(stock_code: str, is_demo: bool = False) -> Dict:
    """종목 개요 간단 조회"""
    collector = KISDataCollector(is_demo=is_demo)
    return collector.get_stock_overview(stock_code)
