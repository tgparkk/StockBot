"""
KIS 데이터 수집기 (리팩토링 버전)
"""
import time
import asyncio
import threading
from typing import Dict, List, Optional, Any, Callable
from enum import Enum
from utils.logger import setup_logger
from . import kis_data_cache as cache
from ..api.rest_api_manager import KISRestAPIManager
from ..websocket.kis_websocket_manager import KISWebSocketManager

logger = setup_logger(__name__)


class DataSource(Enum):
    """데이터 소스"""
    WEBSOCKET = "websocket"
    REST_API = "rest_api"
    CACHE = "cache"


class KISDataCollector:
    """KIS 데이터 수집기 (간소화 버전)"""

    def __init__(self, websocket_manager: KISWebSocketManager, rest_api_manager: KISRestAPIManager):
        """초기화"""

        self.rest_api = rest_api_manager
        self.websocket = websocket_manager

        # 콜백 등록
        self.data_callbacks: Dict[str, List[Callable]] = {}

        # 통계
        self.stats = {
            'websocket_data': 0,
            'rest_api_calls': 0,
            'cache_hits': 0,
            'total_requests': 0
        }

    def get_current_price(self, stock_code: str, use_cache: bool = False) -> Dict:
        """현재가 조회 (실시간 우선) - 최신성 문제 해결 버전"""
        self.stats['total_requests'] += 1
        diagnostic_info = {'websocket_status': 'unknown', 'cache_status': 'unknown'}

        # 1. WebSocket 실시간 데이터 확인 (가장 우선) - 진단 강화
        try:
            # 🔍 웹소켓 연결 상태 상세 진단
            websocket_connected = False
            websocket_subscribed = False

            if hasattr(self.websocket, 'is_connected'):
                websocket_connected = self.websocket.is_connected
                diagnostic_info['websocket_status'] = 'connected' if websocket_connected else 'disconnected'

                # 구독 상태도 확인
                if websocket_connected and hasattr(self.websocket, 'subscribed_stocks'):
                    websocket_subscribed = stock_code in self.websocket.subscribed_stocks
                    diagnostic_info['websocket_subscribed'] = websocket_subscribed

            # 🔍 웹소켓 캐시 데이터 확인 (웹소켓 전용)
            websocket_data = None
            if websocket_connected:
                websocket_data = cache.get_cached_price(stock_code)

                if websocket_data and websocket_data.get('source') == DataSource.WEBSOCKET.value:
                    data_age = time.time() - websocket_data.get('timestamp', 0)
                    diagnostic_info['cache_status'] = f'websocket_age_{data_age:.1f}s'

                    # 🎯 웹소켓 데이터이고 신선한 경우 우선 사용
                    if data_age < 5:  # 5초 이내
                        self.stats['cache_hits'] += 1
                        logger.debug(f"✅ 웹소켓 실시간 데이터 사용: {stock_code} (나이: {data_age:.1f}초)")
                        return {
                            **websocket_data,
                            'from_cache': True,
                            'diagnostic_info': diagnostic_info
                        }
                    elif data_age < 30:  # 30초 이내도 유효 (REST API보다 신뢰)
                        logger.debug(f"⚠️ 웹소켓 데이터 다소 오래되었지만 사용: {stock_code} (나이: {data_age:.1f}초)")
                        diagnostic_info['cache_status'] = f'websocket_stale_but_used_{data_age:.1f}s'
                        return {
                            **websocket_data,
                            'from_cache': True,
                            'diagnostic_info': diagnostic_info
                        }
                    else:
                        logger.debug(f"⚠️ 웹소켓 데이터 너무 오래됨: {stock_code} (나이: {data_age:.1f}초)")
                        diagnostic_info['cache_status'] = f'websocket_data_stale_{data_age:.1f}s'
                elif websocket_data:
                    # 다른 소스 데이터 (REST API 등)가 있는 경우
                    data_source = websocket_data.get('source', 'unknown')
                    data_age = time.time() - websocket_data.get('timestamp', 0)
                    logger.debug(f"📋 캐시에 다른 소스 데이터 존재: {stock_code} (소스: {data_source}, 나이: {data_age:.1f}초)")
                    diagnostic_info['cache_status'] = f'non_websocket_{data_source}_age_{data_age:.1f}s'
                else:
                    diagnostic_info['cache_status'] = 'no_cache_data'
                    if websocket_subscribed:
                        logger.debug(f"⚠️ 웹소켓 구독 중이지만 캐시 데이터 없음: {stock_code}")
                    else:
                        logger.debug(f"📊 웹소켓 미구독 종목: {stock_code}")
            else:
                diagnostic_info['websocket_status'] = 'no_websocket_manager'
                diagnostic_info['cache_status'] = 'websocket_disconnected'
                logger.debug(f"🔴 웹소켓 연결 안됨: {stock_code}")

        except Exception as e:
            diagnostic_info['websocket_error'] = str(e)
            logger.debug(f"웹소켓 데이터 확인 오류: {e}")

        # 2. REST API 호출 (웹소켓 데이터가 없거나 오래된 경우)
        try:
            logger.debug(f"🌐 REST API 사용: {stock_code} (진단: {diagnostic_info})")
            data = self.rest_api.get_current_price(stock_code)
            self.stats['rest_api_calls'] += 1

            if data.get('status') == 'success':
                # 🎯 중요: REST API 데이터 캐시 저장 전략 개선
                should_cache_rest_data = self._should_cache_rest_api_data(stock_code, data)

                if should_cache_rest_data:
                    cache.cache_current_price(stock_code, data)
                    logger.debug(f"💾 REST API 데이터 캐시 저장: {stock_code}")
                else:
                    logger.debug(f"🚫 REST API 데이터 캐시 저장 안함 (웹소켓 데이터 보호): {stock_code}")

                return {
                    **data,
                    'source': DataSource.REST_API.value,
                    'from_cache': False,
                    'diagnostic_info': diagnostic_info
                }
            else:
                logger.error(f"현재가 조회 실패: {stock_code}")
                return {**data, 'diagnostic_info': diagnostic_info}

        except Exception as e:
            logger.error(f"현재가 조회 오류: {stock_code} - {e}")

        # 3. 마지막 수단으로 캐시 확인 (오류 시에만, 소스 무관)
        if use_cache:
            cached_data = cache.get_cached_price(stock_code)
            if cached_data:
                self.stats['cache_hits'] += 1
                data_age = time.time() - cached_data.get('timestamp', 0)
                logger.warning(f"캐시 데이터 사용 (최신 조회 실패): {stock_code} (나이: {data_age:.1f}초)")
                return {
                    **cached_data,
                    'source': DataSource.CACHE.value,
                    'from_cache': True,
                    'diagnostic_info': diagnostic_info
                }

        return {
            'status': 'error',
            'message': f'현재가 조회 실패: 모든 데이터 소스 사용 불가',
            'source': 'none',
            'diagnostic_info': diagnostic_info
        }

    def _should_cache_rest_api_data(self, stock_code: str, rest_data: Dict) -> bool:
        """🎯 REST API 데이터 캐시 저장 여부 결정"""
        try:
            # 현재 캐시에 있는 데이터 확인
            cached_data = cache.get_cached_price(stock_code)

            if not cached_data:
                # 캐시에 데이터가 없으면 REST API 데이터 저장
                return True

            cached_source = cached_data.get('source', 'unknown')
            cached_timestamp = cached_data.get('timestamp', 0)
            rest_timestamp = time.time()

            # 🎯 웹소켓 데이터가 있는 경우의 보호 로직
            if cached_source == DataSource.WEBSOCKET.value:
                cached_age = rest_timestamp - cached_timestamp

                # 웹소켓 데이터가 5분 이내면 REST API 데이터로 덮어쓰지 않음
                if cached_age < 300:  # 5분
                    logger.debug(f"🛡️ 웹소켓 데이터 보호: {stock_code} (웹소켓 나이: {cached_age:.1f}초)")
                    return False
                else:
                    # 웹소켓 데이터가 너무 오래되었으면 REST API 데이터로 교체
                    logger.debug(f"🔄 오래된 웹소켓 데이터 교체: {stock_code} (나이: {cached_age:.1f}초)")
                    return True
            else:
                # 웹소켓이 아닌 데이터는 항상 교체 가능
                return True

        except Exception as e:
            logger.error(f"REST API 캐시 저장 결정 오류: {stock_code} - {e}")
            return True  # 오류 시 저장

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
                # cached_data가 이미 List[Dict] 형태인지 확인하고 반환
                if isinstance(cached_data, list):
                    return cached_data
                else:
                    # Dict인 경우 List로 감싸서 반환
                    return [cached_data]

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

    def get_multiple_prices(self, stock_codes: List[str], use_cache: bool = False) -> Dict[str, Dict]:
        """여러 종목 현재가 배치 조회 (실시간 우선)"""
        results = {}

        for stock_code in stock_codes:
            results[stock_code] = self.get_current_price(stock_code, use_cache)
            time.sleep(0.05)  # API Rate Limiting (더 빠르게)

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
            # 내부 콜백 등록 (캐시 업데이트용)
            self.register_callback(stock_code, self._websocket_callback)

            # 사용자 콜백 등록
            if callback:
                self.register_callback(stock_code, callback)

            # WebSocket 구독 (체결가 + 호가)
            if hasattr(self.websocket, 'subscribe_stock_sync'):
                # 🆕 동기 방식 통합 구독 (체결 + 호가) - 이벤트 루프 문제 해결
                logger.debug(f"📡 동기 방식 웹소켓 구독 시도: {stock_code}")
                success = self.websocket.subscribe_stock_sync(stock_code, self._websocket_callback)
                logger.debug(f"📡 동기 방식 웹소켓 구독 결과: {stock_code} = {success}")
            elif hasattr(self.websocket, 'subscribe_stock'):
                # 🔧 기존 async 방식 (fallback)
                try:
                    success = asyncio.run(self.websocket.subscribe_stock(stock_code, self._websocket_callback))
                except Exception as e:
                    logger.error(f"async 구독 실패: {stock_code} - {e}")
                    # 호환성 구독으로 fallback
                    success = self.websocket.subscribe_stock_price(stock_code, "data_collector")
            else:
                # 호환성 구독
                success = self.websocket.subscribe_stock_price(stock_code, "data_collector")

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

    def _websocket_callback(self, data_type: str, stock_code: str, data: Dict) -> None:
        """WebSocket 데이터 콜백 - 🆕 data_type 파라미터 추가"""
        try:
            self.stats['websocket_data'] += 1

            # 실시간 현재가 데이터로 변환
            if data.get('type') == 'contract':
                price_data = {
                    'status': 'success',
                    'stock_code': stock_code,
                    'current_price': data.get('current_price', 0),
                    'change_rate': data.get('change_rate', 0.0),
                    'volume': data.get('volume', 0),
                    'timestamp': time.time(),
                    'source': DataSource.WEBSOCKET.value
                }

                # 캐시에 저장 (실시간 데이터)
                cache.cache_current_price(stock_code, price_data)
                logger.debug(f"실시간 현재가 업데이트: {stock_code} = {price_data['current_price']:,}원")

            # 등록된 콜백 실행
            if stock_code in self.data_callbacks:
                for callback in self.data_callbacks[stock_code]:
                    try:
                        # 🆕 기존 콜백 호환성을 위해 data_type 없이 호출
                        if callable(callback):
                            # 콜백 함수 시그니처 확인해서 적절히 호출
                            import inspect
                            sig = inspect.signature(callback)
                            param_count = len(sig.parameters)

                            if param_count >= 3:
                                # 새로운 형식: callback(data_type, stock_code, data)
                                callback(data_type, stock_code, data)
                            else:
                                # 기존 형식: callback(stock_code, data)
                                callback(stock_code, data)
                    except Exception as e:
                        logger.error(f"콜백 실행 오류: {stock_code} - {e}")

        except Exception as e:
            logger.error(f"WebSocket 콜백 처리 오류: {stock_code} - {e}")

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

    # ========== 실시간 현재가 조회 편의 메서드들 ==========

    def get_realtime_price(self, stock_code: str) -> Dict:
        """실시간 현재가만 조회 (WebSocket 우선)"""
        return self.get_current_price(stock_code, use_cache=False)

    def get_fresh_price(self, stock_code: str) -> Dict:
        """최신 현재가 조회 (REST API 강제)"""
        try:
            data = self.rest_api.get_current_price(stock_code)
            self.stats['rest_api_calls'] += 1

            if data.get('status') == 'success':
                # 캐시에 저장
                cache.cache_current_price(stock_code, data)
                return {
                    **data,
                    'source': DataSource.REST_API.value,
                    'from_cache': False
                }
            else:
                return data

        except Exception as e:
            logger.error(f"최신 현재가 조회 오류: {stock_code} - {e}")
            return {
                'status': 'error',
                'message': f'최신 현재가 조회 오류: {e}',
                'source': DataSource.REST_API.value
            }

    def is_realtime_available(self, stock_code: str) -> bool:
        """실시간 데이터 사용 가능 여부"""
        try:
            if not hasattr(self.websocket, 'is_connected'):
                return False

            if not self.websocket.is_connected:
                return False

            # 최근 실시간 데이터가 있는지 확인
            cached_data = cache.get_cached_price(stock_code)
            if cached_data and cached_data.get('source') == DataSource.WEBSOCKET.value:
                # 10초 이내 데이터가 있으면 활성 상태
                return time.time() - cached_data.get('timestamp', 0) < 10

            return False

        except Exception:
            return False

    def get_data_freshness(self, stock_code: str) -> Dict:
        """데이터 신선도 정보"""
        try:
            cached_data = cache.get_cached_price(stock_code)
            if not cached_data:
                return {'status': 'no_data'}

            age = time.time() - cached_data.get('timestamp', 0)
            source = cached_data.get('source', 'unknown')

            return {
                'age_seconds': age,
                'source': source,
                'is_fresh': age < 30,  # 30초 이내
                'is_realtime': source == DataSource.WEBSOCKET.value and age < 5
            }

        except Exception as e:
            logger.error(f"데이터 신선도 확인 오류: {stock_code} - {e}")
            return {'status': 'error'}

    # ========== 🔍 진단 및 모니터링 메서드들 ==========

    def get_websocket_diagnostic(self, stock_code: str = None) -> Dict:
        """🔍 웹소켓 상태 상세 진단"""
        try:
            diagnostic = {
                'timestamp': time.time(),
                'websocket_manager': {
                    'exists': hasattr(self, 'websocket'),
                    'connected': False,
                    'running': False,
                    'total_subscriptions': 0,
                    'subscribed_stocks': []
                },
                'cache_status': {},
                'overall_health': 'unhealthy'
            }

            # 웹소켓 매니저 상태 확인
            if hasattr(self, 'websocket') and self.websocket:
                ws = self.websocket
                diagnostic['websocket_manager'].update({
                    'connected': getattr(ws, 'is_connected', False),
                    'running': getattr(ws, 'is_running', False),
                    'total_subscriptions': len(getattr(ws, 'subscribed_stocks', [])),
                    'subscribed_stocks': list(getattr(ws, 'subscribed_stocks', [])),
                    'stats': getattr(ws, 'stats', {}),
                    'health_check': getattr(ws, 'is_healthy', lambda: False)()
                })

            # 특정 종목 캐시 상태 확인
            if stock_code:
                cached_data = cache.get_cached_price(stock_code)
                if cached_data:
                    data_age = time.time() - cached_data.get('timestamp', 0)
                    diagnostic['cache_status'][stock_code] = {
                        'exists': True,
                        'source': cached_data.get('source', 'unknown'),
                        'age_seconds': data_age,
                        'current_price': cached_data.get('current_price', 0),
                        'is_fresh': data_age < 30,
                        'is_realtime': cached_data.get('source') == DataSource.WEBSOCKET.value and data_age < 5
                    }
                else:
                    diagnostic['cache_status'][stock_code] = {
                        'exists': False,
                        'reason': 'no_cached_data'
                    }

            # 전체 건강성 판단
            ws_manager = diagnostic['websocket_manager']
            if (ws_manager['exists'] and ws_manager['connected'] and
                ws_manager['running'] and ws_manager['health_check']):
                diagnostic['overall_health'] = 'healthy'
            elif ws_manager['connected']:
                diagnostic['overall_health'] = 'partially_healthy'

            return diagnostic

        except Exception as e:
            return {
                'error': f'진단 중 오류: {e}',
                'timestamp': time.time(),
                'overall_health': 'error'
            }

    def test_websocket_data_flow(self, stock_code: str = "005930") -> Dict:
        """🧪 웹소켓 데이터 플로우 테스트"""
        test_results = {
            'test_stock': stock_code,
            'timestamp': time.time(),
            'steps': {},
            'success': False
        }

        try:
            # 1단계: 웹소켓 연결 확인
            test_results['steps']['1_connection'] = self._test_websocket_connection()

            # 2단계: 구독 상태 확인
            test_results['steps']['2_subscription'] = self._test_websocket_subscription(stock_code)

            # 3단계: 캐시 데이터 확인
            test_results['steps']['3_cache_data'] = self._test_cache_data(stock_code)

            # 4단계: 실시간 데이터 수신 테스트
            test_results['steps']['4_realtime_test'] = self._test_realtime_data_reception(stock_code)

            # 전체 성공 여부 판단
            all_passed = all(step.get('passed', False) for step in test_results['steps'].values())
            test_results['success'] = all_passed

            return test_results

        except Exception as e:
            test_results['error'] = f'테스트 중 오류: {e}'
            return test_results

    def _test_websocket_connection(self) -> Dict:
        """웹소켓 연결 테스트"""
        try:
            if not hasattr(self, 'websocket') or not self.websocket:
                return {'passed': False, 'message': '웹소켓 매니저 없음'}

            ws = self.websocket
            connected = getattr(ws, 'is_connected', False)
            running = getattr(ws, 'is_running', False)
            healthy = getattr(ws, 'is_healthy', lambda: False)()

            if connected and running and healthy:
                return {'passed': True, 'message': '웹소켓 연결 정상'}
            else:
                return {
                    'passed': False,
                    'message': f'웹소켓 상태 이상 (연결:{connected}, 실행:{running}, 건강:{healthy})'
                }

        except Exception as e:
            return {'passed': False, 'message': f'연결 테스트 오류: {e}'}

    def _test_websocket_subscription(self, stock_code: str) -> Dict:
        """웹소켓 구독 상태 테스트"""
        try:
            if not hasattr(self, 'websocket') or not self.websocket:
                return {'passed': False, 'message': '웹소켓 매니저 없음'}

            ws = self.websocket
            subscribed_stocks = getattr(ws, 'subscribed_stocks', set())
            is_subscribed = stock_code in subscribed_stocks

            if is_subscribed:
                return {'passed': True, 'message': f'{stock_code} 구독 중'}
            else:
                return {
                    'passed': False,
                    'message': f'{stock_code} 미구독 (구독 종목: {list(subscribed_stocks)})'
                }

        except Exception as e:
            return {'passed': False, 'message': f'구독 테스트 오류: {e}'}

    def _test_cache_data(self, stock_code: str) -> Dict:
        """캐시 데이터 테스트"""
        try:
            cached_data = cache.get_cached_price(stock_code)

            if not cached_data:
                return {'passed': False, 'message': f'{stock_code} 캐시 데이터 없음'}

            source = cached_data.get('source', 'unknown')
            age = time.time() - cached_data.get('timestamp', 0)

            if source == DataSource.WEBSOCKET.value and age < 30:
                return {
                    'passed': True,
                    'message': f'웹소켓 캐시 데이터 정상 (나이: {age:.1f}초)'
                }
            else:
                return {
                    'passed': False,
                    'message': f'캐시 데이터 문제 (소스: {source}, 나이: {age:.1f}초)'
                }

        except Exception as e:
            return {'passed': False, 'message': f'캐시 테스트 오류: {e}'}

    def _test_realtime_data_reception(self, stock_code: str) -> Dict:
        """실시간 데이터 수신 테스트 (30초간 모니터링)"""
        try:
            # 테스트 시작 시점의 캐시 타임스탬프 기록
            initial_data = cache.get_cached_price(stock_code)
            initial_timestamp = initial_data.get('timestamp', 0) if initial_data else 0

            # 30초 대기하며 새로운 데이터 수신 확인
            import time
            start_time = time.time()
            timeout = 30  # 30초 타임아웃

            while time.time() - start_time < timeout:
                current_data = cache.get_cached_price(stock_code)
                if current_data:
                    current_timestamp = current_data.get('timestamp', 0)
                    source = current_data.get('source', 'unknown')

                    # 새로운 웹소켓 데이터가 수신되었는지 확인
                    if (current_timestamp > initial_timestamp and
                        source == DataSource.WEBSOCKET.value):
                        return {
                            'passed': True,
                            'message': f'실시간 데이터 수신 확인 ({time.time() - start_time:.1f}초 후)'
                        }

                time.sleep(1)  # 1초마다 확인

            return {
                'passed': False,
                'message': f'{timeout}초 동안 새로운 웹소켓 데이터 수신 없음'
            }

        except Exception as e:
            return {'passed': False, 'message': f'실시간 테스트 오류: {e}'}

    def monitor_websocket_data_updates(self, duration_seconds: int = 60) -> Dict:
        """🎬 웹소켓 데이터 업데이트 모니터링"""
        monitoring_results = {
            'duration': duration_seconds,
            'start_time': time.time(),
            'updates': [],
            'stats': {
                'total_updates': 0,
                'unique_stocks': set(),
                'avg_update_interval': 0
            }
        }

        try:
            start_time = time.time()
            last_check_data = {}

            logger.info(f"🎬 웹소켓 데이터 업데이트 모니터링 시작 ({duration_seconds}초)")

            while time.time() - start_time < duration_seconds:
                # 웹소켓 매니저의 구독 종목들 확인
                if hasattr(self, 'websocket') and self.websocket:
                    subscribed_stocks = getattr(self.websocket, 'subscribed_stocks', set())

                    for stock_code in subscribed_stocks:
                        current_data = cache.get_cached_price(stock_code)

                        if current_data and current_data.get('source') == DataSource.WEBSOCKET.value:
                            current_timestamp = current_data.get('timestamp', 0)
                            last_timestamp = last_check_data.get(stock_code, 0)

                            # 새로운 업데이트 감지
                            if current_timestamp > last_timestamp:
                                update_info = {
                                    'stock_code': stock_code,
                                    'timestamp': current_timestamp,
                                    'price': current_data.get('current_price', 0),
                                    'age_seconds': time.time() - current_timestamp
                                }

                                monitoring_results['updates'].append(update_info)
                                monitoring_results['stats']['unique_stocks'].add(stock_code)
                                last_check_data[stock_code] = current_timestamp

                time.sleep(2)  # 2초마다 확인

            # 통계 계산
            total_updates = len(monitoring_results['updates'])
            monitoring_results['stats']['total_updates'] = total_updates
            monitoring_results['stats']['unique_stocks'] = len(monitoring_results['stats']['unique_stocks'])

            if total_updates > 1:
                timestamps = [update['timestamp'] for update in monitoring_results['updates']]
                intervals = [timestamps[i] - timestamps[i-1] for i in range(1, len(timestamps))]
                monitoring_results['stats']['avg_update_interval'] = sum(intervals) / len(intervals)

            logger.info(f"🎬 모니터링 완료: {total_updates}개 업데이트, "
                       f"{monitoring_results['stats']['unique_stocks']}개 종목")

            return monitoring_results

        except Exception as e:
            monitoring_results['error'] = f'모니터링 오류: {e}'
            return monitoring_results
