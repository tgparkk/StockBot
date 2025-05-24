"""
하이브리드 데이터 관리자
WebSocket(실시간) + REST API(보완) 하이브리드 데이터 수집 시스템
"""
import asyncio
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from utils.logger import setup_logger
from core.rest_api_manager import KISRestAPIManager
from core.websocket_manager import KISWebSocketManager, TradingTimeSlot

logger = setup_logger(__name__)

class DataPriority(Enum):
    """데이터 우선순위"""
    CRITICAL = 1      # WebSocket 실시간 (체결가+호가)
    HIGH = 2          # WebSocket 준실시간 (체결가만)
    MEDIUM = 3        # REST API 30초 간격
    LOW = 4           # REST API 1분 간격
    BACKGROUND = 5    # REST API 5분 간격

@dataclass
class StockDataRequest:
    """종목 데이터 요청"""
    stock_code: str
    priority: DataPriority
    strategy_name: str
    callback: Optional[Callable] = None  # 데이터 수신시 콜백
    last_update: datetime = field(default_factory=datetime.now)
    update_count: int = 0

class HybridDataManager:
    """하이브리드 데이터 관리자"""
    
    def __init__(self, websocket_manager=None):
        # 컴포넌트 초기화
        self.rest_api = KISRestAPIManager()
        self.websocket_manager = websocket_manager or KISWebSocketManager()
        
        # 데이터 요청 관리
        self.data_requests: Dict[str, StockDataRequest] = {}
        self.request_lock = threading.RLock()
        
        # REST API 폴링 관리
        self.rest_polling_pools = {
            DataPriority.MEDIUM: [],    # 30초 간격
            DataPriority.LOW: [],       # 1분 간격
            DataPriority.BACKGROUND: [] # 5분 간격
        }
        
        # 스레드 풀
        self.rest_executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="REST-Poller")
        
        # 데이터 캐시
        self.data_cache: Dict[str, Dict] = {}
        self.cache_timestamps: Dict[str, datetime] = {}
        self.cache_lock = threading.Lock()
        
        # 폴링 상태
        self.polling_active = False
        self.polling_tasks = {}
        
        # 통계
        self.stats = {
            'websocket_updates': 0,
            'rest_api_calls': 0,
            'cache_hits': 0,
            'total_requests': 0
        }
    
    def add_stock_request(self, stock_code: str, priority: DataPriority, 
                         strategy_name: str, callback: Callable = None) -> bool:
        """종목 데이터 요청 추가"""
        with self.request_lock:
            request = StockDataRequest(
                stock_code=stock_code,
                priority=priority,
                strategy_name=strategy_name,
                callback=callback
            )
            
            self.data_requests[stock_code] = request
            self.stats['total_requests'] += 1
            
            # 우선순위에 따른 데이터 수집 방식 결정
            success = self._assign_data_source(request)
            
            logger.info(f"종목 요청 추가: {stock_code} (우선순위: {priority.name}, 전략: {strategy_name})")
            return success
    
    def _assign_data_source(self, request: StockDataRequest) -> bool:
        """데이터 소스 할당"""
        stock_code = request.stock_code
        priority = request.priority
        
        if priority == DataPriority.CRITICAL:
            # WebSocket 실시간 (체결가 + 호가)
            success = self.websocket_manager.add_emergency_subscription(
                stock_code=stock_code,
                strategy_name=request.strategy_name,
                priority=95
            )
            
            # 호가도 추가 (별도 요청)
            # 실제 구현시에는 WebSocketManager에서 처리
            return success
            
        elif priority == DataPriority.HIGH:
            # WebSocket 준실시간 (체결가만)
            return self.websocket_manager.add_emergency_subscription(
                stock_code=stock_code,
                strategy_name=request.strategy_name,
                priority=85
            )
            
        elif priority in [DataPriority.MEDIUM, DataPriority.LOW, DataPriority.BACKGROUND]:
            # REST API 폴링에 추가
            self.rest_polling_pools[priority].append(request)
            return True
            
        return False
    
    def remove_stock_request(self, stock_code: str) -> bool:
        """종목 데이터 요청 제거"""
        with self.request_lock:
            if stock_code not in self.data_requests:
                return False
            
            request = self.data_requests[stock_code]
            
            # 데이터 소스에서 제거
            if request.priority in [DataPriority.CRITICAL, DataPriority.HIGH]:
                # WebSocket에서 제거
                subscription_key = f"H0STCNT0|{stock_code}"  # 체결가
                self.websocket_manager.remove_subscription(subscription_key)
                
                if request.priority == DataPriority.CRITICAL:
                    # 호가도 제거
                    orderbook_key = f"H0STASP0|{stock_code}"
                    self.websocket_manager.remove_subscription(orderbook_key)
            
            else:
                # REST 폴링에서 제거
                pool = self.rest_polling_pools.get(request.priority, [])
                self.rest_polling_pools[request.priority] = [
                    r for r in pool if r.stock_code != stock_code
                ]
            
            # 요청 제거
            del self.data_requests[stock_code]
            
            # 캐시에서도 제거
            with self.cache_lock:
                self.data_cache.pop(stock_code, None)
                self.cache_timestamps.pop(stock_code, None)
            
            logger.info(f"종목 요청 제거: {stock_code}")
            return True
    
    def upgrade_priority(self, stock_code: str, new_priority: DataPriority) -> bool:
        """우선순위 업그레이드"""
        with self.request_lock:
            if stock_code not in self.data_requests:
                return False
            
            request = self.data_requests[stock_code]
            old_priority = request.priority
            
            if new_priority.value >= old_priority.value:
                logger.warning(f"우선순위 업그레이드 실패: {old_priority.name} → {new_priority.name}")
                return False
            
            # 기존 소스에서 제거
            self.remove_stock_request(stock_code)
            
            # 새 우선순위로 추가
            return self.add_stock_request(
                stock_code=stock_code,
                priority=new_priority,
                strategy_name=request.strategy_name,
                callback=request.callback
            )
    
    def start_polling(self):
        """REST API 폴링 시작"""
        if self.polling_active:
            logger.warning("폴링이 이미 활성화됨")
            return
        
        self.polling_active = True
        
        # 각 우선순위별 폴링 태스크 시작
        polling_intervals = {
            DataPriority.MEDIUM: 30,     # 30초
            DataPriority.LOW: 60,        # 1분
            DataPriority.BACKGROUND: 300 # 5분
        }
        
        for priority, interval in polling_intervals.items():
            task = asyncio.create_task(
                self._polling_loop(priority, interval)
            )
            self.polling_tasks[priority] = task
        
        logger.info("REST API 폴링 시작")
    
    def stop_polling(self):
        """REST API 폴링 중지"""
        self.polling_active = False
        
        # 모든 폴링 태스크 취소
        for task in self.polling_tasks.values():
            task.cancel()
        
        self.polling_tasks.clear()
        logger.info("REST API 폴링 중지")
    
    async def _polling_loop(self, priority: DataPriority, interval: int):
        """폴링 루프"""
        while self.polling_active:
            try:
                pool = self.rest_polling_pools.get(priority, [])
                
                if pool:
                    await self._batch_rest_api_calls(pool, priority)
                
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"{priority.name} 폴링 중 오류: {e}")
                await asyncio.sleep(5)  # 오류시 5초 대기
    
    async def _batch_rest_api_calls(self, requests: List[StockDataRequest], priority: DataPriority):
        """배치 REST API 호출"""
        batch_size = 5  # Rate Limiting 고려
        
        for i in range(0, len(requests), batch_size):
            batch = requests[i:i + batch_size]
            
            # 비동기 배치 처리
            tasks = []
            for request in batch:
                task = asyncio.create_task(self._fetch_rest_data(request))
                tasks.append(task)
            
            # 모든 배치 완료 대기
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 결과 처리
            for request, result in zip(batch, results):
                if isinstance(result, Exception):
                    logger.error(f"REST API 호출 실패: {request.stock_code} - {result}")
                else:
                    self._process_data_update(request.stock_code, result, 'REST')
            
            # Rate Limiting을 위한 배치간 대기
            if i + batch_size < len(requests):
                await asyncio.sleep(0.5)  # 0.5초 대기
    
    async def _fetch_rest_data(self, request: StockDataRequest) -> Dict:
        """REST API 데이터 조회"""
        loop = asyncio.get_event_loop()
        
        # 스레드 풀에서 동기 API 호출
        data = await loop.run_in_executor(
            self.rest_executor,
            self._sync_fetch_data,
            request.stock_code
        )
        
        self.stats['rest_api_calls'] += 1
        return data
    
    def _sync_fetch_data(self, stock_code: str) -> Dict:
        """동기 데이터 조회"""
        try:
            # 현재가 조회
            current_price = self.rest_api.get_current_price(stock_code)
            
            # 캐시 확인 - 호가는 자주 호출하지 않음
            orderbook = self._get_cached_orderbook(stock_code)
            if not orderbook:
                orderbook = self.rest_api.get_orderbook(stock_code)
                self._cache_orderbook(stock_code, orderbook)
            
            return {
                'current_price': current_price,
                'orderbook': orderbook,
                'source': 'REST',
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            logger.error(f"동기 데이터 조회 실패: {stock_code} - {e}")
            return {}
    
    def _get_cached_orderbook(self, stock_code: str) -> Optional[Dict]:
        """캐시된 호가 조회"""
        with self.cache_lock:
            if stock_code in self.data_cache:
                cache_time = self.cache_timestamps.get(stock_code)
                if cache_time and datetime.now() - cache_time < timedelta(seconds=30):
                    self.stats['cache_hits'] += 1
                    return self.data_cache[stock_code].get('orderbook')
            return None
    
    def _cache_orderbook(self, stock_code: str, orderbook: Dict):
        """호가 데이터 캐시"""
        with self.cache_lock:
            if stock_code not in self.data_cache:
                self.data_cache[stock_code] = {}
            
            self.data_cache[stock_code]['orderbook'] = orderbook
            self.cache_timestamps[stock_code] = datetime.now()
    
    def handle_websocket_data(self, stock_code: str, data: Dict):
        """WebSocket 데이터 처리"""
        self._process_data_update(stock_code, data, 'WebSocket')
        self.stats['websocket_updates'] += 1
    
    def _process_data_update(self, stock_code: str, data: Dict, source: str):
        """데이터 업데이트 처리"""
        if stock_code not in self.data_requests:
            return
        
        request = self.data_requests[stock_code]
        request.last_update = datetime.now()
        request.update_count += 1
        
        # 데이터 캐시 업데이트
        with self.cache_lock:
            if stock_code not in self.data_cache:
                self.data_cache[stock_code] = {}
            
            self.data_cache[stock_code].update(data)
            self.cache_timestamps[stock_code] = datetime.now()
        
        # 콜백 실행
        if request.callback:
            try:
                request.callback(stock_code, data, source)
            except Exception as e:
                logger.error(f"콜백 실행 실패: {stock_code} - {e}")
        
        logger.debug(f"데이터 업데이트: {stock_code} ({source})")
    
    def get_latest_data(self, stock_code: str) -> Optional[Dict]:
        """최신 데이터 조회"""
        with self.cache_lock:
            data = self.data_cache.get(stock_code)
            if data:
                timestamp = self.cache_timestamps.get(stock_code)
                return {
                    **data,
                    'cache_timestamp': timestamp,
                    'age_seconds': (datetime.now() - timestamp).total_seconds() if timestamp else None
                }
            return None
    
    def optimize_allocation(self):
        """할당 최적화"""
        # 시간대에 따른 자동 전략 전환
        current_time_slot = self.websocket_manager.get_current_time_slot()
        
        # WebSocket 전략 전환
        self.websocket_manager.switch_strategy(current_time_slot)
        
        # REST API 폴링 주기 조정
        self._adjust_polling_intervals(current_time_slot)
        
        logger.info(f"할당 최적화 완료: {current_time_slot.value}")
    
    def _adjust_polling_intervals(self, time_slot: TradingTimeSlot):
        """시간대별 폴링 주기 조정"""
        # 시간대별로 폴링 주기를 다르게 설정
        # 예: 골든타임에는 더 빠르게, 점심시간에는 느리게
        
        if time_slot == TradingTimeSlot.GOLDEN_TIME:
            # 골든타임: 더 빠른 폴링
            self._update_polling_intervals({
                DataPriority.MEDIUM: 15,    # 15초
                DataPriority.LOW: 30,       # 30초
                DataPriority.BACKGROUND: 60 # 1분
            })
        elif time_slot == TradingTimeSlot.LUNCH_TIME:
            # 점심시간: 느린 폴링
            self._update_polling_intervals({
                DataPriority.MEDIUM: 60,     # 1분
                DataPriority.LOW: 120,       # 2분
                DataPriority.BACKGROUND: 300 # 5분
            })
        else:
            # 기본 주기
            self._update_polling_intervals({
                DataPriority.MEDIUM: 30,    # 30초
                DataPriority.LOW: 60,       # 1분
                DataPriority.BACKGROUND: 300 # 5분
            })
    
    def _update_polling_intervals(self, new_intervals: Dict[DataPriority, int]):
        """폴링 주기 업데이트"""
        # 기존 태스크들 취소하고 새로운 주기로 재시작
        # 실제 구현시에는 더 정교한 방식 필요
        logger.debug(f"폴링 주기 업데이트: {new_intervals}")
    
    def get_system_status(self) -> Dict:
        """시스템 상태 조회"""
        with self.request_lock:
            # 우선순위별 요청 수 계산
            priority_counts = defaultdict(int)
            for request in self.data_requests.values():
                priority_counts[request.priority.name] += 1
            
            # WebSocket 상태
            ws_status = self.websocket_manager.get_subscription_status()
            
            # REST 폴링 상태
            rest_pool_sizes = {
                priority.name: len(pool) 
                for priority, pool in self.rest_polling_pools.items()
            }
            
            return {
                'total_stocks': len(self.data_requests),
                'priority_breakdown': dict(priority_counts),
                'websocket_status': ws_status,
                'rest_polling_pools': rest_pool_sizes,
                'polling_active': self.polling_active,
                'cache_size': len(self.data_cache),
                'statistics': self.stats.copy(),
                'api_stats': KISRestAPIManager.get_api_stats()
            }
    
    def cleanup(self):
        """리소스 정리"""
        self.stop_polling()
        self.rest_executor.shutdown(wait=True)
        
        with self.cache_lock:
            self.data_cache.clear()
            self.cache_timestamps.clear()
        
        logger.info("하이브리드 데이터 관리자 정리 완료")