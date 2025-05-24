# 🧵 멀티스레드 환경에서의 KIS API 사용 가이드

## 📋 개요

KIS API는 다음과 같은 제한사항이 있어 멀티스레드 환경에서 주의깊게 사용해야 합니다:
- **REST API**: 초당 20건 제한
- **WebSocket**: 실시간 데이터 합산 41건까지 등록 가능

본 가이드는 안전하고 효율적인 멀티스레드 매매 시스템 구현 방법을 제시합니다.

## 🔧 개선된 broker.py 주요 기능

### 1. **Rate Limiting (속도 제한)**
```python
# 초당 20건 제한 자동 적용
broker = KISBroker()
# API 호출시 자동으로 rate limiting 적용
current_price = broker.get_current_price("005930")
```

### 2. **Thread Safety (스레드 안전성)**
```python
# 토큰 관리가 스레드 안전함
import threading
from concurrent.futures import ThreadPoolExecutor

def worker_thread(thread_id):
    broker = KISBroker()  # 각 스레드마다 인스턴스 생성
    return broker.get_current_price("005930")

# 동시에 여러 스레드에서 안전하게 사용 가능
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(worker_thread, i) for i in range(5)]
    results = [future.result() for future in futures]
```

### 3. **Connection Pool (연결 풀)**
```python
# HTTP 연결 재사용으로 성능 향상
# 자동으로 연결 풀 적용 (설정 불필요)
```

### 4. **API 통계 모니터링**
```python
# API 호출 통계 확인
stats = KISBroker.get_api_stats()
print(f"총 호출: {stats['total_calls']}")
print(f"성공률: {stats['success_rate']}%")
print(f"현재 초당 호출 수: {stats['calls_in_last_second']}")
```

## 🏗️ 권장 아키텍처

### 1. **분석 스레드 풀 + 순차 주문 처리**

```python
"""
권장 구조:
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   분석 스레드 1   │    │                 │    │                 │
│  (종목 1~10)    │ ──▶│   매매 신호 큐    │ ──▶│   주문 실행기     │
│                 │    │  (우선순위 큐)    │    │   (단일 스레드)   │
├─────────────────┤    │                 │    │                 │
│   분석 스레드 2   │ ──▶│                 │    │                 │
│  (종목 11~20)   │    └─────────────────┘    └─────────────────┘
├─────────────────┤
│   분석 스레드 N   │
│  (종목 N1~NN)   │
└─────────────────┘
"""

from core.broker import KISBroker
from concurrent.futures import ThreadPoolExecutor
import queue
import threading

# 스레드 안전한 신호 큐
signal_queue = queue.PriorityQueue()

def analyze_stocks(thread_id, stock_codes):
    """종목 분석 스레드"""
    broker = KISBroker()
    
    for stock_code in stock_codes:
        try:
            # 시장 데이터 수집 (Rate Limiting 자동 적용)
            current = broker.get_current_price_model(stock_code)
            historical = broker.get_historical_data_model(stock_code)
            
            # 전략 실행
            signal = my_strategy(current, historical)
            if signal:
                signal_queue.put((signal.priority, signal))
                
        except Exception as e:
            print(f"Thread-{thread_id}: {stock_code} 분석 실패 - {e}")

def execute_orders():
    """주문 실행 (단일 스레드)"""
    broker = KISBroker()
    
    while True:
        try:
            priority, signal = signal_queue.get(timeout=1)
            
            # 주문 실행
            if signal.type == 'BUY':
                result = broker.buy_order(
                    stock_code=signal.stock_code,
                    quantity=signal.quantity,
                    price=signal.price
                )
            
            print(f"주문 실행: {signal.stock_code} - {result}")
            
        except queue.Empty:
            continue
        except Exception as e:
            print(f"주문 실행 실패: {e}")
```

### 2. **실제 운영 예제**

```python
# examples/concurrent_trading_guide.py 참조
from examples.concurrent_trading_guide import ConcurrentTradingSystem

# 30개 종목을 5개 스레드로 분석
stock_list = ['005930', '000660', ...]  # 스크리닝된 종목들
trading_system = ConcurrentTradingSystem(
    stock_list=stock_list,
    num_analysis_threads=5  # 5개 스레드
)

trading_system.start()  # 시스템 시작
```

## ⚠️ 주의사항 및 모범 사례

### 1. **Rate Limiting 준수**
```python
# ❌ 잘못된 방법: 무제한 API 호출
def bad_concurrent_calls():
    broker = KISBroker()
    with ThreadPoolExecutor(max_workers=20) as executor:
        # 동시에 20개 스레드가 API 호출 → Rate Limit 초과!
        futures = [executor.submit(broker.get_current_price, f"00593{i}") 
                  for i in range(100)]

# ✅ 올바른 방법: 자동 Rate Limiting 적용
def good_concurrent_calls():
    broker = KISBroker()
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Rate Limiting이 자동 적용되어 안전
        futures = [executor.submit(broker.get_current_price, f"00593{i}") 
                  for i in range(100)]
```

### 2. **토큰 관리**
```python
# ✅ 스레드별 별도 인스턴스 권장
def worker_thread(thread_id):
    broker = KISBroker()  # 각 스레드마다 별도 인스턴스
    return broker.get_current_price("005930")

# ⚠️ 인스턴스 공유시 주의사항
shared_broker = KISBroker()  # 클래스 레벨에서 토큰 관리하므로 안전함
# 하지만 별도 인스턴스 사용을 권장
```

### 3. **에러 처리**
```python
def robust_api_call(broker, stock_code, max_retries=3):
    """견고한 API 호출"""
    for attempt in range(max_retries):
        try:
            return broker.get_current_price(stock_code)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)  # 지수 백오프
```

### 4. **메모리 관리**
```python
# ✅ 적절한 스레드 수 설정
num_threads = min(10, len(stock_list) // 3)  # 종목당 최소 3개씩 할당

# ✅ 큐 크기 제한
signal_queue = queue.PriorityQueue(maxsize=1000)  # 메모리 사용량 제한
```

## 📊 성능 모니터링

### 1. **API 통계 모니터링**
```python
import time

def monitor_api_performance():
    while True:
        stats = KISBroker.get_api_stats()
        print(f"API 호출: {stats['total_calls']}, "
              f"성공률: {stats['success_rate']}%, "
              f"초당 호출: {stats['calls_in_last_second']}")
        
        time.sleep(30)  # 30초마다 모니터링

# 별도 스레드에서 실행
monitor_thread = threading.Thread(target=monitor_api_performance)
monitor_thread.daemon = True
monitor_thread.start()
```

### 2. **시스템 리소스 모니터링**
```python
import psutil

def monitor_system_resources():
    """시스템 리소스 모니터링"""
    cpu_percent = psutil.cpu_percent()
    memory_percent = psutil.virtual_memory().percent
    
    print(f"CPU: {cpu_percent}%, 메모리: {memory_percent}%")
    
    if cpu_percent > 80 or memory_percent > 80:
        print("⚠️ 리소스 사용량 높음 - 스레드 수 조정 필요")
```

## 🧪 테스트 방법

### 1. **스레드 안전성 테스트**
```bash
# 멀티스레드 안전성 테스트 실행
python examples/test_thread_safety.py
```

### 2. **부하 테스트**
```python
# 고부하 상황에서의 동작 확인
from examples.test_thread_safety import ThreadSafetyTester

tester = ThreadSafetyTester()
tester.test_concurrent_api_calls(num_threads=15, calls_per_thread=10)
```

## 📈 성능 최적화 팁

### 1. **WebSocket 활용**
```python
# REST API 호출 최소화
from core.websocket_manager import KISWebSocketManager

ws_manager = KISWebSocketManager()
# 실시간 데이터는 WebSocket으로, 주문만 REST API 사용
```

### 2. **데이터 캐싱**
```python
import time
from functools import lru_cache

class CachedBroker(KISBroker):
    def __init__(self):
        super().__init__()
        self._cache = {}
        self._cache_expiry = {}
    
    def get_current_price_cached(self, stock_code, ttl=1):
        """1초 캐시된 현재가 조회"""
        now = time.time()
        
        if (stock_code in self._cache and 
            now < self._cache_expiry.get(stock_code, 0)):
            return self._cache[stock_code]
        
        # 캐시 만료시 새로 조회
        result = self.get_current_price(stock_code)
        self._cache[stock_code] = result
        self._cache_expiry[stock_code] = now + ttl
        
        return result
```

### 3. **배치 처리**
```python
def batch_process_stocks(stock_codes, batch_size=5):
    """배치 단위로 종목 처리"""
    broker = KISBroker()
    
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i + batch_size]
        
        results = []
        for stock_code in batch:
            results.append(broker.get_current_price(stock_code))
            time.sleep(0.05)  # 배치 내 호출 간격
        
        # 배치 처리 완료 후 잠시 대기
        time.sleep(0.2)
        
        yield results
```

## 🔍 문제 해결

### 1. **Rate Limit 초과시**
```python
# 에러 메시지: "API rate limit exceeded"
# 해결: 스레드 수 줄이기 또는 호출 간격 늘리기

# 현재 rate limit 상태 확인
stats = KISBroker.get_api_stats()
if stats['calls_in_last_second'] >= 18:  # 거의 한계
    time.sleep(1)  # 1초 대기
```

### 2. **토큰 만료시**
```python
# 토큰 상태 확인
broker = KISBroker()
token_info = broker.get_token_info()

if not token_info['is_valid']:
    broker.force_token_refresh()  # 수동 갱신
```

### 3. **메모리 부족시**
```python
# 큐 크기 모니터링
queue_size = signal_queue.qsize()
if queue_size > 500:  # 큐가 너무 클 때
    print("⚠️ 신호 큐 크기 초과 - 처리 속도 증가 필요")
```

## 📚 추가 자료

- **예제 코드**: `examples/concurrent_trading_guide.py`
- **테스트 코드**: `examples/test_thread_safety.py`
- **WebSocket 가이드**: `docs/WEBSOCKET_GUIDE.md`
- **API 제한사항**: [KIS API 문서](https://apiportal.koreainvestment.com/)

---

**⚠️ 중요**: 실제 운영시에는 반드시 모의투자 환경에서 충분한 테스트를 거친 후 사용하세요. 