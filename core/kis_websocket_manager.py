"""
KIS API 통합 웹소켓 매니저
- 실시간 주식 데이터 처리
- 다중 콜백 시스템 지원
- 41건 웹소켓 제한 최적화
- 자동 재연결 및 에러 처리
"""
import asyncio
import json
import threading
import time
import websockets
import requests
from typing import Dict, List, Optional, Callable, Set, Any, Union
from enum import Enum
from utils.logger import setup_logger
from . import kis_auth as kis

# AES 복호화 (체결통보용)
try:
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad
    from base64 import b64decode
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

logger = setup_logger(__name__)


class KIS_WSReq(Enum):
    """웹소켓 요청 타입"""
    BID_ASK = 'H0STASP0'     # 실시간 국내주식 호가
    CONTRACT = 'H0STCNT0'    # 실시간 국내주식 체결
    NOTICE = 'H0STCNI0'      # 실시간 계좌체결발생통보 (실전)
    NOTICE_DEMO = 'H0STCNI9' # 실시간 계좌체결발생통보 (모의)
    MARKET_INDEX = 'H0UPCNT0' # 실시간 시장지수


class DataType(Enum):
    """데이터 타입"""
    STOCK_PRICE = 'stock_price'          # 주식체결가
    STOCK_ORDERBOOK = 'stock_orderbook'  # 주식호가
    STOCK_EXECUTION = 'stock_execution'  # 주식체결통보
    MARKET_INDEX = 'market_index'        # 시장지수


class KISWebSocketManager:
    """KIS API 통합 웹소켓 매니저"""

    def __init__(self):
        # 웹소켓 제한
        self.WEBSOCKET_LIMIT = 41
        self.MAX_STOCKS = 13  # 체결(13) + 호가(13) = 26건 + 여유분

        # 연결 정보 (실전투자용 고정)
        self.ws_url = 'ws://ops.koreainvestment.com:21000'
        self.approval_key: Optional[str] = None
        self.websocket: Optional[Any] = None

        # 구독 관리
        self.subscribed_stocks: Set[str] = set()
        self.subscription_lock = threading.Lock()

        # 다중 콜백 시스템
        self.stock_callbacks: Dict[str, List[Callable]] = {}  # 종목별 콜백들
        self.global_callbacks: Dict[str, List[Callable]] = {   # 데이터 타입별 글로벌 콜백들
            DataType.STOCK_PRICE.value: [],
            DataType.STOCK_ORDERBOOK.value: [],
            DataType.STOCK_EXECUTION.value: [],
            DataType.MARKET_INDEX.value: []
        }

        # 운영 상태
        self.is_connected = False
        self.is_running = False
        self.connection_attempts = 0
        self.max_reconnect_attempts = 5

        # 체결통보용 복호화 키
        self.aes_key: Optional[str] = None
        self.aes_iv: Optional[str] = None

        # 통계
        self.stats = {
            'messages_received': 0,
            'data_processed': 0,
            'subscriptions': 0,
            'errors': 0,
            'reconnections': 0,
            'last_message_time': None,
            'ping_pong_count': 0
        }

        logger.info(f"KIS 통합 웹소켓 매니저 초기화 (실전투자)")

    def ensure_connection(self):
        """웹소켓 연결 보장 (동기 방식)"""
        try:
            logger.info("🔗 웹소켓 연결 보장 시도")
            
            # 이미 연결되어 있다면 건강성 체크
            if self.is_connected and self.is_running:
                if self.is_healthy():
                    logger.info("✅ 웹소켓이 이미 연결되어 있고 건강한 상태")
                    return
                else:
                    logger.warning("⚠️ 웹소켓이 연결되어 있지만 건강하지 않음 - 재연결 시도")
            
            # 🔧 기존 루프와 태스크 정리
            if hasattr(self, '_event_loop') and self._event_loop and not self._event_loop.is_closed():
                try:
                    # 기존 메시지 핸들러 태스크 취소
                    if hasattr(self, '_message_handler_task') and not self._message_handler_task.done():
                        self._message_handler_task.cancel()
                    self._event_loop.call_soon_threadsafe(self._event_loop.stop)
                    self._event_loop = None
                except Exception as e:
                    logger.debug(f"기존 이벤트 루프 정리 중 오류: {e}")
            
            # 🆕 백그라운드에서 지속적으로 실행될 이벤트 루프 생성
            import threading
            success_flag = threading.Event()
            error_container = []
            
            def run_persistent_loop():
                """백그라운드에서 지속적으로 실행될 이벤트 루프"""
                try:
                    # 새 이벤트 루프 생성
                    self._event_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(self._event_loop)
                    
                    # 웹소켓 연결 및 메시지 처리 루프 시작
                    async def setup_and_run():
                        try:
                            # 연결 시도
                            if await self._ensure_connection_async():
                                success_flag.set()
                                logger.info("🎯 백그라운드 웹소켓 연결 성공")
                                
                                # 🆕 구독 안정화 대기 시간 추가
                                await asyncio.sleep(2)  # 충분한 안정화 시간
                                
                                # 메시지 처리 루프 계속 실행 (태스크 분리하지 않고 직접 실행)
                                logger.info("📨 백그라운드 메시지 처리 루프 시작")
                                
                                # 🔧 별도 태스크 생성하지 않고 직접 메시지 처리
                                while self.is_running and self.is_connected:
                                    try:
                                        # 🆕 이벤트 루프 상태 확인
                                        try:
                                            current_loop = asyncio.get_running_loop()
                                            if current_loop.is_closed():
                                                logger.info("백그라운드 루프 - 이벤트 루프가 닫혀있음")
                                                break
                                        except RuntimeError:
                                            logger.info("백그라운드 루프 - 실행 중인 이벤트 루프가 없음")
                                            break
                                        
                                        # 메시지 수신 (타임아웃 짧게)
                                        try:
                                            message = await asyncio.wait_for(self.websocket.recv(), timeout=10)
                                            self.stats['messages_received'] += 1
                                            from datetime import datetime
                                            self.stats['last_message_time'] = datetime.now()

                                            if message[0] in ('0', '1'):
                                                # 실시간 데이터
                                                await self._handle_realtime_data(message)
                                            else:
                                                # 시스템 메시지
                                                await self._handle_system_message(message)
                                                
                                        except asyncio.TimeoutError:
                                            # 타임아웃 - 정상적인 상황, 계속 진행
                                            continue
                                        except websockets.exceptions.ConnectionClosed:
                                            logger.info("백그라운드 루프 - 웹소켓 연결이 종료됨")
                                            self.is_connected = False
                                            break
                                            
                                    except asyncio.CancelledError:
                                        logger.info("백그라운드 메시지 처리 루프 취소됨")
                                        break
                                    except Exception as loop_error:
                                        logger.debug(f"백그라운드 메시지 처리 루프 내부 오류: {loop_error}")
                                        if not self.is_running:
                                            break
                                        await asyncio.sleep(1)  # 오류 후 잠시 대기
                                        
                                logger.info("📨 백그라운드 메시지 처리 루프 종료")
                            else:
                                error_container.append("웹소켓 연결 실패")
                                success_flag.set()  # 실패라도 대기 해제
                        except asyncio.CancelledError:
                            logger.info("setup_and_run 태스크 취소됨")
                        except Exception as e:
                            error_container.append(str(e))
                            success_flag.set()
                    
                    # 🔧 이벤트 루프에서 안전하게 실행
                    try:
                        self._event_loop.run_until_complete(setup_and_run())
                    except asyncio.CancelledError:
                        logger.info("백그라운드 이벤트 루프 태스크 취소됨")
                    except RuntimeError as e:
                        if "Event loop stopped" in str(e):
                            logger.debug("이벤트 루프가 중단됨 (정상 종료)")
                        else:
                            logger.warning(f"백그라운드 이벤트 루프 런타임 오류: {e}")
                            error_container.append(str(e))
                            success_flag.set()
                    
                except Exception as e:
                    logger.error(f"백그라운드 이벤트 루프 오류: {e}")
                    error_container.append(str(e))
                    success_flag.set()
                finally:
                    # 🆕 루프 정리는 완전히 종료될 때만
                    if hasattr(self, '_event_loop') and self._event_loop:
                        try:
                            if not self._event_loop.is_closed():
                                # 🔧 안전하게 태스크 취소
                                try:
                                    pending = asyncio.all_tasks(self._event_loop)
                                    for task in pending:
                                        if not task.done():
                                            task.cancel()
                                except Exception as task_error:
                                    logger.debug(f"태스크 취소 중 오류: {task_error}")
                                
                                # 🔧 루프 정리 시도
                                try:
                                    self._event_loop.close()
                                except Exception as close_error:
                                    logger.debug(f"이벤트 루프 닫기 오류: {close_error}")
                        except Exception as e:
                            logger.debug(f"이벤트 루프 정리 오류: {e}")
            
            # 백그라운드 스레드에서 실행
            self._websocket_thread = threading.Thread(target=run_persistent_loop, daemon=True)
            self._websocket_thread.start()
            
            # 연결 완료까지 대기 (최대 15초)
            if success_flag.wait(timeout=15):
                if error_container:
                    logger.error(f"❌ 웹소켓 연결 오류: {error_container[0]}")
                else:
                    logger.info("✅ 웹소켓 연결 보장 성공")
                    # 🆕 추가 안정화 대기
                    time.sleep(1)
            else:
                logger.error("❌ 웹소켓 연결 보장 시간 초과")
                
        except Exception as e:
            logger.error(f"웹소켓 연결 보장 오류: {e}")
            # 연결 실패해도 예외는 발생시키지 않음 (동기 방식 구독이 있으므로)

    async def _ensure_connection_async(self) -> bool:
        """웹소켓 연결 보장 (비동기)"""
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"웹소켓 연결 시도 {attempt}/{max_attempts}")
                
                if await self.connect():
                    # 연결 성공 시 충분한 안정화 대기
                    await asyncio.sleep(2)  # 🔧 안정화 시간 증가 (1초 → 2초)
                    
                    if self.is_connected:
                        logger.info(f"✅ 웹소켓 연결 성공 (시도 {attempt})")
                        
                        # 🆕 is_running 플래그 설정
                        self.is_running = True
                        
                        # 🔧 백그라운드 루프에서 메시지 처리하므로 별도 태스크 생성하지 않음
                        logger.info("📨 백그라운드 메시지 처리 루프가 이미 실행 중")
                        
                        # 🆕 추가 안정화 확인
                        await asyncio.sleep(1)
                        
                        # 🆕 최종 상태 확인
                        if self.is_connected and self.is_running:
                            logger.info("🎉 웹소켓 연결 및 메시지 처리 완전 준비")
                            return True
                        else:
                            logger.warning(f"⚠️ 연결 후 상태 불안정: connected={self.is_connected}, running={self.is_running}")
                        
                logger.warning(f"⚠️ 웹소켓 연결 실패 - 시도 {attempt}/{max_attempts}")
                
                if attempt < max_attempts:
                    await asyncio.sleep(2 * attempt)  # 지수적 백오프
                    
            except Exception as e:
                logger.error(f"웹소켓 연결 시도 {attempt} 오류: {e}")
                if attempt < max_attempts:
                    await asyncio.sleep(2 * attempt)
                    
        logger.error("❌ 모든 웹소켓 연결 시도 실패")
        return False

    def get_approval_key(self) -> str:
        """웹소켓 접속키 발급"""
        try:
            auth = kis.getTREnv()
            if auth is None:
                raise Exception("KIS 인증 정보가 없습니다")

            url = getattr(auth, 'my_url', None)
            app_key = getattr(auth, 'my_app', None)
            secret_key = getattr(auth, 'my_sec', None)

            if not all([url, app_key, secret_key]):
                raise Exception("KIS 인증 정보가 불완전합니다")

            headers = {"content-type": "application/json"}
            body = {
                "grant_type": "client_credentials",
                "appkey": app_key,
                "secretkey": secret_key
            }

            response = requests.post(f"{url}/oauth2/Approval", headers=headers, data=json.dumps(body))
            response.raise_for_status()

            approval_key = response.json()["approval_key"]
            logger.info("웹소켓 접속키 발급 성공")
            return approval_key

        except Exception as e:
            logger.error(f"웹소켓 접속키 발급 실패: {e}")
            raise

    def _build_message(self, tr_id: str, tr_key: str, tr_type: str = '1') -> str:
        """웹소켓 메시지 생성"""
        message = {
            "header": {
                "approval_key": self.approval_key,
                "custtype": "P",  # 개인
                "tr_type": tr_type,  # 1:등록, 2:해제
                "content-type": "utf-8"
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": tr_key
                }
            }
        }
        return json.dumps(message)

    async def connect(self) -> bool:
        """웹소켓 연결"""
        try:
            # 접속키 발급
            self.approval_key = self.get_approval_key()

            # 웹소켓 연결
            logger.info(f"웹소켓 연결 시도: {self.ws_url}")
            self.websocket = await websockets.connect(
                self.ws_url,
                ping_interval=60,  # 🆕 KIS 공식 가이드와 동일하게 60초
                ping_timeout=None,  # 🔧 ping_timeout 제거 (기본값 사용)
                close_timeout=10,
                max_size=None,  # 🆕 메시지 크기 제한 해제
                max_queue=None   # 🆕 큐 크기 제한 해제
            )

            self.is_connected = True
            self.connection_attempts = 0
            logger.info("✅ 웹소켓 연결 성공")

            return True

        except Exception as e:
            logger.error(f"웹소켓 연결 실패: {e}")
            self.is_connected = False
            return False

    async def disconnect(self):
        """안전한 웹소켓 연결 해제"""
        logger.info("🔌 웹소켓 연결 해제 시작...")
        
        try:
            # 1️⃣ 상태 변경
            self.is_connected = False
            self.is_running = False
            
            # 2️⃣ 백그라운드 태스크 안전하게 종료
            try:
                if hasattr(self, '_listener_task') and self._listener_task and not self._listener_task.done():
                    logger.debug("🔄 listener 태스크 종료 중...")
                    self._listener_task.cancel()
                    try:
                        await asyncio.wait_for(self._listener_task, timeout=3.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        logger.debug("🔄 listener 태스크 종료 완료")
                    except Exception as e:
                        logger.warning(f"listener 태스크 종료 오류: {e}")

                if hasattr(self, '_keepalive_task') and self._keepalive_task and not self._keepalive_task.done():
                    logger.debug("🔄 keepalive 태스크 종료 중...")
                    self._keepalive_task.cancel()
                    try:
                        await asyncio.wait_for(self._keepalive_task, timeout=3.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        logger.debug("🔄 keepalive 태스크 종료 완료")
                    except Exception as e:
                        logger.warning(f"keepalive 태스크 종료 오류: {e}")
                        
            except Exception as e:
                logger.warning(f"백그라운드 태스크 정리 오류: {e}")
            
            # 3️⃣ 웹소켓 연결 해제
            try:
                if self._is_websocket_connected():
                    logger.debug("🔌 웹소켓 연결 닫는 중...")
                    await self.websocket.close()
                    # 연결 완전 종료 대기
                    try:
                        await asyncio.wait_for(self.websocket.wait_closed(), timeout=3.0)
                    except asyncio.TimeoutError:
                        logger.warning("웹소켓 종료 대기 시간 초과")
            except Exception as e:
                logger.warning(f"웹소켓 연결 해제 오류: {e}")
            
            # 4️⃣ 정리
            self.websocket = None
            
            # 5️⃣ 구독 정보 정리
            with self.subscription_lock:
                self.subscribed_stocks.clear()
                self.stock_callbacks.clear()
            
            logger.info("✅ 웹소켓 연결 해제 완료")
            
        except Exception as e:
            logger.error(f"웹소켓 연결 해제 중 오류: {e}")

    def safe_disconnect(self):
        """동기식 안전한 웹소켓 해제 (메인 스레드 용)"""
        try:
            logger.info("🔌 동기식 웹소켓 연결 해제 시작...")
            
            # 1️⃣ 상태 변경
            self.is_connected = False
            self.is_running = False
            
            # 2️⃣ 이벤트 루프 체크 및 안전한 정리
            if hasattr(self, '_event_loop') and self._event_loop:
                try:
                    if not self._event_loop.is_closed():
                        # 새 스레드에서 비동기 정리 수행
                        def cleanup_async():
                            try:
                                # 새 이벤트 루프에서 정리
                                import asyncio
                                cleanup_loop = asyncio.new_event_loop()
                                asyncio.set_event_loop(cleanup_loop)
                                try:
                                    cleanup_loop.run_until_complete(self._async_cleanup())
                                finally:
                                    cleanup_loop.close()
                            except Exception as e:
                                logger.debug(f"비동기 정리 오류: {e}")
                        
                        import threading
                        cleanup_thread = threading.Thread(target=cleanup_async, daemon=True)
                        cleanup_thread.start()
                        cleanup_thread.join(timeout=5)  # 최대 5초 대기
                        
                        # 기존 이벤트 루프 정리
                        self._event_loop_closed = True
                        
                except Exception as e:
                    logger.warning(f"이벤트 루프 정리 오류: {e}")
            
            # 3️⃣ 동기적 정리
            try:
                self.websocket = None
                
                with self.subscription_lock:
                    self.subscribed_stocks.clear()
                    self.stock_callbacks.clear()
                    
            except Exception as e:
                logger.warning(f"동기적 정리 오류: {e}")
            
            logger.info("✅ 동기식 웹소켓 연결 해제 완료")
            
        except Exception as e:
            logger.error(f"동기식 웹소켓 연결 해제 오류: {e}")

    async def _async_cleanup(self):
        """비동기 정리 작업"""
        try:
            # 백그라운드 태스크들 정리
            tasks_to_cancel = []
            
            if hasattr(self, '_listener_task') and self._listener_task and not self._listener_task.done():
                tasks_to_cancel.append(self._listener_task)
                
            if hasattr(self, '_keepalive_task') and self._keepalive_task and not self._keepalive_task.done():
                tasks_to_cancel.append(self._keepalive_task)
            
            # 모든 태스크 취소
            for task in tasks_to_cancel:
                task.cancel()
            
            # 취소 완료 대기 (시간 제한)
            if tasks_to_cancel:
                await asyncio.wait(tasks_to_cancel, timeout=2.0, return_when=asyncio.ALL_COMPLETED)
            
            # 웹소켓 닫기
            if self._is_websocket_connected():
                await self.websocket.close()
                await asyncio.wait_for(self.websocket.wait_closed(), timeout=2.0)
                
        except Exception as e:
            logger.debug(f"비동기 정리 오류: {e}")

    def cleanup(self):
        """리소스 정리 (프로그램 종료 시)"""
        logger.info("🧹 웹소켓 매니저 정리 중...")
        
        try:
            # 1️⃣ 동기식 안전한 해제
            self.safe_disconnect()
            
            # 2️⃣ 모든 pending task 강제 정리 (이벤트 루프 문제 방지)
            try:
                # asyncio 라이브러리 import 안전성 체크
                import asyncio
                
                # 현재 실행 중인 모든 태스크 강제 종료
                try:
                    all_tasks = asyncio.all_tasks()
                    pending_tasks = [task for task in all_tasks if not task.done()]
                    
                    if pending_tasks:
                        logger.info(f"🧹 {len(pending_tasks)}개 pending 태스크 강제 정리 중...")
                        for task in pending_tasks:
                            try:
                                if hasattr(task, 'get_name'):
                                    task_name = task.get_name()
                                    if 'keepalive' in task_name.lower() or 'connection' in task_name.lower():
                                        task.cancel()
                                        logger.debug(f"💥 태스크 강제 취소: {task_name}")
                            except Exception as e:
                                logger.debug(f"태스크 정리 오류: {e}")
                                
                except RuntimeError:
                    # 이벤트 루프가 없는 경우
                    logger.debug("실행 중인 이벤트 루프 없음 - pending 태스크 정리 생략")
                    
            except Exception as e:
                logger.debug(f"pending 태스크 정리 오류: {e}")
            
            # 3️⃣ 추가 안전장치: 모든 상태 초기화
            try:
                self.is_connected = False
                self.is_running = False
                self._event_loop_closed = True
                self.websocket = None
                
                # 구독 정보 정리
                try:
                    with self.subscription_lock:
                        self.subscribed_stocks.clear()
                        self.stock_callbacks.clear()
                except:
                    pass
                    
            except Exception as e:
                logger.debug(f"상태 초기화 오류: {e}")
            
            logger.info("✅ 웹소켓 매니저 정리 완료")
            
        except Exception as e:
            logger.error(f"웹소켓 매니저 정리 오류: {e}")

    def __del__(self):
        """소멸자에서 정리 작업"""
        try:
            # 동기적 정리만 수행 (소멸자에서는 비동기 작업 피함)
            if hasattr(self, 'is_connected') and self.is_connected:
                self.is_connected = False
                self.is_running = False
                
            if hasattr(self, '_event_loop_closed'):
                self._event_loop_closed = True
                
        except Exception:
            pass  # 소멸자에서는 예외 발생 방지

    # ========== 콜백 시스템 ==========

    def add_callback(self, data_type: str, callback: Callable[[Dict], None]):
        """글로벌 콜백 함수 추가"""
        if data_type in self.global_callbacks:
            self.global_callbacks[data_type].append(callback)
            logger.debug(f"글로벌 콜백 추가: {data_type}")

    def remove_callback(self, data_type: str, callback: Callable[[Dict], None]):
        """글로벌 콜백 함수 제거"""
        if data_type in self.global_callbacks and callback in self.global_callbacks[data_type]:
            self.global_callbacks[data_type].remove(callback)
            logger.debug(f"글로벌 콜백 제거: {data_type}")

    def add_stock_callback(self, stock_code: str, callback: Callable):
        """종목별 콜백 함수 추가"""
        if stock_code not in self.stock_callbacks:
            self.stock_callbacks[stock_code] = []
        self.stock_callbacks[stock_code].append(callback)
        logger.debug(f"종목별 콜백 추가: {stock_code}")

    def remove_stock_callback(self, stock_code: str, callback: Callable):
        """종목별 콜백 함수 제거"""
        if stock_code in self.stock_callbacks and callback in self.stock_callbacks[stock_code]:
            self.stock_callbacks[stock_code].remove(callback)
            if not self.stock_callbacks[stock_code]:
                del self.stock_callbacks[stock_code]
            logger.debug(f"종목별 콜백 제거: {stock_code}")

    async def _execute_callbacks(self, data_type: str, data: Dict):
        """콜백 함수들 실행"""
        try:
            # 글로벌 콜백 실행
            for callback in self.global_callbacks.get(data_type, []):
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(data)
                    else:
                        callback(data)
                except Exception as e:
                    logger.error(f"글로벌 콜백 실행 오류 ({data_type}): {e}")

            # 종목별 콜백 실행 (stock_code가 있는 경우)
            stock_code = data.get('stock_code')
            if stock_code and stock_code in self.stock_callbacks:
                for callback in self.stock_callbacks[stock_code]:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(stock_code, data)
                        else:
                            callback(stock_code, data)
                    except Exception as e:
                        logger.error(f"종목별 콜백 실행 오류 ({stock_code}): {e}")

        except Exception as e:
            logger.error(f"콜백 실행 오류: {e}")

    # ========== 구독 관리 ==========

    def subscribe_stock_price(self, stock_code: str, strategy_name: str = "") -> bool:
        """주식체결가 구독 (호환성 메서드)"""
        logger.debug(f"주식체결가 구독 요청: {stock_code} (전략: {strategy_name})")
        return True  # 실제 구독은 subscribe_stock에서 처리

    def subscribe_stock_orderbook(self, stock_code: str, strategy_name: str = "") -> bool:
        """주식호가 구독 (호환성 메서드)"""
        logger.debug(f"주식호가 구독 요청: {stock_code} (전략: {strategy_name})")
        return True  # 실제 구독은 subscribe_stock에서 처리

    def subscribe_stock_execution(self, strategy_name: str = "") -> bool:
        """주식체결통보 구독 (호환성 메서드)"""
        logger.debug(f"주식체결통보 구독 요청 (전략: {strategy_name})")
        return True  # subscribe_notice 사용

    def subscribe_market_index(self, index_code: str, strategy_name: str = "") -> bool:
        """시장지수 구독"""
        # TODO: 시장지수 구독 구현
        logger.debug(f"시장지수 구독 요청: {index_code} (전략: {strategy_name})")
        return True

    def unsubscribe(self, tr_id: str, tr_key: str) -> bool:
        """구독 해제 (호환성 메서드)"""
        logger.debug(f"구독 해제 요청: {tr_id}|{tr_key}")
        return True

    def unsubscribe_strategy(self, strategy_name: str) -> int:
        """전략별 구독 해제 (호환성 메서드)"""
        logger.debug(f"전략별 구독 해제: {strategy_name}")
        return 0

    async def apply_subscriptions(self):
        """대기 중인 구독 적용 (호환성 메서드)"""
        logger.debug("구독 적용 요청")
        pass

    async def start_listening(self):
        """메시지 수신 시작 (호환성 메서드)"""
        await self._message_handler()

    async def subscribe_stock(self, stock_code: str, callback: Callable) -> bool:
        """종목 구독 (체결 + 호가)"""
        if not self.is_connected:
            logger.error("웹소켓이 연결되지 않음")
            return False

        # 🔧 락 사용 최적화: 락 보유 시간 최소화
        with self.subscription_lock:
            # 빠른 체크만 락 안에서 수행
            if len(self.subscribed_stocks) >= self.MAX_STOCKS:
                logger.warning(f"구독 한계 도달: {len(self.subscribed_stocks)}/{self.MAX_STOCKS}")
                return False

            # 이미 구독 중인지 확인
            if stock_code in self.subscribed_stocks:
                logger.warning(f"이미 구독 중인 종목: {stock_code}")
                return True
            
            # 🆕 미리 구독 목록에 추가 (웹소켓 통신 전에)
            self.subscribed_stocks.add(stock_code)

        # 🔧 락 해제 후 웹소켓 통신 수행 (시간이 오래 걸리는 작업)
        try:
            # 🔧 웹소켓 연결 재확인
            if not self._is_websocket_connected():
                logger.error(f"웹소켓 연결이 닫혀있음: {stock_code}")
                with self.subscription_lock:
                    self.subscribed_stocks.discard(stock_code)
                return False

            # 실시간 체결 구독
            contract_msg = self._build_message(KIS_WSReq.CONTRACT.value, stock_code)
            await self.websocket.send(contract_msg)
            await asyncio.sleep(0.2)  # 🔧 간격 증가 (0.1초 → 0.2초)

            # 실시간 호가 구독
            bid_ask_msg = self._build_message(KIS_WSReq.BID_ASK.value, stock_code)
            await self.websocket.send(bid_ask_msg)
            await asyncio.sleep(0.2)  # 🔧 간격 증가

            # 🔧 구독 성공 후 콜백 추가 (락 외부에서)
            self.add_stock_callback(stock_code, callback)
            self.stats['subscriptions'] += 1

            logger.info(f"종목 구독 성공: {stock_code} ({len(self.subscribed_stocks)}/{self.MAX_STOCKS})")
            return True

        except Exception as e:
            logger.error(f"종목 구독 실패: {stock_code} - {e}")
            
            # 🔧 실패 시 구독 목록에서 제거 (락 사용 최소화)
            with self.subscription_lock:
                self.subscribed_stocks.discard(stock_code)
            return False

    def subscribe_stock_sync(self, stock_code: str, callback: Callable) -> bool:
        """종목 구독 (동기 방식) - 메인 스레드에서 안전하게 호출 가능"""
        try:
            logger.debug(f"🔄 동기 방식 종목 구독 시도: {stock_code}")
            
            # 🔧 웹소켓 연결 상태 엄격 체크
            if not self.is_connected or not self.websocket:
                logger.error(f"웹소켓 연결 상태 불량: connected={self.is_connected}, websocket={self.websocket is not None}")
                return False
            
            # 🔧 락 경합 방지: 미리 빠른 체크
            with self.subscription_lock:
                if len(self.subscribed_stocks) >= self.MAX_STOCKS:
                    logger.warning(f"구독 한계 도달: {len(self.subscribed_stocks)}/{self.MAX_STOCKS}")
                    return False
                
                if stock_code in self.subscribed_stocks:
                    logger.debug(f"이미 구독 중인 종목: {stock_code}")
                    return True
            
            # 새로운 이벤트 루프에서 async 함수 실행 (락 해제 후)
            def run_subscribe():
                try:
                    import asyncio
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        result = loop.run_until_complete(self.subscribe_stock(stock_code, callback))
                        return result
                    finally:
                        loop.close()
                except Exception as e:
                    logger.error(f"동기 구독 내부 오류 ({stock_code}): {e}")
                    return False
            
            # 🔧 타임아웃 연장: 별도 스레드에서 실행하여 메인 스레드 블록 방지
            import threading
            result_container = [False]  # 결과를 담을 컨테이너
            
            def thread_target():
                result_container[0] = run_subscribe()
            
            subscribe_thread = threading.Thread(target=thread_target, daemon=True)
            subscribe_thread.start()
            subscribe_thread.join(timeout=10)  # 🔧 타임아웃 10초로 연장
            
            if subscribe_thread.is_alive():
                logger.warning(f"⚠️ 동기 구독 시간 초과: {stock_code}")
                # 🔧 타임아웃 시 구독 목록에서 제거 (미리 추가되었을 수 있으므로)
                with self.subscription_lock:
                    self.subscribed_stocks.discard(stock_code)
                return False
            
            success = result_container[0]
            if success:
                logger.debug(f"✅ 동기 방식 구독 성공: {stock_code}")
            else:
                logger.debug(f"❌ 동기 방식 구독 실패: {stock_code}")
            
            return success
            
        except Exception as e:
            logger.error(f"동기 구독 오류 ({stock_code}): {e}")
            # 🔧 오류 시 구독 목록에서 제거
            with self.subscription_lock:
                self.subscribed_stocks.discard(stock_code)
            return False

    async def unsubscribe_stock(self, stock_code: str) -> bool:
        """종목 구독 해제"""
        if not self.is_connected:
            return False

        # 🔧 락 사용 최적화: 빠른 체크만 락 안에서
        with self.subscription_lock:
            if stock_code not in self.subscribed_stocks:
                return True
            
            # 🆕 미리 구독 목록에서 제거 (웹소켓 통신 전에)
            self.subscribed_stocks.discard(stock_code)

        # 🔧 락 해제 후 웹소켓 통신 수행
        try:
            # 실시간 체결 구독 해제
            contract_msg = self._build_message(KIS_WSReq.CONTRACT.value, stock_code, '2')
            await self.websocket.send(contract_msg)
            await asyncio.sleep(0.1)

            # 실시간 호가 구독 해제
            bid_ask_msg = self._build_message(KIS_WSReq.BID_ASK.value, stock_code, '2')
            await self.websocket.send(bid_ask_msg)
            await asyncio.sleep(0.1)

            # 🔧 콜백 제거 (락 외부에서)
            self.stock_callbacks.pop(stock_code, None)

            logger.info(f"종목 구독 해제: {stock_code}")
            return True

        except Exception as e:
            logger.error(f"종목 구독 해제 실패: {stock_code} - {e}")
            
            # 🔧 실패 시 구독 목록에 다시 추가
            with self.subscription_lock:
                self.subscribed_stocks.add(stock_code)
            return False

    async def subscribe_notice(self, hts_id: str) -> bool:
        """체결통보 구독 (실전투자 고정)"""
        if not self.is_connected:
            return False

        try:
            notice_type = KIS_WSReq.NOTICE  # 실전투자용 고정
            notice_msg = self._build_message(notice_type.value, hts_id)
            await self.websocket.send(notice_msg)

            logger.info(f"체결통보 구독 성공: {hts_id}")
            return True

        except Exception as e:
            logger.error(f"체결통보 구독 실패: {e}")
            return False

    def _parse_contract_data(self, data: str) -> Dict:
        """실시간 체결 데이터 파싱"""
        try:
            parts = data.split('^')
            if len(parts) < 20:
                return {}

            from datetime import datetime
            parsed_data = {
                'stock_code': parts[0],
                'time': parts[1],
                'current_price': int(parts[2]) if parts[2] else 0,
                'change_sign': parts[3],
                'change': int(parts[4]) if parts[4] else 0,
                'change_rate': float(parts[5]) if parts[5] else 0.0,
                'volume': int(parts[13]) if parts[13] else 0,
                'acc_volume': int(parts[14]) if parts[14] else 0,
                'strength': float(parts[18]) if parts[18] else 0.0,
                'timestamp': datetime.now(),
                'source': 'websocket',
                'type': 'contract'
            }

            # 통계 업데이트
            self.stats['data_processed'] += 1
            self.stats['last_message_time'] = datetime.now()

            return parsed_data

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"체결 데이터 파싱 오류: {e}")
            return {}

    def _parse_bid_ask_data(self, data: str) -> Dict:
        """실시간 호가 데이터 파싱"""
        try:
            parts = data.split('^')
            if len(parts) < 45:
                return {}

            from datetime import datetime
            parsed_data = {
                'stock_code': parts[0],
                'time': parts[1],
                'ask_price1': int(parts[3]) if parts[3] else 0,
                'bid_price1': int(parts[13]) if parts[13] else 0,
                'ask_qty1': int(parts[23]) if parts[23] else 0,
                'bid_qty1': int(parts[33]) if parts[33] else 0,
                'total_ask_qty': int(parts[43]) if parts[43] else 0,
                'total_bid_qty': int(parts[44]) if parts[44] else 0,
                'timestamp': datetime.now(),
                'source': 'websocket',
                'type': 'bid_ask'
            }

            # 통계 업데이트
            self.stats['data_processed'] += 1
            self.stats['last_message_time'] = datetime.now()

            return parsed_data

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"호가 데이터 파싱 오류: {e}")
            return {}

    def _decrypt_notice_data(self, encrypted_data: str) -> str:
        """체결통보 데이터 복호화"""
        if not CRYPTO_AVAILABLE or not self.aes_key or not self.aes_iv:
            return ""

        try:
            cipher = AES.new(self.aes_key.encode('utf-8'), AES.MODE_CBC, self.aes_iv.encode('utf-8'))
            decrypted = unpad(cipher.decrypt(b64decode(encrypted_data)), AES.block_size)
            return decrypted.decode('utf-8')

        except Exception as e:
            logger.error(f"체결통보 복호화 오류: {e}")
            return ""

    async def _handle_realtime_data(self, data: str):
        """실시간 데이터 처리"""
        try:
            parts = data.split('|')
            if len(parts) < 4:
                return

            tr_id = parts[1]
            raw_data = parts[3]

            if tr_id == KIS_WSReq.CONTRACT.value:
                # 실시간 체결
                parsed_data = self._parse_contract_data(raw_data)
                if parsed_data:
                    stock_code = parsed_data['stock_code']
                    await self._execute_callbacks(DataType.STOCK_PRICE.value, parsed_data)

            elif tr_id == KIS_WSReq.BID_ASK.value:
                # 실시간 호가
                parsed_data = self._parse_bid_ask_data(raw_data)
                if parsed_data:
                    stock_code = parsed_data['stock_code']
                    await self._execute_callbacks(DataType.STOCK_ORDERBOOK.value, parsed_data)

            elif tr_id in [KIS_WSReq.NOTICE.value, KIS_WSReq.NOTICE_DEMO.value]:
                # 체결통보 (실전투자는 NOTICE만 사용)
                if tr_id == KIS_WSReq.NOTICE.value:
                    decrypted_data = self._decrypt_notice_data(raw_data)
                    if decrypted_data:
                        logger.info(f"체결통보 수신: {decrypted_data}")
                        from datetime import datetime
                        await self._execute_callbacks(DataType.STOCK_EXECUTION.value, {'data': decrypted_data, 'timestamp': datetime.now()})

        except Exception as e:
            logger.error(f"실시간 데이터 처리 오류: {e}")

    async def _handle_system_message(self, data: str):
        """시스템 메시지 처리"""
        try:
            # 🆕 이벤트 루프 안전성 미리 확인
            try:
                current_loop = asyncio.get_running_loop()
                if current_loop.is_closed():
                    logger.debug("시스템 메시지 처리 - 이벤트 루프가 닫혀있음, 건너뜀")
                    return
            except RuntimeError:
                logger.debug("시스템 메시지 처리 - 실행 중인 이벤트 루프가 없음, 건너뜀")
                return
            
            json_data = json.loads(data)
            tr_id = json_data.get('header', {}).get('tr_id', '')

            if tr_id == "PINGPONG":
                # 🆕 KIS 공식 가이드에 맞춘 PING/PONG 응답
                logger.debug(f"### RECV [PINGPONG] [{data}]")
                
                # 🔧 PINGPONG 처리를 더 안전하게 (이벤트 루프 문제 완전 방지)
                try:
                    # 🆕 웹소켓 연결 상태 미리 확인
                    if not self.websocket or not self.is_connected:
                        logger.debug("PINGPONG 처리 - 웹소켓 연결 없음, 건너뜀")
                        return
                    
                    # 🆕 이벤트 루프 재확인 (PINGPONG 직전)
                    try:
                        loop_check = asyncio.get_running_loop()
                        if loop_check.is_closed():
                            logger.debug("PINGPONG 처리 - 루프 닫힘 감지, 건너뜀")
                            return
                    except RuntimeError:
                        logger.debug("PINGPONG 처리 - 루프 없음 감지, 건너뜀")
                        return
                    
                    # 🔧 PONG 응답 시도 (더 방어적으로)
                    try:
                        # websockets 라이브러리 PONG 메서드 사용
                        if hasattr(self.websocket, 'pong'):
                            if isinstance(data, str):
                                await self.websocket.pong(data.encode('utf-8'))
                            else:
                                await self.websocket.pong(data)
                        else:
                            # 대안: send로 직접 pong 전송
                            await self.websocket.send(data)
                        
                        logger.debug(f"### SEND [PINGPONG] [{data}]")
                        self.stats['ping_pong_count'] = self.stats.get('ping_pong_count', 0) + 1
                        from datetime import datetime
                        self.stats['last_ping_pong_time'] = datetime.now()
                        
                    except asyncio.CancelledError:
                        logger.debug("PONG 응답 중 태스크 취소됨")
                        raise  # CancelledError는 다시 발생시켜야 함
                    except Exception as pong_error:
                        # 🆕 PONG 실패 시 더 자세한 진단
                        error_msg = str(pong_error)
                        if "Event loop is closed" in error_msg:
                            logger.debug("PONG 실패: 이벤트 루프 닫힘 - 연결 상태 업데이트")
                            self.is_connected = False
                        elif "different loop" in error_msg:
                            logger.debug("PONG 실패: 다른 루프 문제 - 무시하고 계속")
                        else:
                            logger.debug(f"PONG 실패: {error_msg}")
                        
                        # 🆕 PONG 실패해도 치명적이지 않으므로 계속 진행
                        pass
                        
                except Exception as ping_error:
                    logger.debug(f"PINGPONG 전체 처리 오류: {ping_error}")

            else:
                body = json_data.get('body', {})
                rt_cd = body.get('rt_cd', '')
                msg = body.get('msg1', '')

                if rt_cd == '0':  # 성공
                    logger.debug(f"시스템 메시지: {msg}")

                    # 체결통보 암호화 키 저장
                    output = body.get('output', {})
                    if 'key' in output and 'iv' in output:
                        self.aes_key = output['key']
                        self.aes_iv = output['iv']
                        logger.info("체결통보 암호화 키 수신")

                elif rt_cd == '1':  # 오류
                    logger.error(f"시스템 오류: {msg}")
                    self.stats['errors'] += 1

        except asyncio.CancelledError:
            logger.debug("시스템 메시지 처리 - 태스크 취소됨")
            raise  # CancelledError는 다시 발생시켜야 함
        except Exception as e:
            # 🆕 오류 로깅 최소화 (너무 많은 로그 방지)
            if "Event loop is closed" in str(e):
                logger.debug(f"시스템 메시지 처리 - 이벤트 루프 닫힘: {e}")
            elif "PINGPONG" in data:
                logger.debug(f"PINGPONG 처리 중 오류: {e}")
            else:
                logger.error(f"시스템 메시지 처리 오류: {e}")

    async def _message_handler(self):
        """메시지 수신 루프"""
        try:
            logger.debug("📨 메시지 처리 루프 시작")
            while self.is_running and self.is_connected:
                try:
                    # 🆕 이벤트 루프 안전성 확인
                    try:
                        current_loop = asyncio.get_running_loop()
                        if current_loop.is_closed():
                            logger.warning("메시지 처리 루프 - 이벤트 루프가 닫혀있음")
                            break
                    except RuntimeError:
                        logger.warning("메시지 처리 루프 - 실행 중인 이벤트 루프가 없음")
                        break
                    
                    # 🔧 태스크 생성 시 현재 루프 사용 보장
                    message = await asyncio.wait_for(self.websocket.recv(), timeout=30)
                    self.stats['messages_received'] += 1

                    if message[0] in ('0', '1'):
                        # 실시간 데이터
                        await self._handle_realtime_data(message)
                    else:
                        # 시스템 메시지
                        await self._handle_system_message(message)

                except asyncio.TimeoutError:
                    # 타임아웃 - 정상적인 상황
                    continue

                except asyncio.CancelledError:
                    logger.info("메시지 처리 루프 - 태스크 취소됨")
                    break

                except websockets.exceptions.ConnectionClosed:
                    logger.warning("웹소켓 연결이 종료됨")
                    self.is_connected = False
                    break

        except Exception as e:
            logger.error(f"메시지 처리 루프 오류: {e}")
            self.is_connected = False
        finally:
            logger.debug("📨 메시지 처리 루프 종료")

    async def start(self) -> bool:
        """웹소켓 시작"""
        try:
            self.is_running = True

            # 연결
            if not await self.connect():
                return False

            # 메시지 처리 루프 시작
            await self._message_handler()

            return True

        except Exception as e:
            logger.error(f"웹소켓 시작 실패: {e}")
            return False

    async def stop(self):
        """웹소켓 중지"""
        try:
            logger.info("웹소켓 중지 중...")
            self.is_running = False

            # 모든 구독 해제
            stock_codes = list(self.subscribed_stocks)
            for stock_code in stock_codes:
                await self.unsubscribe_stock(stock_code)

            # 연결 해제
            await self.disconnect()

            logger.info("웹소켓 중지 완료")

        except Exception as e:
            logger.error(f"웹소켓 중지 오류: {e}")

    def get_status(self) -> Dict:
        """상태 조회"""
        # 🆕 실시간 연결 상태 체크
        actual_connected = self._check_actual_connection_status()
        
        # 🔧 락 사용 최소화: 빠른 읽기만 락 안에서
        with self.subscription_lock:
            subscribed_count = len(self.subscribed_stocks)
            websocket_usage = f"{subscribed_count * 2}/{self.WEBSOCKET_LIMIT}"
        
        return {
            'is_connected': self.is_connected,
            'is_running': self.is_running,
            'actual_connected': actual_connected,  # 🆕 실제 연결 상태
            'subscribed_stocks': subscribed_count,
            'max_stocks': self.MAX_STOCKS,
            'websocket_usage': websocket_usage,
            'stats': self.stats.copy(),
            'ping_pong_responses': self.stats.get('ping_pong_count', 0),
            'last_ping_pong': self.stats.get('last_ping_pong_time', None),
            'last_message_time': self.stats.get('last_message_time', None)
        }

    def _check_actual_connection_status(self) -> bool:
        """실제 웹소켓 연결 상태 체크"""
        try:
            # 1차: 기본 플래그 확인
            if not self.is_connected or not self.is_running:
                return False
            
            # 2차: 웹소켓 객체 존재 확인
            if not self.websocket:
                return False
            
            # 3차: websockets 라이브러리 상태 체크 (안전성 개선)
            try:
                # 🆕 closed 속성 체크 (더 안전하게)
                closed_state = None
                try:
                    if hasattr(self.websocket, 'closed'):
                        closed_state = self.websocket.closed
                        if closed_state:
                            logger.debug("웹소켓이 닫혀있음")
                            return False
                except AttributeError:
                    logger.debug("웹소켓 객체에 closed 속성이 없음")
                
                # 🆕 open 속성 체크 (추가 확인)
                try:
                    if hasattr(self.websocket, 'open'):
                        open_state = self.websocket.open
                        if not open_state:
                            logger.debug("웹소켓이 열려있지 않음")
                            return False
                except AttributeError:
                    logger.debug("웹소켓 객체에 open 속성이 없음")
                
                # 🆕 state 속성 체크 (더 안전하게)
                try:
                    if hasattr(self.websocket, 'state'):
                        state = self.websocket.state
                        # websockets 라이브러리 State enum 확인
                        try:
                            import websockets.protocol
                            if hasattr(websockets.protocol, 'State'):
                                if state != websockets.protocol.State.OPEN:
                                    logger.debug(f"웹소켓 상태가 OPEN이 아님: {state}")
                                    return False
                        except ImportError:
                            # 정수 값으로 확인 (1 = OPEN)
                            state_value = getattr(state, 'value', state)
                            if state_value != 1:
                                logger.debug(f"웹소켓 상태 값이 1이 아님: {state_value}")
                                return False
                except AttributeError:
                    logger.debug("웹소켓 객체에 state 속성이 없음")
                
                return True
                
            except Exception as state_error:
                logger.debug(f"웹소켓 상태 체크 오류: {state_error}")
                # 상태 체크 실패시 기본 플래그만으로 판단
                return self.is_connected and self.is_running
            
        except Exception as e:
            logger.debug(f"연결 상태 체크 오류: {e}")
            return False

    def is_healthy(self) -> bool:
        """웹소켓 연결 건강성 체크"""
        try:
            # 기본 연결 상태 확인
            if not self._check_actual_connection_status():
                return False
            
            # 최근 메시지 수신 확인 (5분 이내)
            last_message_time = self.stats.get('last_message_time')
            if last_message_time:
                from datetime import datetime, timedelta
                if datetime.now() - last_message_time > timedelta(minutes=5):
                    logger.warning("웹소켓 메시지 수신이 5분 이상 없음")
                    return False
            
            # PINGPONG 응답 확인 (10분 이내)
            last_ping_pong = self.stats.get('last_ping_pong_time')
            if last_ping_pong:
                from datetime import datetime, timedelta
                if datetime.now() - last_ping_pong > timedelta(minutes=10):
                    logger.warning("PINGPONG 응답이 10분 이상 없음")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"건강성 체크 오류: {e}")
            return False

    def get_subscription_count(self) -> int:
        """구독 수 조회"""
        with self.subscription_lock:
            return len(self.subscribed_stocks)

    def has_subscription_capacity(self) -> bool:
        """구독 가능 여부 확인"""
        with self.subscription_lock:
            return len(self.subscribed_stocks) < self.MAX_STOCKS

    def get_websocket_usage(self) -> str:
        """웹소켓 사용량 문자열"""
        with self.subscription_lock:
            return f"{len(self.subscribed_stocks) * 2}/{self.WEBSOCKET_LIMIT}"

    def get_subscribed_stocks(self) -> List[str]:
        """구독 중인 종목 목록"""
        with self.subscription_lock:
            return list(self.subscribed_stocks)

    async def reconnect_and_restore(self) -> bool:
        """재연결 및 구독 복구"""
        try:
            logger.info("웹소켓 재연결 시도 중...")
            self.stats['reconnections'] += 1

            # 기존 연결 정리
            await self.disconnect()
            await asyncio.sleep(2)

            # 재연결
            if not await self.connect():
                logger.error("재연결 실패")
                return False

            # 구독 복구
            stock_codes = list(self.subscribed_stocks)
            callbacks = dict(self.stock_callbacks)

            # 기존 구독 정보 초기화
            self.subscribed_stocks.clear()
            self.stock_callbacks.clear()

            # 구독 재등록
            for stock_code in stock_codes:
                if stock_code in callbacks:
                    for callback in callbacks[stock_code]:
                        success = await self.subscribe_stock(stock_code, callback)
                        if success:
                            logger.info(f"구독 복구 성공: {stock_code}")
                        else:
                            logger.error(f"구독 복구 실패: {stock_code}")
                        await asyncio.sleep(0.1)

            logger.info(f"재연결 완료 - 복구된 구독: {len(self.subscribed_stocks)}개")
            return True

        except Exception as e:
            logger.error(f"재연결 실패: {e}")
            return False

    async def _handle_message(self, message: str):
        """메시지 처리 (호환성 메서드)"""
        try:
            self.stats['messages_received'] += 1
            from datetime import datetime
            self.stats['last_message_time'] = datetime.now()

            # 실시간 데이터인지 확인 (0 또는 1로 시작)
            if message.startswith(('0', '1')):
                await self._handle_realtime_data(message)
            else:
                await self._handle_system_message(message)

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"메시지 처리 오류: {e}")
            logger.error(f"문제 메시지: {message[:200]}...")

    # ========== 편의 메서드들 ==========

    def is_websocket_connected(self) -> bool:
        """웹소켓 연결 상태 확인"""
        return self.is_connected

    def get_subscription_count(self) -> int:
        """구독 수 조회"""
        with self.subscription_lock:
            return len(self.subscribed_stocks)

    def has_subscription_capacity(self) -> bool:
        """구독 가능 여부 확인"""
        with self.subscription_lock:
            return len(self.subscribed_stocks) < self.MAX_STOCKS

    def get_websocket_usage(self) -> str:
        """웹소켓 사용량 문자열"""
        with self.subscription_lock:
            return f"{len(self.subscribed_stocks) * 2}/{self.WEBSOCKET_LIMIT}"

    async def cleanup(self):
        """리소스 정리"""
        try:
            logger.info("🧹 웹소켓 매니저 정리 중...")
            
            self.is_running = False
            
            # 🆕 메시지 핸들러는 백그라운드 루프에서 직접 처리되므로 별도 태스크 취소 불필요
            logger.debug("📨 백그라운드 메시지 처리는 자동으로 is_running=False로 종료됨")
            
            # 웹소켓 연결 해제
            if self.websocket:
                try:
                    await self.websocket.close()
                except Exception as e:
                    logger.debug(f"웹소켓 연결 해제 오류: {e}")
                self.websocket = None
                
            self.is_connected = False
            self.subscribed_stocks.clear()
            
            # 🆕 백그라운드 이벤트 루프 정리
            if hasattr(self, '_event_loop') and self._event_loop and not self._event_loop.is_closed():
                try:
                    # 이벤트 루프의 모든 태스크 취소
                    self._event_loop.call_soon_threadsafe(self._event_loop.stop)
                    
                    # 백그라운드 스레드 종료 대기
                    if hasattr(self, '_websocket_thread') and self._websocket_thread.is_alive():
                        self._websocket_thread.join(timeout=3)
                        if self._websocket_thread.is_alive():
                            logger.warning("⚠️ 백그라운드 웹소켓 스레드 종료 시간 초과")
                        else:
                            logger.debug("✅ 백그라운드 웹소켓 스레드 정상 종료")
                    
                    self._event_loop = None
                except Exception as e:
                    logger.debug(f"백그라운드 이벤트 루프 정리 오류: {e}")
            
            logger.info("✅ 웹소켓 매니저 정리 완료")
        except Exception as e:
            logger.error(f"웹소켓 정리 오류: {e}")

    # ========== 🆕 웹소켓 초기화 및 준비 상태 관리 메서드들 ==========

    def force_ready(self) -> bool:
        """🔧 웹소켓 강제 준비 (이벤트 루프 확실히 시작)"""
        try:
            logger.info("🔧 웹소켓 강제 준비 시작...")
            
            # 1️⃣ 현재 상태 확인
            logger.info(f"🔍 웹소켓 현재 상태: connected={self.is_connected}, running={self.is_running}")
            
            # 2️⃣ 연결되지 않았다면 강제 연결
            if not self.is_connected or not self.is_running:
                logger.info("🔄 웹소켓 강제 재연결 시도...")
                
                # 별도 스레드에서 async 함수 실행
                def force_connect():
                    try:
                        import asyncio
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            # 기존 연결 정리
                            if hasattr(self, 'cleanup'):
                                loop.run_until_complete(self.cleanup())
                            
                            # 새로 연결
                            loop.run_until_complete(self.connect())
                            
                            # 잠시 대기 (이벤트 루프 안정화)
                            loop.run_until_complete(asyncio.sleep(2))
                            
                            logger.info("✅ 웹소켓 강제 재연결 완료")
                        finally:
                            loop.close()
                    except Exception as e:
                        logger.error(f"❌ 웹소켓 강제 재연결 실패: {e}")
                
                # 재연결 실행
                import threading
                connect_thread = threading.Thread(target=force_connect, daemon=True)
                connect_thread.start()
                connect_thread.join(timeout=10)  # 최대 10초 대기
            
            # 3️⃣ 최종 상태 확인 및 대기
            for i in range(20):  # 10초간 0.5초마다 확인
                if self.is_connected and self.is_running:
                    # 이벤트 루프 상태 확인
                    if not getattr(self, '_event_loop_closed', True):
                        logger.info(f"✅ 웹소켓 완전 준비 완료 ({i*0.5:.1f}초 소요)")
                        time.sleep(1)  # 추가 안정화 대기
                        return True
                
                time.sleep(0.5)
            
            logger.warning("⚠️ 웹소켓 강제 준비 시간 초과 - 그래도 시도")
            return False
            
        except Exception as e:
            logger.error(f"❌ 웹소켓 강제 준비 오류: {e}")
            return False

    def ensure_ready_for_subscriptions(self) -> bool:
        """🔧 웹소켓이 구독 준비 상태인지 확인 및 대기 (락 경합 방지 버전)"""
        try:
            logger.info("🔗 웹소켓 구독 준비 상태 확인 중...")
            
            max_wait_time = 3.0  # 🔧 최대 대기 시간 (3초)
            check_interval = 0.3  # 🔧 체크 간격 단축 (0.5초 → 0.3초)
            total_waited = 0.0
            
            while total_waited < max_wait_time:
                # 🔧 락을 사용하지 않는 기본 준비 상태 확인
                if self.is_connected and self.is_running:
                    # 🆕 백그라운드 이벤트 루프 확인
                    if hasattr(self, '_event_loop') and self._event_loop and not self._event_loop.is_closed():
                        # 🆕 웹소켓 객체 상태 확인
                        if self.websocket and self._check_actual_connection_status():
                            # 🔧 추가 안정성 확인: 간단한 헬스체크
                            if self.perform_health_check():
                                logger.info(f"✅ 웹소켓 구독 준비 완료 ({total_waited:.1f}초 소요)")
                                
                                # 🆕 첫 구독을 위한 추가 안정화 대기 (짧게)
                                time.sleep(0.5)  # 🔧 안정화 시간 단축 (1초 → 0.5초)
                                logger.info("🎯 첫 구독을 위한 추가 안정화 완료")
                                return True
                            else:
                                logger.debug(f"⚠️ 웹소켓 헬스체크 실패 - 계속 대기 중...")
                        else:
                            logger.debug(f"⚠️ 웹소켓 연결 상태 불안정 - 계속 대기 중...")
                    else:
                        logger.debug(f"⚠️ 백그라운드 이벤트 루프 미준비 - 계속 대기 중...")
                else:
                    logger.debug(f"⚠️ 웹소켓 기본 상태: connected={self.is_connected}, running={self.is_running}")
                
                time.sleep(check_interval)
                total_waited += check_interval
                
                # 🔧 진행 상황 로깅 간격 조정 (1초마다)
                if total_waited % 1.0 == 0:
                    logger.info(f"🔄 웹소켓 준비 대기 중... ({total_waited:.1f}/{max_wait_time}초)")
                    
                    # 🔧 중간 점검: 연결 상태 재확인
                    if total_waited >= 2.0 and not self.is_connected:
                        logger.warning("🔄 웹소켓 연결이 끊어짐 - 재연결 시도")
                        self.ensure_connection()  # 재연결 시도
            
            # 🆕 대기 시간 초과 시에도 동기 방식 구독은 시도 가능
            logger.warning(f"⚠️ 웹소켓 준비 대기 시간 초과 ({max_wait_time}초)")
            logger.info("📡 동기 방식 구독으로 계속 시도")
            return True  # 동기 방식은 자체적으로 이벤트 루프를 생성하므로 시도 가능
            
        except Exception as e:
            logger.error(f"웹소켓 준비 상태 확인 오류: {e}")
            return True  # 오류 시에도 동기 방식 구독은 시도 가능

    def test_subscription_capability(self) -> bool:
        """🧪 웹소켓 구독 테스트 (이벤트 루프 상태 확인)"""
        try:
            # 간단한 상태 확인
            return (hasattr(self, 'websocket') and 
                   self.websocket is not None and
                   not getattr(self, '_event_loop_closed', True))
            
        except Exception as e:
            logger.debug(f"웹소켓 구독 테스트 오류: {e}")
            return False

    def test_actual_subscription(self) -> bool:
        """🧪 실제 구독 가능 여부 테스트"""
        try:
            # 웹소켓 매니저의 내부 상태 확인
            websocket_obj = getattr(self, 'websocket', None)
            if not websocket_obj:
                logger.debug("웹소켓 객체가 없음")
                return False
            
            # 이벤트 루프 상태 확인
            event_loop_closed = getattr(self, '_event_loop_closed', True)
            if event_loop_closed:
                logger.debug("이벤트 루프가 닫혀있음")
                return False
            
            # 연결 상태 확인
            try:
                import websockets
                if hasattr(websocket_obj, 'state'):
                    state_ok = websocket_obj.state == websockets.protocol.State.OPEN
                    logger.debug(f"웹소켓 상태: {websocket_obj.state}, OK: {state_ok}")
                    return state_ok
            except Exception as e:
                logger.debug(f"웹소켓 상태 확인 오류: {e}")
            
            # 기본적으로 연결되어 있다고 판단
            return True
            
        except Exception as e:
            logger.debug(f"실제 구독 테스트 오류: {e}")
            return False

    def perform_health_check(self) -> bool:
        """🏥 웹소켓 헬스체크 (실제 동작 확인)"""
        try:
            # 1️⃣ 기본 상태 확인
            if not getattr(self, 'is_connected', False):
                return False
            
            # 2️⃣ 이벤트 루프 확인  
            if getattr(self, '_event_loop_closed', True):
                return False
            
            # 3️⃣ 웹소켓 객체 확인
            websocket_obj = getattr(self, 'websocket', None)
            if not websocket_obj:
                return False
            
            # 4️⃣ 연결 상태 확인 (websocket 라이브러리)
            try:
                import websockets
                if hasattr(websocket_obj, 'state'):
                    return websocket_obj.state == websockets.protocol.State.OPEN
            except:
                pass
            
            # 기본적으로 연결되어 있다고 판단
            return True
            
        except Exception as e:
            logger.debug(f"웹소켓 헬스체크 오류: {e}")
            return False

    def cleanup_failed_subscription(self, stock_code: str):
        """🧹 실패한 구독 상태 정리 (async 함수 올바른 처리)"""
        try:
            logger.debug(f"🧹 {stock_code} 실패한 구독 상태 정리 중...")
            
            # 1️⃣ 구독 목록에서 제거
            with self.subscription_lock:
                self.subscribed_stocks.discard(stock_code)
            
            # 2️⃣ 웹소켓에서 구독 해제 (async 함수 올바른 처리)
            try:
                if hasattr(self, 'unsubscribe_stock'):
                    # 🔧 async 함수를 별도 스레드에서 실행
                    def async_unsubscribe():
                        try:
                            import asyncio
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            try:
                                loop.run_until_complete(self.unsubscribe_stock(stock_code))
                                logger.debug(f"🧹 {stock_code} 웹소켓에서 구독 해제 완료")
                            finally:
                                loop.close()
                        except Exception as e:
                            logger.debug(f"웹소켓 구독 해제 오류 ({stock_code}): {e}")
                    
                    # 별도 스레드에서 실행 (메인 스레드 블록 방지)
                    import threading
                    unsubscribe_thread = threading.Thread(target=async_unsubscribe, daemon=True)
                    unsubscribe_thread.start()
                    unsubscribe_thread.join(timeout=3)  # 최대 3초 대기
                    
            except Exception as e:
                logger.debug(f"웹소켓 구독 해제 처리 오류 ({stock_code}): {e}")
            
            # 3️⃣ 잠시 대기 (상태 정리 완료 대기)
            time.sleep(0.2)
            
        except Exception as e:
            logger.debug(f"구독 상태 정리 오류 ({stock_code}): {e}")

    def _is_websocket_connected(self) -> bool:
        """🔧 웹소켓 연결 상태를 안전하게 확인"""
        try:
            if not self.websocket:
                return False
            
            # 🆕 websockets 라이브러리 버전 호환성 처리
            if hasattr(self.websocket, 'closed'):
                return not self.websocket.closed
            elif hasattr(self.websocket, 'close_code'):
                return self.websocket.close_code is None
            elif hasattr(self.websocket, 'state'):
                # websockets v12+ State enum 사용
                from websockets.protocol import State
                return self.websocket.state == State.OPEN
            else:
                # fallback: 기본 속성들로 추정
                return hasattr(self.websocket, 'send') and callable(self.websocket.send)
                
        except Exception as e:
            logger.debug(f"웹소켓 연결 상태 확인 오류: {e}")
            return False
