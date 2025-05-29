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
from datetime import datetime
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
            
            # 새 이벤트 루프 생성
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # 연결 시도
                success = loop.run_until_complete(self._ensure_connection_async())
                
                if success:
                    logger.info("✅ 웹소켓 연결 보장 성공")
                else:
                    logger.error("❌ 웹소켓 연결 보장 실패")
                    raise Exception("웹소켓 연결 실패")
                    
            finally:
                loop.close()
                
        except Exception as e:
            logger.error(f"웹소켓 연결 보장 오류: {e}")
            raise

    async def _ensure_connection_async(self) -> bool:
        """웹소켓 연결 보장 (비동기)"""
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"웹소켓 연결 시도 {attempt}/{max_attempts}")
                
                if await self.connect():
                    # 연결 성공 시 간단한 테스트
                    await asyncio.sleep(1)  # 연결 안정화 대기
                    
                    if self.is_connected:
                        logger.info(f"✅ 웹소켓 연결 성공 (시도 {attempt})")
                        
                        # 🆕 is_running 플래그 설정
                        self.is_running = True
                        
                        # 🆕 메시지 처리 루프 자동 시작
                        if not hasattr(self, '_message_handler_task') or self._message_handler_task.done():
                            self._message_handler_task = asyncio.create_task(self._message_handler())
                            logger.info("📨 웹소켓 메시지 처리 루프 시작")
                        
                        return True
                        
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
        """웹소켓 연결 해제"""
        try:
            self.is_running = False

            if self.websocket:
                await self.websocket.close()
                self.websocket = None

            self.is_connected = False
            logger.info("웹소켓 연결 해제 완료")

        except Exception as e:
            logger.error(f"웹소켓 연결 해제 오류: {e}")

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

        if len(self.subscribed_stocks) >= self.MAX_STOCKS:
            logger.warning(f"구독 한계 도달: {len(self.subscribed_stocks)}/{self.MAX_STOCKS}")
            return False

        try:
            with self.subscription_lock:
                # 이미 구독 중인지 확인
                if stock_code in self.subscribed_stocks:
                    logger.warning(f"이미 구독 중인 종목: {stock_code}")
                    return True

                # 실시간 체결 구독
                contract_msg = self._build_message(KIS_WSReq.CONTRACT.value, stock_code)
                await self.websocket.send(contract_msg)
                await asyncio.sleep(0.1)

                # 실시간 호가 구독
                bid_ask_msg = self._build_message(KIS_WSReq.BID_ASK.value, stock_code)
                await self.websocket.send(bid_ask_msg)
                await asyncio.sleep(0.1)

                # 구독 정보 저장
                self.subscribed_stocks.add(stock_code)
                self.add_stock_callback(stock_code, callback)
                self.stats['subscriptions'] += 1

                logger.info(f"종목 구독 성공: {stock_code} ({len(self.subscribed_stocks)}/{self.MAX_STOCKS})")
                return True

        except Exception as e:
            logger.error(f"종목 구독 실패: {stock_code} - {e}")
            return False

    async def unsubscribe_stock(self, stock_code: str) -> bool:
        """종목 구독 해제"""
        if not self.is_connected:
            return False

        try:
            with self.subscription_lock:
                if stock_code not in self.subscribed_stocks:
                    return True

                # 실시간 체결 구독 해제
                contract_msg = self._build_message(KIS_WSReq.CONTRACT.value, stock_code, '2')
                await self.websocket.send(contract_msg)
                await asyncio.sleep(0.1)

                # 실시간 호가 구독 해제
                bid_ask_msg = self._build_message(KIS_WSReq.BID_ASK.value, stock_code, '2')
                await self.websocket.send(bid_ask_msg)
                await asyncio.sleep(0.1)

                # 구독 정보 제거
                self.subscribed_stocks.discard(stock_code)
                self.stock_callbacks.pop(stock_code, None)

                logger.info(f"종목 구독 해제: {stock_code}")
                return True

        except Exception as e:
            logger.error(f"종목 구독 해제 실패: {stock_code} - {e}")
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
                        await self._execute_callbacks(DataType.STOCK_EXECUTION.value, {'data': decrypted_data, 'timestamp': datetime.now()})

        except Exception as e:
            logger.error(f"실시간 데이터 처리 오류: {e}")

    async def _handle_system_message(self, data: str):
        """시스템 메시지 처리"""
        try:
            json_data = json.loads(data)
            tr_id = json_data.get('header', {}).get('tr_id', '')

            if tr_id == "PINGPONG":
                # 🆕 KIS 공식 가이드에 맞춘 PING/PONG 응답
                logger.debug(f"### RECV [PINGPONG] [{data}]")
                
                # 🔧 websockets 라이브러리에 맞는 pong 응답
                try:
                    # websockets 라이브러리는 pong 메서드에 bytes 데이터를 요구할 수 있음
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
                    self.stats['last_ping_pong_time'] = datetime.now()  # 🆕 마지막 PINGPONG 시간 기록
                    
                except Exception as pong_error:
                    logger.error(f"PONG 응답 전송 실패: {pong_error}")
                    # 연결 상태 재확인
                    if not self.websocket or self.websocket.closed:
                        logger.error("웹소켓 연결이 닫혀있음 - 재연결 필요")
                        self.is_connected = False

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

        except Exception as e:
            logger.error(f"시스템 메시지 처리 오류: {e}")
            # PINGPONG 처리 실패 시 더 자세한 로깅
            if "PINGPONG" in data:
                logger.error(f"PINGPONG 처리 실패 - 데이터: {data}")
                logger.error(f"웹소켓 연결 상태: {self.is_connected}")
                logger.error(f"웹소켓 객체: {type(self.websocket)}")

    async def _message_handler(self):
        """메시지 수신 루프"""
        try:
            while self.is_running and self.is_connected:
                try:
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

                except websockets.exceptions.ConnectionClosed:
                    logger.warning("웹소켓 연결이 종료됨")
                    self.is_connected = False
                    break

        except Exception as e:
            logger.error(f"메시지 처리 루프 오류: {e}")
            self.is_connected = False

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
        
        return {
            'is_connected': self.is_connected,
            'is_running': self.is_running,
            'actual_connected': actual_connected,  # 🆕 실제 연결 상태
            'subscribed_stocks': len(self.subscribed_stocks),
            'max_stocks': self.MAX_STOCKS,
            'websocket_usage': f"{len(self.subscribed_stocks) * 2}/{self.WEBSOCKET_LIMIT}",
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
            
            # 3차: websockets 라이브러리 상태 체크
            try:
                # closed 속성 체크 (가장 확실한 방법)
                if hasattr(self.websocket, 'closed') and self.websocket.closed:
                    return False
                
                # open 속성 체크 (추가 확인)
                if hasattr(self.websocket, 'open') and not self.websocket.open:
                    return False
                
                # state 속성 체크 (더 안전하게)
                if hasattr(self.websocket, 'state'):
                    # state가 1이면 OPEN (websockets 라이브러리 기준)
                    if hasattr(self.websocket.state, 'value'):
                        # Enum 타입인 경우
                        if self.websocket.state.value != 1:
                            return False
                    else:
                        # 정수 타입인 경우
                        if self.websocket.state != 1:
                            return False
                
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
                if datetime.now() - last_ping_pong > timedelta(minutes=10):
                    logger.warning("PINGPONG 응답이 10분 이상 없음")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"건강성 체크 오류: {e}")
            return False

    def get_subscribed_stocks(self) -> List[str]:
        """구독 중인 종목 목록"""
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
        return len(self.subscribed_stocks)

    def has_subscription_capacity(self) -> bool:
        """구독 가능 여부 확인"""
        return len(self.subscribed_stocks) < self.MAX_STOCKS

    def get_websocket_usage(self) -> str:
        """웹소켓 사용량 문자열"""
        return f"{len(self.subscribed_stocks) * 2}/{self.WEBSOCKET_LIMIT}"

    async def cleanup(self):
        """정리 작업 (호환성 메서드)"""
        await self.stop()
