"""
KIS API 실시간 웹소켓 매니저
공식 문서 예제 기반으로 구현
"""
import asyncio
import json
import threading
import time
import websockets
import requests
from datetime import datetime
from typing import Dict, List, Optional, Callable, Set, Any
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
    BID_ASK = 'H0STASP0'   # 실시간 국내주식 호가
    CONTRACT = 'H0STCNT0'  # 실시간 국내주식 체결
    NOTICE = 'H0STCNI0'    # 실시간 계좌체결발생통보 (실전)
    NOTICE_DEMO = 'H0STCNI9'  # 실시간 계좌체결발생통보 (모의)


class KISWebSocketManager:
    """KIS API 웹소켓 매니저"""

    def __init__(self, is_demo: bool = False):
        self.is_demo = is_demo

        # 웹소켓 제한
        self.WEBSOCKET_LIMIT = 41
        self.MAX_STOCKS = 13  # 체결(13) + 호가(13) = 26건 + 여유분

        # 연결 정보
        self.ws_url = 'ws://ops.koreainvestment.com:31000' if is_demo else 'ws://ops.koreainvestment.com:21000'
        self.approval_key: Optional[str] = None
        self.websocket: Optional[Any] = None

        # 구독 관리
        self.subscribed_stocks: Set[str] = set()
        self.callbacks: Dict[str, Callable] = {}
        self.subscription_lock = threading.Lock()

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
            'subscriptions': 0,
            'errors': 0,
            'reconnections': 0
        }

        logger.info(f"KIS 웹소켓 매니저 초기화 ({'모의투자' if is_demo else '실전투자'})")

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
                ping_interval=60,
                ping_timeout=30,
                close_timeout=10
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
                self.callbacks[stock_code] = callback
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
                self.callbacks.pop(stock_code, None)

                logger.info(f"종목 구독 해제: {stock_code}")
                return True

        except Exception as e:
            logger.error(f"종목 구독 해제 실패: {stock_code} - {e}")
            return False

    async def subscribe_notice(self, hts_id: str) -> bool:
        """체결통보 구독"""
        if not self.is_connected:
            return False

        try:
            notice_type = KIS_WSReq.NOTICE_DEMO if self.is_demo else KIS_WSReq.NOTICE
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

            return {
                'stock_code': parts[0],
                'time': parts[1],
                'current_price': int(parts[2]) if parts[2] else 0,
                'change_sign': parts[3],
                'change': int(parts[4]) if parts[4] else 0,
                'change_rate': float(parts[5]) if parts[5] else 0.0,
                'volume': int(parts[13]) if parts[13] else 0,
                'acc_volume': int(parts[14]) if parts[14] else 0,
                'strength': float(parts[18]) if parts[18] else 0.0
            }

        except Exception as e:
            logger.error(f"체결 데이터 파싱 오류: {e}")
            return {}

    def _parse_bid_ask_data(self, data: str) -> Dict:
        """실시간 호가 데이터 파싱"""
        try:
            parts = data.split('^')
            if len(parts) < 45:
                return {}

            return {
                'stock_code': parts[0],
                'time': parts[1],
                'ask_price1': int(parts[3]) if parts[3] else 0,
                'bid_price1': int(parts[13]) if parts[13] else 0,
                'ask_qty1': int(parts[23]) if parts[23] else 0,
                'bid_qty1': int(parts[33]) if parts[33] else 0,
                'total_ask_qty': int(parts[43]) if parts[43] else 0,
                'total_bid_qty': int(parts[44]) if parts[44] else 0
            }

        except Exception as e:
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
                    callback = self.callbacks.get(stock_code)
                    if callback:
                        callback(stock_code, {
                            'type': 'contract',
                            'current_price': parsed_data['current_price'],
                            'change_rate': parsed_data['change_rate'],
                            'volume': parsed_data['acc_volume'],
                            'strength': parsed_data['strength'],
                            'timestamp': datetime.now(),
                            'source': 'websocket'
                        })

            elif tr_id == KIS_WSReq.BID_ASK.value:
                # 실시간 호가
                parsed_data = self._parse_bid_ask_data(raw_data)
                if parsed_data:
                    stock_code = parsed_data['stock_code']
                    callback = self.callbacks.get(stock_code)
                    if callback:
                        callback(stock_code, {
                            'type': 'bid_ask',
                            'ask_price': parsed_data['ask_price1'],
                            'bid_price': parsed_data['bid_price1'],
                            'ask_qty': parsed_data['ask_qty1'],
                            'bid_qty': parsed_data['bid_qty1'],
                            'timestamp': datetime.now(),
                            'source': 'websocket'
                        })

            elif tr_id in [KIS_WSReq.NOTICE.value, KIS_WSReq.NOTICE_DEMO.value]:
                # 체결통보
                decrypted_data = self._decrypt_notice_data(raw_data)
                if decrypted_data:
                    logger.info(f"체결통보 수신: {decrypted_data}")

        except Exception as e:
            logger.error(f"실시간 데이터 처리 오류: {e}")

    async def _handle_system_message(self, data: str):
        """시스템 메시지 처리"""
        try:
            json_data = json.loads(data)
            tr_id = json_data.get('header', {}).get('tr_id', '')

            if tr_id == "PINGPONG":
                # PING/PONG 응답
                await self.websocket.pong(data.encode())
                logger.debug("PONG 응답 전송")

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
        return {
            'is_connected': self.is_connected,
            'is_running': self.is_running,
            'subscribed_stocks': len(self.subscribed_stocks),
            'max_stocks': self.MAX_STOCKS,
            'websocket_usage': f"{len(self.subscribed_stocks) * 2}/{self.WEBSOCKET_LIMIT}",
            'stats': self.stats.copy()
        }

    def get_subscribed_stocks(self) -> List[str]:
        """구독 중인 종목 목록"""
        return list(self.subscribed_stocks)
