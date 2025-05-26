"""
KIS API WebSocket 클라이언트 (순수 연결/통신)
"""
import json
import asyncio
import websockets
import traceback
from datetime import datetime, timedelta
from typing import Dict, Optional, Callable, Any
from utils.logger import setup_logger
from .rest_api_manager import KISRestAPIManager

# 설정 import (settings.py에서 .env 파일을 읽어서 제공)
from config.settings import (
    APP_KEY, SECRET_KEY, HTS_ID
)

logger = setup_logger(__name__)


class KISWebSocketClient:
    """KIS WebSocket 순수 클라이언트"""

    # 웹소켓 엔드포인트
    WS_URL_REAL = "ws://ops.koreainvestment.com:21000"     # 실전투자계좌
    WS_URL_DEMO = "ws://ops.koreainvestment.com:31000"     # 모의투자계좌

    def __init__(self, is_demo: bool = False):
        """초기화"""
        # API 인증 정보 (settings.py에서 가져옴)
        self.app_key = APP_KEY
        self.app_secret = SECRET_KEY
        self.hts_id = HTS_ID
        self.cust_type = 'P'  # 개인:'P', 법인:'B'

        if not all([self.app_key, self.app_secret]):
            raise ValueError("KIS 인증 정보가 설정되지 않았습니다. .env 파일을 확인하세요.")

        # 웹소켓 URL 설정
        self.ws_url = self.WS_URL_DEMO if is_demo else self.WS_URL_REAL

        # REST API 관리자 (접속키 발급용)
        self.rest_api = KISRestAPIManager()
        self.approval_key = None
        self.approval_key_created_at = None

        # 웹소켓 연결
        self.websocket = None
        self.is_connected = False
        self.encryption_keys = {}  # AES 키 저장

        # 메시지 처리 콜백
        self.message_handler: Optional[Callable[[str], Any]] = None

        # 연결 상태
        self.connection_start_time = None
        self.reconnect_count = 0

    async def get_approval_key(self) -> str:
        """웹소켓 접속키 발급"""
        try:
            # 기존 키가 유효한지 확인
            if self._is_approval_key_valid():
                return self.approval_key

            # 새로운 접속키 발급
            url = self.rest_api.base_url + "/oauth2/Approval"
            headers = {"content-type": "application/json"}
            body = {
                "grant_type": "client_credentials",
                "appkey": self.app_key,
                "secretkey": self.app_secret
            }

            response = await self.rest_api._make_request('POST', url, headers=headers, data=json.dumps(body))

            if response and 'approval_key' in response:
                self.approval_key = response['approval_key']
                self.approval_key_created_at = datetime.now()
                logger.info(f"웹소켓 접속키 발급 완료: {self.approval_key[:20]}...")
                return self.approval_key
            else:
                raise Exception("접속키 발급 실패")

        except Exception as e:
            logger.error(f"접속키 발급 오류: {e}")
            raise

    def _is_approval_key_valid(self) -> bool:
        """접속키 유효성 확인 (24시간)"""
        if not self.approval_key or not self.approval_key_created_at:
            return False

        # 23시간 후 갱신 (24시간 만료 전 여유)
        return datetime.now() - self.approval_key_created_at < timedelta(hours=23)

    async def connect(self) -> bool:
        """웹소켓 연결"""
        try:
            # 접속키 확인/발급
            await self.get_approval_key()

            # 웹소켓 연결
            logger.info(f"웹소켓 연결 시도: {self.ws_url}")
            self.websocket = await websockets.connect(self.ws_url, ping_interval=None)

            self.is_connected = True
            self.connection_start_time = datetime.now()
            logger.info("웹소켓 연결 성공")

            return True

        except Exception as e:
            logger.error(f"웹소켓 연결 실패: {e}")
            self.is_connected = False
            self.websocket = None
            return False

    async def disconnect(self):
        """웹소켓 연결 해제"""
        if self.websocket:
            await self.websocket.close()

        self.is_connected = False
        self.websocket = None
        self.encryption_keys.clear()

        logger.info("웹소켓 연결 해제")

    def create_subscribe_message(self, tr_id: str, tr_key: str) -> str:
        """구독 메시지 생성"""
        # 체결통보는 tr_key가 HTS ID
        if tr_id in ['H0STCNI0', 'H0STCNI9']:
            tr_key = self.hts_id

        message = {
            "header": {
                "approval_key": self.approval_key,
                "custtype": self.cust_type,
                "tr_type": "1",  # 등록
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

    def create_unsubscribe_message(self, tr_id: str, tr_key: str) -> str:
        """구독 해제 메시지 생성"""
        # 체결통보는 tr_key가 HTS ID
        if tr_id in ['H0STCNI0', 'H0STCNI9']:
            tr_key = self.hts_id

        message = {
            "header": {
                "approval_key": self.approval_key,
                "custtype": self.cust_type,
                "tr_type": "2",  # 해제
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

    async def subscribe(self, tr_id: str, tr_key: str) -> bool:
        """구독 등록"""
        if not self.is_connected:
            logger.warning("웹소켓이 연결되지 않음")
            return False

        try:
            message = self.create_subscribe_message(tr_id, tr_key)
            await self.websocket.send(message)
            logger.debug(f"구독 등록: {tr_id}|{tr_key}")
            return True

        except Exception as e:
            logger.error(f"구독 등록 실패: {e}")
            return False

    async def unsubscribe(self, tr_id: str, tr_key: str) -> bool:
        """구독 해제"""
        if not self.is_connected:
            return False

        try:
            message = self.create_unsubscribe_message(tr_id, tr_key)
            await self.websocket.send(message)
            logger.debug(f"구독 해제: {tr_id}|{tr_key}")
            return True

        except Exception as e:
            logger.error(f"구독 해제 실패: {e}")
            return False

    def set_message_handler(self, handler: Callable[[str], Any]):
        """메시지 처리 핸들러 설정"""
        self.message_handler = handler

    async def start_listening(self):
        """메시지 수신 시작"""
        if not self.is_connected or not self.websocket:
            logger.error("웹소켓이 연결되지 않음")
            return

        try:
            logger.info("웹소켓 메시지 수신 시작")

            while self.is_connected:
                try:
                    # 메시지 수신 (타임아웃 설정)
                    message = await asyncio.wait_for(
                        self.websocket.recv(),
                        timeout=30.0
                    )

                    # 메시지 처리
                    if self.message_handler:
                        await self._handle_message_safely(message)

                except asyncio.TimeoutError:
                    # 30초 타임아웃 - 정상 상황
                    continue

                except websockets.exceptions.ConnectionClosed as e:
                    logger.warning(f"웹소켓 연결 종료: {e}")
                    break

                except Exception as e:
                    logger.error(f"메시지 수신 오류: {e}")
                    break

        except Exception as e:
            logger.error(f"메시지 수신 중 오류: {e}")

        finally:
            self.is_connected = False
            logger.info("웹소켓 메시지 수신 종료")

    async def _handle_message_safely(self, message: str):
        """안전한 메시지 처리"""
        try:
            await self.message_handler(message)
        except Exception as e:
            logger.error(f"메시지 처리 중 오류: {e}")
            logger.error(f"문제 메시지: {message[:200]}...")

    async def reconnect(self, max_attempts: int = 3) -> bool:
        """웹소켓 재연결"""
        logger.info(f"웹소켓 재연결 시도 (최대 {max_attempts}회)")

        for attempt in range(1, max_attempts + 1):
            try:
                # 기존 연결 정리
                await self.disconnect()

                # 재연결 대기
                await asyncio.sleep(min(attempt * 2, 10))

                # 연결 시도
                if await self.connect():
                    self.reconnect_count += 1
                    logger.info(f"웹소켓 재연결 성공 (시도 {attempt}/{max_attempts})")
                    return True

            except Exception as e:
                logger.error(f"재연결 시도 {attempt} 실패: {e}")

        logger.error("웹소켓 재연결 실패")
        return False

    def get_status(self) -> Dict:
        """연결 상태 정보"""
        uptime = None
        if self.connection_start_time:
            uptime = (datetime.now() - self.connection_start_time).total_seconds()

        return {
            'is_connected': self.is_connected,
            'ws_url': self.ws_url,
            'uptime_seconds': uptime,
            'reconnect_count': self.reconnect_count,
            'approval_key_valid': self._is_approval_key_valid(),
            'encryption_keys_count': len(self.encryption_keys)
        }
