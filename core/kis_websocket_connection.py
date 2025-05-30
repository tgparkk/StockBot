#!/usr/bin/env python3
"""
KIS 웹소켓 연결 관리 전담 클래스
"""
import asyncio
import json
import threading
import time
import websockets
import requests
from typing import Optional, Any, Dict
from utils.logger import setup_logger
from . import kis_auth as kis

logger = setup_logger(__name__)


class KISWebSocketConnection:
    """KIS 웹소켓 연결 관리 전담 클래스"""

    def __init__(self):
        # 연결 정보 (실전투자용 고정)
        self.ws_url = 'ws://ops.koreainvestment.com:21000'
        self.approval_key: Optional[str] = None
        self.websocket: Optional[Any] = None

        # 운영 상태
        self.is_connected = False
        self.is_running = False
        self.connection_attempts = 0
        self.max_reconnect_attempts = 5

        # 백그라운드 이벤트 루프 관리
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._websocket_thread: Optional[threading.Thread] = None

        # 통계
        self.stats = {
            'connection_attempts': 0,
            'successful_connections': 0,
            'disconnections': 0,
            'messages_sent': 0,
            'messages_received': 0,
            'last_ping_time': None,
            'last_pong_time': None
        }

    async def get_approval_key(self) -> Optional[str]:
        """승인키 발급"""
        try:
            logger.debug("🔑 웹소켓 승인키 발급 요청")

            # 기존 승인키가 유효하면 재사용
            if self.approval_key:
                logger.debug("✅ 기존 승인키 재사용")
                return self.approval_key

            # 새로운 승인키 발급
            url = f"{kis.get_base_url()}/oauth2/Approval"
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {kis.get_access_token()}",
                "appkey": kis.get_app_key(),
                "appsecret": kis.get_app_secret(),
                "tr_id": "CTRP6548R",
                "custtype": "P"
            }

            body = {
                "grant_type": "client_credentials",
                "appkey": kis.get_app_key(),
                "secretkey": kis.get_app_secret()
            }

            response = requests.post(url, headers=headers, json=body, timeout=10)

            if response.status_code == 200:
                data = response.json()
                self.approval_key = data.get('approval_key')
                logger.info(f"✅ 웹소켓 승인키 발급 성공: {self.approval_key[:20]}...")
                return self.approval_key
            else:
                logger.error(f"❌ 승인키 발급 실패: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            logger.error(f"❌ 승인키 발급 오류: {e}")
            return None

    async def connect(self) -> bool:
        """웹소켓 연결"""
        try:
            if self.is_connected:
                logger.debug("이미 웹소켓에 연결되어 있습니다")
                return True

            logger.info("🔗 웹소켓 연결 시도...")
            self.stats['connection_attempts'] += 1

            # 승인키 발급
            approval_key = await self.get_approval_key()
            if not approval_key:
                logger.error("❌ 승인키 발급 실패로 웹소켓 연결 불가")
                return False

            # 웹소켓 연결
            self.websocket = await websockets.connect(
                self.ws_url,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=10
            )

            self.is_connected = True
            self.stats['successful_connections'] += 1
            logger.info("✅ 웹소켓 연결 성공")
            return True

        except Exception as e:
            logger.error(f"❌ 웹소켓 연결 실패: {e}")
            self.is_connected = False
            return False

    async def disconnect(self):
        """웹소켓 연결 해제"""
        try:
            logger.info("🔌 웹소켓 연결 해제 중...")

            self.is_connected = False
            self.is_running = False

            if self.websocket:
                await self.websocket.close()
                self.websocket = None
                self.stats['disconnections'] += 1

            logger.info("✅ 웹소켓 연결 해제 완료")

        except Exception as e:
            logger.error(f"❌ 웹소켓 연결 해제 오류: {e}")

    async def send_message(self, message: str) -> bool:
        """메시지 전송"""
        try:
            if not self.is_connected or not self.websocket:
                logger.warning("웹소켓이 연결되지 않음")
                return False

            await self.websocket.send(message)
            self.stats['messages_sent'] += 1
            logger.debug(f"📤 메시지 전송: {message[:100]}...")
            return True

        except Exception as e:
            logger.error(f"❌ 메시지 전송 실패: {e}")
            return False

    async def send_pong(self, ping_data: str) -> bool:
        """PONG 응답 전송 (공식 샘플 방식)"""
        try:
            if not self.websocket:
                logger.warning("웹소켓이 연결되지 않음")
                return False

            # 공식 샘플에 맞춘 pong() 메서드 사용
            await self.websocket.pong(ping_data.encode('utf-8'))
            self.stats['last_pong_time'] = time.time()
            logger.debug(f"🏓 PONG 응답 전송: {ping_data[:50]}...")
            return True

        except Exception as e:
            logger.error(f"❌ PONG 응답 전송 실패: {e}")
            return False

    async def receive_message(self) -> Optional[str]:
        """메시지 수신"""
        try:
            if not self.is_connected or not self.websocket:
                return None

            message = await self.websocket.recv()
            self.stats['messages_received'] += 1
            return message

        except websockets.exceptions.ConnectionClosed:
            logger.warning("⚠️ 웹소켓 연결이 닫혔습니다")
            self.is_connected = False
            return None
        except Exception as e:
            logger.error(f"❌ 메시지 수신 오류: {e}")
            return None

    def check_actual_connection_status(self) -> bool:
        """실제 웹소켓 연결 상태 체크"""
        try:
            if not self.websocket:
                return False

            # 웹소켓 상태 확인
            if hasattr(self.websocket, 'closed'):
                return not self.websocket.closed

            return self.is_connected

        except Exception as e:
            logger.debug(f"연결 상태 확인 오류: {e}")
            return False

    def is_healthy(self) -> bool:
        """웹소켓 연결 건강성 체크"""
        try:
            if not self.is_connected:
                return False

            # 최근 PONG 응답 시간 확인
            if self.stats.get('last_pong_time'):
                time_since_pong = time.time() - self.stats['last_pong_time']
                if time_since_pong > 60:  # 60초 이상 PONG 없으면 비정상
                    return False

            return self.check_actual_connection_status()

        except Exception as e:
            logger.debug(f"건강성 체크 오류: {e}")
            return False

    def get_stats(self) -> Dict:
        """연결 통계 조회"""
        return self.stats.copy()

    def cleanup_failed_subscription(self, stock_code: str):
        """실패한 구독 정리 (호환성 메서드)"""
        logger.debug(f"🧹 실패한 구독 정리: {stock_code}")
        # 실제 정리 로직은 subscription_manager에서 처리

    def ensure_ready_for_subscriptions(self):
        """구독 준비 상태 확인 (호환성 메서드)"""
        logger.debug("📡 구독 준비 상태 확인")
        # 실제 로직은 subscription_manager에서 처리

    def force_ready(self):
        """강제 준비 상태 설정 (호환성 메서드)"""
        logger.debug("🔧 강제 준비 상태 설정")
        # 실제 로직은 다른 컴포넌트에서 처리

    def ensure_connection(self):
        """연결 보장 (호환성 메서드)"""
        logger.debug("🔗 연결 보장 확인")
        # 실제 연결 로직은 별도 스레드에서 처리
