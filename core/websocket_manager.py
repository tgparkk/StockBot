"""
KIS WebSocket 통합 관리자 (리팩토링 버전)
공식 문서 기반 + 41건 제한 최적화
"""
import json
import asyncio
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime
from utils.logger import setup_logger
from .kis_websocket_client import KISWebSocketClient
from .kis_subscription_manager import KISSubscriptionManager, Subscription
from .kis_data_parsers import (
    parse_stock_price_data,
    parse_stock_orderbook_data,
    parse_stock_execution_data,
    parse_market_index_data
)

logger = setup_logger(__name__)


class KISWebSocketManager:
    """통합 WebSocket 관리자"""

    def __init__(self, is_demo: bool = False):
        """초기화"""
        # 클라이언트 및 관리자
        self.client = KISWebSocketClient(is_demo)
        self.subscription_manager = KISSubscriptionManager()

        # 메시지 처리 핸들러 설정
        # self.client.set_message_handler(self._handle_message)  # 비동기 호환성 문제로 주석 처리

        # 콜백 함수들
        self.callbacks = {
            'stock_price': [],      # 주식체결가 (H0STCNT0)
            'stock_orderbook': [],  # 주식호가 (H0STASP0)
            'stock_execution': [],  # 주식체결통보 (H0STCNI0/H0STCNI9)
            'market_index': []      # 시장지수 (H0UPCNT0)
        }

        # 암호화 키 저장 (체결통보용)
        self.encryption_keys = {}

        # 통계
        self.stats = {
            'messages_received': 0,
            'data_processed': 0,
            'errors': 0,
            'last_message_time': None
        }

    async def connect(self) -> bool:
        """웹소켓 연결"""
        return await self.client.connect()

    async def disconnect(self):
        """웹소켓 연결 해제"""
        await self.client.disconnect()

    def add_callback(self, data_type: str, callback: Callable[[Dict], None]):
        """콜백 함수 추가"""
        if data_type in self.callbacks:
            self.callbacks[data_type].append(callback)
            logger.debug(f"콜백 추가: {data_type}")

    def remove_callback(self, data_type: str, callback: Callable[[Dict], None]):
        """콜백 함수 제거"""
        if data_type in self.callbacks and callback in self.callbacks[data_type]:
            self.callbacks[data_type].remove(callback)
            logger.debug(f"콜백 제거: {data_type}")

    def subscribe_stock_price(self, stock_code: str, strategy_name: str = "") -> bool:
        """주식체결가 구독"""
        return self.subscription_manager.add_subscription(
            tr_id="H0STCNT0",
            tr_key=stock_code,
            strategy_name=strategy_name
        )

    def subscribe_stock_orderbook(self, stock_code: str, strategy_name: str = "") -> bool:
        """주식호가 구독"""
        return self.subscription_manager.add_subscription(
            tr_id="H0STASP0",
            tr_key=stock_code,
            strategy_name=strategy_name
        )

    def subscribe_stock_execution(self, strategy_name: str = "") -> bool:
        """주식체결통보 구독"""
        return self.subscription_manager.add_subscription(
            tr_id="H0STCNI9",  # 모의투자용
            tr_key="",  # HTS ID 사용
            strategy_name=strategy_name
        )

    def subscribe_market_index(self, index_code: str, strategy_name: str = "") -> bool:
        """시장지수 구독"""
        return self.subscription_manager.add_subscription(
            tr_id="H0UPCNT0",
            tr_key=index_code,
            strategy_name=strategy_name
        )

    def unsubscribe(self, tr_id: str, tr_key: str) -> bool:
        """구독 해제"""
        subscription_key = f"{tr_id}|{tr_key}"
        return self.subscription_manager.remove_subscription(subscription_key)

    def unsubscribe_strategy(self, strategy_name: str) -> int:
        """전략별 구독 해제"""
        return self.subscription_manager.remove_by_strategy(strategy_name)

    async def apply_subscriptions(self):
        """대기 중인 구독 적용"""
        # 추가할 구독들
        for subscription in self.subscription_manager.get_pending_additions():
            success = await self.client.subscribe(subscription.tr_id, subscription.tr_key)
            if success:
                self.subscription_manager.confirm_addition(subscription.subscription_key)
                await asyncio.sleep(0.1)  # API 호출 간격

        # 제거할 구독들
        for subscription_key in self.subscription_manager.get_pending_removals():
            tr_id, tr_key = subscription_key.split('|', 1)
            success = await self.client.unsubscribe(tr_id, tr_key)
            if success:
                self.subscription_manager.confirm_removal(subscription_key)
                await asyncio.sleep(0.1)  # API 호출 간격

    async def start_listening(self):
        """메시지 수신 시작"""
        await self.client.start_listening()

    async def _handle_message(self, message: str):
        """메시지 처리"""
        try:
            self.stats['messages_received'] += 1
            self.stats['last_message_time'] = datetime.now()

            # 실시간 데이터인지 확인 (0 또는 1로 시작)
            if message.startswith(('0', '1')):
                await self._process_realtime_data(message)
            else:
                await self._process_response_data(message)

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"메시지 처리 오류: {e}")
            logger.error(f"문제 메시지: {message[:200]}...")

    async def _process_realtime_data(self, message: str):
        """실시간 데이터 처리"""
        try:
            parts = message.split('|')
            if len(parts) < 4:
                return

            tr_id = parts[1]
            data = parts[3]

            # TR ID별 데이터 처리
            if tr_id == "H0STCNT0":  # 주식체결가
                parsed_data = parse_stock_price_data(data)
                await self._execute_callbacks('stock_price', parsed_data)

            elif tr_id == "H0STASP0":  # 주식호가
                parsed_data = parse_stock_orderbook_data(data)
                await self._execute_callbacks('stock_orderbook', parsed_data)

            elif tr_id in ["H0STCNI0", "H0STCNI9"]:  # 주식체결통보
                if tr_id in self.encryption_keys:
                    aes_key = self.encryption_keys[tr_id]['key']
                    aes_iv = self.encryption_keys[tr_id]['iv']
                    parsed_data = parse_stock_execution_data(data, aes_key, aes_iv)
                    if parsed_data:
                        await self._execute_callbacks('stock_execution', parsed_data)

            elif tr_id == "H0UPCNT0":  # 시장지수
                parsed_data = parse_market_index_data(data)
                await self._execute_callbacks('market_index', parsed_data)

            self.stats['data_processed'] += 1

        except Exception as e:
            logger.error(f"실시간 데이터 처리 오류: {e}")

    async def _process_response_data(self, message: str):
        """응답 데이터 처리 (JSON)"""
        try:
            data = json.loads(message)
            tr_id = data.get('header', {}).get('tr_id')

            if tr_id == "PINGPONG":
                # PING 응답
                await self.client.websocket.pong(message.encode())
                logger.debug("PINGPONG 응답 전송")

            elif tr_id in ["H0STCNI0", "H0STCNI9"]:
                # 체결통보 암호화 키 저장
                rt_cd = data.get('body', {}).get('rt_cd')
                if rt_cd == '0':  # 성공
                    output = data.get('body', {}).get('output', {})
                    if 'key' in output and 'iv' in output:
                        self.encryption_keys[tr_id] = {
                            'key': output['key'],
                            'iv': output['iv']
                        }
                        logger.info(f"체결통보 암호화 키 저장: {tr_id}")

            else:
                # 일반 응답 처리
                rt_cd = data.get('body', {}).get('rt_cd')
                msg = data.get('body', {}).get('msg1', '')

                if rt_cd == '0':
                    logger.debug(f"구독 성공: {tr_id} - {msg}")
                else:
                    logger.warning(f"구독 오류: {tr_id} - {msg}")

        except Exception as e:
            logger.error(f"응답 데이터 처리 오류: {e}")

    async def _execute_callbacks(self, data_type: str, data: Dict):
        """콜백 함수 실행"""
        if data_type not in self.callbacks:
            return

        for callback in self.callbacks[data_type]:
            try:
                # 비동기 콜백인지 확인
                if asyncio.iscoroutinefunction(callback):
                    await callback(data)
                else:
                    callback(data)
            except Exception as e:
                logger.error(f"콜백 실행 오류 ({data_type}): {e}")

    def get_status(self) -> Dict:
        """상태 정보"""
        client_status = self.client.get_status()
        subscription_status = self.subscription_manager.get_subscription_status()

        return {
            'client': client_status,
            'subscriptions': subscription_status,
            'stats': self.stats.copy(),
            'encryption_keys': list(self.encryption_keys.keys())
        }

    async def reconnect_and_restore(self) -> bool:
        """재연결 및 구독 복원"""
        logger.info("웹소켓 재연결 및 구독 복원 시작")

        # 재연결
        if not await self.client.reconnect():
            return False

        # 기존 구독 복원
        subscriptions_by_type = self.subscription_manager.get_subscriptions_by_type()

        for tr_id, tr_keys in subscriptions_by_type.items():
            for tr_key in tr_keys:
                await self.client.subscribe(tr_id, tr_key)
                await asyncio.sleep(0.1)  # API 호출 간격

        logger.info(f"구독 복원 완료: {sum(len(keys) for keys in subscriptions_by_type.values())}건")
        return True
