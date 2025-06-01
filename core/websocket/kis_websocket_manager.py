"""
KIS 웹소켓 매니저 (리팩토링 버전 - Facade 패턴)
"""
import asyncio
import threading
import time
from typing import Dict, List, Optional, Callable, Any
from utils.logger import setup_logger

# 분리된 컴포넌트들
from .kis_websocket_connection import KISWebSocketConnection
from .kis_websocket_data_parser import KISWebSocketDataParser
from .kis_websocket_subscription_manager import KISWebSocketSubscriptionManager
from .kis_websocket_message_handler import KISWebSocketMessageHandler, KIS_WSReq

logger = setup_logger(__name__)


class KISWebSocketManager:
    """
    KIS 웹소켓 매니저 (리팩토링 버전 - Facade 패턴)

    기존 인터페이스를 100% 유지하면서 내부적으로는 분리된 컴포넌트들을 사용
    """

    def __init__(self):
        """초기화"""
        # 분리된 컴포넌트들 초기화
        self.connection = KISWebSocketConnection()
        self.data_parser = KISWebSocketDataParser()
        self.subscription_manager = KISWebSocketSubscriptionManager()
        self.message_handler = KISWebSocketMessageHandler(
            self.data_parser,
            self.subscription_manager
        )

        # 백그라운드 작업 관리
        self._message_loop_task: Optional[asyncio.Task] = None
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._websocket_thread: Optional[threading.Thread] = None
        self._event_loop_closed = True
        self._shutdown_event = threading.Event()

        # 통계
        self.stats = {
            'start_time': time.time(),
            'total_messages': 0,
            'connection_count': 0
        }

        logger.info("KIS 웹소켓 매니저 초기화 완료 (리팩토링 버전)")

    # ==========================================
    # 기존 인터페이스 호환성 유지 (Property)
    # ==========================================

    @property
    def is_connected(self) -> bool:
        """연결 상태 (기존 인터페이스 유지)"""
        return self.connection.is_connected

    @property
    def is_running(self) -> bool:
        """실행 상태 (기존 인터페이스 유지)"""
        return self.connection.is_running

    @property
    def websocket(self):
        """웹소켓 객체 (기존 인터페이스 유지)"""
        return self.connection.websocket

    @property
    def subscribed_stocks(self) -> set:
        """구독 중인 종목 목록 (기존 인터페이스 유지)"""
        return set(self.subscription_manager.get_subscribed_stocks())

    # ==========================================
    # 연결 관리 메서드들 (기존 인터페이스 유지)
    # ==========================================

    async def connect_websocket(self) -> bool:
        """웹소켓 연결 (기존 인터페이스)"""
        try:
            logger.info("웹소켓 연결 시작...")
            success = await self.connection.connect()
            if success:
                self.stats['connection_count'] += 1
                logger.info("✅ 웹소켓 연결 성공")
            return success
        except Exception as e:
            logger.error(f"웹소켓 연결 실패: {e}")
            return False

    async def disconnect_websocket(self):
        """웹소켓 연결 해제 (기존 인터페이스)"""
        await self.connection.disconnect()

    def connect(self) -> bool:
        """웹소켓 연결 (동기 방식 - 기존 인터페이스 호환)"""
        try:
            logger.debug("🔄 동기 방식 웹소켓 연결 시도")

            # 🆕 이미 연결되어 있으면 성공 반환
            if self.connection.is_connected and self.connection.check_actual_connection_status():
                logger.debug("✅ 이미 연결된 상태")
                return True

            # 🆕 웹소켓 스레드가 실행 중이면 연결 대기
            if self._websocket_thread and self._websocket_thread.is_alive():
                logger.debug("웹소켓 스레드 실행 중 - 연결 대기")
                max_wait = 10
                wait_count = 0
                while wait_count < max_wait:
                    if self.connection.is_connected and self.connection.check_actual_connection_status():
                        logger.debug(f"✅ 웹소켓 연결 확인됨 ({wait_count}초 대기)")
                        return True
                    time.sleep(1)
                    wait_count += 1

                logger.warning("⚠️ 웹소켓 연결 대기 시간 초과")
                return False

            # 🆕 스레드가 없으면 메시지 루프 시작
            logger.debug("웹소켓 스레드 시작")
            self.start_message_loop()

            # 연결 완료 대기
            max_wait = 15
            wait_count = 0
            while wait_count < max_wait:
                if self.connection.is_connected and self.connection.check_actual_connection_status():
                    logger.debug(f"✅ 웹소켓 연결 성공 ({wait_count}초 대기)")
                    self.stats['connection_count'] += 1
                    return True
                time.sleep(1)
                wait_count += 1

            logger.error("❌ 웹소켓 연결 시간 초과")
            return False

        except Exception as e:
            logger.error(f"동기 연결 오류: {e}")
            return False

    def start_message_loop(self):
        """메시지 루프 시작 (기존 인터페이스)"""
        try:
            if self._websocket_thread and self._websocket_thread.is_alive():
                logger.warning("메시지 루프가 이미 실행 중입니다")
                return

            logger.info("웹소켓 백그라운드 스레드 시작...")
            self._shutdown_event.clear()
            self._websocket_thread = threading.Thread(
                target=self._run_websocket_thread,
                name="WebSocketThread",
                daemon=True
            )
            self._websocket_thread.start()
            logger.info("✅ 웹소켓 백그라운드 스레드 시작 완료")

        except Exception as e:
            logger.error(f"메시지 루프 시작 실패: {e}")

    def start(self):
        """웹소켓 시작 (기존 인터페이스 호환 - start_message_loop 별칭)"""
        self.start_message_loop()

    def _run_websocket_thread(self):
        """웹소켓 스레드 실행"""
        try:
            # 🆕 기존 이벤트 루프 완전 정리
            if hasattr(self, '_event_loop') and self._event_loop:
                try:
                    if not self._event_loop.is_closed():
                        # 기존 루프의 모든 작업 취소
                        pending_tasks = asyncio.all_tasks(self._event_loop)
                        for task in pending_tasks:
                            task.cancel()

                        # 루프 정리
                        if self._event_loop.is_running():
                            self._event_loop.call_soon_threadsafe(self._event_loop.stop)
                        self._event_loop.close()
                except Exception as e:
                    logger.debug(f"기존 이벤트 루프 정리 중 오류: {e}")

            # 🆕 완전히 새로운 이벤트 루프 생성
            self._event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._event_loop)
            self._event_loop_closed = False

            # 🆕 안전한 연결 및 메시지 루프 실행
            try:
                self._event_loop.run_until_complete(self._websocket_main_loop())
            except asyncio.CancelledError:
                logger.info("웹소켓 스레드가 정상적으로 취소되었습니다")
            except Exception as e:
                logger.error(f"웹소켓 메인 루프 실행 오류: {e}")

        except Exception as e:
            logger.error(f"웹소켓 스레드 오류: {e}")
        finally:
            # 🆕 확실한 정리
            try:
                if hasattr(self, '_event_loop') and self._event_loop:
                    # 모든 미완료 작업 취소
                    pending_tasks = asyncio.all_tasks(self._event_loop)
                    if pending_tasks:
                        logger.debug(f"미완료 작업 {len(pending_tasks)}개 취소 중...")
                        for task in pending_tasks:
                            task.cancel()

                        # 취소된 작업들 완료 대기
                        try:
                            self._event_loop.run_until_complete(
                                asyncio.gather(*pending_tasks, return_exceptions=True)
                            )
                        except Exception as e:
                            logger.debug(f"작업 취소 중 오류: {e}")

                    # 루프 종료
                    if not self._event_loop.is_closed():
                        self._event_loop.close()
                    self._event_loop_closed = True

            except Exception as e:
                logger.error(f"이벤트 루프 정리 오류: {e}")

    async def _websocket_main_loop(self):
        """🆕 개선된 웹소켓 메인 루프 - 이벤트 루프 안전성 강화"""
        try:
            # 연결
            if not await self.connection.connect():
                logger.error("초기 웹소켓 연결 실패")
                return

            self.connection.is_running = True
            logger.info("✅ 웹소켓 메인 루프 시작")

            # 🆕 계좌 체결통보 구독 (프로그램 시작시 자동)
            await self._subscribe_account_notices()

            # 🆕 메시지 루프 - 안전한 예외 처리
            consecutive_errors = 0
            max_consecutive_errors = 5

            while self.connection.is_running and not self._shutdown_event.is_set():
                try:
                    # 🆕 현재 루프 상태 확인
                    current_loop = asyncio.get_running_loop()
                    if current_loop.is_closed():
                        logger.warning("이벤트 루프가 닫혔습니다 - 메인 루프 종료")
                        break

                    # 연결 상태 확인
                    if not self.connection.check_actual_connection_status():
                        logger.warning("웹소켓 연결 끊어짐 - 재연결 시도")

                        # 🆕 안전한 재연결
                        if not await self._safe_reconnect():
                            await asyncio.sleep(5)
                            continue

                    # 🆕 안전한 메시지 수신
                    try:
                        message = await asyncio.wait_for(
                            self.connection.receive_message(),
                            timeout=30
                        )

                        if message:
                            self.stats['total_messages'] += 1
                            consecutive_errors = 0  # 성공시 오류 카운터 리셋

                            # 메시지 처리
                            result = await self.message_handler.process_message(message)

                            # PINGPONG 응답 처리
                            if result and result[0] == 'PINGPONG':
                                await self.connection.send_pong(result[1])

                    except asyncio.TimeoutError:
                        logger.debug("메시지 수신 타임아웃 (정상)")
                        continue
                    except asyncio.CancelledError:
                        logger.info("메시지 수신이 취소되었습니다")
                        break
                    except Exception as recv_error:
                        consecutive_errors += 1
                        logger.error(f"메시지 수신 오류 (연속 {consecutive_errors}회): {recv_error}")

                        # 🆕 연속 오류가 많으면 재연결
                        if consecutive_errors >= max_consecutive_errors:
                            logger.error(f"연속 오류 {max_consecutive_errors}회 발생 - 재연결 시도")
                            if not await self._safe_reconnect():
                                logger.error("재연결 실패 - 메인 루프 종료")
                                break
                            consecutive_errors = 0
                        else:
                            await asyncio.sleep(1)

                except asyncio.CancelledError:
                    logger.info("웹소켓 메인 루프가 취소되었습니다")
                    break
                except Exception as e:
                    consecutive_errors += 1
                    logger.error(f"메시지 루프 오류 (연속 {consecutive_errors}회): {e}")

                    if consecutive_errors >= max_consecutive_errors:
                        logger.error("치명적 오류 발생 - 메인 루프 종료")
                        break

                    await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"웹소켓 메인 루프 치명적 오류: {e}")
        finally:
            # 안전한 연결 해제
            try:
                # 연결 정리
                try:
                    # 동기식이므로 await 사용 불가
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(self.connection.safe_disconnect())
                    finally:
                        loop.close()
                except Exception as e:
                    logger.debug(f"연결 정리 중 오류: {e}")
            except Exception as e:
                logger.debug(f"연결 해제 중 오류: {e}")

            logger.info("🛑 웹소켓 메인 루프 종료")

    async def _safe_reconnect(self) -> bool:
        """🆕 안전한 재연결 메서드"""
        try:
            logger.info("🔄 안전한 웹소켓 재연결 시도...")

            # 기존 연결 정리
            try:
                await self.connection.disconnect()
                await asyncio.sleep(1)  # 정리 대기
            except Exception as e:
                logger.debug(f"기존 연결 정리 중 오류: {e}")

            # 새로 연결
            success = await self.connection.connect()
            if success:
                logger.info("✅ 웹소켓 재연결 성공")
                return True
            else:
                logger.error("❌ 웹소켓 재연결 실패")
                return False

        except Exception as e:
            logger.error(f"재연결 과정 중 오류: {e}")
            return False

    # ==========================================
    # 구독 관리 메서드들 (기존 인터페이스 유지)
    # ==========================================

    async def subscribe_stock(self, stock_code: str, callback: Optional[Callable] = None) -> bool:
        """종목 구독 (기존 인터페이스)"""
        try:
            # 구독 가능 여부 확인
            if not self.subscription_manager.can_subscribe(stock_code):
                logger.warning(f"구독 불가: {stock_code} (한계 도달)")
                return False

            # 체결가 구독
            contract_msg = self.connection.build_message(
                KIS_WSReq.CONTRACT.value, stock_code, '1'
            )
            await self.connection.send_message(contract_msg)

            # 호가 구독
            bid_ask_msg = self.connection.build_message(
                KIS_WSReq.BID_ASK.value, stock_code, '1'
            )
            await self.connection.send_message(bid_ask_msg)

            # 구독 등록
            self.subscription_manager.add_subscription(stock_code)

            # 콜백 등록
            if callback:
                self.subscription_manager.add_stock_callback(stock_code, callback)

            logger.info(f"✅ 종목 구독 성공: {stock_code}")
            return True

        except Exception as e:
            logger.error(f"종목 구독 실패 ({stock_code}): {e}")
            return False

    def subscribe_stock_sync(self, stock_code: str, callback: Optional[Callable] = None) -> bool:
        """종목 구독 (동기 방식 - 기존 인터페이스 호환)"""
        try:
            logger.debug(f"🔄 동기 방식 종목 구독 시도: {stock_code}")

            # 웹소켓 연결 상태 확인
            if not self.connection.is_connected:
                logger.error(f"웹소켓 연결 상태 불량: connected={self.connection.is_connected}")
                return False

            # 구독 가능 여부 확인
            if not self.subscription_manager.can_subscribe(stock_code):
                logger.warning(f"구독 한계 도달: {stock_code}")
                return False

            # 🆕 이미 구독된 종목인지 확인
            if self.subscription_manager.is_subscribed(stock_code):
                logger.debug(f"이미 구독된 종목: {stock_code}")
                if callback:
                    self.subscription_manager.add_stock_callback(stock_code, callback)
                return True

            # 🔧 개선된 이벤트 루프 안전성 확인
            event_loop_available = False
            loop_error_message = ""

            try:
                # 웹소켓 스레드의 이벤트 루프 상태 확인
                if hasattr(self, '_event_loop') and self._event_loop:
                    if not self._event_loop.is_closed() and not self._event_loop.is_running():
                        # 루프가 있지만 실행 중이 아닌 경우
                        logger.debug("이벤트 루프가 중지된 상태 - 재시작 필요")
                        loop_error_message = "루프 중지됨"
                    elif self._event_loop.is_closed():
                        # 루프가 닫힌 경우
                        logger.debug("이벤트 루프가 닫힌 상태")
                        loop_error_message = "루프 닫힘"
                    else:
                        # 정상 상태
                        event_loop_available = True
                else:
                    loop_error_message = "루프 없음"

            except Exception as e:
                loop_error_message = f"루프 상태 확인 오류: {e}"

            # 🔧 이벤트 루프 사용 가능한 경우
            if event_loop_available and self._event_loop:
                try:
                    # 이벤트 루프에 비동기 작업 예약
                    future = asyncio.run_coroutine_threadsafe(
                        self.subscribe_stock(stock_code, callback),
                        self._event_loop
                    )

                    # 결과 대기 (최대 10초)
                    success = future.result(timeout=10)

                    if success:
                        logger.debug(f"✅ 동기 방식 구독 성공: {stock_code}")
                    else:
                        logger.debug(f"❌ 동기 방식 구독 실패: {stock_code}")

                    return success

                except Exception as e:
                    logger.error(f"이벤트 루프 구독 오류 ({stock_code}): {e}")
                    event_loop_available = False  # 백업 방식으로 전환

            # 🆕 백업 방식: 이벤트 루프 없이도 구독 시도
            if not event_loop_available:
                logger.warning(f"이벤트 루프 사용 불가능 ({loop_error_message}) - 백업 방식 사용: {stock_code}")

                try:
                    # 웹소켓 직접 메시지 전송 시도 (동기)
                    if hasattr(self.connection, 'websocket') and self.connection.websocket:
                        # 체결가 구독 메시지 빌드
                        contract_msg = self.connection.build_message(
                            KIS_WSReq.CONTRACT.value, stock_code, '1'
                        )

                        # 호가 구독 메시지 빌드
                        bid_ask_msg = self.connection.build_message(
                            KIS_WSReq.BID_ASK.value, stock_code, '1'
                        )

                        # 메시지 전송은 웹소켓 연결이 살아있을 때만 가능
                        # 여기서는 일단 구독 매니저에만 등록
                        self.subscription_manager.add_subscription(stock_code)

                        if callback:
                            self.subscription_manager.add_stock_callback(stock_code, callback)

                        logger.info(f"⚠️ 백업 방식 구독 등록: {stock_code} (실제 구독은 이벤트 루프 복구 후)")
                        return True

                except Exception as e:
                    logger.error(f"백업 구독 실패 ({stock_code}): {e}")

            return False

        except Exception as e:
            logger.error(f"동기 구독 오류 ({stock_code}): {e}")
            # 오류 시 구독 목록에서 제거
            self.subscription_manager.remove_subscription(stock_code)
            return False

    async def unsubscribe_stock(self, stock_code: str) -> bool:
        """종목 구독 해제 (기존 인터페이스)"""
        try:
            # 체결가 구독 해제
            contract_msg = self.connection.build_message(
                KIS_WSReq.CONTRACT.value, stock_code, '2'
            )
            await self.connection.send_message(contract_msg)

            # 호가 구독 해제
            bid_ask_msg = self.connection.build_message(
                KIS_WSReq.BID_ASK.value, stock_code, '2'
            )
            await self.connection.send_message(bid_ask_msg)

            # 구독 제거
            self.subscription_manager.remove_subscription(stock_code)

            logger.info(f"✅ 종목 구독 해제: {stock_code}")
            return True

        except Exception as e:
            logger.error(f"종목 구독 해제 실패 ({stock_code}): {e}")
            return False

    def add_stock_callback(self, stock_code: str, callback: Callable):
        """종목별 콜백 추가 (기존 인터페이스)"""
        self.subscription_manager.add_stock_callback(stock_code, callback)

    def remove_stock_callback(self, stock_code: str, callback: Callable):
        """종목별 콜백 제거 (기존 인터페이스)"""
        self.subscription_manager.remove_stock_callback(stock_code, callback)

    def add_global_callback(self, data_type: str, callback: Callable):
        """글로벌 콜백 추가 (기존 인터페이스)"""
        self.subscription_manager.add_global_callback(data_type, callback)

    def remove_global_callback(self, data_type: str, callback: Callable):
        """글로벌 콜백 제거 (기존 인터페이스)"""
        self.subscription_manager.remove_global_callback(data_type, callback)

    # ==========================================
    # 상태 조회 메서드들 (기존 인터페이스 유지)
    # ==========================================

    def get_subscribed_stocks(self) -> List[str]:
        """구독 중인 종목 목록 (기존 인터페이스)"""
        return self.subscription_manager.get_subscribed_stocks()

    def get_subscription_count(self) -> int:
        """구독 수 조회 (기존 인터페이스)"""
        return self.subscription_manager.get_subscription_count()

    def has_subscription_capacity(self) -> bool:
        """구독 가능 여부 (기존 인터페이스)"""
        return self.subscription_manager.has_subscription_capacity()

    def get_websocket_usage(self) -> str:
        """웹소켓 사용량 (기존 인터페이스)"""
        return self.subscription_manager.get_websocket_usage()

    def is_subscribed(self, stock_code: str) -> bool:
        """구독 여부 확인 (기존 인터페이스)"""
        return self.subscription_manager.is_subscribed(stock_code)

    def _check_actual_connection_status(self) -> bool:
        """실제 웹소켓 연결 상태 체크 (기존 인터페이스 호환)"""
        return self.connection.check_actual_connection_status()

    def is_healthy(self) -> bool:
        """웹소켓 연결 건강성 체크 (기존 인터페이스 호환)"""
        return self.connection.is_healthy()

    def get_status(self) -> Dict:
        """전체 상태 조회 (기존 인터페이스)"""
        connection_status = self.connection.get_status()
        subscription_status = self.subscription_manager.get_status()
        handler_stats = self.message_handler.get_stats()
        parser_stats = self.data_parser.get_stats()

        return {
            'connection': connection_status,
            'subscriptions': subscription_status,
            'message_handler': handler_stats,
            'data_parser': parser_stats,
            'total_stats': self.stats.copy(),
            'uptime': time.time() - self.stats['start_time']
        }

    # ==========================================
    # 유틸리티 메서드들 (기존 인터페이스 유지)
    # ==========================================

    def ensure_ready_for_subscriptions(self):
        """구독 준비 상태 확인 (기존 인터페이스)"""
        max_wait = 10  # 10초 대기
        wait_count = 0

        while wait_count < max_wait:
            if self.connection.check_actual_connection_status():
                logger.debug(f"✅ 웹소켓 구독 준비 완료 ({wait_count}초 대기)")
                return True

            time.sleep(1)
            wait_count += 1

        logger.warning(f"⚠️ 웹소켓 구독 준비 실패 ({max_wait}초 대기)")
        return False

    def cleanup_failed_subscription(self, stock_code: str):
        """실패한 구독 정리 (기존 인터페이스)"""
        self.subscription_manager.remove_subscription(stock_code)

    def force_ready(self):
        """강제 준비 상태 (기존 인터페이스)"""
        if not self.connection.is_connected:
            logger.info("웹소켓 강제 연결 시도...")
            self.ensure_connection()

    def ensure_connection(self):
        """연결 보장 (기존 인터페이스)"""
        if not self.connection.is_connected and not self._websocket_thread:
            self.start_message_loop()
            time.sleep(2)  # 연결 대기

    # ==========================================
    # 정리 및 종료
    # ==========================================

    async def cleanup(self):
        """정리 작업 (기존 인터페이스)"""
        try:
            logger.info("웹소켓 매니저 정리 시작...")

            # 종료 신호 설정
            self._shutdown_event.set()

            # 웹소켓 연결 해제
            await self.connection.disconnect()

            # 구독 정리
            self.subscription_manager.clear_all_subscriptions()

            # 스레드 종료 대기
            if self._websocket_thread and self._websocket_thread.is_alive():
                self._websocket_thread.join(timeout=5)

            logger.info("✅ 웹소켓 매니저 정리 완료")

        except Exception as e:
            logger.error(f"웹소켓 매니저 정리 오류: {e}")

    def safe_cleanup(self):
        """동기식 안전한 정리 (기존 인터페이스)"""
        try:
            logger.info("웹소켓 매니저 동기식 정리 시작...")

            # 종료 신호
            self._shutdown_event.set()

            # 🔧 안전한 연결 정리 (이벤트 루프 충돌 방지)
            try:
                # 현재 실행 중인 이벤트 루프 확인
                try:
                    current_loop = asyncio.get_running_loop()
                    logger.debug("이미 실행 중인 이벤트 루프가 있음 - 동기 방식으로 정리")

                    # 웹소켓 동기 방식 정리
                    if hasattr(self.connection, 'websocket') and self.connection.websocket:
                        # 웹소켓 직접 종료 (동기)
                        try:
                            if not getattr(self.connection.websocket, 'closed', True):
                                self.connection.websocket.close()
                        except Exception as e:
                            logger.debug(f"웹소켓 직접 종료 중 오류: {e}")

                    # 연결 상태 플래그 정리
                    self.connection.is_connected = False
                    self.connection.is_running = False

                except RuntimeError:
                    # 실행 중인 이벤트 루프가 없는 경우 - 새 루프 생성
                    logger.debug("실행 중인 이벤트 루프 없음 - 새 루프로 정리")
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(self.connection.safe_disconnect())
                    finally:
                        loop.close()

            except Exception as e:
                logger.debug(f"연결 정리 중 오류: {e}")

            # 구독 정리
            self.subscription_manager.clear_all_subscriptions()

            # 스레드 정리
            if self._websocket_thread and self._websocket_thread.is_alive():
                self._websocket_thread.join(timeout=3)

            logger.info("✅ 웹소켓 매니저 동기식 정리 완료")

        except Exception as e:
            logger.error(f"웹소켓 매니저 동기식 정리 오류: {e}")

    def __del__(self):
        """소멸자"""
        try:
            self.safe_cleanup()
        except Exception:
            pass  # 소멸자에서는 오류 무시

    async def _subscribe_account_notices(self):
        """계좌 체결통보 구독 (공식 문서 준수)"""
        try:
            # 🆕 HTS ID 가져오기
            from ..api import kis_auth as kis
            hts_id = kis.get_hts_id()

            if not hts_id:
                logger.error("❌ HTS ID가 설정되지 않음 - 계좌 체결통보 구독 불가")
                return False

            logger.debug(f"📋 HTS ID 사용: {hts_id}")

            # 🆕 공식 문서에 맞춘 H0STCNI0 구독
            # TR_KEY: HTS ID (12자리) - 빈 문자열이 아님!
            notice_msg = self.connection.build_message(
                KIS_WSReq.NOTICE.value,  # H0STCNI0
                hts_id,  # 🔧 HTS ID 사용 (공식 문서 준수)
                "1"  # 구독
            )
            await self.connection.send_message(notice_msg)

            logger.info(f"✅ 계좌 체결통보 구독 성공 (H0STCNI0) - HTS ID: {hts_id}")
            return True

        except Exception as e:
            logger.error(f"계좌 체결통보 구독 실패: {e}")
            return False
