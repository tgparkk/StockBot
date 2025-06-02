#!/usr/bin/env python3
"""
워커 관리자 - 백그라운드 작업 스레드 관리 (간소화 버전)
"""
import threading
import time
from typing import List, Optional, TYPE_CHECKING
from utils.logger import setup_logger

# 순환 import 방지를 위한 TYPE_CHECKING 사용
if TYPE_CHECKING:
    from main import StockBot

logger = setup_logger(__name__)


class WorkerManager:
    """간소화된 백그라운드 워커 관리자"""

    def __init__(self, shutdown_event: threading.Event):
        """초기화"""
        self.shutdown_event = shutdown_event
        self.workers: List[threading.Thread] = []
        logger.info("✅ WorkerManager 초기화 완료")

    def _safe_get_manager(self, bot_instance: "StockBot", manager_name: str):
        """안전하게 매니저 객체 가져오기"""
        try:
            if hasattr(bot_instance, manager_name):
                manager = getattr(bot_instance, manager_name)
                return manager if manager else None
            return None
        except:
            return None

    def _start_worker(self, target_func, args, name: str):
        """워커 시작 헬퍼"""
        try:
            if self.shutdown_event.is_set():
                return

            worker_thread = threading.Thread(
                target=target_func,
                args=args,
                daemon=True,
                name=name
            )
            worker_thread.start()
            self.workers.append(worker_thread)
            logger.info(f"✅ {name} 워커 시작")

        except Exception as e:
            logger.error(f"❌ {name} 워커 시작 오류: {e}")

    def start_all_workers(self, bot_instance: "StockBot"):
        """모든 워커 시작"""
        try:
            logger.info("🔧 워커 매니저 시작...")

            # 주문 정리 워커 (여전히 필요 - TradeExecutor의 만료된 주문 정리)
            self._start_worker(self._order_cleanup_worker, (bot_instance,), "order_cleanup")

            # 웹소켓 모니터링 워커 (여전히 필요 - 연결 상태 관리)
            self._start_worker(self._websocket_monitor_worker, (bot_instance,), "websocket_monitor")

            logger.info(f"✅ {len(self.workers)}개 워커 시작 완료")
            logger.info("📝 참고: 포지션 관리는 이제 캔들 트레이드 매니저에서 처리됩니다")

        except Exception as e:
            logger.error(f"❌ 워커 시작 오류: {e}")

    def _order_cleanup_worker(self, bot_instance: "StockBot"):
        """주문 정리 워커 (간소화)"""
        logger.info("🧹 주문 정리 워커 시작")

        while not self.shutdown_event.is_set():
            try:
                trade_executor = self._safe_get_manager(bot_instance, 'trade_executor')
                if trade_executor and hasattr(trade_executor, 'cleanup_expired_orders'):
                    trade_executor.cleanup_expired_orders()

                self.shutdown_event.wait(timeout=120)  # 2분마다

            except Exception as e:
                logger.error(f"❌ 주문 정리 오류: {e}")
                self.shutdown_event.wait(timeout=60)

        logger.info("🛑 주문 정리 워커 종료")

    def _websocket_monitor_worker(self, bot_instance: "StockBot"):
        """웹소켓 모니터링 워커 (간소화)"""
        logger.info("🔍 웹소켓 모니터링 워커 시작")

        while not self.shutdown_event.is_set():
            try:
                data_manager = self._safe_get_manager(bot_instance, 'data_manager')

                if data_manager and hasattr(data_manager, 'websocket_manager'):
                    websocket_manager = data_manager.websocket_manager
                    is_connected = getattr(websocket_manager, 'is_connected', False)

                    if not is_connected:
                        logger.warning("⚠️ 웹소켓 연결 끊김 - 재연결 시도")
                        try:
                            if hasattr(data_manager, 'ensure_websocket_connection'):
                                data_manager.ensure_websocket_connection()
                        except Exception as e:
                            logger.error(f"❌ 웹소켓 재연결 실패: {e}")

                self.shutdown_event.wait(timeout=60)  # 1분마다 확인

            except Exception as e:
                logger.error(f"❌ 웹소켓 모니터링 오류: {e}")
                self.shutdown_event.wait(timeout=30)

        logger.info("🛑 웹소켓 모니터링 워커 종료")

    def stop_all_workers(self, timeout: float = 30.0) -> bool:
        """모든 워커 중지"""
        try:
            logger.info("🛑 모든 워커 중지 중...")
            self.shutdown_event.set()

            for worker in self.workers:
                if worker.is_alive():
                    worker.join(timeout=timeout)

            alive_count = sum(1 for w in self.workers if w.is_alive())

            if alive_count == 0:
                logger.info("✅ 모든 워커 정상 종료")
                return True
            else:
                logger.warning(f"⚠️ {alive_count}개 워커가 아직 실행 중")
                return False

        except Exception as e:
            logger.error(f"❌ 워커 중지 오류: {e}")
            return False

    def get_status(self) -> dict:
        """워커 상태 조회"""
        try:
            alive_workers = [w.name for w in self.workers if w.is_alive()]
            return {
                'total_workers': len(self.workers),
                'alive_workers': len(alive_workers),
                'alive_worker_names': alive_workers,
                'shutdown_requested': self.shutdown_event.is_set()
            }
        except Exception as e:
            logger.error(f"❌ 워커 상태 조회 오류: {e}")
            return {'error': str(e)}
