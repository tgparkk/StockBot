#!/usr/bin/env python3
"""
워커 관리자 - 백그라운드 작업 스레드 관리
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
    """간단한 백그라운드 워커 관리자"""

    def __init__(self, shutdown_event: threading.Event):
        """초기화"""
        self.shutdown_event = shutdown_event
        self.workers: List[threading.Thread] = []

        logger.info("✅ WorkerManager 초기화 완료")

    def start_all_workers(self, bot_instance: "StockBot"):
        """모든 워커 시작"""
        try:
            logger.info("🔧 워커 매니저 시작...")
            
            # 1. 웹소켓 연결 모니터링 워커
            self.start_websocket_monitor(bot_instance)
            
            # 2. 포지션 모니터링 워커
            self.start_position_monitor(bot_instance)
            
            # 🆕 3. 통합 매도 신호 워커 (기존 자동 매도 + 전략적 매도 통합)
            self.start_unified_sell_signal_worker(bot_instance)
            
            # 4. 주문 정리 워커 (만료된 주문 정리)
            self.start_order_cleanup_worker(bot_instance)
            
            logger.info(f"✅ {len(self.workers)}개 워커 시작 완료")
            
        except Exception as e:
            logger.error(f"❌ 워커 시작 오류: {e}")

    def start_websocket_monitor(self, bot_instance: "StockBot"):
        """🎯 웹소켓 연결 모니터링 워커 시작"""
        try:
            if self.shutdown_event.is_set():
                return

            worker_thread = threading.Thread(
                target=self._websocket_monitor_worker,
                args=(bot_instance,),
                daemon=True,
                name="websocket_monitor"
            )

            worker_thread.start()
            self.workers.append(worker_thread)

            logger.info("🔍 웹소켓 연결 모니터링 워커 시작")

        except Exception as e:
            logger.error(f"❌ 웹소켓 모니터링 워커 시작 오류: {e}")

    def start_position_monitor(self, bot_instance: "StockBot"):
        """포지션 모니터링 워커 시작"""
        try:
            if self.shutdown_event.is_set():
                return

            worker_thread = threading.Thread(
                target=self._position_monitor_worker,
                args=(bot_instance,),
                daemon=True,
                name="position_monitor"
            )

            worker_thread.start()
            self.workers.append(worker_thread)

            logger.info("📊 포지션 모니터링 워커 시작")

        except Exception as e:
            logger.error(f"❌ 포지션 모니터링 워커 시작 오류: {e}")

    def start_unified_sell_signal_worker(self, bot_instance: "StockBot"):
        """🆕 통합 매도 신호 워커 시작 (자동 매도 + 전략적 매도 통합)"""
        try:
            if self.shutdown_event.is_set():
                return

            worker_thread = threading.Thread(
                target=self._unified_sell_signal_worker,
                args=(bot_instance,),
                daemon=True,
                name="unified_sell_signal"
            )

            worker_thread.start()
            self.workers.append(worker_thread)

            logger.info("🎯 통합 매도 신호 워커 시작 (10초 주기)")

        except Exception as e:
            logger.error(f"❌ 통합 매도 신호 워커 시작 오류: {e}")

    def start_order_cleanup_worker(self, bot_instance: "StockBot"):
        """🧹 주문 정리 워커 시작"""
        try:
            if self.shutdown_event.is_set():
                return

            worker_thread = threading.Thread(
                target=self._order_cleanup_worker,
                args=(bot_instance,),
                daemon=True,
                name="order_cleanup"
            )

            worker_thread.start()
            self.workers.append(worker_thread)

            logger.info("🧹 주문 정리 워커 시작")

        except Exception as e:
            logger.error(f"❌ 주문 정리 워커 시작 오류: {e}")

    def _position_monitor_worker(self, bot_instance: "StockBot"):
        """포지션 모니터링 워커 (🎯 개선된 실시간성)"""
        logger.info("📊 포지션 모니터링 워커 시작됨 (실시간 모드)")

        while not self.shutdown_event.is_set():
            try:
                # 포지션 현재가 업데이트
                if (hasattr(bot_instance, 'position_manager') and
                    bot_instance.position_manager):
                    bot_instance.position_manager.update_position_prices()

                # 🎯 10초마다 실행 (기존 30초에서 단축)
                # 웹소켓이 정상이면 부담이 적고, REST API 백업 시에만 호출 증가
                self.shutdown_event.wait(timeout=10)

            except Exception as e:
                logger.error(f"❌ 포지션 모니터링 워커 오류: {e}")
                self.shutdown_event.wait(timeout=5)  # 오류 시 더 짧은 대기

        logger.info("🛑 포지션 모니터링 워커 종료")

    def _unified_sell_signal_worker(self, bot_instance: "StockBot"):
        """🆕 통합 매도 신호 워커 (기존 자동 매도 + 전략적 매도 통합)"""
        logger.info("🎯 통합 매도 신고 워커 시작됨 (10초 주기 - 고빈도 모드)")

        while not self.shutdown_event.is_set():
            try:
                # 🎯 1. 기존 자동 매도 로직 (손절/익절/이격도 등)
                if (hasattr(bot_instance, 'position_manager') and
                    bot_instance.position_manager):
                    
                    # 포지션 현재가 업데이트
                    bot_instance.position_manager.update_position_prices()
                    
                    # 매도 조건 확인 및 실행
                    sell_signals = bot_instance.position_manager.check_exit_conditions()
                    
                    for sell_signal in sell_signals:
                        try:
                            # 🎯 개선된 매도 신호를 전략 스케줄러 형식으로 변환
                            unified_signal = {
                                'stock_code': sell_signal['stock_code'],
                                'signal_type': 'SELL',
                                'strategy': f"auto_{sell_signal.get('strategy_type', 'default')}",
                                'price': sell_signal.get('optimal_sell_price', sell_signal.get('current_price', 0)),
                                'strength': 0.8,  # 자동 매도는 높은 강도
                                'reason': sell_signal['reason'],
                                'target_price': sell_signal.get('optimal_sell_price', 0),
                                'stop_loss': 0,
                                'position_size': 1.0,  # 전량 매도
                                'risk_reward': 1.0,
                                'confidence': 0.9,  # 높은 신뢰도
                                'warnings': [],
                                'unified_sell_signal': True,  # 통합 신호 표시
                                'urgency': sell_signal.get('urgency', 'MEDIUM')
                            }
                            
                            # 메인 봇에 신호 전달
                            if hasattr(bot_instance, 'handle_trading_signal'):
                                bot_instance.handle_trading_signal(unified_signal)
                                logger.info(f"🎯 통합매도신호 전달: {sell_signal['stock_code']} - {sell_signal['reason']}")
                            
                        except Exception as e:
                            logger.error(f"❌ 통합 매도 신호 처리 오류: {sell_signal.get('stock_code', 'Unknown')} - {e}")

                # 🎯 2. 추가적인 전략 신호 체크 (필요시 여기에 추가)
                # 현재는 strategy_scheduler의 실시간 웹소켓 콜백이 주로 담당하므로 
                # 여기서는 주기적 보완 체크만 수행

                # 🎯 10초마다 실행 (30초에서 단축)
                # 빠른 손절/익절을 위한 고빈도 체크
                self.shutdown_event.wait(timeout=10)

            except Exception as e:
                logger.error(f"❌ 통합 매도 신호 워커 오류: {e}")
                self.shutdown_event.wait(timeout=5)  # 오류 시 더 짧은 대기

        logger.info("🛑 통합 매도 신호 워커 종료")

    def _order_cleanup_worker(self, bot_instance: "StockBot"):
        """🧹 주문 정리 워커 (만료된 주문 정리)"""
        logger.info("🧹 주문 정리 워커 시작됨")

        while not self.shutdown_event.is_set():
            try:
                # 만료된 주문 정리
                if (hasattr(bot_instance, 'trade_executor') and
                    bot_instance.trade_executor):
                    bot_instance.trade_executor.cleanup_expired_orders()

                # TradeExecutor의 pending_orders (set) 정리도 함께
                if (hasattr(bot_instance.trade_executor, 'pending_orders') and
                    isinstance(bot_instance.trade_executor.pending_orders, set)):
                    # pending_orders (set)는 거래 신호 처리시 자동으로 정리되므로 별도 처리 불필요
                    pass

                # 🎯 2분마다 실행 (만료 시간이 5분이므로 적당한 간격)
                self.shutdown_event.wait(timeout=120)

            except Exception as e:
                logger.error(f"❌ 주문 정리 워커 오류: {e}")
                self.shutdown_event.wait(timeout=60)  # 오류 시 1분 대기

        logger.info("🛑 주문 정리 워커 종료")

    def _websocket_monitor_worker(self, bot_instance: "StockBot"):
        """🎯 웹소켓 연결 상태 모니터링 및 자동 재연결"""
        logger.info("🔍 웹소켓 모니터링 워커 시작됨")

        check_interval = 60  # 1분마다 확인
        reconnect_attempts = 0
        max_reconnect_attempts = 5

        while not self.shutdown_event.is_set():
            try:
                # 웹소켓 연결 상태 확인
                is_connected = False

                if (hasattr(bot_instance, 'data_manager') and
                    bot_instance.data_manager and
                    hasattr(bot_instance.data_manager, 'websocket_manager') and
                    bot_instance.data_manager.websocket_manager):

                    websocket_manager = bot_instance.data_manager.websocket_manager
                    is_connected = getattr(websocket_manager, 'is_connected', False)
                    is_running = getattr(websocket_manager, 'is_running', False)

                    if not is_connected and is_running:
                        # 연결이 끊어진 상태
                        logger.warning("⚠️ 웹소켓 연결 끊김 감지")

                        if reconnect_attempts < max_reconnect_attempts:
                            reconnect_attempts += 1
                            logger.info(f"🔄 웹소켓 자동 재연결 시도 {reconnect_attempts}/{max_reconnect_attempts}")

                            # 재연결 시도
                            try:
                                success = bot_instance.data_manager.ensure_websocket_connection()
                                if success:
                                    logger.info("✅ 웹소켓 자동 재연결 성공")
                                    reconnect_attempts = 0  # 성공시 카운터 리셋
                                else:
                                    logger.warning(f"⚠️ 웹소켓 자동 재연결 실패 ({reconnect_attempts}/{max_reconnect_attempts})")

                            except Exception as e:
                                logger.error(f"❌ 웹소켓 자동 재연결 중 오류: {e}")
                        else:
                            logger.error(f"❌ 웹소켓 자동 재연결 한계 도달 ({max_reconnect_attempts}회) - 모니터링 일시 중단")
                            # 10분 후 재시도 카운터 리셋
                            self.shutdown_event.wait(timeout=600)  # 10분 대기
                            if not self.shutdown_event.is_set():
                                reconnect_attempts = 0
                                logger.info("🔄 웹소켓 재연결 카운터 리셋 - 모니터링 재개")
                                continue

                    elif is_connected and is_running:
                        # 정상 연결 상태
                        if reconnect_attempts > 0:
                            logger.info("✅ 웹소켓 연결 상태 정상화")
                            reconnect_attempts = 0
                        logger.debug("🔍 웹소켓 연결 상태: 정상")

                    elif not is_running:
                        # 웹소켓이 시작되지 않은 상태
                        logger.debug("🔍 웹소켓 미실행 상태 - 필요시 자동 시작")

                else:
                    logger.debug("🔍 웹소켓 관리자 없음")

                # 다음 확인까지 대기
                self.shutdown_event.wait(timeout=check_interval)

            except Exception as e:
                logger.error(f"❌ 웹소켓 모니터링 워커 오류: {e}")
                self.shutdown_event.wait(timeout=30)  # 오류시 30초 대기

        logger.info("🛑 웹소켓 모니터링 워커 종료")

    def stop_all_workers(self, timeout: float = 30.0) -> bool:
        """모든 워커 중지"""
        try:
            logger.info("🛑 모든 워커 중지 중...")

            # 종료 시그널 설정
            self.shutdown_event.set()

            # 모든 워커 종료 대기
            for worker in self.workers:
                if worker.is_alive():
                    worker.join(timeout=timeout)
                    if worker.is_alive():
                        logger.warning(f"⚠️ 워커 '{worker.name}' 종료 시간 초과")

            alive_count = sum(1 for w in self.workers if w.is_alive())
            if alive_count == 0:
                logger.info("✅ 모든 워커 정상 종료")
                return True
            else:
                logger.warning(f"⚠️ {alive_count}개 워커가 아직 실행 중")
                return False

        except Exception as e:
            logger.error(f"❌ 워커 중지 중 오류: {e}")
            return False

    def get_status(self) -> dict:
        """워커 상태 조회"""
        try:
            alive_workers = [w.name for w in self.workers if w.is_alive()]
            dead_workers = [w.name for w in self.workers if not w.is_alive()]

            return {
                'total_workers': len(self.workers),
                'alive_workers': len(alive_workers),
                'dead_workers': len(dead_workers),
                'alive_worker_names': alive_workers,
                'dead_worker_names': dead_workers,
                'shutdown_requested': self.shutdown_event.is_set()
            }

        except Exception as e:
            logger.error(f"❌ 워커 상태 조회 오류: {e}")
            return {'error': str(e)}
