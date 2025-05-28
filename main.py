#!/usr/bin/env python3
"""
StockBot 메인 실행 파일 (리팩토링 버전)
StrategyScheduler를 이용한 시간대별 전략 시스템
"""
import os
import sys
import time
import signal
import asyncio
import threading
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Optional

# 프로젝트 루트 경로 설정
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# 로깅 설정
from utils.logger import setup_logger

# 핵심 모듈들
from core.trading_manager import TradingManager
from core.position_manager import PositionManager
from core.strategy_scheduler import StrategyScheduler
from core.rest_api_manager import KISRestAPIManager
from core.hybrid_data_manager import SimpleHybridDataManager
from core.kis_websocket_manager import KISWebSocketManager
from core.data_priority import DataPriority
from core.stock_discovery import StockDiscovery
from core.trade_database import TradeDatabase
from core.trade_executor import TradeExecutor, TradeConfig
from core.worker_manager import WorkerManager, WorkerConfig

# 설정
from config.settings import (
    IS_DEMO, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, LOG_LEVEL
)

logger = setup_logger(__name__)

# 🆕 텔레그램 봇 조건부 import
TelegramBot = None
try:
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        from telegram_bot.telegram_manager import TelegramBot
        logger.info("✅ 텔레그램 봇 모듈 로드 완료")
except ImportError as e:
    logger.warning(f"⚠️ 텔레그램 봇 모듈 로드 실패: {e}")
    logger.info("📱 텔레그램 알림 없이 계속 진행")
except Exception as e:
    logger.warning(f"⚠️ 텔레그램 봇 설정 오류: {e}")
    logger.info("📱 텔레그램 알림 없이 계속 진행")


class StockBot:
    """간소화된 메인 StockBot 클래스 - 오케스트레이션 역할"""

    def __init__(self, is_demo: bool = IS_DEMO):
        """초기화"""
        self.is_demo = is_demo
        self.is_running = False
        self.shutdown_event = threading.Event()

        logger.info("📈 StockBot 시작 중...")

        # 1. REST API 관리자
        self.rest_api = KISRestAPIManager(is_demo=is_demo)

        # 2. 하이브리드 데이터 관리자
        self.data_manager = SimpleHybridDataManager(is_demo=is_demo)

        # 3. 거래 관리자
        self.trading_manager = TradingManager(is_demo=is_demo)

        # 4. 포지션 관리자
        self.position_manager = PositionManager(self.trading_manager)

        # 5. 거래 데이터베이스
        self.trade_db = TradeDatabase()

        # 6. 🆕 거래 실행자 (핵심 비즈니스 로직 분리)
        trade_config = TradeConfig()  # 기본 설정 사용
        self.trade_executor = TradeExecutor(
            self.trading_manager,
            self.position_manager,
            self.data_manager,
            self.trade_db,
            trade_config
        )

        # 7. 웹소켓 관리자 (선택적)
        self.websocket_manager = None
        try:
            self.websocket_manager = KISWebSocketManager()
            self.data_manager.websocket_manager = self.websocket_manager
            logger.info("✅ WebSocket 관리자 초기화 및 연결 완료")
        except Exception as e:
            logger.warning(f"⚠️ WebSocket 관리자 초기화 실패: {e}")

        # 8. 전략 스케줄러 (핵심!)
        self.strategy_scheduler = StrategyScheduler(self.rest_api, self.data_manager)
        self.strategy_scheduler.set_bot_instance(self)

        # 9. 🆕 워커 매니저 (스레드 관리 전담)
        self.worker_manager = WorkerManager(self.shutdown_event)

        # 10. 🆕 텔레그램 봇 (선택적 - 조건부 초기화)
        self.telegram_bot = self._initialize_telegram_bot()

        # 통계
        self.stats = {
            'start_time': time.time(),
            'signals_processed': 0,
            'orders_executed': 0,
            'positions_opened': 0,
            'positions_closed': 0
        }

        # 중복 매도 방지용 추적
        self.pending_sell_orders = set()

        # 워커 등록
        self._register_workers()

        logger.info("🚀 StockBot 초기화 완료!")

    def _initialize_telegram_bot(self) -> Optional[object]:
        """텔레그램 봇 조건부 초기화"""
        if not TelegramBot:
            logger.info("📱 텔레그램 봇 비활성화 (모듈 로드 실패)")
            return None
            
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            logger.info("📱 텔레그램 봇 비활성화 (설정 누락)")
            return None
            
        try:
            telegram_bot = TelegramBot()  # 🆕 파라미터 제거 (설정에서 직접 가져옴)
            telegram_bot.set_main_bot_reference(self)
            logger.info("✅ 텔레그램 봇 초기화 완료")
            return telegram_bot
        except Exception as e:
            logger.error(f"❌ 텔레그램 봇 초기화 실패: {e}")
            logger.info("📱 텔레그램 알림 없이 계속 진행")
            return None

    def _register_workers(self):
        """워커들을 WorkerManager에 등록"""
        try:
            # 1. 포지션 관리 워커
            position_config = WorkerConfig(
                name="PositionWorker",
                target_function=self._position_worker,
                daemon=True,
                auto_restart=True,
                restart_delay=10.0,
                max_restart_attempts=5,
                heartbeat_interval=60.0
            )
            self.worker_manager.register_worker(position_config)

            # 2. 통계 수집 워커
            stats_config = WorkerConfig(
                name="StatsWorker", 
                target_function=self._stats_worker,
                daemon=True,
                auto_restart=True,
                restart_delay=5.0,
                max_restart_attempts=3,
                heartbeat_interval=300.0
            )
            self.worker_manager.register_worker(stats_config)

            # 3. 텔레그램 봇 워커 (조건부)
            if self.telegram_bot:
                telegram_config = WorkerConfig(
                    name="TelegramBot",
                    target_function=self._telegram_worker,
                    daemon=True,
                    auto_restart=True,
                    restart_delay=15.0,
                    max_restart_attempts=3,
                    heartbeat_interval=120.0
                )
                self.worker_manager.register_worker(telegram_config)

            # 4. 전략 스케줄러 워커
            scheduler_config = WorkerConfig(
                name="StrategyScheduler",
                target_function=self._strategy_scheduler_worker,
                daemon=True,
                auto_restart=False,  # 전략 스케줄러는 자동 재시작하지 않음
                restart_delay=30.0,
                max_restart_attempts=1,
                heartbeat_interval=180.0
            )
            self.worker_manager.register_worker(scheduler_config)

            logger.info("✅ 모든 워커 등록 완료")
            
        except Exception as e:
            logger.error(f"❌ 워커 등록 실패: {e}")

    async def _setup_existing_positions(self):
        """보유 종목 자동 모니터링 설정"""
        try:
            logger.info("📊 보유 종목 모니터링 설정 시작")
            
            balance = self.trading_manager.get_balance()
            holdings = balance.get('holdings', [])
            
            if not holdings:
                logger.info("보유 종목이 없습니다.")
                return
            
            logger.info(f"📈 보유 종목 {len(holdings)}개 발견 - 자동 모니터링 설정")
            
            for holding in holdings:
                stock_code = holding.get('pdno', '')
                stock_name = holding.get('prdt_name', '')
                quantity = int(holding.get('hldg_qty', 0))
                current_price = int(holding.get('prpr', 0))
                avg_price = int(float(holding.get('pchs_avg_pric', current_price)))
                
                if stock_code and quantity > 0:
                    # 🆕 간소화된 전략 타입 결정 로직
                    strategy_type = "existing_holding"  # 기본값
                    
                    try:
                        # DB에서 해당 종목의 미결제 포지션 확인
                        open_positions = self.trade_db.get_open_positions()
                        for pos in open_positions:
                            if (pos['stock_code'] == stock_code and 
                                pos['buy_price'] == avg_price and
                                pos['quantity'] == quantity):
                                # 동일한 종목/가격/수량의 미결제 포지션 발견
                                strategy_type = pos['strategy_type']
                                logger.info(f"🔄 {stock_code} 기존 전략 발견: {strategy_type}")
                                break
                        else:
                            # DB에 해당하는 미결제 포지션이 없음 -> 순수 기존 보유
                            logger.info(f"📝 {stock_code} 새로운 기존 보유 종목")
                            
                            # 기존 보유 종목으로 DB에 기록
                            trade_id = self.trade_db.record_existing_position_if_not_exists(
                                stock_code=stock_code,
                                stock_name=stock_name,
                                quantity=quantity,
                                avg_price=avg_price,
                                current_price=current_price
                            )
                            
                            if trade_id > 0:
                                logger.info(f"💾 기존 보유 종목 DB 기록: {stock_code}({stock_name}) (ID: {trade_id})")
                                
                    except Exception as e:
                        logger.debug(f"전략 타입 확인 실패 ({stock_code}): {e}")
                    
                    # 포지션 매니저에 전략 타입과 함께 추가
                    self.position_manager.add_position(
                        stock_code=stock_code,
                        quantity=quantity,
                        buy_price=avg_price,
                        strategy_type=strategy_type  # 🆕 결정된 전략 타입 사용
                    )
                    
                    # 웹소켓 실시간 모니터링 추가
                    callback = self._create_position_monitoring_callback(stock_code)
                    success = self.data_manager.add_stock_request(
                        stock_code=stock_code,
                        priority=DataPriority.HIGH,
                        strategy_name="position_monitoring",
                        callback=callback
                    )
                    
                    if success:
                        logger.info(f"✅ 보유종목 모니터링 추가: {stock_code}({stock_name}) {quantity:,}주 @ {avg_price:,}원")
                    else:
                        logger.warning(f"⚠️ 보유종목 모니터링 추가 실패: {stock_code}")
            
            logger.info(f"📊 보유 종목 자동 모니터링 설정 완료: {len(holdings)}개")
            
        except Exception as e:
            logger.error(f"보유 종목 모니터링 설정 오류: {e}")

    def _create_position_monitoring_callback(self, stock_code: str) -> Callable:
        """보유 종목 모니터링용 콜백 생성"""
        def position_callback(stock_code: str, data: Dict, source: str = 'websocket') -> None:
            """보유 종목 모니터링 콜백"""
            try:
                if not data or data.get('status') != 'success':
                    return

                current_price = data.get('current_price', 0)
                if current_price <= 0:
                    return

                existing_positions = self.position_manager.get_positions('active')
                if stock_code not in existing_positions:
                    return
                
                position = existing_positions[stock_code]
                buy_price = position.get('buy_price', 0)
                quantity = position.get('quantity', 0)
                
                if buy_price <= 0 or quantity <= 0:
                    return
                
                profit_rate = ((current_price - buy_price) / buy_price) * 100
                logger.debug(f"📊 보유종목 업데이트: {stock_code} {current_price:,}원 (수익률: {profit_rate:.2f}%)")
                
            except Exception as e:
                logger.error(f"보유종목 모니터링 콜백 오류 ({stock_code}): {e}")

        return position_callback

    def start(self):
        """StockBot 시작"""
        if self.is_running:
            logger.warning("StockBot이 이미 실행 중입니다.")
            return

        try:
            self.is_running = True
            logger.info("🔄 StockBot 가동 시작...")

            signal.signal(signal.SIGINT, self._signal_handler)
            
            # 🆕 워커 매니저를 통한 백그라운드 작업 시작
            self.worker_manager.start_all_workers()
            
            logger.info("✅ StockBot 완전 가동!")
            self._main_loop()

        except Exception as e:
            logger.error(f"❌ StockBot 시작 오류: {e}")
            self.stop()
        finally:
            self.is_running = False

    def stop(self):
        """StockBot 중지"""
        if not self.is_running:
            return

        logger.info("🛑 StockBot 종료 중...")

        self.shutdown_event.set()
        self.is_running = False

        # 🆕 워커 매니저를 통한 모든 워커 중지
        try:
            self.worker_manager.stop_all_workers(timeout=30.0)
            logger.info("✅ 모든 워커 정리 완료")
        except Exception as e:
            logger.error(f"❌ 워커 정리 오류: {e}")

        # 전략 스케줄러 중지
        try:
            self.strategy_scheduler.stop_scheduler()
            logger.info("✅ 전략 스케줄러 정리 완료")
        except Exception as e:
            logger.error(f"❌ 전략 스케줄러 정리 오류: {e}")

        # 데이터 관리자 정리
        try:
            self.data_manager.cleanup()
            logger.info("✅ 데이터 관리자 정리 완료")
        except Exception as e:
            logger.error(f"❌ 데이터 관리자 정리 오류: {e}")

        # 웹소켓 정리
        if self.websocket_manager:
            try:
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(self.websocket_manager.cleanup())
                finally:
                    loop.close()
                logger.info("✅ WebSocket 관리자 정리 완료")
            except Exception as e:
                logger.error(f"❌ WebSocket 정리 오류: {e}")

        # 관리자들 정리
        try:
            self.position_manager.cleanup()
            self.trading_manager.cleanup()
            logger.info("✅ 모든 관리자 정리 완료")
        except Exception as e:
            logger.error(f"❌ 관리자 정리 오류: {e}")

        self._print_final_stats()
        logger.info("🏁 StockBot 종료 완료")

    def _main_loop(self):
        """메인 실행 루프"""
        logger.info("🔄 메인 루프 시작")

        while self.is_running and not self.shutdown_event.is_set():
            try:
                market_status = self._check_market_status()

                if market_status['is_open']:
                    logger.debug("📈 시장 개장 중 - 전략 스케줄러가 관리")
                else:
                    logger.debug("💤 시장 휴장 중")

                self.shutdown_event.wait(timeout=10.0)

            except KeyboardInterrupt:
                logger.info("⌨️ 사용자 중단 요청")
                break
            except Exception as e:
                logger.error(f"❌ 메인 루프 오류: {e}")
                time.sleep(5)

        logger.info("🛑 메인 루프 종료")

    # === 🆕 워커 함수들 (WorkerManager 호환) ===

    def _position_worker(self, shutdown_event: threading.Event, worker_manager: WorkerManager):
        """포지션 관리 백그라운드 작업 - WorkerManager 호환"""
        logger.info("🏃 포지션 관리 워커 시작")
        worker_manager.update_heartbeat("PositionWorker")

        while self.is_running and not shutdown_event.is_set():
            try:
                # 포지션별 현재가 업데이트
                self.position_manager.update_position_prices()

                # 매도 조건 확인 및 자동 매도 실행
                sell_signals = self.position_manager.check_exit_conditions()
                for sell_signal in sell_signals:
                    self._handle_auto_sell_signal(sell_signal)

                # 하트비트 업데이트
                worker_manager.update_heartbeat("PositionWorker")

                shutdown_event.wait(timeout=60)  # 1분마다

            except Exception as e:
                logger.error(f"포지션 관리 작업 오류: {e}")
                shutdown_event.wait(timeout=120)

        logger.info("🏁 포지션 관리 워커 종료")

    def _stats_worker(self, shutdown_event: threading.Event, worker_manager: WorkerManager):
        """통계 수집 백그라운드 작업 - WorkerManager 호환"""
        logger.info("🏃 통계 수집 워커 시작")
        worker_manager.update_heartbeat("StatsWorker")

        while self.is_running and not shutdown_event.is_set():
            try:
                self._update_stats()

                if int(time.time()) % 3600 < 300:  # 1시간마다
                    self._generate_hourly_report()

                # 하트비트 업데이트
                worker_manager.update_heartbeat("StatsWorker")

                shutdown_event.wait(timeout=300)  # 5분 간격

            except Exception as e:
                logger.error(f"통계 수집 작업 오류: {e}")
                shutdown_event.wait(timeout=300)

        logger.info("🏁 통계 수집 워커 종료")

    def _telegram_worker(self, shutdown_event: threading.Event, worker_manager: WorkerManager):
        """텔레그램 봇 워커 - WorkerManager 호환"""
        if not self.telegram_bot:
            logger.warning("텔레그램 봇이 없어 워커를 종료합니다.")
            return

        logger.info("🏃 텔레그램 봇 워커 시작")
        worker_manager.update_heartbeat("TelegramBot")

        try:
            # 텔레그램 봇 시작 (블로킹)
            self.telegram_bot.start_bot()
            
            # 주기적으로 하트비트 업데이트
            while self.is_running and not shutdown_event.is_set():
                worker_manager.update_heartbeat("TelegramBot")
                shutdown_event.wait(timeout=60)  # 1분마다 하트비트
                
        except Exception as e:
            logger.error(f"텔레그램 봇 워커 오류: {e}")
        finally:
            if self.telegram_bot:
                try:
                    self.telegram_bot.stop_bot()
                except Exception as e:
                    logger.error(f"텔레그램 봇 중지 오류: {e}")
            
            logger.info("🏁 텔레그램 봇 워커 종료")

    def _strategy_scheduler_worker(self, shutdown_event: threading.Event, worker_manager: WorkerManager):
        """전략 스케줄러 워커 - WorkerManager 호환"""
        logger.info("🏃 전략 스케줄러 워커 시작")
        worker_manager.update_heartbeat("StrategyScheduler")

        try:
            def run_strategy_scheduler():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(self._setup_existing_positions())
                    loop.run_until_complete(self.strategy_scheduler.start_scheduler())
                except Exception as e:
                    logger.error(f"전략 스케줄러 실행 오류: {e}")
                finally:
                    loop.close()

            # 별도 스레드에서 비동기 스케줄러 실행
            scheduler_thread = threading.Thread(
                target=run_strategy_scheduler,
                name="AsyncScheduler",
                daemon=True
            )
            scheduler_thread.start()

            # 주기적으로 하트비트 업데이트 및 스케줄러 상태 확인
            while self.is_running and not shutdown_event.is_set():
                worker_manager.update_heartbeat("StrategyScheduler")
                
                # 스케줄러 스레드가 죽었는지 확인
                if not scheduler_thread.is_alive():
                    logger.error("전략 스케줄러 스레드가 예상치 못하게 종료됨")
                    break
                
                shutdown_event.wait(timeout=180)  # 3분마다 하트비트

            # 스케줄러 스레드 종료 대기
            scheduler_thread.join(timeout=10)

        except Exception as e:
            logger.error(f"전략 스케줄러 워커 오류: {e}")
        finally:
            logger.info("🏁 전략 스케줄러 워커 종료")

    def _handle_auto_sell_signal(self, sell_signal: Dict):
        """자동 매도 신호 처리 - TradeExecutor 사용"""
        try:
            stock_code = sell_signal['stock_code']
            
            # 중복 매도 방지 체크
            if stock_code in self.pending_sell_orders:
                logger.warning(f"⚠️ 이미 매도 주문 진행 중: {stock_code}")
                return

            logger.info(f"🚨 매도 조건 발생: {stock_code} - {sell_signal['reason']}")

            # 중복 방지용 등록
            self.pending_sell_orders.add(stock_code)

            try:
                # TradeExecutor를 통한 자동 매도 실행
                trade_result = self.trade_executor.execute_auto_sell(sell_signal)
                
                if trade_result.success:
                    # 포지션에서 제거
                    self.position_manager.remove_position(
                        stock_code,
                        trade_result.quantity,
                        trade_result.price
                    )

                    self.stats['orders_executed'] += 1
                    self.stats['positions_closed'] += 1

                    logger.info(f"✅ 자동 매도 완료: {stock_code} {trade_result.quantity:,}주 @ {trade_result.price:,}원")

                    # 텔레그램 알림 (조건부)
                    if self.telegram_bot:
                        sell_signal['auto_sell_price'] = trade_result.price
                        self.telegram_bot.send_auto_sell_notification(sell_signal, trade_result.order_no)
                else:
                    logger.error(f"❌ 자동 매도 실패: {trade_result.error_message}")

            finally:
                # 중복 방지용 해제
                self.pending_sell_orders.discard(stock_code)

        except Exception as e:
            logger.error(f"자동 매도 신호 처리 오류: {sell_signal.get('stock_code', 'UNKNOWN')} - {e}")
            self.pending_sell_orders.discard(sell_signal.get('stock_code', 'UNKNOWN'))

    # === 전략 스케줄러에서 호출되는 메서드들 (간소화됨) ===

    def handle_trading_signal(self, signal: dict):
        """거래 신호 처리 - TradeExecutor 위임"""
        try:
            logger.info(f"📊 거래신호 수신: {signal['stock_code']} {signal['signal_type']}")

            self.stats['signals_processed'] += 1

            # TradeExecutor를 통한 거래 실행
            if signal['signal_type'] == 'BUY':
                trade_result = self.trade_executor.execute_buy_signal(signal)
                if trade_result.success:
                    self.stats['orders_executed'] += 1
                    self.stats['positions_opened'] += 1
                    logger.info(f"✅ 매수 완료: {trade_result.stock_code}")
                else:
                    logger.warning(f"⚠️ 매수 실패: {trade_result.error_message}")

            elif signal['signal_type'] == 'SELL':
                trade_result = self.trade_executor.execute_sell_signal(signal)
                if trade_result.success:
                    self.stats['orders_executed'] += 1
                    self.stats['positions_closed'] += 1
                    logger.info(f"✅ 매도 완료: {trade_result.stock_code}")
                else:
                    logger.warning(f"⚠️ 매도 실패: {trade_result.error_message}")

            # 텔레그램 알림 (조건부)
            if self.telegram_bot:
                self.telegram_bot.send_signal_notification(signal)

        except Exception as e:
            logger.error(f"거래 신호 처리 오류: {e}")

    # === 헬퍼 메서드들 ===

    def _check_market_status(self) -> dict:
        """시장 상태 확인"""
        try:
            current_time = time.localtime()
            hour = current_time.tm_hour
            minute = current_time.tm_min
            weekday = current_time.tm_wday

            if weekday >= 5:  # 주말
                return {'is_open': False, 'reason': '주말'}

            current_minutes = hour * 60 + minute
            market_open = 9 * 60
            market_close = 15 * 60 + 30

            if market_open <= current_minutes <= market_close:
                return {'is_open': True, 'session': 'regular'}
            else:
                return {'is_open': False, 'reason': '장외시간'}

        except Exception as e:
            logger.error(f"시장 상태 확인 오류: {e}")
            return {'is_open': False, 'reason': '오류'}

    def _update_stats(self):
        """통계 정보 업데이트"""
        try:
            current_time = time.time()
            runtime = current_time - self.stats['start_time']

            position_summary = self.position_manager.get_position_summary()
            scheduler_status = self.strategy_scheduler.get_status()

            websocket_info = {
                'connected': False,
                'subscriptions': 0,
                'usage': '0/41',
                'stocks': []
            }
            
            if self.websocket_manager:
                websocket_info['connected'] = getattr(self.websocket_manager, 'is_connected', False)
                if hasattr(self.websocket_manager, 'get_subscribed_stocks'):
                    websocket_info['stocks'] = self.websocket_manager.get_subscribed_stocks()
                    websocket_info['subscriptions'] = len(websocket_info['stocks'])
                if hasattr(self.websocket_manager, 'get_websocket_usage'):
                    websocket_info['usage'] = self.websocket_manager.get_websocket_usage()

            # 🆕 워커 상태 정보 추가
            worker_status = self.worker_manager.get_all_status()
            running_workers = sum(1 for w in worker_status['workers'].values() if w['is_alive'])
            total_workers = len(worker_status['workers'])
            worker_info = f"✅ {running_workers}/{total_workers}개 실행중"

            self.stats.update({
                'runtime_hours': runtime / 3600,
                'positions_count': position_summary.get('total_positions', 0),
                'total_value': position_summary.get('total_value', 0),
                'current_slot': scheduler_status.get('current_phase', 'Unknown'),
                'active_strategies': len(scheduler_status.get('active_strategies', {})),
                'websocket_connected': websocket_info['connected'],
                'websocket_subscriptions': websocket_info['subscriptions'],
                'websocket_usage': websocket_info['usage'],
                'worker_stats': worker_status,
                'last_update': current_time
            })

            if int(current_time) % 300 < 5:  # 5분마다
                logger.info(
                    f"📊 시스템 통계 - "
                    f"실행시간: {self.stats['runtime_hours']:.1f}h, "
                    f"포지션: {self.stats['positions_count']}개, "
                    f"현재전략: {self.stats['current_slot']}, "
                    f"웹소켓: {'✅' if websocket_info['connected'] else '❌'} ({websocket_info['subscriptions']}종목), "
                    f"워커: {worker_info}, "
                    f"처리신호: {self.stats['signals_processed']}개"
                )

        except Exception as e:
            logger.error(f"통계 업데이트 오류: {e}")

    def _generate_hourly_report(self):
        """1시간마다 상세 리포트 생성"""
        try:
            report = self._generate_status_report()
            logger.info(f"📊 1시간 리포트:\n{report}")

            if self.telegram_bot:
                self.telegram_bot.send_hourly_report(report)

        except Exception as e:
            logger.error(f"리포트 생성 오류: {e}")

    def _generate_status_report(self) -> str:
        """상태 리포트 생성"""
        try:
            runtime = time.time() - self.stats['start_time']
            runtime_hours = runtime / 3600

            scheduler_status = self.strategy_scheduler.get_status()
            position_summary = self.position_manager.get_position_summary()

            websocket_info = "❌ 연결 안됨"
            if self.websocket_manager and getattr(self.websocket_manager, 'is_connected', False):
                subscriptions = 0
                usage = "0/41"
                if hasattr(self.websocket_manager, 'get_subscribed_stocks'):
                    subscriptions = len(self.websocket_manager.get_subscribed_stocks())
                if hasattr(self.websocket_manager, 'get_websocket_usage'):
                    usage = self.websocket_manager.get_websocket_usage()
                websocket_info = f"✅ 연결됨 ({subscriptions}종목, {usage})"

            # 🆕 워커 상태 정보 추가
            worker_status = self.worker_manager.get_all_status()
            running_workers = sum(1 for w in worker_status['workers'].values() if w['is_alive'])
            total_workers = len(worker_status['workers'])
            worker_info = f"✅ {running_workers}/{total_workers}개 실행중"

            report = (
                f"🕐 실행시간: {runtime_hours:.1f}시간\n"
                f"📈 활성포지션: {position_summary.get('total_positions', 0)}개\n"
                f"💰 총평가금액: {position_summary.get('total_value', 0):,}원\n"
                f"📊 수익률: {position_summary.get('total_profit_rate', 0):.2f}%\n"
                f"🎯 현재전략: {scheduler_status.get('current_phase', 'N/A')}\n"
                f"📋 활성전략수: {len(scheduler_status.get('active_strategies', {}))}\n"
                f"📡 웹소켓: {websocket_info}\n"
                f"🤖 워커: {worker_info}\n"
                f"🎰 처리신호: {self.stats['signals_processed']}개"
            )

            return report

        except Exception as e:
            logger.error(f"리포트 생성 오류: {e}")
            return "리포트 생성 실패"

    def _print_final_stats(self):
        """최종 통계 출력"""
        try:
            runtime = time.time() - self.stats['start_time']
            worker_status = self.worker_manager.get_all_status()

            logger.info("=" * 50)
            logger.info("📊 StockBot 최종 통계")
            logger.info("=" * 50)
            logger.info(f"🕐 총 실행시간: {runtime/3600:.2f}시간")
            logger.info(f"📊 처리한 신호: {self.stats['signals_processed']}개")
            logger.info(f"📋 실행한 주문: {self.stats['orders_executed']}개")
            logger.info(f"📈 열린 포지션: {self.stats['positions_opened']}개")
            logger.info(f"📉 닫힌 포지션: {self.stats['positions_closed']}개")
            logger.info(f"🤖 워커 재시작: {worker_status['stats']['total_restarts']}회")
            logger.info("=" * 50)

        except Exception as e:
            logger.error(f"최종 통계 출력 오류: {e}")

    def _signal_handler(self, signum, frame):
        """시스템 신호 처리"""
        logger.info(f"🛑 종료 신호 수신: {signum}")
        self.stop()

    # === 텔레그램 봇용 인터페이스 메서드들 ===

    def get_balance(self) -> dict:
        """잔고 조회 (텔레그램용)"""
        return self.trading_manager.get_balance()

    def get_system_status(self) -> dict:
        """시스템 상태 조회 (텔레그램용)"""
        try:
            position_summary = self.position_manager.get_position_summary()
            scheduler_status = self.strategy_scheduler.get_status()
            worker_status = self.worker_manager.get_all_status()

            websocket_connected = False
            websocket_subscriptions = 0
            websocket_usage = "0/41"
            subscribed_stocks = []
            
            if self.websocket_manager:
                websocket_connected = getattr(self.websocket_manager, 'is_connected', False)
                if hasattr(self.websocket_manager, 'get_subscribed_stocks'):
                    subscribed_stocks = self.websocket_manager.get_subscribed_stocks()
                    websocket_subscriptions = len(subscribed_stocks)
                if hasattr(self.websocket_manager, 'get_websocket_usage'):
                    websocket_usage = self.websocket_manager.get_websocket_usage()

            return {
                'bot_running': self.is_running,
                'websocket_connected': websocket_connected,
                'websocket_subscriptions': websocket_subscriptions,
                'websocket_usage': websocket_usage,
                'subscribed_stocks': subscribed_stocks[:10],
                'positions_count': position_summary.get('total_positions', 0),
                'pending_orders_count': 0,
                'order_history_count': 0,
                'scheduler': {
                    'current_slot': scheduler_status.get('current_phase', 'None'),
                    'active_strategies': scheduler_status.get('active_strategies', {}),
                    'total_active_stocks': len(scheduler_status.get('active_strategies', {}))
                },
                'workers': worker_status
            }
        except Exception as e:
            logger.error(f"시스템 상태 조회 오류: {e}")
            return {
                'bot_running': self.is_running,
                'websocket_connected': False,
                'websocket_subscriptions': 0,
                'websocket_usage': "0/41",
                'subscribed_stocks': [],
                'positions_count': 0,
                'pending_orders_count': 0,
                'order_history_count': 0,
                'scheduler': {
                    'current_slot': 'Unknown',
                    'active_strategies': {},
                    'total_active_stocks': 0
                },
                'workers': {'workers': {}, 'stats': {}, 'manager_running': False}
            }

    @property
    def trading_api(self):
        """trading_api 속성 (텔레그램 봇 호환용)"""
        return self.trading_manager


def main():
    """메인 실행 함수"""
    try:
        bot = StockBot()
        bot.start()

    except KeyboardInterrupt:
        logger.info("⌨️ 사용자 중단")
    except Exception as e:
        logger.error(f"❌ 메인 실행 오류: {e}")
    finally:
        logger.info("🏁 프로그램 종료")


if __name__ == "__main__":
    main()
