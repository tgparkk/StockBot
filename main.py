#!/usr/bin/env python3
"""
StockBot 메인 실행 파일 (리팩토링 버전)
StrategyScheduler를 이용한 시간대별 전략 시스템
"""
import sys
import time
import signal
import asyncio
import threading
from pathlib import Path
from typing import Optional, Dict
import pytz

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
from core.trade_database import TradeDatabase
from core.trade_executor import TradeConfig, TradeExecutor
from core.worker_manager import WorkerManager
from core.kis_data_collector import KISDataCollector

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

    def __init__(self):
        """초기화"""
        self.is_running = False
        self.shutdown_event = threading.Event()

        logger.info("📈 StockBot 시작 중...")

        # 1. REST API 관리자 (단일 인스턴스)
        self.rest_api = KISRestAPIManager()
        logger.info("✅ REST API 관리자 초기화 완료 (단일 인스턴스)")

        # 2. 웹소켓 관리자 (단일 인스턴스)
        self.websocket_manager = None
        try:
            logger.info("🔗 웹소켓 관리자 초기화 중... (단일 인스턴스)")
            self.websocket_manager = KISWebSocketManager()
            logger.info("✅ WebSocket 관리자 초기화 완료")
        except Exception as e:
            logger.error(f"❌ WebSocket 관리자 초기화 실패: {e}")
            logger.warning("⚠️ 웹소켓은 필수 구성요소입니다 - 연결 문제를 확인하세요")

        # 3. 데이터 수집기 (단일 인스턴스)
        self.data_collector = KISDataCollector(
            websocket_manager=self.websocket_manager,
            rest_api_manager=self.rest_api
        )
        logger.info("✅ 데이터 수집기 초기화 완료 (단일 인스턴스)")

        # 4. 하이브리드 데이터 관리자 (데이터 수집기 주입)
        self.data_manager = SimpleHybridDataManager(
            websocket_manager=self.websocket_manager, 
            rest_api_manager=self.rest_api,
            data_collector=self.data_collector
        )

        # 5. 거래 관리자 (데이터 수집기 주입)
        self.trading_manager = TradingManager(
            websocket_manager=self.websocket_manager,
            rest_api_manager=self.rest_api,
            data_collector=self.data_collector
        )

        # 6. 포지션 매니저
        self.position_manager = PositionManager(self.trading_manager)

        # 7. 거래 데이터베이스
        self.trade_db = TradeDatabase()

        # 8. 거래 실행자 (핵심 비즈니스 로직 분리)
        trade_config = TradeConfig()  # 기본 설정 사용
        self.trade_executor = TradeExecutor(
            self.trading_manager,
            self.position_manager,
            self.data_manager,
            self.trade_db,
            trade_config
        )

        # 9. 전략 스케줄러 (핵심!)
        self.strategy_scheduler = StrategyScheduler(self.rest_api, self.data_manager)
        self.strategy_scheduler.set_bot_instance(self)
        
        # 🆕 StockDiscovery에 TradeExecutor 연결
        self.strategy_scheduler.stock_discovery.set_trade_executor(self.trade_executor)

        # 10. 워커 매니저 (스레드 관리 전담)
        self.worker_manager = WorkerManager(self.shutdown_event)

        # 11. 텔레그램 봇
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

        # 🔄 웹소켓 연결은 start() 메서드에서 안전하게 처리
        logger.info("🔄 웹소켓은 start() 시점에 연결됩니다")

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
            telegram_bot = TelegramBot(stock_bot_instance=self)  # 🆕 명시적 파라미터 전달
            telegram_bot.set_main_bot_reference(self)
            logger.info("✅ 텔레그램 봇 초기화 완료")
            return telegram_bot
        except Exception as e:
            logger.error(f"❌ 텔레그램 봇 초기화 실패: {e}")
            logger.info("📱 텔레그램 알림 없이 계속 진행")
            return None

    def _setup_existing_positions_sync(self):
        """보유 종목 자동 모니터링 설정 (동기 버전) - active_stocks 통합"""
        try:
            logger.info("📊 보유 종목 모니터링 설정 시작")
            
            balance = self.trading_manager.get_balance()
            holdings = balance.get('holdings', [])
            
            if not holdings:
                logger.info("보유 종목이 없습니다.")
                return
            
            logger.info(f"📈 보유 종목 {len(holdings)}개 발견 - 자동 모니터링 설정")
            
            # 🆕 웹소켓 준비 상태 확인 및 대기 (웹소켓 매니저 메서드 사용)
            if self.websocket_manager:
                self.websocket_manager.ensure_ready_for_subscriptions()
            
            # 🆕 기존 보유 종목을 strategy_scheduler에도 등록하기 위한 리스트
            existing_stock_codes = []
            
            for index, holding in enumerate(holdings):
                stock_code = holding.get('pdno', '')
                stock_name = holding.get('prdt_name', '')
                quantity = int(holding.get('hldg_qty', 0))
                current_price = int(holding.get('prpr', 0))
                avg_price = int(float(holding.get('pchs_avg_pric', current_price)))
                
                if stock_code and quantity > 0:
                    # 간소화된 전략 타입 결정 로직
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
                    
                    # 1️⃣ 포지션 매니저에 전략 타입과 함께 추가
                    self.position_manager.add_position(
                        stock_code=stock_code,
                        quantity=quantity,
                        buy_price=avg_price,
                        strategy_type=strategy_type
                    )
                    
                    # 2️⃣ 🆕 active_stocks 리스트에 추가
                    existing_stock_codes.append(stock_code)
                    
                    # 3️⃣ 🆕 데이터 관리자에도 실시간 모니터링 등록 (재시도 로직 포함)
                    try:
                        from core.hybrid_data_manager import DataPriority
                        
                        # 🔧 웹소켓 구독 안정성 보장 (웹소켓 매니저 메서드 사용)
                        subscription_success = self._safe_subscribe_stock(
                            stock_code=stock_code,
                            strategy_name="existing_holding",
                            max_retries=3,
                            retry_delay=0.5
                        )
                        
                        if subscription_success:
                            logger.info(f"📡 {stock_code} 실시간 모니터링 등록 성공")
                        else:
                            logger.warning(f"⚠️ {stock_code} 실시간 모니터링 등록 실패 - REST API 백업 사용")
                            
                    except Exception as e:
                        logger.error(f"데이터 관리자 등록 오류 ({stock_code}): {e}")
                    
                    logger.info(f"✅ 보유종목 등록: {stock_code}({stock_name}) {quantity:,}주 @ {avg_price:,}원")
                    
                    # 🕐 종목 간 간격 (웹소켓 안정성)
                    if index < len(holdings) - 1:  # 마지막이 아니면
                        time.sleep(0.5)  # 🔧 간격 증가 (300ms → 500ms)
            
            # 4️⃣ 🆕 strategy_scheduler의 active_stocks에 기존 보유 종목 추가
            if existing_stock_codes:
                if hasattr(self.strategy_scheduler, 'active_stocks'):
                    self.strategy_scheduler.active_stocks['existing_holding'] = existing_stock_codes
                    logger.info(f"📋 기존 보유 종목 {len(existing_stock_codes)}개를 active_stocks에 등록 완료")
                else:
                    logger.warning("⚠️ strategy_scheduler.active_stocks가 초기화되지 않음")
            
            logger.info(f"📊 보유 종목 자동 모니터링 설정 완료: {len(existing_stock_codes)}개 (통합 관리)")
            
        except Exception as e:
            logger.error(f"보유 종목 모니터링 설정 오류: {e}")

    def _safe_subscribe_stock(self, stock_code: str, strategy_name: str, 
                             max_retries: int = 3, retry_delay: float = 0.5) -> bool:
        """🔧 안전한 종목 구독 (재시도 로직 포함)"""
        try:
            from core.hybrid_data_manager import DataPriority
            
            for attempt in range(1, max_retries + 1):
                try:
                    logger.debug(f"📡 {stock_code} 구독 시도 {attempt}/{max_retries}")
                    
                    # 🆕 재시도 전에 기존 실패한 구독 상태 정리 (웹소켓 매니저 메서드 사용)
                    if attempt > 1:
                        if self.websocket_manager:
                            self.websocket_manager.cleanup_failed_subscription(stock_code)
                        time.sleep(0.3)  # 정리 후 잠시 대기
                    
                    # 구독 시도
                    success = self.data_manager.add_stock_request(
                        stock_code=stock_code,
                        priority=DataPriority.HIGH,
                        strategy_name=strategy_name,
                        callback=self._create_existing_holding_callback(stock_code)
                    )
                    
                    if success:
                        # 구독 성공 확인 (짧은 대기 후)
                        time.sleep(0.5)  # 더 충분한 대기 시간
                        
                        # 실제 구독 상태 확인
                        if self._verify_subscription_success(stock_code):
                            logger.debug(f"✅ {stock_code} 구독 성공 (시도 {attempt})")
                            return True
                        else:
                            logger.debug(f"⚠️ {stock_code} 구독 응답 성공이지만 실제 미구독 (시도 {attempt})")
                            # 🆕 실제 구독되지 않았으면 정리 (웹소켓 매니저 메서드 사용)
                            if self.websocket_manager:
                                self.websocket_manager.cleanup_failed_subscription(stock_code)
                    else:
                        logger.debug(f"❌ {stock_code} 구독 실패 (시도 {attempt})")
                        # 🆕 구독 실패 시 정리 (웹소켓 매니저 메서드 사용)
                        if self.websocket_manager:
                            self.websocket_manager.cleanup_failed_subscription(stock_code)
                    
                    # 재시도 전 대기
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                        retry_delay *= 1.5  # 점진적 대기 시간 증가
                    
                except Exception as e:
                    logger.debug(f"❌ {stock_code} 구독 시도 {attempt} 중 오류: {e}")
                    # 🆕 오류 시에도 정리 (웹소켓 매니저 메서드 사용)
                    if self.websocket_manager:
                        self.websocket_manager.cleanup_failed_subscription(stock_code)
                    if attempt < max_retries:
                        time.sleep(retry_delay)
            
            logger.warning(f"❌ {stock_code} 모든 구독 시도 실패 ({max_retries}회)")
            # 🆕 최종 실패 시에도 정리 (웹소켓 매니저 메서드 사용)
            if self.websocket_manager:
                self.websocket_manager.cleanup_failed_subscription(stock_code)
            return False
            
        except Exception as e:
            logger.error(f"안전한 구독 프로세스 오류 ({stock_code}): {e}")
            if self.websocket_manager:
                self.websocket_manager.cleanup_failed_subscription(stock_code)
            return False

    def _verify_subscription_success(self, stock_code: str) -> bool:
        """🔍 구독 성공 여부 확인"""
        try:
            if not self.data_manager:
                return False
            
            # 데이터 매니저의 구독 상태 확인
            status = self.data_manager.get_status()
            websocket_details = status.get('websocket_details', {})
            subscribed_stocks = websocket_details.get('subscribed_stocks', [])
            
            return stock_code in subscribed_stocks
            
        except Exception as e:
            logger.debug(f"구독 상태 확인 오류 ({stock_code}): {e}")
            return False

    def _create_existing_holding_callback(self, stock_code: str):
        """🆕 기존 보유 종목용 콜백 함수 생성"""
        def existing_holding_callback(stock_code: str, data: Dict, source: str = 'websocket') -> None:
            """기존 보유 종목 데이터 콜백"""
            try:
                # 기본 데이터 검증
                if not data or data.get('status') != 'success':
                    return

                current_price = data.get('current_price', 0)
                if current_price <= 0:
                    return

                # 포지션 매니저의 현재가 업데이트
                if hasattr(self, 'position_manager'):
                    self.position_manager._update_position_price(stock_code, current_price)

                # 변화율 기반 알림 (기존 보유 종목용)
                change_rate = data.get('change_rate', 0)
                
                # 큰 변화가 있을 때만 로그
                if abs(change_rate) >= 3.0:  # 3% 이상 변화
                    direction = "📈" if change_rate > 0 else "📉"
                    logger.info(f"{direction} 기존보유 {stock_code}: {current_price:,}원 ({change_rate:+.1f}%)")
                
                # 5% 이상 급등/급락 시 텔레그램 알림
                if abs(change_rate) >= 5.0 and hasattr(self, 'telegram_bot') and self.telegram_bot:
                    alert_msg = f"🚨 기존보유 종목 급변동\n📊 {stock_code}: {current_price:,}원 ({change_rate:+.1f}%)"
                    self.telegram_bot.send_message_async(alert_msg)

            except Exception as e:
                logger.error(f"기존 보유 종목 콜백 오류 ({stock_code}): {e}")

        return existing_holding_callback

    def _setup_existing_positions_threaded(self):
        """보유 종목 설정을 별도 스레드에서 실행 (더 안전한 버전)"""
        try:
            logger.info("📊 보유 종목 모니터링 설정 시작 (별도 스레드)")
            
            def run_setup():
                """별도 스레드에서 보유 종목 설정 실행"""
                try:
                    # 🔧 웹소켓이 완전히 준비될 때까지 더 오래 대기
                    time.sleep(5)  # 5초 대기 (기존 2초에서 증가)
                    
                    # 🆕 웹소켓 상태 강제 확인 및 재연결 (웹소켓 매니저 메서드 사용)
                    if self.websocket_manager:
                        self.websocket_manager.force_ready()
                    
                    self._setup_existing_positions_sync()
                except Exception as e:
                    logger.error(f"보유 종목 설정 오류: {e}")
            
            # 별도 스레드에서 실행
            setup_thread = threading.Thread(
                target=run_setup,
                name="ExistingPositionsSetup",
                daemon=True
            )
            setup_thread.start()
            
            logger.info("✅ 보유 종목 모니터링 설정 스레드 시작")
            
        except Exception as e:
            logger.error(f"보유 종목 설정 스레드 시작 실패: {e}")

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
            self.worker_manager.start_all_workers(self)
            
            # 🆕 전략 스케줄러 백그라운드 시작
            self._start_strategy_scheduler()
            
            # 🆕 텔레그램 봇 시작
            self._start_telegram_bot()
            
            # 🆕 웹소켓 연결 상태 확인 (이벤트 루프 충돌 방지)
            self._check_websocket_status()
            
            # 🆕 보유 종목 자동 모니터링 설정
            self._setup_existing_positions_threaded()
            
            logger.info("✅ StockBot 완전 가동!")
            
            # 🆕 텔레그램 시작 알림 전송
            if self.telegram_bot:
                self.telegram_bot.send_startup_notification()
            else:
                logger.debug("📱 텔레그램 봇이 비활성화되어 시작 알림을 보내지 않습니다")
            
            self._main_loop()

        except Exception as e:
            logger.error(f"❌ StockBot 시작 오류: {e}")
            self.stop()
        finally:
            self.is_running = False

    def _start_strategy_scheduler(self):
        """전략 스케줄러 백그라운드 시작"""
        try:
            logger.info("📅 전략 스케줄러 백그라운드 시작...")
            
            def run_scheduler():
                """스케줄러 실행 함수"""
                try:
                    # 새 이벤트 루프 생성
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    # 스케줄러 시작
                    loop.run_until_complete(self.strategy_scheduler.start_scheduler())
                except Exception as e:
                    logger.error(f"전략 스케줄러 실행 오류: {e}")
                finally:
                    loop.close()
            
            # 별도 스레드에서 스케줄러 실행
            scheduler_thread = threading.Thread(
                target=run_scheduler,
                name="StrategyScheduler",
                daemon=True
            )
            scheduler_thread.start()
            
            logger.info("✅ 전략 스케줄러 백그라운드 시작 완료")
            
        except Exception as e:
            logger.error(f"전략 스케줄러 시작 실패: {e}")

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
        
        # 초기 상태 확인 및 로깅
        initial_market_status = self._check_market_status()
        logger.info(f"📅 현재 시간: {initial_market_status['current_date']} {initial_market_status['current_time']} (KST)")
        logger.info(f"📈 시장 상태: {initial_market_status['status']}")
        
        if initial_market_status['is_premarket']:
            logger.info("🌅 프리마켓 시간 - 전략 스케줄러가 프리마켓 준비를 시작합니다")
        elif initial_market_status['is_open']:
            logger.info("📈 정규장 시간 - 전략 스케줄러가 활발히 동작 중입니다")
        elif initial_market_status['is_weekday']:
            logger.info("💤 장외시간 - 다음 거래시간까지 대기합니다")
        else:
            logger.info("📅 주말 - 평일 개장까지 대기합니다")

        loop_count = 0
        while self.is_running and not self.shutdown_event.is_set():
            try:
                loop_count += 1
                
                # 10분마다 상세 상태 로깅
                if loop_count % 60 == 1:  # 10초 * 60 = 10분
                    market_status = self._check_market_status()
                    system_status = self.get_system_status()
                    
                    logger.info(f"🔄 시스템 상태 체크 #{loop_count//60 + 1}")
                    logger.info(f"   📅 시간: {market_status['current_time']} ({market_status['status']})")
                    logger.info(f"   🤖 봇 가동시간: {system_status['uptime']//60:.0f}분")
                    logger.info(f"   📊 처리된 신호: {system_status['stats']['signals_processed']}개")
                    logger.info(f"   💰 실행된 주문: {system_status['stats']['orders_executed']}개")
                    logger.info(f"   🔗 웹소켓 연결: {'✅' if system_status['websocket_connected'] else '❌'}")
                    
                    # 전략 스케줄러 상태
                    scheduler_status = self.strategy_scheduler.get_status()
                    logger.info(f"   📅 현재 시간대: {scheduler_status.get('current_slot', 'None')}")
                    logger.info(f"   🎯 활성 전략: {scheduler_status.get('total_stocks', 0)}개 종목")

                # 기본 체크
                market_status = self._check_market_status()

                if market_status['is_trading_time']:
                    logger.debug(f"📈 거래시간 중 ({market_status['status']}) - 전략 스케줄러 활성")
                else:
                    logger.debug(f"💤 거래시간 외 ({market_status['status']})")

                self.shutdown_event.wait(timeout=10.0)

            except KeyboardInterrupt:
                logger.info("⌨️ 사용자 중단 요청")
                break
            except Exception as e:
                logger.error(f"❌ 메인 루프 오류: {e}")
                time.sleep(5)

        logger.info("🛑 메인 루프 종료")

    def handle_trading_signal(self, signal: dict):
        """거래 신호 처리 - TradeExecutor 사용"""
        try:
            logger.info(f"📊 거래 신호 수신: {signal.get('stock_code', 'UNKNOWN')}")
            
            # TradeExecutor를 통한 신호 처리
            result = self.trade_executor.handle_signal(signal)
            
            if result.get('success'):
                logger.info(f"✅ 거래 신호 처리 성공: {result.get('message', '')}")
                
                # 통계 업데이트
                self.stats['signals_processed'] += 1
                if result.get('order_executed'):
                    self.stats['orders_executed'] += 1
                    if signal.get('action') == 'BUY':
                        self.stats['positions_opened'] += 1
                    elif signal.get('action') == 'SELL':
                        self.stats['positions_closed'] += 1
            else:
                logger.warning(f"⚠️ 거래 신호 처리 실패: {result.get('message', '')}")
                
        except Exception as e:
            logger.error(f"❌ 거래 신호 처리 오류: {e}")

    def _check_market_status(self) -> dict:
        """시장 상태 확인"""
        try:
            from datetime import datetime
            
            # 한국 시간대 사용
            kst = pytz.timezone('Asia/Seoul')
            now = datetime.now(kst)
            
            # 평일 체크 (월~금: 0~4)
            is_weekday = now.weekday() < 5
            
            # 시장 시간 체크 (9:00~15:30)
            current_time = now.time()
            market_open = datetime.strptime("09:00", "%H:%M").time()
            market_close = datetime.strptime("15:30", "%H:%M").time()
            is_market_hours = market_open <= current_time <= market_close
            
            # 프리마켓 시간 체크 (8:30~9:00)
            premarket_open = datetime.strptime("08:30", "%H:%M").time()
            is_premarket = premarket_open <= current_time < market_open
            
            # 전체 거래 가능 시간 (프리마켓 + 정규장)
            is_trading_time = is_weekday and (is_premarket or is_market_hours)
            
            status_text = "휴장"
            if is_weekday:
                if is_premarket:
                    status_text = "프리마켓"
                elif is_market_hours:
                    status_text = "정규장"
                else:
                    status_text = "장외시간"
            else:
                status_text = "주말"
            
            return {
                'is_open': is_market_hours,
                'is_trading_time': is_trading_time,
                'is_premarket': is_premarket,
                'is_weekday': is_weekday,
                'current_time': now.strftime('%H:%M:%S'),
                'current_date': now.strftime('%Y-%m-%d'),
                'status': status_text,
                'kst_time': now
            }
        except Exception as e:
            logger.error(f"시장 상태 확인 오류: {e}")
            return {'is_open': False, 'status': '확인불가'}

    def _print_final_stats(self):
        """최종 통계 출력"""
        try:
            uptime = time.time() - self.stats['start_time']
            hours = int(uptime // 3600)
            minutes = int((uptime % 3600) // 60)
            
            print("\n" + "="*50)
            print("📊 StockBot 실행 통계")
            print("="*50)
            print(f"⏱️ 총 실행 시간: {hours}시간 {minutes}분")
            print(f"📈 처리된 신호: {self.stats['signals_processed']}개")
            print(f"💰 실행된 주문: {self.stats['orders_executed']}개")
            print(f"📊 열린 포지션: {self.stats['positions_opened']}개")
            print(f"💵 닫힌 포지션: {self.stats['positions_closed']}개")
            print("="*50)
            
        except Exception as e:
            logger.error(f"최종 통계 출력 오류: {e}")

    def _signal_handler(self, signum, frame):
        """시그널 핸들러"""
        logger.info("⌨️ 종료 신호 받음")
        self.stop()

    def get_system_status(self) -> dict:
        """시스템 상태 조회"""
        try:
            return {
                'is_running': self.is_running,
                'uptime': time.time() - self.stats['start_time'],
                'stats': self.stats.copy(),
                'market_status': self._check_market_status(),
                'positions': len(self.position_manager.get_positions('active')),
                'websocket_connected': (
                    self.websocket_manager.is_connected 
                    if self.websocket_manager else False
                )
            }
        except Exception as e:
            logger.error(f"시스템 상태 조회 오류: {e}")
            return {'error': str(e)}

    def _start_telegram_bot(self):
        """텔레그램 봇 시작"""
        if self.telegram_bot:
            try:
                # 봇이 이미 실행 중인지 더 확실히 확인
                if hasattr(self.telegram_bot, 'running') and self.telegram_bot.running:
                    logger.info("📨 텔레그램 봇이 이미 실행 중입니다")
                    return
                
                if hasattr(self.telegram_bot, 'application') and self.telegram_bot.application:
                    if hasattr(self.telegram_bot.application, 'running') and self.telegram_bot.application.running:
                        logger.info("📨 텔레그램 애플리케이션이 이미 실행 중입니다")
                        return
                
                logger.info("📨 텔레그램 봇 시작 시도")
                
                # 기존 인스턴스 정리
                if hasattr(self.telegram_bot, 'stop_bot'):
                    try:
                        self.telegram_bot.stop_bot()
                        time.sleep(1)  # 정리 대기
                    except Exception as e:
                        logger.debug(f"기존 텔레그램 봇 정리 중 오류 (무시): {e}")
                
                # 새로 시작
                self.telegram_bot.start_bot()
                
                # 시작 확인
                time.sleep(2)  # 충분한 대기
                
                if hasattr(self.telegram_bot, 'running') and self.telegram_bot.running:
                    logger.info("✅ 텔레그램 봇 시작 완료")
                else:
                    logger.warning("⚠️ 텔레그램 봇 시작 상태 확인 불가")
                    
            except Exception as e:
                logger.error(f"❌ 텔레그램 봇 시작 실패: {e}")
                logger.info("📱 텔레그램 봇이 비활성화되어 시작하지 않습니다")
        else:
            logger.debug("📱 텔레그램 봇이 비활성화되어 시작하지 않습니다")

    def _check_websocket_status(self):
        """웹소켓 연결 상태 확인 (안전한 방식)"""
        try:
            logger.info("🔗 웹소켓 연결 상태 확인")
            
            if self.websocket_manager:
                # 현재 연결 상태만 확인
                is_connected = getattr(self.websocket_manager, 'is_connected', False)
                
                if is_connected:
                    logger.info("✅ 웹소켓 이미 연결됨")
                else:
                    logger.warning("⚠️ 웹소켓 연결되지 않음 - 백그라운드에서 자동 연결됩니다")
                    
                    # 별도 스레드에서 안전하게 연결 시도
                    def websocket_connect_thread():
                        try:
                            time.sleep(2)  # 초기화 완료 대기
                            self.websocket_manager.ensure_connection()
                            logger.info("✅ 웹소켓 백그라운드 연결 완료")
                        except Exception as e:
                            logger.error(f"❌ 웹소켓 백그라운드 연결 실패: {e}")
                    
                    import threading
                    websocket_thread = threading.Thread(
                        target=websocket_connect_thread,
                        name="WebSocketConnect",
                        daemon=True
                    )
                    websocket_thread.start()
            else:
                logger.error("❌ 웹소켓 매니저가 없습니다")
                
        except Exception as e:
            logger.error(f"❌ 웹소켓 상태 확인 실패: {e}")
            logger.warning("⚠️ 웹소켓은 필수 구성요소입니다 - 연결 문제를 확인하세요")


def main():
    """메인 함수"""
    try:
        logger.info("🚀 StockBot 시작...")
        
        bot = StockBot()
        bot.start()
        
    except KeyboardInterrupt:
        logger.info("⌨️ 사용자가 중단했습니다.")
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")
    finally:
        logger.info("👋 StockBot 종료")


if __name__ == "__main__":
    main()
