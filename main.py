#!/usr/bin/env python3
"""
StockBot 메인 실행 파일 (리팩토링 버전)
🕯️ CandleTradeManager를 이용한 캔들 패턴 기반 트레이딩 시스템
"""
import sys
import time
import signal
import asyncio
import threading
from pathlib import Path
from typing import Optional, Dict, TYPE_CHECKING
import pytz
import os
from dotenv import load_dotenv

# 프로젝트 루트 경로 설정
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# 로깅 설정
from utils.logger import setup_logger

# 핵심 모듈들
from core import (
    TradingManager, PositionManager,
    KISRestAPIManager, SimpleHybridDataManager, KISWebSocketManager,
    TradeDatabase, TradeConfig, TradeExecutor, WorkerManager, KISDataCollector
)

# 설정
from config.settings import (
    IS_DEMO, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, LOG_LEVEL
)

# 🆕 TYPE_CHECKING을 이용한 순환 import 방지
if TYPE_CHECKING:
    from telegram_bot.telegram_manager import TelegramBot

logger = setup_logger(__name__)

# 🆕 텔레그램 봇 조건부 import
TelegramBotClass = None
try:
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        from telegram_bot.telegram_manager import TelegramBot as TelegramBotClass
        logger.info("✅ 텔레그램 봇 모듈 로드 완료")
except ImportError as e:
    logger.warning(f"⚠️ 텔레그램 봇 모듈 로드 실패: {e}")
    logger.info("📱 텔레그램 알림 없이 계속 진행")
except Exception as e:
    logger.warning(f"⚠️ 텔레그램 봇 설정 오류: {e}")
    logger.info("📱 텔레그램 알림 없이 계속 진행")

# 프로젝트 루트 디렉토리 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 환경변수 로드
load_dotenv()

# 🆕 새로운 캔들 트레이딩 시스템
from core.strategy.candle_trade_manager import CandleTradeManager
from core.strategy.candle_stock_manager import CandleStockManager
from core.strategy.candle_pattern_detector import CandlePatternDetector

class StockBot:
    """간소화된 메인 StockBot 클래스 - 오케스트레이션 역할"""

    def __init__(self):
        """초기화"""
        self.is_running = False
        self.shutdown_event = threading.Event()

        logger.info("📈 StockBot 시작 중...")

        try:
            # 1. REST API 관리자 (단일 인스턴스)
            logger.info("🔑 KIS API 연결 중...")
            self.rest_api = KISRestAPIManager()
            logger.info("✅ KIS API 연결 성공")
        except Exception as e:
            logger.error(f"❌ KIS API 연결 실패: {e}")
            logger.error("📋 해결 방법:")
            logger.error("  1. .env 파일이 프로젝트 루트 디렉토리에 있는지 확인")
            logger.error("  2. .env 파일에 실제 KIS API 키를 입력했는지 확인")
            logger.error("  3. 네트워크 연결 상태 확인")
            logger.error("🛑 StockBot을 중단합니다.")
            raise SystemExit(1)

        # 2. 웹소켓 관리자 (단일 인스턴스)
        self.websocket_manager = KISWebSocketManager()

        # 3. 데이터 수집기 (단일 인스턴스)
        self.data_collector = KISDataCollector(
            websocket_manager=self.websocket_manager,
            rest_api_manager=self.rest_api
        )

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

        # 6. 거래 데이터베이스
        self.trade_db = TradeDatabase()

        # 7. 거래 실행자 (핵심 비즈니스 로직 분리)
        trade_config = TradeConfig()  # 기본 설정 사용
        self.trade_executor = TradeExecutor(
            self.trading_manager,
            self.data_manager,
            self.trade_db,
            trade_config
        )

        # 🆕 캔들 기반 트레이딩 매니저 (기존 전략 대체)
        self.candle_trade_manager = CandleTradeManager(
            kis_api_manager=self.rest_api,
            data_manager=self.data_manager,
            trade_executor=self.trade_executor,
            websocket_manager=self.websocket_manager  # 🆕 웹소켓 매니저 전달
        )

        # 10. 워커 매니저 (스레드 관리 전담)
        self.worker_manager = WorkerManager(self.shutdown_event)

        # 11. 텔레그램 봇 (타입 힌트 수정)
        self.telegram_bot: Optional["TelegramBot"] = self._initialize_telegram_bot()

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

    def _initialize_telegram_bot(self) -> Optional["TelegramBot"]:
        """텔레그램 봇 조건부 초기화"""
        if not TelegramBotClass:
            logger.info("📱 텔레그램 봇 비활성화 (모듈 로드 실패)")
            return None

        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            logger.info("📱 텔레그램 봇 비활성화 (설정 누락)")
            return None

        try:
            # 올바른 매개변수로 텔레그램 봇 초기화
            telegram_bot = TelegramBotClass(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
            # StockBot 인스턴스 설정
            telegram_bot.set_stock_bot(self)
            logger.info("✅ 텔레그램 봇 초기화 완료")
            return telegram_bot
        except Exception as e:
            logger.error(f"❌ 텔레그램 봇 초기화 실패: {e}")
            logger.info("📱 텔레그램 알림 없이 계속 진행")
            return None

    def start(self):
        """StockBot 시작"""
        if self.is_running:
            logger.warning("StockBot이 이미 실행 중입니다.")
            return

        try:
            self.is_running = True
            logger.info("🚀 StockBot 시작!")

            # 신호 핸들러 등록 (우아한 종료를 위해)
            signal.signal(signal.SIGINT, self._signal_handler)

            # 🆕 워커 매니저를 통한 백그라운드 작업 시작
            self.worker_manager.start_all_workers(self)

            # 🆕 캔들 트레이딩 시스템 시작 (기존 전략 스케줄러 대체)
            self._start_candle_trading_system()

            # 🆕 텔레그램 봇 시작
            self._start_telegram_bot()

            # 🆕 웹소켓 연결 상태 확인 (이벤트 루프 충돌 방지)
            self._check_websocket_status()

            logger.info("✅ StockBot 완전 가동!")

            # 🆕 텔레그램 시작 알림 전송
            if self.telegram_bot:
                self.telegram_bot.send_startup_notification()

            self._main_loop()

        except Exception as e:
            logger.error(f"❌ StockBot 시작 오류: {e}")
            self.stop()
        finally:
            self.is_running = False

    def _start_candle_trading_system(self):
        """🕯️ 캔들 트레이딩 시스템 시작"""
        try:
            logger.info("🕯️ 캔들 트레이딩 시스템 시작")

            def run_candle_trading():
                """캔들 트레이딩 백그라운드 실행"""
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                    async def candle_trading_main():
                        """캔들 트레이딩 메인 루프"""
                        try:
                            logger.info("🚀 캔들 트레이딩 메인 루프 시작")

                            # 🆕 CandleTradeManager의 start_trading() 메서드 호출
                            logger.info("🕯️ CandleTradeManager.start_trading() 시작")
                            await self.candle_trade_manager.start_trading()

                        except Exception as e:
                            logger.error(f"캔들 트레이딩 메인 루프 오류: {e}")

                    # 비동기 루프 실행
                    loop.run_until_complete(candle_trading_main())

                except Exception as e:
                    logger.error(f"캔들 트레이딩 시스템 오류: {e}")
                finally:
                    if 'loop' in locals():
                        loop.close()

            # 별도 스레드에서 실행
            candle_thread = threading.Thread(
                target=run_candle_trading,
                name="CandleTradingSystem",
                daemon=True
            )
            candle_thread.start()

            logger.info("✅ 캔들 트레이딩 시스템 백그라운드 시작 완료")

        except Exception as e:
            logger.error(f"캔들 트레이딩 시스템 시작 오류: {e}")

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

        # 🆕 캔들 트레이딩 시스템 중지 (기존 전략 스케줄러 대체)
        try:
            self.candle_trade_manager.stop_trading()
            logger.info("✅ 캔들 트레이딩 시스템 정리 완료")
        except Exception as e:
            logger.error(f"❌ 캔들 트레이딩 시스템 정리 오류: {e}")

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
            # 시스템 정리 (새로운 캔들 시스템만 정리)
            if hasattr(self, 'candle_trade_manager'):
                self.candle_trade_manager.stop_trading()

            if hasattr(self, 'trading_manager') and hasattr(self.trading_manager, 'cleanup'):
                self.trading_manager.cleanup()
            logger.info("✅ 모든 관리자 정리 완료")
        except Exception as e:
            logger.error(f"❌ 관리자 정리 오류: {e}")

        self._print_final_stats()
        logger.info("🛑 StockBot 종료 완료")

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

                    # 🆕 캔들 트레이딩 시스템 상태 (기존 전략 스케줄러 대체)
                    candle_status = self.candle_trade_manager.get_current_status()
                    logger.info(f"   🕯️ 관찰 중인 종목: {candle_status.get('watching_stocks', 0)}개")
                    logger.info(f"   📈 활성 포지션: {candle_status.get('active_positions', 0)}개")

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
            # 캔들 트레이딩 시스템 상태 조회
            candle_stats = {'total_trades': 0, 'successful_trades': 0, 'failed_trades': 0}
            if hasattr(self, 'candle_trade_manager') and hasattr(self.candle_trade_manager, 'get_current_status'):
                candle_status = self.candle_trade_manager.get_current_status()
                candle_stats.update(candle_status.get('daily_stats', {}))

            return {
                'is_running': self.is_running,
                'uptime': time.time() - self.stats['start_time'],
                'stats': self.stats.copy(),  # 기본 통계 (signals_processed 포함)
                'market_status': self._check_market_status(),
                'positions': 0,  # 새로운 시스템에서 관리
                'websocket_connected': (
                    self.websocket_manager.is_connected
                    if self.websocket_manager else False
                ),
                'data_collector_running': (
                    self.data_collector is not None and
                    hasattr(self.data_collector, 'is_running') and
                    getattr(self.data_collector, 'is_running', False)
                ),
                'trading_active': (
                    hasattr(self, 'candle_trade_manager') and
                    hasattr(self.candle_trade_manager, 'is_running') and
                    self.candle_trade_manager.is_running
                ),
                'active_positions': 0,  # 새로운 시스템에서 관리
                'candle_stats': candle_stats  # 캔들 트레이딩 통계는 별도 키로
            }
        except Exception as e:
            logger.error(f"시스템 상태 조회 오류: {e}")
            return {'error': str(e)}

    def get_status(self) -> dict:
        """텔레그램 봇용 상태 조회 (별칭)"""
        try:
            system_status = self.get_system_status()

            # 🆕 캔들 트레이딩 시스템 상태 추가 (기존 스케줄러 상태 대체)
            candle_status = {}
            if hasattr(self.candle_trade_manager, 'get_current_status'):
                candle_status = self.candle_trade_manager.get_current_status()

            return {
                'bot_running': self.is_running,
                'websocket_connected': system_status.get('websocket_connected', False),
                'api_connected': True,  # REST API는 항상 사용 가능하다고 가정
                'data_collector_running': system_status.get('data_collector_running', False),
                'candle_trading': candle_status,  # 🆕 캔들 트레이딩 상태
                'uptime': system_status.get('uptime', 0),
                'stats': system_status.get('stats', {}),
                'positions_count': system_status.get('positions', 0)
            }
        except Exception as e:
            logger.error(f"상태 조회 오류: {e}")
            return {
                'bot_running': self.is_running,
                'websocket_connected': False,
                'api_connected': False,
                'data_collector_running': False,
                'error': str(e)
            }

    def shutdown(self):
        """시스템 종료 (텔레그램 봇용)"""
        logger.info("🛑 텔레그램 봇에서 종료 요청")
        self.stop()

    def _start_telegram_bot(self):
        """텔레그램 봇 시작"""
        if self.telegram_bot:
            try:
                # 봇이 이미 실행 중인지 더 확실히 확인
                if hasattr(self.telegram_bot, 'running') and getattr(self.telegram_bot, 'running', False):
                    logger.info("📨 텔레그램 봇이 이미 실행 중입니다")
                    return

                if hasattr(self.telegram_bot, 'application') and getattr(self.telegram_bot, 'application', None):
                    application = getattr(self.telegram_bot, 'application', None)
                    if application and hasattr(application, 'running') and getattr(application, 'running', False):
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
                if hasattr(self.telegram_bot, 'start_bot'):
                    self.telegram_bot.start_bot()
                else:
                    logger.warning("⚠️ 텔레그램 봇에 start_bot 메서드가 없습니다")
                    return

                # 시작 확인
                time.sleep(2)  # 충분한 대기

                if hasattr(self.telegram_bot, 'running') and getattr(self.telegram_bot, 'running', False):
                    logger.info("✅ 텔레그램 봇 시작 완료")
                else:
                    logger.warning("⚠️ 텔레그램 봇 시작 상태 확인 불가")

            except Exception as e:
                logger.error(f"❌ 텔레그램 봇 시작 실패: {e}")
                logger.info("📱 텔레그램 봇이 비활성화되어 시작하지 않습니다")
        else:
            logger.debug("📱 텔레그램 봇이 비활성화되어 시작하지 않습니다")

    def _check_websocket_status(self):
        """웹소켓 상태 확인 (안전한 방식 - RuntimeWarning 방지)"""
        try:
            logger.info("🔍 웹소켓 상태 확인")

            if self.websocket_manager:
                # 🔧 단순한 상태 확인만 수행 (연결 시도 없이)
                is_connected = getattr(self.websocket_manager, 'is_connected', False)
                is_healthy = getattr(self.websocket_manager, 'is_healthy', lambda: False)()

                logger.info(f"📡 웹소켓 연결 상태: {'✅' if is_connected else '❌'}")
                logger.info(f"🏥 웹소켓 건강 상태: {'✅' if is_healthy else '❌'}")

                if is_connected and is_healthy:
                    logger.info("✅ 웹소켓 상태 정상")

                    # 구독 정보도 확인
                    try:
                        subscribed_count = len(self.websocket_manager.get_subscribed_stocks())
                        usage = self.websocket_manager.get_websocket_usage()
                        logger.info(f"📊 구독 현황: {subscribed_count}개 종목, 사용량: {usage}")
                    except Exception as e:
                        logger.debug(f"구독 정보 확인 중 오류: {e}")

                elif is_connected and not is_healthy:
                    logger.warning("⚠️ 웹소켓이 연결되었지만 상태가 불안정합니다")
                    logger.info("💡 백그라운드에서 자동 복구가 진행됩니다")
                else:
                    logger.warning("❌ 웹소켓이 연결되지 않았습니다")
                    logger.info("💡 백그라운드에서 자동 연결이 시도됩니다")

                    # 🆕 비동기 연결 시도 (별도 스레드에서 안전하게)
                    def async_reconnect():
                        try:
                            if hasattr(self.websocket_manager, 'start'):
                                self.websocket_manager.start()
                                logger.info("🔄 웹소켓 재연결 백그라운드 시도 완료")
                        except Exception as e:
                            logger.debug(f"백그라운드 재연결 시도 중 오류: {e}")

                    # 별도 스레드에서 연결 시도 (메인 스레드 블로킹 방지)
                    import threading
                    reconnect_thread = threading.Thread(
                        target=async_reconnect,
                        name="WebSocketReconnect",
                        daemon=True
                    )
                    reconnect_thread.start()

            else:
                logger.warning("⚠️ 웹소켓 매니저가 초기화되지 않았습니다")

        except Exception as e:
            logger.error(f"웹소켓 상태 확인 오류: {e}")
            logger.info("💡 웹소켓 매니저가 백그라운드에서 자동으로 관리됩니다")

def main():
    """메인 함수"""
    try:
        bot = StockBot()
        bot.start()

    except KeyboardInterrupt:
        print("DEBUG: KeyboardInterrupt 발생")
        logger.info("⌨️ 사용자가 중단했습니다.")
    except Exception as e:
        print(f"DEBUG: Exception 발생: {e}")
        logger.error(f"❌ 예상치 못한 오류: {e}")
    finally:
        print("DEBUG: main() 함수 종료")
        logger.info("👋 StockBot 종료")

if __name__ == "__main__":
    main()
