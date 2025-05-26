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
from core.websocket_manager import KISWebSocketManager

# 텔레그램 봇
from telegram_bot.bot import TelegramBot

# 설정
from config.settings import (
    IS_DEMO, TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_ID, LOG_LEVEL
)

logger = setup_logger(__name__)


class StockBot:
    """간소화된 메인 StockBot 클래스 - StrategyScheduler 기반"""

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

        # 5. 전략 스케줄러 (핵심!)
        self.strategy_scheduler = StrategyScheduler(self.rest_api, self.data_manager)
        self.strategy_scheduler.set_bot_instance(self)

        # 6. 웹소켓 관리자 (선택적)
        self.websocket_manager = None
        try:
            self.websocket_manager = KISWebSocketManager(is_demo=is_demo)
            logger.info("✅ WebSocket 관리자 초기화 완료")
        except Exception as e:
            logger.warning(f"⚠️ WebSocket 관리자 초기화 실패: {e}")

        # 7. 텔레그램 봇 (선택적)
        self.telegram_bot = None
        if TELEGRAM_BOT_TOKEN and TELEGRAM_ADMIN_ID:
            try:
                self.telegram_bot = TelegramBot(stock_bot_instance=self)
                logger.info("✅ 텔레그램 봇 초기화 완료")
            except Exception as e:
                logger.warning(f"⚠️ 텔레그램 봇 초기화 실패: {e}")

        # 통계
        self.stats = {
            'start_time': time.time(),
            'signals_processed': 0,
            'orders_executed': 0,
            'positions_opened': 0,
            'positions_closed': 0
        }

        logger.info("🚀 StockBot 초기화 완료!")

    def start(self):
        """StockBot 시작"""
        if self.is_running:
            logger.warning("StockBot이 이미 실행 중입니다.")
            return

        try:
            self.is_running = True
            logger.info("🔄 StockBot 가동 시작...")

            # 신호 처리기 등록
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)

            # 백그라운드 작업 시작
            self._start_background_workers()

            # 텔레그램 봇 시작
            if self.telegram_bot:
                self._start_telegram_bot()

            # 전략 스케줄러 시작 (메인 로직)
            self._start_strategy_scheduler()

            logger.info("✅ StockBot 완전 가동!")

            # 메인 루프
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
                # WebSocket 관리자 정리 (cleanup 메서드 사용)
                if hasattr(self.websocket_manager, 'cleanup'):
                    self.websocket_manager.cleanup()
                logger.info("✅ WebSocket 관리자 정리 완료")
            except Exception as e:
                logger.error(f"❌ WebSocket 정리 오류: {e}")

        # 텔레그램 봇 정리
        if self.telegram_bot:
            try:
                self.telegram_bot.stop_bot()
                logger.info("✅ 텔레그램 봇 정리 완료")
            except Exception as e:
                logger.error(f"❌ 텔레그램 봇 정리 오류: {e}")

        # 관리자들 정리
        try:
            self.position_manager.cleanup()
            self.trading_manager.cleanup()
            logger.info("✅ 모든 관리자 정리 완료")
        except Exception as e:
            logger.error(f"❌ 관리자 정리 오류: {e}")

        # 최종 통계 출력
        self._print_final_stats()

        logger.info("🏁 StockBot 종료 완료")

    def _start_background_workers(self):
        """백그라운드 작업 시작"""
        # 포지션 관리 작업
        position_thread = threading.Thread(
            target=self._position_worker,
            name="PositionWorker",
            daemon=True
        )
        position_thread.start()
        logger.info("✅ 포지션 관리 스레드 시작")

        # 통계 수집 작업
        stats_thread = threading.Thread(
            target=self._stats_worker,
            name="StatsWorker",
            daemon=True
        )
        stats_thread.start()
        logger.info("✅ 통계 수집 스레드 시작")

    def _start_telegram_bot(self):
        """텔레그램 봇 시작"""
        try:
            def run_telegram_bot():
                self.telegram_bot.start_bot()

            telegram_thread = threading.Thread(
                target=run_telegram_bot,
                name="TelegramBot",
                daemon=True
            )
            telegram_thread.start()
            logger.info("✅ 텔레그램 봇 시작 완료")
        except Exception as e:
            logger.error(f"❌ 텔레그램 봇 시작 오류: {e}")

    def _start_strategy_scheduler(self):
        """전략 스케줄러 시작"""
        try:
            def run_strategy_scheduler():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(self.strategy_scheduler.start_scheduler())
                except Exception as e:
                    logger.error(f"전략 스케줄러 실행 오류: {e}")
                finally:
                    loop.close()

            scheduler_thread = threading.Thread(
                target=run_strategy_scheduler,
                name="StrategyScheduler",
                daemon=True
            )
            scheduler_thread.start()
            logger.info("✅ 전략 스케줄러 시작 완료")
        except Exception as e:
            logger.error(f"❌ 전략 스케줄러 시작 오류: {e}")

    def _main_loop(self):
        """메인 실행 루프"""
        logger.info("🔄 메인 루프 시작")

        while self.is_running and not self.shutdown_event.is_set():
            try:
                # 시장 상황 체크
                market_status = self._check_market_status()

                if market_status['is_open']:
                    logger.debug("📈 시장 개장 중 - 전략 스케줄러가 관리")
                else:
                    logger.debug("💤 시장 휴장 중")

                # 10초마다 체크
                self.shutdown_event.wait(timeout=10.0)

            except KeyboardInterrupt:
                logger.info("⌨️ 사용자 중단 요청")
                break
            except Exception as e:
                logger.error(f"❌ 메인 루프 오류: {e}")
                time.sleep(5)

        logger.info("🛑 메인 루프 종료")

    def _position_worker(self):
        """포지션 관리 백그라운드 작업"""
        logger.info("포지션 관리 작업 시작")

        while self.is_running and not self.shutdown_event.is_set():
            try:
                # 포지션별 현재가 업데이트
                self.position_manager.update_position_prices()

                # 1분마다 업데이트
                self.shutdown_event.wait(timeout=60)

            except Exception as e:
                logger.error(f"포지션 관리 작업 오류: {e}")
                self.shutdown_event.wait(timeout=120)

    def _stats_worker(self):
        """통계 수집 백그라운드 작업"""
        logger.info("통계 수집 작업 시작")

        while self.is_running and not self.shutdown_event.is_set():
            try:
                # 5분마다 통계 업데이트
                self._update_stats()

                # 1시간마다 상세 리포트
                if int(time.time()) % 3600 < 300:  # 정시 5분 이내
                    self._generate_hourly_report()

                self.shutdown_event.wait(timeout=300)  # 5분 간격

            except Exception as e:
                logger.error(f"통계 수집 작업 오류: {e}")
                self.shutdown_event.wait(timeout=300)

    def _check_market_status(self) -> dict:
        """시장 상태 확인"""
        try:
            current_time = time.localtime()
            hour = current_time.tm_hour
            minute = current_time.tm_min
            weekday = current_time.tm_wday  # 0=월요일, 6=일요일

            # 주말 체크
            if weekday >= 5:  # 토요일, 일요일
                return {'is_open': False, 'reason': '주말'}

            # 평일 장 시간 체크 (9:00~15:30)
            current_minutes = hour * 60 + minute
            market_open = 9 * 60  # 09:00
            market_close = 15 * 60 + 30  # 15:30

            if market_open <= current_minutes <= market_close:
                return {'is_open': True, 'session': 'regular'}
            else:
                return {'is_open': False, 'reason': '장외시간'}

        except Exception as e:
            logger.error(f"시장 상태 확인 오류: {e}")
            return {'is_open': False, 'reason': '오류'}

    def _update_stats(self):
        """통계 업데이트"""
        try:
            # 포지션 통계
            position_summary = self.position_manager.get_position_summary()

            # 전략 스케줄러 통계
            scheduler_stats = self.strategy_scheduler.get_status()

            # 통계 업데이트
            self.stats.update({
                'current_positions': position_summary.get('total_positions', 0),
                'total_value': position_summary.get('total_value', 0),
                'active_strategies': len(scheduler_stats.get('active_strategies', {}))
            })

        except Exception as e:
            logger.error(f"통계 업데이트 오류: {e}")

    def _generate_hourly_report(self):
        """1시간마다 상세 리포트 생성"""
        try:
            report = self._generate_status_report()
            logger.info(f"📊 1시간 리포트:\n{report}")

            # 텔레그램 전송
            if self.telegram_bot:
                self._send_telegram_notification(f"📊 1시간 리포트\n{report}")

        except Exception as e:
            logger.error(f"리포트 생성 오류: {e}")

    def _generate_status_report(self) -> str:
        """상태 리포트 생성"""
        try:
            runtime = time.time() - self.stats['start_time']
            runtime_hours = runtime / 3600

            # 전략 스케줄러 상태
            scheduler_status = self.strategy_scheduler.get_status()

            # 포지션 요약
            position_summary = self.position_manager.get_position_summary()

            report = (
                f"🕐 실행시간: {runtime_hours:.1f}시간\n"
                f"📈 활성포지션: {position_summary.get('total_positions', 0)}개\n"
                f"💰 총평가금액: {position_summary.get('total_value', 0):,}원\n"
                f"📊 수익률: {position_summary.get('total_profit_rate', 0):.2f}%\n"
                f"🎯 현재전략: {scheduler_status.get('current_phase', 'N/A')}\n"
                f"📋 활성전략수: {len(scheduler_status.get('active_strategies', {}))}\n"
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

            logger.info("=" * 50)
            logger.info("📊 StockBot 최종 통계")
            logger.info("=" * 50)
            logger.info(f"🕐 총 실행시간: {runtime/3600:.2f}시간")
            logger.info(f"📊 처리한 신호: {self.stats['signals_processed']}개")
            logger.info(f"📋 실행한 주문: {self.stats['orders_executed']}개")
            logger.info(f"📈 열린 포지션: {self.stats['positions_opened']}개")
            logger.info(f"📉 닫힌 포지션: {self.stats['positions_closed']}개")
            logger.info("=" * 50)

        except Exception as e:
            logger.error(f"최종 통계 출력 오류: {e}")

    def _send_telegram_notification(self, message: str):
        """텔레그램 알림 전송"""
        if self.telegram_bot:
            try:
                self.telegram_bot.send_notification_sync(message)
            except Exception as e:
                logger.error(f"텔레그램 알림 전송 오류: {e}")

    def _signal_handler(self, signum, frame):
        """시스템 신호 처리"""
        logger.info(f"🛑 종료 신호 수신: {signum}")
        self.stop()

    # === 전략 스케줄러에서 호출되는 메서드들 ===

    def handle_trading_signal(self, signal: dict):
        """거래 신호 처리 (전략 스케줄러에서 호출)"""
        try:
            logger.info(f"📊 거래신호 수신: {signal['stock_code']} {signal['signal_type']}")

            # 신호 통계 업데이트
            self.stats['signals_processed'] += 1

            # 텔레그램 알림
            if self.telegram_bot:
                self._send_signal_notification(signal)

            # 실제 거래는 여기서 구현
            # TODO: 실제 매수/매도 로직 추가

        except Exception as e:
            logger.error(f"거래 신호 처리 오류: {e}")

    def _send_signal_notification(self, signal: dict):
        """신호 알림 전송"""
        message = (
            f"📊 거래신호 감지\n"
            f"종목: {signal['stock_code']}\n"
            f"신호: {signal['signal_type']}\n"
            f"전략: {signal['strategy']}\n"
            f"가격: {signal.get('price', 'N/A')}\n"
            f"시간: {datetime.now().strftime('%H:%M:%S')}"
        )
        self._send_telegram_notification(message)

    # === 텔레그램 봇용 인터페이스 메서드들 ===

    def get_balance(self) -> dict:
        """잔고 조회 (텔레그램용)"""
        return self.trading_manager.get_balance()

    def get_positions(self) -> dict:
        """포지션 조회 (텔레그램용)"""
        return self.position_manager.get_position_summary()

    def get_strategy_status(self) -> dict:
        """전략 상태 조회 (텔레그램용)"""
        return self.strategy_scheduler.get_status()


def main():
    """메인 실행 함수"""
    try:
        # StockBot 인스턴스 생성
        bot = StockBot()

        # 시작
        bot.start()

    except KeyboardInterrupt:
        logger.info("⌨️ 사용자 중단")
    except Exception as e:
        logger.error(f"❌ 메인 실행 오류: {e}")
    finally:
        logger.info("🏁 프로그램 종료")


if __name__ == "__main__":
    main()
