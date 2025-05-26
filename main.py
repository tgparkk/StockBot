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
from core.kis_websocket_manager import KISWebSocketManager

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
                if self.telegram_bot:
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

                # 매도 조건 확인 및 자동 매도 실행
                sell_signals = self.position_manager.check_exit_conditions()
                for sell_signal in sell_signals:
                    try:
                        logger.info(f"🚨 매도 조건 발생: {sell_signal['stock_code']} - {sell_signal['reason']}")

                                                # 자동 매도용 지정가 계산
                        current_price = sell_signal['current_price']
                        strategy_type = sell_signal['strategy_type']
                        auto_sell_price = self._calculate_sell_price(current_price, strategy_type, is_auto_sell=True)

                        # 자동 매도 실행 (지정가)
                        order_no = self.trading_manager.execute_order(
                            stock_code=sell_signal['stock_code'],
                            order_type="SELL",
                            quantity=sell_signal['quantity'],
                            price=auto_sell_price,  # 계산된 자동매도 지정가
                            strategy_type=f"auto_sell_{sell_signal['reason']}"
                        )

                        if order_no:
                            # 포지션 제거
                            self.position_manager.remove_position(
                                sell_signal['stock_code'],
                                sell_signal['quantity'],
                                auto_sell_price
                            )

                            self.stats['orders_executed'] += 1
                            self.stats['positions_closed'] += 1

                            logger.info(f"✅ 자동 매도 주문 완료: {sell_signal['stock_code']} {sell_signal['quantity']:,}주 @ {auto_sell_price:,}원 (현재가: {current_price:,}원)")

                            # 텔레그램 알림 (업데이트된 정보로)
                            sell_signal['auto_sell_price'] = auto_sell_price
                            if self.telegram_bot:
                                self._send_auto_sell_notification(sell_signal, order_no)

                    except Exception as e:
                        logger.error(f"자동 매도 실행 오류: {sell_signal['stock_code']} - {e}")

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

    def _calculate_buy_price(self, current_price: int, strategy: str = 'default') -> int:
        """매수 지정가 계산 (현재가 기준)"""
        try:
            # 전략별 매수 프리미엄 설정
            buy_premiums = {
                'gap_trading': 0.01,      # 갭 거래: 1.0% 위
                'volume_breakout': 0.012,  # 거래량 돌파: 1.2% 위
                'momentum': 0.015,         # 모멘텀: 1.5% 위
                'default': 0.005           # 기본: 0.5% 위
            }

            premium = buy_premiums.get(strategy, buy_premiums['default'])

            # 계산된 매수가 (상승여력 고려)
            buy_price = int(current_price * (1 + premium))

            # 호가 단위로 조정
            buy_price = self._adjust_to_tick_size(buy_price)

            logger.debug(f"매수가 계산: {current_price:,}원 → {buy_price:,}원 (프리미엄: {premium:.1%})")
            return buy_price

        except Exception as e:
            logger.error(f"매수가 계산 오류: {e}")
            return int(current_price * 1.005)  # 기본 0.5% 프리미엄

    def _calculate_sell_price(self, current_price: int, strategy: str = 'default', is_auto_sell: bool = False) -> int:
        """매도 지정가 계산 (현재가 기준)"""
        try:
            if is_auto_sell:
                # 자동매도시 빠른 체결을 위해 더 낮은 가격
                discount = 0.008  # 0.8% 할인
            else:
                # 전략별 매도 할인 설정
                sell_discounts = {
                    'gap_trading': 0.005,    # 갭 거래: 0.5% 아래
                    'volume_breakout': 0.006, # 거래량 돌파: 0.6% 아래
                    'momentum': 0.004,       # 모멘텀: 0.4% 아래
                    'default': 0.005         # 기본: 0.5% 아래
                }
                discount = sell_discounts.get(strategy, sell_discounts['default'])

            # 계산된 매도가 (빠른 체결 고려)
            sell_price = int(current_price * (1 - discount))

            # 호가 단위로 조정
            sell_price = self._adjust_to_tick_size(sell_price)

            logger.debug(f"매도가 계산: {current_price:,}원 → {sell_price:,}원 (할인: {discount:.1%})")
            return sell_price

        except Exception as e:
            logger.error(f"매도가 계산 오류: {e}")
            return int(current_price * 0.995)  # 기본 0.5% 할인

    def _adjust_to_tick_size(self, price: int) -> int:
        """호가 단위로 가격 조정"""
        try:
            # 한국 주식 호가 단위
            if price < 1000:
                return price  # 1원 단위
            elif price < 5000:
                return (price // 5) * 5  # 5원 단위
            elif price < 10000:
                return (price // 10) * 10  # 10원 단위
            elif price < 50000:
                return (price // 50) * 50  # 50원 단위
            elif price < 100000:
                return (price // 100) * 100  # 100원 단위
            elif price < 500000:
                return (price // 500) * 500  # 500원 단위
            else:
                return (price // 1000) * 1000  # 1000원 단위

        except Exception as e:
            logger.error(f"호가 단위 조정 오류: {e}")
            return price

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

            # 실제 거래 로직
            if signal['signal_type'] == 'BUY':
                success = self._execute_buy_signal(signal)
                if success:
                    self.stats['orders_executed'] += 1
                    self.stats['positions_opened'] += 1
            elif signal['signal_type'] == 'SELL':
                success = self._execute_sell_signal(signal)
                if success:
                    self.stats['orders_executed'] += 1
                    self.stats['positions_closed'] += 1

        except Exception as e:
            logger.error(f"거래 신호 처리 오류: {e}")

    def _execute_buy_signal(self, signal: dict) -> bool:
        """매수 신호 실행"""
        try:
            stock_code = signal['stock_code']
            strategy = signal['strategy']
            price = signal.get('price', 0)
            strength = signal.get('strength', 0.5)

            # 1. 포지션 중복 체크
            existing_positions = self.position_manager.get_positions('active')
            if stock_code in existing_positions:
                logger.warning(f"이미 보유 중인 종목: {stock_code}")
                return False

            # 2. 잔고 확인
            balance = self.trading_manager.get_balance()
            available_cash = balance.get('available_cash', 0)

            if available_cash < 10000:  # 최소 1만원
                logger.warning(f"잔고 부족: {available_cash:,}원")
                return False

            # 3. 최신 현재가 조회 (정확한 가격 계산을 위해)
            current_data = self.data_manager.get_latest_data(stock_code)
            if not current_data or current_data.get('status') != 'success':
                logger.error(f"현재가 조회 실패: {stock_code}")
                return False

            current_price = current_data.get('current_price', 0)
            if current_price <= 0:
                logger.error(f"유효하지 않은 현재가: {stock_code} = {current_price}")
                return False

            # 4. 지정가 계산 (전략별 프리미엄 적용)
            buy_price = self._calculate_buy_price(current_price, strategy)

            # 5. 매수 수량 계산 (신호 강도에 따라 조절)
            position_size = min(available_cash * 0.1 * strength, available_cash * 0.05)  # 잔고의 5-10%
            quantity = int(position_size // buy_price) if buy_price > 0 else 0

            if quantity <= 0:
                logger.warning(f"매수 수량 부족: {stock_code} 현재가={current_price:,}원, 매수가={buy_price:,}원")
                return False

            # 최종 매수 금액 확인
            total_buy_amount = quantity * buy_price
            if total_buy_amount > available_cash:
                # 수량 재조정
                quantity = int(available_cash // buy_price)
                total_buy_amount = quantity * buy_price
                logger.info(f"매수 수량 재조정: {stock_code} {quantity}주, 총액={total_buy_amount:,}원")

            # 6. 실제 매수 주문 (지정가)
            order_no = self.trading_manager.execute_order(
                stock_code=stock_code,
                order_type="BUY",
                quantity=quantity,
                price=buy_price,  # 계산된 지정가
                strategy_type=strategy
            )

            if order_no:
                # 포지션 추가
                self.position_manager.add_position(
                    stock_code=stock_code,
                    quantity=quantity,
                    buy_price=buy_price,
                    strategy_type=strategy
                )

                logger.info(f"✅ 매수 주문 완료: {stock_code} {quantity:,}주 @ {buy_price:,}원 (현재가: {current_price:,}원, 주문번호: {order_no})")

                # 텔레그램 알림
                if self.telegram_bot:
                    self._send_order_notification('매수', stock_code, quantity, buy_price, strategy)

                return True
            else:
                logger.error(f"❌ 매수 주문 실패: {stock_code}")
                return False

        except Exception as e:
            logger.error(f"매수 신호 실행 오류: {e}")
            return False

    def _execute_sell_signal(self, signal: dict) -> bool:
        """매도 신호 실행"""
        try:
            stock_code = signal['stock_code']
            strategy = signal['strategy']
            price = signal.get('price', 0)

            # 1. 포지션 확인
            existing_positions = self.position_manager.get_positions('active')
            if stock_code not in existing_positions:
                logger.warning(f"보유하지 않은 종목: {stock_code}")
                return False

            position = existing_positions[stock_code]
            quantity = position.get('quantity', 0)
            if quantity <= 0:
                logger.warning(f"매도할 수량이 없음: {stock_code}")
                return False

            # 2. 최신 현재가 조회 (정확한 가격 계산을 위해)
            current_data = self.data_manager.get_latest_data(stock_code)
            if not current_data or current_data.get('status') != 'success':
                logger.error(f"현재가 조회 실패: {stock_code}")
                return False

            current_price = current_data.get('current_price', 0)
            if current_price <= 0:
                logger.error(f"유효하지 않은 현재가: {stock_code} = {current_price}")
                return False

            # 3. 지정가 계산 (전략별 할인 적용)
            sell_price = self._calculate_sell_price(current_price, strategy, is_auto_sell=False)

            # 4. 실제 매도 주문 (지정가)
            order_no = self.trading_manager.execute_order(
                stock_code=stock_code,
                order_type="SELL",
                quantity=quantity,
                price=sell_price,  # 계산된 지정가
                strategy_type=strategy
            )

            if order_no:
                # 포지션 제거 (실제 체결가는 매도가로 기록)
                self.position_manager.remove_position(stock_code, quantity, sell_price)

                # 수익률 계산
                buy_price = position.get('buy_price', sell_price)
                profit_rate = ((sell_price - buy_price) / buy_price * 100) if buy_price > 0 else 0

                logger.info(f"✅ 매도 주문 완료: {stock_code} {quantity:,}주 @ {sell_price:,}원 (현재가: {current_price:,}원, 수익률: {profit_rate:.2f}%, 주문번호: {order_no})")

                # 텔레그램 알림
                if self.telegram_bot:
                    self._send_order_notification('매도', stock_code, quantity, sell_price, strategy)

                return True
            else:
                logger.error(f"❌ 매도 주문 실패: {stock_code}")
                return False

        except Exception as e:
            logger.error(f"매도 신호 실행 오류: {e}")
            return False

    def _send_order_notification(self, order_type: str, stock_code: str, quantity: int, price: int, strategy: str):
        """주문 알림 전송"""
        try:
            total_amount = quantity * price
            message = (
                f"🎯 {order_type} 주문 체결\n"
                f"종목: {stock_code}\n"
                f"수량: {quantity:,}주\n"
                f"가격: {price:,}원\n"
                f"금액: {total_amount:,}원\n"
                f"전략: {strategy}\n"
                f"시간: {datetime.now().strftime('%H:%M:%S')}"
            )
            self._send_telegram_notification(message)
        except Exception as e:
            logger.error(f"주문 알림 전송 오류: {e}")

    def _send_auto_sell_notification(self, sell_signal: dict, order_no: str):
        """자동 매도 알림 전송"""
        try:
            stock_code = sell_signal['stock_code']
            quantity = sell_signal['quantity']
            current_price = sell_signal['current_price']
            auto_sell_price = sell_signal.get('auto_sell_price', current_price)
            profit_rate = sell_signal['profit_rate']
            reason = sell_signal['reason']

            total_amount = quantity * auto_sell_price

            message = (
                f"🤖 자동 매도 주문 완료\n"
                f"종목: {stock_code}\n"
                f"수량: {quantity:,}주\n"
                f"주문가: {auto_sell_price:,}원\n"
                f"현재가: {current_price:,}원\n"
                f"주문금액: {total_amount:,}원\n"
                f"수익률: {profit_rate:.2f}%\n"
                f"사유: {reason}\n"
                f"주문번호: {order_no}\n"
                f"시간: {datetime.now().strftime('%H:%M:%S')}"
            )
            self._send_telegram_notification(message)
        except Exception as e:
            logger.error(f"자동 매도 알림 전송 오류: {e}")

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
