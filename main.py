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
from typing import Callable, Dict

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

# 텔레그램 봇
from telegram_bot.bot import TelegramBot

# 설정
from config.settings import (
    IS_DEMO, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, LOG_LEVEL
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

        # 5. 웹소켓 관리자 (선택적)
        self.websocket_manager = None
        try:
            self.websocket_manager = KISWebSocketManager()
            # 🔧 데이터 매니저에 웹소켓 매니저 연결
            self.data_manager.websocket_manager = self.websocket_manager
            logger.info("✅ WebSocket 관리자 초기화 및 연결 완료")
        except Exception as e:
            logger.warning(f"⚠️ WebSocket 관리자 초기화 실패: {e}")

        # 6. 전략 스케줄러 (핵심!)
        self.strategy_scheduler = StrategyScheduler(self.rest_api, self.data_manager)
        self.strategy_scheduler.set_bot_instance(self)

        # 7. 텔레그램 봇 (선택적)
        self.telegram_bot = None
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                self.telegram_bot = TelegramBot(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
                # 🎯 메인 봇 참조 설정 (데이터베이스 접근용)
                self.telegram_bot.set_main_bot_reference(self)
                # 텔레그램 봇은 별도 스레드에서 시작
                logger.info("✅ 텔레그램 봇 초기화 완료")
            except Exception as e:
                logger.warning(f"⚠️ 텔레그램 봇 초기화 실패: {e}")
        else:
            logger.info("⚠️ 텔레그램 설정 누락 - 봇 비활성화")

        # 통계
        self.stats = {
            'start_time': time.time(),
            'signals_processed': 0,
            'orders_executed': 0,
            'positions_opened': 0,
            'positions_closed': 0
        }

        # 중복 매도 방지용 추적
        self.pending_sell_orders = set()  # 매도 주문 중인 종목들

        # 🎯 거래 데이터베이스 초기화
        self.trade_db = TradeDatabase()
        
        # 데이터 매니저 초기화

        logger.info("🚀 StockBot 초기화 완료!")



    async def _setup_existing_positions(self):
        """보유 종목 자동 모니터링 설정"""
        try:
            logger.info("📊 보유 종목 모니터링 설정 시작")
            
            # 현재 보유 종목 조회
            balance = self.trading_manager.get_balance()
            holdings = balance.get('holdings', [])
            
            if not holdings:
                logger.info("보유 종목이 없습니다.")
                return
            
            logger.info(f"📈 보유 종목 {len(holdings)}개 발견 - 자동 모니터링 설정")
            
            # 포지션 매니저에 보유 종목 추가 (KIS API 응답 구조에 맞게 수정)
            for holding in holdings:
                stock_code = holding.get('pdno', '')  # KIS API: 상품번호(종목코드)
                stock_name = holding.get('prdt_name', '')  # KIS API: 상품명
                quantity = int(holding.get('hldg_qty', 0))  # KIS API: 보유수량
                current_price = int(holding.get('prpr', 0))  # KIS API: 현재가
                avg_price = int(float(holding.get('pchs_avg_pric', current_price)))  # KIS API: 매입평균가격
                
                if stock_code and quantity > 0:
                    # 🆕 데이터베이스에 기존 보유 종목 매수 기록 저장 (중복 체크)
                    try:
                        trade_id = self.trade_db.record_existing_position_if_not_exists(
                            stock_code=stock_code,
                            stock_name=stock_name,
                            quantity=quantity,
                            avg_price=avg_price,
                            current_price=current_price
                        )
                        
                        if trade_id > 0:
                            logger.info(f"💾 기존 보유 종목 DB 기록: {stock_code}({stock_name}) {quantity:,}주 @ {avg_price:,}원 (ID: {trade_id})")
                        elif trade_id == -1:
                            logger.debug(f"📝 기존 보유 종목 이미 기록됨: {stock_code}")
                        
                    except Exception as e:
                        logger.error(f"기존 보유 종목 DB 기록 오류 ({stock_code}): {e}")
                    
                    # 포지션 매니저에 추가 (기존 보유)
                    self.position_manager.add_position(
                        stock_code=stock_code,
                        quantity=quantity,
                        buy_price=avg_price,
                        strategy_type="existing_holding"
                    )
                    
                    # 웹소켓 실시간 모니터링 추가 (높은 우선순위)
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
                # 기본 데이터 검증
                if not data or data.get('status') != 'success':
                    return

                current_price = data.get('current_price', 0)
                if current_price <= 0:
                    return

                # 포지션 정보 업데이트
                existing_positions = self.position_manager.get_positions('active')
                if stock_code not in existing_positions:
                    return
                
                position = existing_positions[stock_code]
                buy_price = position.get('buy_price', 0)
                quantity = position.get('quantity', 0)
                
                if buy_price <= 0 or quantity <= 0:
                    return
                
                # 수익률 계산
                profit_rate = ((current_price - buy_price) / buy_price) * 100
                
                # 포지션 정보 업데이트 (내부적으로 처리됨)
                # position_manager.update_position_prices()에서 자동 처리
                
                # 매도 조건 체크 (기존 로직 활용)
                # _position_worker에서 자동으로 매도 조건을 체크하므로 별도 처리 불필요
                
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

            # 신호 처리기 등록 (플랫폼별)
            signal.signal(signal.SIGINT, self._signal_handler)  # Ctrl+C (모든 플랫폼)

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
        """전략 스케줄러 시작 (보유 종목 모니터링 포함)"""
        try:
            def run_strategy_scheduler():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    # 🆕 보유 종목 자동 모니터링 설정
                    loop.run_until_complete(self._setup_existing_positions())
                    
                    # 전략 스케줄러 시작
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
            logger.info("✅ 전략 스케줄러 시작 완료 (보유종목 모니터링 포함)")
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
                        stock_code = sell_signal['stock_code']
                        logger.info(f"🚨 매도 조건 발생: {stock_code} - {sell_signal['reason']}")

                        # 🔒 중복 매도 방지 체크
                        if stock_code in self.pending_sell_orders:
                            logger.warning(f"⚠️ 이미 매도 주문 진행 중: {stock_code} - 중복 매도 방지")
                            continue

                        # 📊 실제 보유 수량 확인 (중요!)
                        actual_quantity = self._get_actual_holding_quantity(stock_code)
                        if actual_quantity <= 0:
                            logger.warning(f"⚠️ 실제 보유 수량 없음: {stock_code} - 매도 신호 무시")
                            # 포지션 매니저에서도 제거
                            if stock_code in self.position_manager.positions:
                                del self.position_manager.positions[stock_code]
                            continue

                        # 매도 수량 결정 (실제 보유 수량과 신호 수량 중 작은 값)
                        sell_quantity = min(sell_signal['quantity'], actual_quantity)
                        
                        if sell_quantity != sell_signal['quantity']:
                            logger.warning(f"⚠️ 매도 수량 조정: {stock_code} {sell_signal['quantity']:,}주 → {sell_quantity:,}주 (실제보유: {actual_quantity:,}주)")

                        # 🔒 매도 주문 시작 - 중복 방지용 등록
                        self.pending_sell_orders.add(stock_code)

                        # 자동 매도용 지정가 계산
                        current_price = sell_signal['current_price']
                        strategy_type = sell_signal['strategy_type']
                        auto_sell_price = self._calculate_sell_price(current_price, strategy_type, is_auto_sell=True)

                        # 자동 매도 실행 (검증된 수량으로)
                        order_no = self.trading_manager.execute_order(
                            stock_code=stock_code,
                            order_type="SELL",
                            quantity=sell_quantity,  # 검증된 실제 매도 가능 수량
                            price=auto_sell_price,
                            strategy_type=f"auto_sell_{sell_signal['reason']}"
                        )

                        if order_no:
                            # 포지션 제거 (실제 매도된 수량으로)
                            self.position_manager.remove_position(
                                stock_code,
                                sell_quantity,  # 실제 매도된 수량
                                auto_sell_price
                            )

                            self.stats['orders_executed'] += 1
                            self.stats['positions_closed'] += 1

                            logger.info(f"✅ 자동 매도 주문 완료: {stock_code} {sell_quantity:,}주 @ {auto_sell_price:,}원 (현재가: {current_price:,}원)")

                            # 텔레그램 알림 (직접 호출)
                            if self.telegram_bot:
                                sell_signal['auto_sell_price'] = auto_sell_price
                                self.telegram_bot.send_auto_sell_notification(sell_signal, order_no)
                        
                        # 🔓 매도 주문 완료 - 중복 방지용 해제
                        self.pending_sell_orders.discard(stock_code)

                    except Exception as e:
                        logger.error(f"자동 매도 실행 오류: {sell_signal['stock_code']} - {e}")
                        # 🔓 오류 발생 시에도 중복 방지용 해제
                        self.pending_sell_orders.discard(sell_signal['stock_code'])

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
        """통계 정보 업데이트"""
        try:
            current_time = time.time()
            runtime = current_time - self.stats['start_time']

            # 포지션 요약
            position_summary = self.position_manager.get_position_summary()

            # 전략 스케줄러 상태
            scheduler_status = self.strategy_scheduler.get_status()

            # 웹소켓 구독 상태
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

            # 통계 업데이트
            self.stats.update({
                'runtime_hours': runtime / 3600,
                'positions_count': position_summary.get('total_positions', 0),
                'total_value': position_summary.get('total_value', 0),
                'current_slot': scheduler_status.get('current_phase', 'Unknown'),
                'active_strategies': len(scheduler_status.get('active_strategies', {})),
                'websocket_connected': websocket_info['connected'],
                'websocket_subscriptions': websocket_info['subscriptions'],
                'websocket_usage': websocket_info['usage'],
                'last_update': current_time
            })

            # 주기적 로그 (5분마다)
            if int(current_time) % 300 < 5:  # 5분마다 정확히 한 번만
                logger.info(
                    f"📊 시스템 통계 - "
                    f"실행시간: {self.stats['runtime_hours']:.1f}h, "
                    f"포지션: {self.stats['positions_count']}개, "
                    f"현재전략: {self.stats['current_slot']}, "
                    f"웹소켓: {'✅' if websocket_info['connected'] else '❌'} ({websocket_info['subscriptions']}종목, {websocket_info['usage']}), "
                    f"처리신호: {self.stats['signals_processed']}개"
                )

        except Exception as e:
            logger.error(f"통계 업데이트 오류: {e}")

    def _generate_hourly_report(self):
        """1시간마다 상세 리포트 생성"""
        try:
            report = self._generate_status_report()
            logger.info(f"📊 1시간 리포트:\n{report}")

            # 텔레그램 전송 (직접 호출)
            if self.telegram_bot:
                self.telegram_bot.send_hourly_report(report)

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

            # 웹소켓 구독 상태
            websocket_info = "❌ 연결 안됨"
            if self.websocket_manager and getattr(self.websocket_manager, 'is_connected', False):
                subscriptions = 0
                usage = "0/41"
                if hasattr(self.websocket_manager, 'get_subscribed_stocks'):
                    subscriptions = len(self.websocket_manager.get_subscribed_stocks())
                if hasattr(self.websocket_manager, 'get_websocket_usage'):
                    usage = self.websocket_manager.get_websocket_usage()
                websocket_info = f"✅ 연결됨 ({subscriptions}종목, {usage})"

            report = (
                f"🕐 실행시간: {runtime_hours:.1f}시간\n"
                f"📈 활성포지션: {position_summary.get('total_positions', 0)}개\n"
                f"💰 총평가금액: {position_summary.get('total_value', 0):,}원\n"
                f"📊 수익률: {position_summary.get('total_profit_rate', 0):.2f}%\n"
                f"🎯 현재전략: {scheduler_status.get('current_phase', 'N/A')}\n"
                f"📋 활성전략수: {len(scheduler_status.get('active_strategies', {}))}\n"
                f"📡 웹소켓: {websocket_info}\n"
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

    def _calculate_buy_price(self, current_price: int, strategy: str = 'default') -> int:
        """매수 지정가 계산 (현재가 기준) - 체결률 개선 버전"""
        try:
            # 🎯 개선된 전략별 매수 프리미엄 (체결률 고려)
            buy_premiums = {
                'gap_trading': 0.003,      # 갭 거래: 0.3% 위 (빠른 상승 예상)
                'volume_breakout': 0.005,  # 거래량 돌파: 0.5% 위 (안정적 진입)
                'momentum': 0.007,         # 모멘텀: 0.7% 위 (트렌드 추종)
                'existing_holding': 0.002, # 기존 보유: 0.2% 위 (보수적)
                'default': 0.003           # 기본: 0.3% 위 (균형)
            }

            base_premium = buy_premiums.get(strategy, buy_premiums['default'])
            
            # 📊 시장 상황별 동적 조정
            # 현재가 기준 변동성 고려 (호가 스프레드 추정)
            volatility_adjustment = 0
            if current_price < 5000:
                volatility_adjustment = 0.002   # 저가주: +0.2% (활발한 거래)
            elif current_price > 100000:
                volatility_adjustment = -0.001  # 고가주: -0.1% (보수적)
            
            # 📈 신호 강도별 추가 조정 (향후 확장 가능)
            signal_strength_adjustment = 0  # 현재는 기본값
            
            # 최종 프리미엄 계산
            final_premium = base_premium + volatility_adjustment + signal_strength_adjustment
            final_premium = max(0.001, min(final_premium, 0.01))  # 0.1%~1.0% 범위 제한
            
            # 계산된 매수가
            buy_price = int(current_price * (1 + final_premium))

            # 호가 단위로 조정
            buy_price = self._adjust_to_tick_size(buy_price)

            logger.info(f"💰 매수가 계산: {current_price:,}원 → {buy_price:,}원 (프리미엄: {final_premium:.1%}, 전략: {strategy})")
            return buy_price

        except Exception as e:
            logger.error(f"매수가 계산 오류: {e}")
            return int(current_price * 1.003)  # 기본 0.3% 프리미엄

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

    def _get_actual_holding_quantity(self, stock_code: str) -> int:
        """실제 보유 수량 확인 (KIS API 조회)"""
        try:
            balance = self.trading_manager.get_balance()
            holdings = balance.get('holdings', [])
            
            for holding in holdings:
                if holding.get('pdno') == stock_code:
                    quantity = int(holding.get('hldg_qty', 0))
                    logger.debug(f"📊 실제 보유 수량 확인: {stock_code} = {quantity:,}주")
                    return quantity
            
            logger.debug(f"📊 실제 보유 수량 확인: {stock_code} = 0주 (보유하지 않음)")
            return 0
            
        except Exception as e:
            logger.error(f"실제 보유 수량 확인 오류 ({stock_code}): {e}")
            return 0

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

            # 텔레그램 알림 (직접 호출)
            if self.telegram_bot:
                self.telegram_bot.send_signal_notification(signal)

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

            # 5. 🎯 개선된 매수 수량 계산 (신호 강도 및 리스크 고려)
            base_position_ratio = 0.08  # 기본 8% (기존 5-10%에서 조정)
            
            # 전략별 포지션 사이즈 조정
            strategy_multipliers = {
                'gap_trading': 0.7,      # 갭 거래: 보수적 (5.6%)
                'volume_breakout': 0.9,  # 거래량: 적극적 (7.2%)
                'momentum': 1.2,         # 모멘텀: 공격적 (9.6%)
                'existing_holding': 0.5, # 기존 보유: 매우 보수적 (4%)
                'default': 1.0           # 기본: 8%
            }
            
            # 신호 강도 고려 (0.3 ~ 1.2 범위)
            strength_adjusted = max(0.3, min(strength, 1.2))
            
            # 최종 포지션 비율 계산
            strategy_multiplier = strategy_multipliers.get(strategy, 1.0)
            final_position_ratio = base_position_ratio * strategy_multiplier * strength_adjusted
            
            # 최대 투자 금액 계산
            max_investment = min(
                available_cash * final_position_ratio,  # 잔고 비율 기준
                available_cash * 0.12,                  # 최대 12% 제한
                500000                                  # 최대 50만원 제한
            )
            
            # 수량 계산
            quantity = int(max_investment // buy_price) if buy_price > 0 else 0
            
            # 최소 수량 체크 (너무 소액 투자 방지)
            min_investment = 50000  # 최소 5만원
            if quantity * buy_price < min_investment:
                quantity = max(1, int(min_investment // buy_price))
            
            if quantity <= 0:
                logger.warning(f"💰 매수 수량 부족: {stock_code} 현재가={current_price:,}원, 매수가={buy_price:,}원, 예산={max_investment:,}원")
                return False
            
            # 최종 매수 금액 확인 및 재조정
            total_buy_amount = quantity * buy_price
            if total_buy_amount > available_cash:
                quantity = int(available_cash // buy_price)
                total_buy_amount = quantity * buy_price
                logger.info(f"💰 매수 수량 재조정: {stock_code} {quantity:,}주, 총액={total_buy_amount:,}원")
            
            logger.info(f"💰 매수 수량 계산: {stock_code} - 전략={strategy}, 강도={strength:.2f}, 비율={final_position_ratio:.1%}, 수량={quantity:,}주, 금액={total_buy_amount:,}원")

            # 6. 실제 매수 주문 (지정가)
            order_result = self.trading_manager.buy_order(
                stock_code=stock_code,
                quantity=quantity,
                price=buy_price
            )

            if order_result.get('status') == 'success':
                logger.info(f"✅ 매수 주문 성공: {stock_code} {quantity}주 @{buy_price:,}원")
                
                # 🎯 데이터베이스 기록 저장
                try:
                    # 종목명 조회 (간단하게 종목코드 사용)
                    stock_name = stock_code  # 실제로는 종목명 조회 API 사용 가능
                    total_amount = quantity * buy_price
                    reason = signal.get('reason', f'{strategy} 신호')
                    
                    trade_id = self.trade_db.record_buy_trade(
                        stock_code=stock_code,
                        stock_name=stock_name,
                        quantity=quantity,
                        price=buy_price,
                        total_amount=total_amount,
                        strategy_type=strategy,
                        order_id=order_result.get('order_no', ''),
                        status='SUCCESS',
                        market_conditions={
                            'current_price': current_price,
                            'signal_strength': strength,
                            'reason': reason
                        },
                        notes=f"신호강도: {strength:.2f}, 사유: {reason}"
                    )
                    logger.info(f"💾 매수 기록 저장 완료 (ID: {trade_id})")
                    
                    # 🆕 선정된 종목과 거래 연결
                    if trade_id > 0:
                        try:
                            self.trade_db.link_trade_to_selected_stock(stock_code, trade_id)
                        except Exception as e:
                            logger.error(f"선정 종목-거래 연결 오류: {e}")
                except Exception as e:
                    logger.error(f"💾 매수 기록 저장 실패: {e}")

                # 포지션 추가
                self.position_manager.add_position(
                    stock_code=stock_code,
                    quantity=quantity,
                    buy_price=buy_price,
                    strategy_type=strategy
                )

                logger.info(f"✅ 매수 주문 완료: {stock_code} {quantity:,}주 @ {buy_price:,}원 (현재가: {current_price:,}원, 주문번호: {order_result.get('order_no', '')})")

                # 텔레그램 알림 (직접 호출)
                if self.telegram_bot:
                    self.telegram_bot.send_order_notification('매수', stock_code, quantity, buy_price, strategy)

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

            # 🛡️ 매도 수량 검증 - 실제 보유 수량 확인
            actual_quantity = self._get_actual_holding_quantity(stock_code)
            verified_quantity = min(quantity, actual_quantity) if actual_quantity > 0 else 0
            
            if verified_quantity <= 0:
                logger.warning(f"❌ 매도 불가: {stock_code} 실제 보유 수량 부족 (요청: {quantity}, 실제: {actual_quantity})")
                return False

            # 매도 주문 실행
            sell_result = self.trading_manager.sell_order(
                stock_code=stock_code,
                quantity=verified_quantity,
                price=sell_price
            )

            if sell_result.get('status') == 'success':
                logger.info(f"✅ 매도 주문 성공: {stock_code} {verified_quantity}주 @{sell_price:,}원")
                
                # 🎯 데이터베이스 기록 저장
                try:
                    # 매수 거래 ID 찾기
                    buy_trade_id = self.trade_db.find_buy_trade_for_sell(stock_code, verified_quantity)
                    
                    # 수익률 계산
                    buy_price = position.get('buy_price', sell_price)
                    profit_rate = ((sell_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
                    sell_type = "수동매도"
                    condition_reason = signal.get('reason', '매도 신호')
                    
                    trade_id = self.trade_db.record_sell_trade(
                        stock_code=stock_code,
                        stock_name=position.get('stock_name', stock_code),
                        quantity=verified_quantity,
                        price=sell_price,
                        total_amount=verified_quantity * sell_price,
                        strategy_type=position.get('strategy_type', 'unknown'),
                        buy_trade_id=buy_trade_id,
                        order_id=sell_result.get('order_no', ''),
                        status='SUCCESS',
                        market_conditions={
                            'current_price': current_price,
                            'profit_rate': profit_rate,
                            'sell_reason': f"{sell_type}: {condition_reason}"
                        },
                        notes=f"매도사유: {sell_type}, 조건: {condition_reason}"
                    )
                    logger.info(f"💾 매도 기록 저장 완료 (ID: {trade_id})")
                except Exception as e:
                    logger.error(f"💾 매도 기록 저장 실패: {e}")

                # 포지션에서 제거
                self.position_manager.remove_position(stock_code, verified_quantity, sell_price)

                return True
            else:
                logger.error(f"❌ 매도 주문 실패: {stock_code}")
                return False

        except Exception as e:
            logger.error(f"매도 신호 실행 오류: {e}")
            return False

    # === 텔레그램 봇용 인터페이스 메서드들 ===

    def get_balance(self) -> dict:
        """잔고 조회 (텔레그램용)"""
        return self.trading_manager.get_balance()

    def get_system_status(self) -> dict:
        """시스템 상태 조회 (텔레그램용)"""
        try:
            # 포지션 요약
            position_summary = self.position_manager.get_position_summary()

            # 전략 스케줄러 상태
            scheduler_status = self.strategy_scheduler.get_status()

            # 웹소켓 상태 및 구독 정보
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
                'subscribed_stocks': subscribed_stocks[:10],  # 최대 10개만 표시
                'positions_count': position_summary.get('total_positions', 0),
                'pending_orders_count': 0,  # 추후 구현
                'order_history_count': 0,   # 추후 구현
                'scheduler': {
                    'current_slot': scheduler_status.get('current_phase', 'None'),
                    'active_strategies': scheduler_status.get('active_strategies', {}),
                    'total_active_stocks': len(scheduler_status.get('active_strategies', {}))
                }
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
                }
            }

    @property
    def trading_api(self):
        """trading_api 속성 (텔레그램 봇 호환용)"""
        return self.trading_manager


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
