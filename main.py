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
from typing import Callable, Dict, Optional, List

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
from core.worker_manager import WorkerManager

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
            logger.info("✅ WebSocket 관리자 초기화 완료")
            
            # 🎯 웹소켓 우선 사용 정책 - 즉시 연결 시도
            if self._should_prefer_websocket():
                logger.info("🚀 웹소켓 우선 사용 설정 - 즉시 연결 시도")
                websocket_connected = self.data_manager.ensure_websocket_connection()
                if websocket_connected:
                    logger.info("🎉 웹소켓 즉시 연결 성공")
                else:
                    logger.warning("⚠️ 웹소켓 즉시 연결 실패 - 백그라운드에서 재시도 예정")
            
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
        """워커 시스템은 새로운 WorkerManager가 자동으로 처리"""
        try:
            # 새로운 워커 시스템은 start() 메서드에서 자동으로 시작됨
            logger.info("✅ 새로운 워커 시스템 준비 완료")
            
        except Exception as e:
            logger.error(f"❌ 워커 준비 실패: {e}")

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
            self.worker_manager.start_all_workers(self)
            
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
            now = datetime.now()
            
            # 간단한 장시간 체크 (평일 9-15시)
            is_weekday = now.weekday() < 5
            is_market_hours = 9 <= now.hour < 15
            is_open = is_weekday and is_market_hours
            
            return {
                'is_open': is_open,
                'current_time': now.strftime('%H:%M:%S'),
                'status': '개장' if is_open else '휴장'
            }
        except Exception as e:
            logger.error(f"시장 상태 확인 오류: {e}")
            return {'is_open': False, 'status': '확인불가'}

    def _update_stats(self):
        """통계 업데이트"""
        try:
            uptime = time.time() - self.stats['start_time']
            self.stats['uptime'] = uptime
            
            # 포지션 현황 업데이트
            positions = self.position_manager.get_positions('active')
            self.stats['active_positions'] = len(positions)
            
        except Exception as e:
            logger.error(f"통계 업데이트 오류: {e}")

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

    def get_balance(self) -> dict:
        """잔고 조회"""
        return self.trading_manager.get_balance()

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

    def _should_prefer_websocket(self) -> bool:
        """웹소켓 우선 사용 여부 확인"""
        try:
            # config.json에서 웹소켓 설정 확인
            import json
            config_path = Path(__file__).parent / "config" / "config.json"
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    websocket_config = config.get('websocket', {})
                    return websocket_config.get('prefer_websocket', True)
            return True  # 기본값
        except:
            return True


def main():
    """메인 함수"""
    try:
        print("🚀 StockBot 시작...")
        
        bot = StockBot()
        bot.start()
        
    except KeyboardInterrupt:
        print("⌨️ 사용자가 중단했습니다.")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")
        logger.error(f"메인 함수 오류: {e}")
    finally:
        print("👋 StockBot 종료")


if __name__ == "__main__":
    main()
