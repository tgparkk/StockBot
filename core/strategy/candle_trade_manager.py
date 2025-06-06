"""
캔들 기반 매매 전략 통합 관리자
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
from core.api.rest_api_manager import KISRestAPIManager
from utils.logger import setup_logger

from .candle_trade_candidate import (
    CandleTradeCandidate, CandleStatus, TradeSignal, PatternType,
    CandlePatternInfo, EntryConditions, RiskManagement, PerformanceTracking
)
from .candle_stock_manager import CandleStockManager
from .candle_pattern_detector import CandlePatternDetector
from .candle_signal_analyzer import CandleSignalAnalyzer
from .candle_risk_manager import CandleRiskManager
from .candle_execution_manager import CandleExecutionManager
from core.data.hybrid_data_manager import SimpleHybridDataManager
from core.trading.trade_executor import TradeExecutor
from core.websocket.kis_websocket_manager import KISWebSocketManager

logger = setup_logger(__name__)


class CandleTradeManager:
    """캔들 기반 매매 전략 통합 관리자"""

    def __init__(self, kis_api_manager : KISRestAPIManager, data_manager : SimpleHybridDataManager, trade_executor : TradeExecutor, websocket_manager : KISWebSocketManager):
        """
        Args:
            kis_api_manager: KIS API 관리자
            data_manager: 데이터 관리자
            trade_executor: 매매 실행자
            websocket_manager: 웹소켓 관리자 (선택)
        """
        self.kis_api_manager = kis_api_manager
        self.data_manager = data_manager
        self.trade_executor = trade_executor
        self.websocket_manager = websocket_manager

        # 🆕 데이터베이스 참조 (TradeExecutor에서 가져옴)
        self.trade_db = trade_executor.trade_db if hasattr(trade_executor, 'trade_db') else None
        if self.trade_db is None:
            # 백업: 직접 생성
            from ..trading.trade_database import TradeDatabase
            self.trade_db = TradeDatabase()
            logger.info("🗄️ 캔들 트레이딩용 데이터베이스 직접 초기화")
        else:
            logger.info("🗄️ 캔들 트레이딩 데이터베이스 연결 완료")

        # ========== 설정값 먼저 정의 ==========
        self.config = {
            # 기본 스캔 설정
            'scan_interval_seconds': 60,      # 1분마다 스캔
            'max_positions': 15,                # 최대 포지션 수
            'max_scan_stocks': 50,              # 스캔할 최대 종목 수
            'risk_per_trade': 0.02,           # 거래당 리스크 2%
            'pattern_confidence_threshold': 0.6,  # 패턴 신뢰도 임계값
            'volume_threshold': 1.5,           # 거래량 임계값

            # 진입 조건 설정 (누락된 설정들 추가)
            'min_volume_ratio': 2.0,          # 최소 거래량 비율
            'trading_start_time': '09:00',     # 거래 시작 시간
            'trading_end_time': '15:20',       # 거래 종료 시간
            'min_price': 1000,                 # 최소 주가
            'max_price': 500000,               # 최대 주가
            'min_daily_volume': 5000000000,    # 최소 일일 거래대금 (50억)

            # 리스크 관리 설정
            'max_position_size_pct': 30,       # 최대 포지션 크기 (%)
            'default_stop_loss_pct': 1.8,      # 기본 손절 비율 (%) - 1.8%로 더 빠른 손절
            'default_target_profit_pct': 3,    # 기본 목표 수익률 (%) - 3%로 조정 (현실적)
            'max_holding_hours': 6,            # 최대 보유 시간 - 6시간으로 조정 (단기 트레이딩)

            # 패턴별 세부 목표 설정 (더 현실적으로)
            'pattern_targets': {
                'hammer': {'target': 1.5, 'stop': 1.5, 'max_hours': 4},           # 망치형: 4시간
                'inverted_hammer': {'target': 1.2, 'stop': 1.5, 'max_hours': 4},  # 역망치형: 4시간
                'bullish_engulfing': {'target': 1.8, 'stop': 1.2, 'max_hours': 4}, # 장악형: 더 보수적 기준
                'morning_star': {'target': 2.5, 'stop': 1.5, 'max_hours': 8},     # 샛별형: 8시간 (최강, 조금 길게)
                'rising_three': {'target': 3.0, 'stop': 2.0, 'max_hours': 12},    # 삼법형: 12시간 (지속성 패턴)
                'doji': {'target': 1.0, 'stop': 1.0, 'max_hours': 2},             # 도지: 2시간 (신중, 빠른 결정)
            },

            # 시간 기반 청산 설정
            'time_exit_rules': {
                'profit_exit_hours': 3,        # 3시간 후 수익중이면 청산 고려
                'min_profit_for_time_exit': 0.5,  # 시간 청산 최소 수익률 0.5%
                'market_close_exit_minutes': 30,  # 장 마감 30분 전 청산
                'overnight_avoid': False,      # 오버나이트 포지션 허용 (갭 활용)
            },

            # 🆕 투자금액 계산 설정
            'investment_calculation': {
                'cash_usage_ratio': 0.8,       # 현금잔고 사용 비율 (80%)
                'portfolio_usage_ratio': 0.2,  # 총평가액 사용 비율 (20%)
                'min_cash_threshold': 500_000, # 현금 우선 사용 최소 기준 (50만원)
                'max_portfolio_limit': 3_000_000, # 평가액 기준 최대 제한 (300만원)
                'default_investment': 1_000_000,   # 기본 투자 금액 (100만원)
                'min_investment': 100_000,     # 최소 투자 금액 (10만원)
            },
        }

        # 캔들 관련 매니저들
        self.stock_manager = CandleStockManager(max_watch_stocks=100, max_positions=15)
        self.pattern_detector = CandlePatternDetector()

        # 🆕 분리된 모듈들 초기화 (config 정의 후)
        self.signal_analyzer = CandleSignalAnalyzer(self.pattern_detector)
        self.risk_manager = CandleRiskManager(self.config)
        self.execution_manager = CandleExecutionManager(trade_executor, self.trade_db, self.config)

        # 내부 상태
        self._all_stocks: Dict[str, CandleTradeCandidate] = {}
        self._existing_holdings: Dict[str, Dict] = {}
        self._last_scan_time: Optional[datetime] = None  # datetime 타입으로 명시
        self._scan_interval = 60  # 1분
        self.is_running = False

        # 실행 상태
        self.running = False
        self.scan_interval = 30  # 🆕 스캔 간격 (초)

        # ========== 상태 관리 ==========
        self.daily_stats = {
            'trades_count': 0,
            'successful_trades': 0,
            'failed_trades': 0,
            'total_profit_loss': 0.0,
        }

        # ========== 기존 보유 종목 관리 ==========
        self.existing_holdings_callbacks = {}  # {stock_code: callback_function}

        # 🆕 웹소켓 구독 상태 관리
        self.subscribed_stocks = set()

        # 한국 시간대 설정 추가
        self.korea_tz = timezone(timedelta(hours=9))

        logger.info("✅ CandleTradeManager 초기화 완료")

    # ==========================================
    # 🆕 구독 상태 관리 유틸리티
    # ==========================================

    def _check_subscription_status(self, stock_code: str) -> Dict[str, bool]:
        """종목의 구독 상태를 종합적으로 체크"""
        status = {
            'candle_manager_subscribed': stock_code in self.subscribed_stocks,
            'websocket_manager_subscribed': False,
            'all_stocks_tracked': stock_code in self._all_stocks,
            'subscription_capacity_available': True
        }

        try:
            if self.websocket_manager and hasattr(self.websocket_manager, 'subscription_manager'):
                ws_sub_mgr = self.websocket_manager.subscription_manager
                status['websocket_manager_subscribed'] = ws_sub_mgr.is_subscribed(stock_code)
                status['subscription_capacity_available'] = ws_sub_mgr.has_subscription_capacity()

        except Exception as e:
            logger.debug(f"구독 상태 체크 오류 ({stock_code}): {e}")

        return status

    def _synchronize_subscription_state(self, stock_code: str):
        """구독 상태 동기화 (매니저 간 불일치 해결)"""
        try:
            status = self._check_subscription_status(stock_code)

            # 웹소켓 매니저에서는 구독됨, 캔들 매니저에서는 안됨 → 동기화
            if status['websocket_manager_subscribed'] and not status['candle_manager_subscribed']:
                self.subscribed_stocks.add(stock_code)
                logger.debug(f"🔄 {stock_code} 구독 상태 동기화: 캔들매니저에 추가")

            # 둘 다 구독됨
            elif status['websocket_manager_subscribed'] and status['candle_manager_subscribed']:
                logger.debug(f"✅ {stock_code} 구독 상태 일치")

            return status

        except Exception as e:
            logger.error(f"구독 상태 동기화 오류 ({stock_code}): {e}")
            return None

    def _log_subscription_summary(self):
        """구독 현황 요약 로그"""
        try:
            candle_count = len(self.subscribed_stocks)
            ws_count = 0
            ws_max = 0

            if self.websocket_manager and hasattr(self.websocket_manager, 'subscription_manager'):
                ws_sub_mgr = self.websocket_manager.subscription_manager
                ws_count = ws_sub_mgr.get_subscription_count()
                ws_max = ws_sub_mgr.MAX_STOCKS

            logger.info(f"📊 구독 현황: 캔들매니저={candle_count}개, 웹소켓매니저={ws_count}/{ws_max}개")

            # 불일치 체크
            if candle_count != ws_count:
                logger.warning(f"⚠️ 구독 상태 불일치 감지 - 캔들:{candle_count} vs 웹소켓:{ws_count}")

        except Exception as e:
            logger.debug(f"구독 현황 로그 오류: {e}")

    # ==========================================
    # 🆕 기존 보유 종목 웹소켓 구독 관리 (간소화)
    # ==========================================

    async def setup_existing_holdings_monitoring(self):
        """기존 보유 종목 웹소켓 모니터링 설정 및 _all_stocks 통합"""
        try:
            logger.info("📊 기존 보유 종목 웹소켓 모니터링 설정 시작")

            # KIS API로 직접 보유 종목 조회
            from ..api.kis_market_api import get_account_balance
            account_balance = get_account_balance()

            if not account_balance or account_balance['total_stocks'] == 0:
                logger.info("📊 보유 종목이 없습니다.")
                return True

            existing_stocks = account_balance['stocks']
            logger.info(f"📈 보유 종목 {len(existing_stocks)}개 발견")

            subscription_success_count = 0
            added_to_all_stocks_count = 0

            for stock_info in existing_stocks:
                try:
                    stock_code = stock_info['stock_code']
                    stock_name = stock_info['stock_name']
                    current_price = stock_info.get('current_price', 0)
                    buy_price = stock_info.get('avg_price', 0)
                    quantity = stock_info.get('quantity', 0)
                    profit_rate = stock_info.get('profit_loss_rate', 0.0)

                    # 간단한 기본 정보만 출력
                    logger.info(f"📈 {stock_code}({stock_name}): {current_price:,}원, 수익률: {profit_rate:+.1f}%")

                    # 🔄 실시간 캔들차트 분석 (DB 의존성 제거)
                    candle_analysis_result = await self._analyze_existing_holding_patterns(stock_code, stock_name, current_price)

                    # 🆕 _all_stocks에 기존 보유 종목 추가 (CandleTradeCandidate로)
                    if stock_code not in self._all_stocks:
                        # 기존 보유 종목을 CandleTradeCandidate로 생성
                        existing_candidate = CandleTradeCandidate(
                            stock_code=stock_code,
                            stock_name=stock_name,
                            current_price=float(current_price) if current_price else 0.0,
                            market_type="KOSPI",  # 기본값, 나중에 조회 가능
                            status=CandleStatus.ENTERED,  # 이미 진입한 상태
                            trade_signal=TradeSignal.HOLD,  # 보유 중
                            created_at=datetime.now()
                        )

                        # 진입 정보 설정 (이미 보유 중이므로)
                        if buy_price > 0 and quantity > 0:
                            existing_candidate.enter_position(float(buy_price), int(quantity))
                            existing_candidate.update_price(float(current_price))

                            # 🆕 PerformanceTracking의 entry_price 명시적 설정
                            existing_candidate.performance.entry_price = float(buy_price)

                            # 🆕 RiskManagement 설정 - DB 데이터 우선, 없으면 기본값
                            from .candle_trade_candidate import RiskManagement

                            # 🔄 캔들차트 분석 결과 기반 리스크 관리 설정
                            entry_price = float(buy_price)
                            current_price_float = float(current_price)

                            if candle_analysis_result and candle_analysis_result.get('patterns_detected'):
                                # 🎯 캔들 패턴 분석 성공: 패턴별 설정 사용
                                patterns = candle_analysis_result['patterns']
                                strongest_pattern = candle_analysis_result['strongest_pattern']

                                logger.info(f"🔄 {stock_code} 실시간 캔들 패턴 감지: {strongest_pattern['type']} (강도: {strongest_pattern['strength']})")

                                # 패턴별 목표/손절 설정 적용
                                pattern_config = self.config['pattern_targets'].get(strongest_pattern['type'].lower())
                                if pattern_config:
                                    target_pct = pattern_config['target']
                                    stop_pct = pattern_config['stop']
                                    max_holding_hours = pattern_config['max_hours']
                                else:
                                    # 패턴 강도별 기본 설정
                                    if strongest_pattern['strength'] >= 90:
                                        target_pct, stop_pct, max_holding_hours = 15.0, 4.0, 8
                                    elif strongest_pattern['strength'] >= 80:
                                        target_pct, stop_pct, max_holding_hours = 12.0, 3.0, 6
                                    elif strongest_pattern['strength'] >= 70:
                                        target_pct, stop_pct, max_holding_hours = 8.0, 3.0, 4
                                    else:
                                        target_pct, stop_pct, max_holding_hours = 5.0, 2.0, 2

                                target_price = entry_price * (1 + target_pct / 100)
                                stop_loss_price = entry_price * (1 - stop_pct / 100)
                                trailing_stop_pct = stop_pct * 0.6  # 손절의 60%
                                position_size_pct = 20.0
                                risk_score = 100 - strongest_pattern['confidence'] * 100

                                # 패턴 정보 메타데이터에 저장
                                existing_candidate.metadata['original_pattern_type'] = strongest_pattern['type']
                                existing_candidate.metadata['original_pattern_strength'] = strongest_pattern['strength']
                                existing_candidate.metadata['pattern_confidence'] = strongest_pattern['confidence']

                                # 감지된 패턴 정보 추가
                                for pattern in patterns:
                                    existing_candidate.add_pattern(pattern)

                                source_info = f"실시간패턴분석({strongest_pattern['type']})"

                            else:
                                # 🔧 패턴 감지 실패: 기본값 사용
                                logger.info(f"🔧 {stock_code} 캔들 패턴 감지 실패 - 기본 설정 적용")

                                # 기본 3% 목표가, 2% 손절가 설정 (현실적)
                                target_price = entry_price * 1.03  # 3% 익절
                                stop_loss_price = entry_price * 0.98  # 2% 손절

                                # 현재가가 진입가보다 높다면 목표가를 조정 (이미 수익 중인 경우)
                                if current_price_float > entry_price:
                                    # 현재 수익률 계산
                                    current_profit_rate = (current_price_float - entry_price) / entry_price
                                    if current_profit_rate >= 0.02:  # 이미 2% 이상 수익
                                        # 현재가에서 1% 더 상승한 지점을 목표가로 설정
                                        target_price = current_price_float * 1.01
                                        # 손절가는 현재가에서 1.5% 하락으로 조정
                                        stop_loss_price = current_price_float * 0.985

                                trailing_stop_pct = 1.0
                                max_holding_hours = 24
                                position_size_pct = 20.0
                                risk_score = 50
                                source_info = "기본설정(패턴미감지)"

                            existing_candidate.risk_management = RiskManagement(
                                position_size_pct=position_size_pct,
                                position_amount=int(entry_price * quantity),  # 실제 투자 금액
                                stop_loss_price=stop_loss_price,
                                target_price=target_price,
                                trailing_stop_pct=trailing_stop_pct,
                                max_holding_hours=max_holding_hours,
                                risk_score=risk_score
                            )

                            # 설정 완료 확인 로그
                            if existing_candidate.performance.entry_price and existing_candidate.performance.pnl_pct is not None:
                                logger.info(f"📊 {stock_code} 기존 보유 종목 설정 완료 ({source_info}):")
                                logger.info(f"   - 진입가: {existing_candidate.performance.entry_price:,.0f}원")
                                logger.info(f"   - 수량: {existing_candidate.performance.entry_quantity:,}주")
                                logger.info(f"   - 현재가: {current_price:,.0f}원")
                                logger.info(f"   - 수익률: {existing_candidate.performance.pnl_pct:+.2f}%")
                                logger.info(f"   - 목표가: {existing_candidate.risk_management.target_price:,.0f}원")
                                logger.info(f"   - 손절가: {existing_candidate.risk_management.stop_loss_price:,.0f}원")
                            else:
                                logger.warning(f"⚠️ {stock_code} PerformanceTracking 설정 미완료 - 재확인 필요")

                        # 메타데이터에 기존 보유 표시
                        existing_candidate.metadata['is_existing_holding'] = True
                        existing_candidate.metadata['original_entry_source'] = 'realtime_pattern_analysis' if candle_analysis_result and candle_analysis_result.get('patterns_detected') else 'manual_or_app_purchase'

                        # _all_stocks에 추가
                        self._all_stocks[stock_code] = existing_candidate
                        added_to_all_stocks_count += 1

                        logger.debug(f"✅ {stock_code} _all_stocks에 기존 보유 종목으로 추가")

                    # 웹소켓 구독
                    callback = self._create_existing_holding_callback(stock_code, stock_name)
                    if await self._subscribe_existing_holding(stock_code, callback):
                        subscription_success_count += 1
                        self.existing_holdings_callbacks[stock_code] = callback

                    # await asyncio.sleep(0.1)

                except Exception as e:
                    logger.error(f"종목 구독 오류: {e}")
                    continue

            logger.info(f"📊 기존 보유 종목 웹소켓 구독 완료: {subscription_success_count}/{len(existing_stocks)}개")
            logger.info(f"🔄 _all_stocks에 기존 보유 종목 추가: {added_to_all_stocks_count}개")
            return subscription_success_count > 0

        except Exception as e:
            logger.error(f"기존 보유 종목 모니터링 설정 오류: {e}")
            return False

    async def _subscribe_existing_holding(self, stock_code: str, callback) -> bool:
        """기존 보유 종목 웹소켓 구독"""
        try:
            if stock_code in self.subscribed_stocks:
                return True

            if self.websocket_manager:
                success = await self.websocket_manager.subscribe_stock(stock_code, callback)
                if success:
                    self.subscribed_stocks.add(stock_code)
                return success
            return False

        except Exception as e:
            if "ALREADY IN SUBSCRIBE" in str(e):
                self.subscribed_stocks.add(stock_code)
                return True
            logger.error(f"기존 보유 종목 구독 오류 ({stock_code}): {e}")
            return False

    def _create_existing_holding_callback(self, stock_code: str, stock_name: str):
        """기존 보유 종목용 콜백 함수 생성"""
        def existing_holding_callback(data_type: str, received_stock_code: str, data: Dict, source: str = 'websocket') -> None:
            try:
                if data_type == 'price' and 'stck_prpr' in data:
                    current_price = int(data.get('stck_prpr', 0))
                    if current_price > 0:
                        asyncio.create_task(self._check_existing_holding_exit_signal(stock_code, current_price))
            except Exception as e:
                logger.error(f"기존 보유 종목 콜백 오류 ({stock_code}): {e}")
        return existing_holding_callback

    async def _check_existing_holding_exit_signal(self, stock_code: str, current_price: int):
        """🆕 기존 보유 종목 매도 신호 체크 - 강화된 버전"""
        try:
            # 🆕 KIS API로 실제 보유 종목 조회
            from ..api.kis_market_api import get_account_balance

            try:
                account_info = get_account_balance()
                if account_info and 'holdings' in account_info:
                    holdings = account_info['holdings']

                    # 해당 종목이 실제 보유 중인지 확인
                    for holding in holdings:
                        if holding.get('stock_code') == stock_code:
                            buy_price = holding.get('buy_price', 0)
                            quantity = holding.get('quantity', 0)

                            if buy_price > 0:
                                # 수익률 계산
                                profit_pct = ((current_price - buy_price) / buy_price) * 100

                                # 🆕 3% 이상 수익시 확실히 매도 (사용자 요구사항 반영)
                                if profit_pct >= 3.0:
                                    logger.info(f"🎯 {stock_code} 3% 수익 달성 - 매도 신호 ({profit_pct:.2f}%)")
                                    # 실제 매도 실행은 별도 구현 필요
                                    return True

                                # 손절 조건 (-3% 하락)
                                if profit_pct <= -3.0:
                                    logger.info(f"🛑 {stock_code} 손절 기준 도달 - 매도 신호 ({profit_pct:.2f}%)")
                                    return True

            except Exception as e:
                logger.debug(f"계좌 조회 오류: {e}")

        except Exception as e:
            logger.debug(f"기존 보유 종목 매도 시그널 체크 오류 ({stock_code}): {e}")

    def cleanup_existing_holdings_monitoring(self):
        """기존 보유 종목 웹소켓 모니터링 정리"""
        try:
            logger.info("🧹 기존 보유 종목 웹소켓 모니터링 정리 시작")

            for stock_code, callback in self.existing_holdings_callbacks.items():
                try:
                    if self.websocket_manager:
                        self.websocket_manager.remove_stock_callback(stock_code, callback)
                except Exception as e:
                    logger.warning(f"⚠️ {stock_code} 콜백 제거 오류: {e}")

            self.existing_holdings_callbacks.clear()
            logger.info("✅ 기존 보유 종목 모니터링 정리 완료")

        except Exception as e:
            logger.error(f"기존 보유 종목 모니터링 정리 오류: {e}")

    # ==========================================
    # 기존 메서드들 유지...
    # ==========================================

    # ========== 메인 실행 루프 ==========

    async def start_trading(self):
        """캔들 기반 매매 시작"""
        try:
            logger.info("🕯️ 캔들 기반 매매 시스템 시작")

            # 기존 보유 종목 웹소켓 모니터링 설정
            await self.setup_existing_holdings_monitoring()

            # 거래일 초기화
            await self._initialize_trading_day()

            # 메인 트레이딩 루프 시작
            self.running = True
            self._log_status()

            while self.running:
                try:
                    # 시장 스캔 및 패턴 감지
                    await self._scan_and_detect_patterns()

                    # 🆕 주기적 신호 재평가 (모든 _all_stocks 종목 대상)
                    await self.signal_analyzer.periodic_evaluation(self._all_stocks)

                    # 진입 기회 평가 = 매수 준비 종목 평가
                    await self._evaluate_entry_opportunities()

                    # 기존 포지션 관리 - 매도 시그널 체크
                    await self._manage_existing_positions()

                    # 상태 업데이트
                    self._log_status()

                    # 스캔 간격 대기 (기본 30초)
                    await asyncio.sleep(self.scan_interval)

                except Exception as e:
                    logger.error(f"매매 루프 오류: {e}")
                    await asyncio.sleep(10)  # 오류시 10초 대기 후 재시도

        except Exception as e:
            logger.error(f"캔들 매매 시작 오류: {e}")
            self.running = False

    def stop_trading(self):
        """캔들 전략 거래 중지"""
        logger.info("🛑 캔들 전략 거래 중지 요청")
        self.is_running = False

    # ========== 종목 스캔 및 패턴 감지 ==========

    async def _scan_and_detect_patterns(self):
        """종목 스캔 및 패턴 감지"""
        try:
            current_time = datetime.now()

            # 스캔 간격 체크
            # if (self._last_scan_time and
            #     (current_time - self._last_scan_time).total_seconds() < self._scan_interval):
            #     return

            logger.info("🔍 매수 후보 종목 스캔 시작")

            # 시장별 스캔
            markets = ['0001', '1001']  # 코스피, 코스닥
            for market in markets:
                await self._scan_market_for_patterns(market)

            self._last_scan_time = current_time
            logger.info("✅ 종목 스캔 완료")

        except Exception as e:
            logger.error(f"종목 스캔 오류: {e}")

    async def _scan_market_for_patterns(self, market: str):
        """특정 시장에서 패턴 스캔"""
        try:
            market_name = "코스피" if market == "0001" else "코스닥" if market == "1001" else f"시장{market}"
            logger.debug(f"📊 {market_name} 패턴 스캔 시작")

            # 1. 기본 후보 종목 수집 (기존 API 활용)
            candidates = []

            # 등락률 상위 종목
            from ..api.kis_market_api import get_fluctuation_rank
            fluctuation_data = get_fluctuation_rank(
                fid_input_iscd=market,
                fid_rank_sort_cls_code="0",  # 상승률순
                fid_rsfl_rate1="1.0"  # 1% 이상
            )

            if fluctuation_data is not None and not fluctuation_data.empty:
                candidates.extend(fluctuation_data.head(50)['stck_shrn_iscd'].tolist())

            # 거래량 급증 종목
            from ..api.kis_market_api import get_volume_rank
            volume_data = get_volume_rank(
                fid_input_iscd=market,
                fid_blng_cls_code="1",  # 거래증가율
                fid_vol_cnt="50000"
            )

            if volume_data is not None and not volume_data.empty:
                candidates.extend(volume_data.head(50)['mksc_shrn_iscd'].tolist())

            # 중복 제거
            unique_candidates = list(set(candidates))[:self.config['max_scan_stocks']]

            logger.info(f"📈 {market_name} 후보 종목: {len(unique_candidates)}개")

            # 2. 각 종목별 패턴 분석
            pattern_found_count = 0

            for i, stock_code in enumerate(unique_candidates):
                try:
                    # API 제한 방지
                    #if i % 10 == 0 and i > 0:
                    #    await asyncio.sleep(1)

                    # 패턴 분석
                    candidate = await self._analyze_stock_for_patterns(stock_code, market_name)

                    if candidate and candidate.detected_patterns:
                        # 스톡 매니저에 추가
                        if self.stock_manager.add_candidate(candidate):
                            pattern_found_count += 1

                except Exception as e:
                    logger.debug(f"종목 {stock_code} 분석 오류: {e}")
                    continue

            logger.info(f"🎯 {market_name} 패턴 감지: {pattern_found_count}개 종목")

        except Exception as e:
            logger.error(f"시장 {market} 스캔 오류: {e}")

    async def _analyze_stock_for_patterns(self, stock_code: str, market_name: str) -> Optional[CandleTradeCandidate]:
        """개별 종목 패턴 분석"""
        try:
            # 1. 기본 정보 조회
            from ..api.kis_market_api import get_inquire_price
            current_info = get_inquire_price(itm_no=stock_code)
            # ✅ DataFrame ambiguous 오류 해결
            if current_info is None or current_info.empty:
                return None

            # 기본 정보 추출
            current_price = float(current_info.iloc[0].get('stck_prpr', 0))
            stock_name = current_info.iloc[0].get('hts_kor_isnm', f'{stock_code}')

            if current_price <= 0:
                return None

            # 2. 기본 필터링
            if not self._passes_basic_filters(current_price, current_info.iloc[0].to_dict()):
                return None

            # 🆕 3. OHLCV 데이터 준비 (캔들 차트 데이터)
            from ..api.kis_market_api import get_inquire_daily_itemchartprice
            ohlcv_data = get_inquire_daily_itemchartprice(
                output_dv="2",  # ✅ output2 데이터 (일자별 차트 데이터 배열) 조회
                itm_no=stock_code,
                period_code="D",  # 일봉
                adj_prc="1"
            )

            # ✅ DataFrame ambiguous 오류 해결
            if ohlcv_data is None or ohlcv_data.empty:
                logger.debug(f"{stock_code}: OHLCV 데이터 없음")
                return None

            # 4. 캔들 패턴 분석 (async 제거)
            pattern_result = self.pattern_detector.analyze_stock_patterns(stock_code, ohlcv_data)
            if not pattern_result or len(pattern_result) == 0:
                return None

            # 5. 가장 강한 패턴 선택
            strongest_pattern = max(pattern_result, key=lambda p: p.strength)

            # 6. 후보 생성
            candidate = CandleTradeCandidate(
                stock_code=stock_code,
                stock_name=stock_name,
                current_price=int(current_price),
                market_type=market_name  # 시장 타입 추가
            )

            # 패턴 정보 추가
            for pattern in pattern_result:
                candidate.add_pattern(pattern)

            # 🆕 매매 신호 생성 및 설정 (분리된 모듈 사용)
            trade_signal, signal_strength = self.signal_analyzer.generate_trade_signal(candidate, pattern_result)
            candidate.trade_signal = trade_signal
            candidate.signal_strength = signal_strength
            candidate.signal_updated_at = datetime.now()

            # 🆕 진입 우선순위 계산
            candidate.entry_priority = self._calculate_entry_priority(candidate)

            # 🆕 리스크 관리 설정 (분리된 모듈 사용)
            candidate.risk_management = self.risk_manager.calculate_risk_management(candidate)

            logger.info(f"✅ {stock_code}({stock_name}) 신호 생성: {trade_signal.value.upper()} "
                       f"(강도:{signal_strength}) 패턴:{strongest_pattern.pattern_type.value}")

            # 🆕 7. 데이터베이스에 후보 저장
            try:
                if self.trade_db:  # None 체크 추가
                    candidate_id = self.trade_db.record_candle_candidate(
                        stock_code=stock_code,
                        stock_name=stock_name,
                        current_price=int(current_price),
                        pattern_type=strongest_pattern.pattern_type.value,
                        pattern_strength=strongest_pattern.strength,
                        signal_strength='HIGH' if strongest_pattern.strength >= 80 else 'MEDIUM',
                        entry_reason=strongest_pattern.description,
                        risk_score=self._calculate_risk_score({'stck_prpr': current_price}),
                        target_price=int(current_price * 1.05),  # 5% 목표
                        stop_loss_price=int(current_price * 0.95)  # 5% 손절
                    )

                    candidate.metadata['db_id'] = candidate_id  # metadata에 저장
                    logger.info(f"🗄️ {stock_code} 후보 DB 저장 완료 (ID: {candidate_id})")

                    # 패턴 분석 결과도 저장
                    if pattern_result:
                        # ✅ 타입 변환 수정: strength를 문자열로, candle_data를 Dict 리스트로
                        pattern_data_list = []
                        for pattern in pattern_result:
                            pattern_dict = {
                                'pattern_type': pattern.pattern_type.value if hasattr(pattern.pattern_type, 'value') else str(pattern.pattern_type),
                                'strength': pattern.strength,
                                'confidence': pattern.confidence,
                                'description': pattern.description
                            }
                            pattern_data_list.append(pattern_dict)

                        self.trade_db.record_candle_pattern(
                            stock_code=stock_code,
                            pattern_name=strongest_pattern.pattern_type.value,
                            pattern_type='BULLISH' if strongest_pattern.strength >= 80 else 'BEARISH',
                            confidence_score=strongest_pattern.confidence,
                            strength=str(strongest_pattern.strength),  # ✅ 문자열로 변환
                            candle_data=pattern_data_list,  # ✅ Dict 리스트로 변환
                            volume_analysis=None,
                            trend_analysis=None,
                            predicted_direction='UP' if strongest_pattern.strength >= 80 else 'DOWN'
                        )

            except Exception as db_error:
                logger.warning(f"⚠️ {stock_code} DB 저장 실패: {db_error}")
                # DB 저장 실패해도 거래는 계속 진행

            # 7. 웹소켓 구독 (새로운 후보인 경우)
            try:
                if self.websocket_manager and stock_code not in self.subscribed_stocks:
                    success = await self.websocket_manager.subscribe_stock(stock_code)
                    if success:
                        self.subscribed_stocks.add(stock_code)
                        logger.info(f"📡 {stock_code} 웹소켓 구독 성공")
            except Exception as ws_error:
                if "ALREADY IN SUBSCRIBE" in str(ws_error):
                    self.subscribed_stocks.add(stock_code)
                else:
                    logger.warning(f"⚠️ {stock_code} 웹소켓 구독 오류: {ws_error}")

            logger.info(f"✅ {stock_code}({stock_name}) 패턴 감지: {strongest_pattern.pattern_type.value} "
                       f"신뢰도:{strongest_pattern.confidence:.2f} "
                       f"강도:{strongest_pattern.strength}점")

            return candidate

        except Exception as e:
            logger.error(f"❌ {stock_code} 패턴 분석 오류: {e}")
            return None

    async def _check_candidate_status_change(self, candidate: CandleTradeCandidate):
        """후보 종목 상태 변화 체크"""
        try:
            old_status = candidate.status
            old_signal = candidate.trade_signal

            # 🆕 실시간 신호 재평가 (가격 변화시)
            if candidate.detected_patterns:
                trade_signal, signal_strength = self.signal_analyzer.generate_trade_signal(candidate, candidate.detected_patterns)

                # 신호가 변경된 경우만 업데이트
                if trade_signal != candidate.trade_signal:
                    candidate.trade_signal = trade_signal
                    candidate.signal_strength = signal_strength
                    candidate.signal_updated_at = datetime.now()
                    candidate.entry_priority = self._calculate_entry_priority(candidate)

                    logger.info(f"🔄 {candidate.stock_code} 신호 변경: {old_signal.value} → {trade_signal.value} "
                               f"(강도:{signal_strength})")

            # 진입 조건 재평가
            if candidate.status == CandleStatus.WATCHING:
                # 매수 조건 재체크
                if candidate.is_ready_for_entry():
                    candidate.status = CandleStatus.BUY_READY
                    logger.info(f"🎯 {candidate.stock_code} 상태 변경: 관찰 → 매수준비")

            elif candidate.status == CandleStatus.ENTERED:
                # 매도 조건 재체크
                if self.risk_manager.should_exit_position(candidate):
                    await self.execution_manager.execute_exit(candidate, candidate.current_price, "패턴변화")

            # 상태 변경시 stock_manager 업데이트
            if old_status != candidate.status or old_signal != candidate.trade_signal:
                self.stock_manager.update_candidate(candidate)

        except Exception as e:
            logger.debug(f"후보 종목 상태 변화 체크 오류 ({candidate.stock_code}): {e}")

    def _should_exit_position(self, candidate: CandleTradeCandidate) -> bool:
        """포지션 매도 조건 확인 (간단한 버전)"""
        try:
            if not candidate.performance.entry_price:
                return False

            current_price = candidate.current_price
            entry_price = candidate.performance.entry_price

            # 🆕 패턴별 설정 가져오기
            target_profit_pct, stop_loss_pct, max_hours, pattern_based = self._get_pattern_based_target(candidate)

            # 손절 체크 (패턴별)
            pnl_pct = ((current_price - entry_price) / entry_price) * 100
            if pnl_pct <= -stop_loss_pct:
                return True

            # 익절 체크 (패턴별)
            if pnl_pct >= target_profit_pct:
                return True

            # 기존 목표가/손절가 도달
            if current_price <= candidate.risk_management.stop_loss_price:
                return True
            if current_price >= candidate.risk_management.target_price:
                return True

            # 시간 청산 체크 (패턴별)
            if self._should_time_exit_pattern_based(candidate, max_hours):
                return True

            return False

        except Exception as e:
            logger.debug(f"매도 조건 확인 오류: {e}")
            return False

    # ========== 매매 신호 생성 ==========



    async def _check_entry_conditions(self, candidate: CandleTradeCandidate,
                                    current_info: Dict) -> EntryConditions:
        """진입 조건 종합 체크"""
        try:
            conditions = EntryConditions()

            # 1. 거래량 조건
            volume = int(current_info.get('acml_vol', 0))
            avg_volume = int(current_info.get('avrg_vol', 1))
            volume_ratio = volume / max(avg_volume, 1)

            conditions.volume_check = volume_ratio >= self.config['min_volume_ratio']
            if not conditions.volume_check:
                conditions.fail_reasons.append(f"거래량 부족 ({volume_ratio:.1f}배)")

            # 2. RSI 조건 (과매수 체크)
            try:
                from ..analysis.technical_indicators import TechnicalIndicators
                from ..api.kis_market_api import get_inquire_daily_itemchartprice

                # 일봉 데이터 조회 (RSI 계산용)
                daily_data = get_inquire_daily_itemchartprice(
                    output_dv="2",  # 차트 데이터
                    itm_no=candidate.stock_code
                )

                if daily_data is not None and not daily_data.empty and len(daily_data) >= 14:
                    # 종가 데이터 추출
                    close_prices = []
                    for _, row in daily_data.iterrows():
                        try:
                            close_price = float(row.get('stck_clpr', 0))
                            if close_price > 0:
                                close_prices.append(close_price)
                        except (ValueError, TypeError):
                            continue

                    if len(close_prices) >= 14:
                        # 기존 TechnicalIndicators 클래스 사용
                        rsi_values = TechnicalIndicators.calculate_rsi(close_prices)
                        current_rsi = rsi_values[-1] if rsi_values else 50.0

                        # 과매수 구간(65 이상) 체크 - 더 엄격한 기준
                        conditions.rsi_check = current_rsi < 65  # 65 미만일 때 진입 허용

                        if not conditions.rsi_check:
                            conditions.fail_reasons.append(f"RSI 과매수 ({current_rsi:.1f})")

                        logger.debug(f"📊 {candidate.stock_code} RSI: {current_rsi:.1f}")
                    else:
                        conditions.rsi_check = True  # 데이터 부족시 통과
                        logger.debug(f"📊 {candidate.stock_code} RSI 데이터 부족 - 통과")
                else:
                    conditions.rsi_check = True  # 데이터 없을 시 통과
                    logger.debug(f"📊 {candidate.stock_code} 일봉 데이터 없음 - RSI 체크 통과")

            except Exception as e:
                logger.error(f"RSI 계산 오류 ({candidate.stock_code}): {e}")
                conditions.rsi_check = True  # 오류시 통과

            # 3. 시간대 조건
            current_time = datetime.now().time()
            trading_start = datetime.strptime(self.config['trading_start_time'], '%H:%M').time()
            trading_end = datetime.strptime(self.config['trading_end_time'], '%H:%M').time()

            conditions.time_check = trading_start <= current_time <= trading_end
            if not conditions.time_check:
                conditions.fail_reasons.append("거래 시간 외")
            conditions.time_check = True

            # 4. 가격대 조건
            price = candidate.current_price
            conditions.price_check = self.config['min_price'] <= price <= self.config['max_price']
            if not conditions.price_check:
                conditions.fail_reasons.append(f"가격대 부적합 ({price:,.0f}원)")

            # 5. 시가총액 조건 (간접 추정)
            conditions.market_cap_check = price >= 5000  # 간단한 추정

            # 6. 일일 거래대금 조건
            daily_amount = volume * price
            conditions.daily_volume_check = daily_amount >= self.config['min_daily_volume']
            if not conditions.daily_volume_check:
                conditions.fail_reasons.append(f"거래대금 부족 ({daily_amount/100000000:.0f}억원)")

            # 전체 통과 여부
            conditions.overall_passed = all([
                conditions.volume_check,
                conditions.rsi_check,
                conditions.time_check,
                conditions.price_check,
                conditions.market_cap_check,
                conditions.daily_volume_check
            ])

            return conditions

        except Exception as e:
            logger.error(f"진입 조건 체크 오류: {e}")
            return EntryConditions()



    def _calculate_risk_score(self, stock_info: dict) -> int:
        """위험도 점수 계산 (0-100)"""
        try:
            risk_score = 50  # 기본 점수

            current_price = float(stock_info.get('stck_prpr', 0))
            change_rate = float(stock_info.get('prdy_ctrt', 0))

            # 가격대별 위험도
            if current_price < 5000:
                risk_score += 20  # 저가주 위험
            elif current_price > 100000:
                risk_score += 10  # 고가주 위험

            # 변동률별 위험도
            if abs(change_rate) > 10:
                risk_score += 30  # 급등락 위험
            elif abs(change_rate) > 5:
                risk_score += 15

            return min(100, max(0, risk_score))
        except:
            return 50

    def _calculate_entry_priority(self, candidate: CandleTradeCandidate) -> int:
        """🆕 진입 우선순위 계산 (0~100)"""
        try:
            priority = 0

            # 1. 신호 강도 (30%)
            priority += candidate.signal_strength * 0.3

            # 2. 패턴 점수 (30%)
            priority += candidate.pattern_score * 0.3

            # 3. 패턴 신뢰도 (20%)
            if candidate.primary_pattern:
                priority += candidate.primary_pattern.confidence * 100 * 0.2

            # 4. 패턴별 가중치 (20%)
            if candidate.primary_pattern:
                pattern_weights = {
                    PatternType.MORNING_STAR: 20,      # 최고 신뢰도
                    PatternType.BULLISH_ENGULFING: 18,
                    PatternType.HAMMER: 15,
                    PatternType.INVERTED_HAMMER: 15,
                    PatternType.RISING_THREE_METHODS: 12,
                    PatternType.DOJI: 8,               # 가장 낮음
                }
                weight = pattern_weights.get(candidate.primary_pattern.pattern_type, 10)
                priority += weight

            # 정규화 (0~100)
            return min(100, max(0, int(priority)))

        except Exception as e:
            logger.error(f"진입 우선순위 계산 오류: {e}")
            return 50

    # ========== 진입 기회 평가 ==========
    # 진입 기회 평가 = 매수 준비 종목 평가
    async def _evaluate_entry_opportunities(self):
        """진입 기회 평가 및 매수 실행"""
        try:
            # 1단계: 기본 매수 후보들 조회 (패턴 기반)
            potential_candidates = self.stock_manager.get_stocks_by_signal(TradeSignal.STRONG_BUY)
            potential_candidates.extend(self.stock_manager.get_stocks_by_signal(TradeSignal.BUY))

            if not potential_candidates:
                logger.debug("🔍 매수 신호 종목 없음")
                return

            logger.info(f"🎯 매수 신호 종목: {len(potential_candidates)}개 - 진입 조건 재평가 시작")

            # 2단계: 각 후보에 대해 실시간 진입 조건 체크
            validated_candidates = []

            for candidate in potential_candidates:
                try:
                    # 최신 가격 정보 조회
                    from ..api.kis_market_api import get_inquire_price
                    current_data = get_inquire_price("J", candidate.stock_code)

                    if current_data is None or current_data.empty:
                        logger.debug(f"⏸️ {candidate.stock_code} 현재가 조회 실패")
                        continue

                    current_price = float(current_data.iloc[0].get('stck_prpr', 0))
                    if current_price <= 0:
                        continue

                    # 가격 업데이트
                    candidate.update_price(current_price)

                    # 🎯 핵심: 실시간 진입 조건 재평가
                    entry_conditions = await self._check_entry_conditions(
                        candidate,
                        current_data.iloc[0].to_dict()
                    )
                    candidate.entry_conditions = entry_conditions

                    # 진입 조건 통과시만 검증된 후보로 추가
                    if entry_conditions.overall_passed:
                        candidate.status = CandleStatus.BUY_READY
                        validated_candidates.append(candidate)
                        logger.info(f"✅ {candidate.stock_code} 진입 조건 통과 - 매수 준비")
                    else:
                        candidate.status = CandleStatus.WATCHING
                        fail_reasons = ', '.join(entry_conditions.fail_reasons)
                        logger.debug(f"⏸️ {candidate.stock_code} 진입 조건 미통과: {fail_reasons}")

                    # stock_manager 업데이트
                    self.stock_manager.update_candidate(candidate)

                except Exception as e:
                    logger.error(f"❌ {candidate.stock_code} 진입 조건 체크 오류: {e}")
                    continue

            if not validated_candidates:
                logger.info("🎯 진입 조건을 통과한 종목이 없습니다")
                return

            # 3단계: 우선순위별 정렬
            validated_candidates.sort(key=lambda c: c.entry_priority, reverse=True)
            logger.info(f"🎯 진입 조건 통과: {len(validated_candidates)}개 종목 (우선순위순 정렬 완료)")

            # 4단계: 포지션 수 체크
            active_positions = self.stock_manager.get_active_positions()
            max_positions = self.stock_manager.max_positions
            available_slots = max_positions - len(active_positions)

            if available_slots <= 0:
                logger.info("📊 최대 포지션 수 도달 - 신규 진입 제한")
                return

            # 5단계: 상위 후보들 매수 실행
            logger.info(f"💰 매수 실행 시작: {min(len(validated_candidates), available_slots)}개 종목")

            for i, candidate in enumerate(validated_candidates[:available_slots]):
                try:
                    logger.info(f"📈 매수 시도 {i+1}/{min(len(validated_candidates), available_slots)}: "
                            f"{candidate.stock_code}({candidate.stock_name}) - "
                            f"우선순위:{candidate.entry_priority}, "
                            f"신호:{candidate.trade_signal.value}({candidate.signal_strength})")

                    # 🆕 매수 매개변수 계산
                    try:
                        # 최신 가격 확인
                        from ..api.kis_market_api import get_inquire_price
                        current_data = get_inquire_price("J", candidate.stock_code)

                        if current_data is None or current_data.empty:
                            logger.warning(f"❌ {candidate.stock_code} 현재가 조회 실패")
                            continue

                        current_price = float(current_data.iloc[0].get('stck_prpr', 0))
                        if current_price <= 0:
                            logger.warning(f"❌ {candidate.stock_code} 가격 정보 없음")
                            continue

                        # 가격 업데이트
                        candidate.update_price(current_price)

                        # 💰 투자 금액 계산 (개선된 버전)
                        try:
                            from ..api.kis_market_api import get_account_balance
                            balance_info = get_account_balance()

                            # 🆕 설정값 로드
                            inv_config = self.config['investment_calculation']

                            if balance_info and balance_info.get('total_value', 0) > 0:
                                # 📊 계좌 정보 분석 - 실제 매수가능금액을 TradingManager에서 가져오기
                                total_evaluation = balance_info.get('total_value', 0)  # 총평가액 (tot_evlu_amt)

                                # 🎯 실제 매수가능금액 조회 (TradingManager 활용)
                                try:
                                    if hasattr(self, 'trade_executor') and self.trade_executor:
                                        trading_balance = self.trade_executor.trading_manager.get_balance()
                                        if trading_balance.get('success'):
                                            actual_cash = trading_balance.get('available_cash', 0)
                                            cash_balance = actual_cash
                                            calculation_method = "실제매수가능금액"
                                        else:
                                            cash_balance = 0
                                            calculation_method = "매수가능금액조회실패"
                                    else:
                                        cash_balance = 0
                                        calculation_method = "TradeExecutor없음"

                                except Exception as e:
                                    logger.warning(f"⚠️ 매수가능금액 조회 오류: {e}")
                                    cash_balance = 0
                                    calculation_method = "오류시백업"

                                # 🎯 투자 가능 금액 결정 (설정 기반)
                                # 1순위: 현금잔고 * cash_usage_ratio (실제 매수 가능 현금)
                                # 2순위: 총평가액 * portfolio_usage_ratio (전체 포트폴리오 규모 기준)
                                cash_based_amount = int(cash_balance * inv_config['cash_usage_ratio']) if cash_balance > 0 else 0
                                portfolio_based_amount = int(total_evaluation * inv_config['portfolio_usage_ratio'])

                                # 더 안전한 금액 선택 (현금 우선, 없으면 평가액 기준)
                                if cash_based_amount >= inv_config['min_cash_threshold']:  # 설정값 기준 현금이 있을 때
                                    total_available = cash_based_amount
                                    calculation_method = str(calculation_method) + " → 현금잔고기준"
                                else:
                                    total_available = min(portfolio_based_amount, inv_config['max_portfolio_limit'])  # 설정값 기준 최대 제한
                                    calculation_method = str(calculation_method) + " → 총평가액기준"
                            else:
                                # 백업: 기본 투자 금액
                                total_available = inv_config['default_investment']  # 설정값 사용
                                calculation_method = "기본값"
                                logger.warning(f"📊 잔고 조회 실패 - 기본 투자 금액 사용: {total_available:,}원")

                        except Exception as e:
                            logger.warning(f"📊 잔고 조회 오류 - 기본 투자 금액 사용: {e}")
                            inv_config = self.config['investment_calculation']
                            total_available = inv_config['default_investment']  # 설정값 사용
                            calculation_method = "오류시기본값"

                        # 🎯 포지션별 실제 투자 금액 계산
                        # position_size_pct: 패턴별 포지션 크기 (10~30%, 보통 20%)
                        position_amount = int(total_available * candidate.risk_management.position_size_pct / 100)

                        # 📈 매수 수량 계산 (소수점 이하 버림)
                        # current_price: 현재 주가 (원)
                        # quantity: 실제 매수할 주식 수량 (주)
                        quantity = position_amount // current_price

                        # ✅ 최소 투자 조건 체크
                        min_investment = inv_config['min_investment']  # 설정값 사용
                        if quantity <= 0 or position_amount < min_investment:
                            logger.warning(f"❌ {candidate.stock_code}: 투자조건 미달 - "
                                         f"가용금액:{total_available:,}원, "
                                         f"포지션크기:{candidate.risk_management.position_size_pct}%, "
                                         f"투자금액:{position_amount:,}원, "
                                         f"수량:{quantity}주 (최소:{min_investment:,}원)")
                            continue

                        # 매수 신호 생성
                        signal = {
                            'stock_code': candidate.stock_code,
                            'action': 'buy',
                            'strategy': 'candle_pattern',
                            'price': current_price,
                            'quantity': quantity,
                            'total_amount': position_amount,
                            'pattern_type': str(candidate.detected_patterns[0].pattern_type) if candidate.detected_patterns else 'unknown',
                            'pre_validated': True,
                            'validation_source': 'candle_system'
                        }

                        # 매개변수 준비
                        entry_params = {
                            'current_price': current_price,
                            'position_amount': position_amount,
                            'quantity': quantity,
                            'signal': signal
                        }

                    except Exception as e:
                        logger.error(f"❌ {candidate.stock_code} 매수 매개변수 계산 오류: {e}")
                        continue

                    # 🆕 실제 매수 실행 (분리된 모듈 사용)
                    success = await self.execution_manager.execute_entry(candidate, entry_params)
                    if success:
                        logger.info(f"✅ {candidate.stock_code} 매수 성공")
                    else:
                        logger.warning(f"❌ {candidate.stock_code} 매수 실패")

                except Exception as e:
                    logger.error(f"❌ {candidate.stock_code} 매수 실행 오류: {e}")

        except Exception as e:
            logger.error(f"❌ 진입 기회 평가 오류: {e}")

    # 🗑️ DEPRECATED: execution_manager.execute_entry 사용
    # async def _execute_entry(...) 메서드는 execution_manager로 이동됨

    # ========== 포지션 관리 ==========

    async def _manage_existing_positions(self):
        """기존 포지션 관리 (손절/익절/추적손절) - _all_stocks 통합 버전"""
        try:
            # 🆕 _all_stocks에서 ENTERED 상태인 모든 종목 관리 (기존 보유 + 새로 매수)
            entered_positions = [
                stock for stock in self._all_stocks.values()
                if stock.status == CandleStatus.ENTERED
            ]

            if not entered_positions:
                return

            logger.debug(f"📊 포지션 관리: {len(entered_positions)}개 포지션 (_all_stocks 통합)")

            for position in entered_positions:
                try:
                    await self._manage_single_position(position)
                except Exception as e:
                    logger.error(f"포지션 관리 오류 ({position.stock_code}): {e}")

        except Exception as e:
            logger.error(f"포지션 관리 오류: {e}")

    async def _manage_single_position(self, position: CandleTradeCandidate):
        """개별 포지션 관리"""
        try:
            # 🕐 거래 시간 체크 (매도 시간 제한)
            current_time = datetime.now().time()
            trading_start = datetime.strptime(self.config['trading_start_time'], '%H:%M').time()
            trading_end = datetime.strptime(self.config['trading_end_time'], '%H:%M').time()

            is_trading_time = trading_start <= current_time <= trading_end
            if not is_trading_time:
                logger.debug(f"⏰ {position.stock_code} 거래 시간 외 - 매도 대기 중")
                return

            # 최신 가격 조회
            from ..api.kis_market_api import get_inquire_price
            current_data = get_inquire_price("J", position.stock_code)

            if current_data is None or current_data.empty:
                return

            current_price = float(current_data.iloc[0].get('stck_prpr', 0))

            if current_price <= 0:
                return

            # 가격 업데이트
            old_price = position.current_price
            position.update_price(current_price)

            # 수익률 계산
            entry_price = position.performance.entry_price
            if not entry_price:
                return

            pnl_pct = ((current_price - entry_price) / entry_price) * 100

            # 🆕 캔들 패턴별 설정 결정 (목표, 손절, 시간)
            target_profit_pct, stop_loss_pct, max_hours, pattern_based = self._get_pattern_based_target(position)

            # 🔄 추적 손절 항상 실행 (수익시) - 분리된 모듈 사용
            if pnl_pct > 2:  # 2% 이상 수익시 추적 손절 활성화
                self.risk_manager.update_trailing_stop(position, current_price)

            # 🎯 매도 조건 체크 (우선순위순)
            exit_reason = None
            should_exit = False

            # 1. 🆕 패턴별 수익률 체크 (최우선)
            if pnl_pct >= target_profit_pct:
                exit_reason = f"{target_profit_pct}% 수익 달성"
                should_exit = True
                pattern_info = f" (패턴: {position.metadata.get('original_pattern_type', 'Unknown')})" if pattern_based else " (수동매수)"
                logger.info(f"🎯 {position.stock_code} {target_profit_pct}% 수익 달성! 매도 실행 ({pnl_pct:+.2f}%){pattern_info}")

            # 2. 🆕 패턴별 손절 체크
            elif pnl_pct <= -stop_loss_pct:
                exit_reason = f"{stop_loss_pct}% 손절"
                should_exit = True
                logger.info(f"🛑 {position.stock_code} {stop_loss_pct}% 손절 실행 ({pnl_pct:+.2f}%)")

            # 3. 기존 목표가/손절가 도달 (RiskManagement 설정) - 추적손절로 업데이트됨
            elif current_price >= position.risk_management.target_price:
                exit_reason = "목표가 도달"
                should_exit = True
                logger.info(f"🎯 {position.stock_code} 목표가 도달 매도: {current_price:,.0f}원 >= {position.risk_management.target_price:,.0f}원")
            elif current_price <= position.risk_management.stop_loss_price:
                exit_reason = "손절가 도달"
                should_exit = True
                logger.info(f"🛑 {position.stock_code} 손절가 도달 매도: {current_price:,.0f}원 <= {position.risk_management.stop_loss_price:,.0f}원")

            # 4. 🆕 패턴별 시간 청산 체크
            elif self._should_time_exit_pattern_based(position, max_hours):
                exit_reason = f"{max_hours}시간 청산"
                should_exit = True
                logger.info(f"⏰ {position.stock_code} {max_hours}시간 청산 실행")

            # 🔚 청산 실행 (거래 시간 내에서만) - 분리된 모듈 사용
            if should_exit and exit_reason:
                success = await self.execution_manager.execute_exit(position, current_price, exit_reason)
                if success:
                    logger.info(f"✅ {position.stock_code} 청산 완료: {exit_reason} "
                               f"(수익률 {pnl_pct:+.1f}%)")
                return

            # 포지션 상태 업데이트
            self.stock_manager.update_candidate(position)

        except Exception as e:
            logger.error(f"개별 포지션 관리 오류 ({position.stock_code}): {e}")

    def _get_pattern_based_target(self, position: CandleTradeCandidate) -> Tuple[float, float, int, bool]:
        """🎯 캔들 패턴별 수익률 목표, 손절, 시간 설정 결정"""
        try:
            # 1. 캔들 전략으로 매수한 종목인지 확인
            is_candle_strategy = (
                position.metadata.get('restored_from_db', False) or  # DB에서 복원됨
                position.metadata.get('original_entry_source') == 'candle_strategy' or  # 캔들 전략 매수
                len(position.detected_patterns) > 0  # 패턴 정보가 있음
            )

            if not is_candle_strategy:
                # 수동/앱 매수 종목: 큰 수익/손실 허용 (🎯 5% 목표, 5% 손절)
                logger.debug(f"📊 {position.stock_code} 수동 매수 종목 - 기본 설정 적용")
                return 5.0, 5.0, 24, False

            # 2. 🔄 실시간 캔들 패턴 재분석 (DB 의존 제거)
            original_pattern = None

            # 🆕 실시간 캔들 패턴 분석 (가장 우선)
            try:
                from ..api.kis_market_api import get_inquire_daily_itemchartprice
                ohlcv_data = get_inquire_daily_itemchartprice(
                    output_dv="2",
                    itm_no=position.stock_code,
                    period_code="D",
                    adj_prc="1"
                )

                if ohlcv_data is not None and not ohlcv_data.empty:
                    pattern_result = self.pattern_detector.analyze_stock_patterns(position.stock_code, ohlcv_data)
                    if pattern_result and len(pattern_result) > 0:
                        strongest_pattern = max(pattern_result, key=lambda p: p.strength)
                        original_pattern = strongest_pattern.pattern_type.value
                        logger.debug(f"🔄 {position.stock_code} 실시간 패턴 분석: {original_pattern} (강도: {strongest_pattern.strength})")
            except Exception as e:
                logger.debug(f"실시간 패턴 분석 오류 ({position.stock_code}): {e}")

            # DB에서 복원된 경우 (백업)
            if not original_pattern and 'original_pattern_type' in position.metadata:
                original_pattern = position.metadata['original_pattern_type']
                logger.debug(f"📚 {position.stock_code} DB에서 패턴 복원: {original_pattern}")

            # 기존 패턴 정보 활용 (백업)
            elif not original_pattern and position.detected_patterns and len(position.detected_patterns) > 0:
                strongest_pattern = max(position.detected_patterns, key=lambda p: p.strength)
                original_pattern = strongest_pattern.pattern_type.value
                logger.debug(f"📊 {position.stock_code} 기존 패턴 정보 활용: {original_pattern}")

            # 3. 패턴별 목표, 손절, 시간 설정 적용
            if original_pattern:
                # 패턴명을 소문자로 변환하여 config에서 조회
                pattern_key = original_pattern.lower().replace('_', '_')
                pattern_config = self.config['pattern_targets'].get(pattern_key)

                if pattern_config:
                    target_pct = pattern_config['target']
                    stop_pct = pattern_config['stop']
                    max_hours = pattern_config['max_hours']

                    logger.debug(f"📊 {position.stock_code} 패턴 '{original_pattern}' - "
                                f"목표:{target_pct}%, 손절:{stop_pct}%, 시간:{max_hours}h")
                    return target_pct, stop_pct, max_hours, True
                else:
                    # 패턴 config에 없으면 패턴 강도에 따라 결정 (🎯 큰 수익/손실 허용)
                    if position.detected_patterns:
                        strongest_pattern = max(position.detected_patterns, key=lambda p: p.strength)
                        if strongest_pattern.strength >= 90:
                            target_pct, stop_pct, max_hours = 15.0, 4.0, 8  # 매우 강한 패턴
                        elif strongest_pattern.strength >= 80:
                            target_pct, stop_pct, max_hours = 12.0, 3.0, 6  # 강한 패턴
                        elif strongest_pattern.strength >= 70:
                            target_pct, stop_pct, max_hours = 8.0, 3.0, 4  # 중간 패턴
                        else:
                            target_pct, stop_pct, max_hours = 5.0, 2.0, 2  # 약한 패턴

                        logger.debug(f"📊 {position.stock_code} 패턴 강도 {strongest_pattern.strength} - "
                                    f"목표:{target_pct}%, 손절:{stop_pct}%, 시간:{max_hours}h")
                        return target_pct, stop_pct, max_hours, True

            # 4. 기본값: 캔들 전략이지만 패턴 정보 없음 (🎯 큰 수익/손실 허용)
            logger.debug(f"📊 {position.stock_code} 캔들 전략이나 패턴 정보 없음 - 기본 캔들 설정 적용")
            return 10.0, 5.0, 6, True

        except Exception as e:
            logger.error(f"패턴별 설정 결정 오류 ({position.stock_code}): {e}")
            # 오류시 안전하게 기본값 반환 (🎯 큰 수익/손실 허용)
            return 10.0, 5.0, 24, False

    def _should_time_exit_pattern_based(self, position: CandleTradeCandidate, max_hours: int) -> bool:
        """🆕 패턴별 시간 청산 조건 체크"""
        try:
            if not position.performance.entry_time:
                return False

            # 보유 시간 계산
            holding_time = datetime.now() - position.performance.entry_time
            max_holding = timedelta(hours=max_hours)

            # 패턴별 최대 보유시간 초과시 청산
            if holding_time >= max_holding:
                logger.info(f"⏰ {position.stock_code} 패턴별 최대 보유시간({max_hours}h) 초과 청산: {holding_time}")
                return True

            # 새로운 시간 기반 청산 규칙 적용 (선택적)
            time_rules = self.config.get('time_exit_rules', {})

            # 수익 중 시간 청산 (패턴별 시간의 절반 후)
            profit_exit_hours = max_hours // 2  # 패턴별 시간의 절반
            min_profit = time_rules.get('min_profit_for_time_exit', 0.5) / 100

            if (holding_time >= timedelta(hours=profit_exit_hours) and
                position.performance.pnl_pct and
                position.performance.pnl_pct >= min_profit):
                logger.info(f"⏰ {position.stock_code} 패턴별 시간 기반 수익 청산: {holding_time}")
                return True

            return False

        except Exception as e:
            logger.error(f"패턴별 시간 청산 체크 오류: {e}")
            return False

    # 🗑️ DEPRECATED: execution_manager.execute_exit 사용
    # async def _execute_exit(...) 메서드는 execution_manager로 이동됨

    def _calculate_safe_sell_price(self, current_price: float, reason: str) -> int:
        """안전한 매도가 계산 (틱 단위 맞춤)"""
        try:
            # 매도 이유별 할인율 적용
            if reason == "손절":
                discount_pct = 0.008  # 0.8% 할인 (빠른 체결 우선)
            elif reason in ["목표가 도달", "익절"]:
                discount_pct = 0.003  # 0.3% 할인 (적당한 체결)
            elif reason == "시간 청산":
                discount_pct = 0.005  # 0.5% 할인 (중간 속도)
            else:
                discount_pct = 0.005  # 기본 0.5% 할인

            # 할인된 가격 계산
            target_price = int(current_price * (1 - discount_pct))

            # 틱 단위 맞춤
            tick_unit = self._get_tick_unit(target_price)
            safe_price = (target_price // tick_unit) * tick_unit

            # 최소 가격 보정 (너무 낮으면 안됨)
            min_price = int(current_price * 0.97)  # 현재가의 97% 이상
            safe_price = max(safe_price, min_price)

            logger.debug(f"💰 매도가 계산: 현재가{current_price:,.0f}원 → 주문가{safe_price:,.0f}원 "
                        f"({reason}, 할인{discount_pct*100:.1f}%)")

            return safe_price

        except Exception as e:
            logger.error(f"안전한 매도가 계산 오류: {e}")
            # 오류시 현재가의 99% 반환 (안전장치)
            return int(current_price * 0.99)

    def _get_tick_unit(self, price: int) -> int:
        """호가단위 계산"""
        try:
            if price < 2000:
                return 1
            elif price < 5000:
                return 5
            elif price < 20000:
                return 10
            elif price < 50000:
                return 50
            elif price < 200000:
                return 100
            elif price < 500000:
                return 500
            else:
                return 1000
        except:
            return 100  # 기본값

    # ========== 보조 함수들 ==========

    def _is_trading_time(self) -> bool:
        """거래 시간 체크"""
        try:
            current_time = datetime.now().time()
            trading_start = datetime.strptime(self.config['trading_start_time'], '%H:%M').time()
            trading_end = datetime.strptime(self.config['trading_end_time'], '%H:%M').time()

            return trading_start <= current_time <= trading_end
        except Exception as e:
            logger.error(f"거래 시간 체크 오류: {e}")
            return False

    def _passes_basic_filters(self, price: float, stock_info: Dict) -> bool:
        """기본 필터링 통과 여부"""
        try:
            # 가격대 필터
            if not (self.config['min_price'] <= price <= self.config['max_price']):
                return False

            # 거래량 필터 (간단 체크)
            volume = int(stock_info.get('acml_vol', 0))
            if volume < 10000:  # 최소 1만주
                return False

            return True

        except Exception as e:
            logger.error(f"기본 필터링 오류: {e}")
            return False

    def _should_time_exit(self, position: CandleTradeCandidate) -> bool:
        """시간 청산 조건 체크 (개선된 버전)"""
        try:
            if not position.performance.entry_time:
                return False

            # 보유 시간 계산
            holding_time = datetime.now() - position.performance.entry_time
            max_holding = timedelta(hours=position.risk_management.max_holding_hours)

            # 1. 최대 보유시간 초과시 무조건 청산
            if holding_time >= max_holding:
                logger.info(f"⏰ {position.stock_code} 최대 보유시간 초과 청산: {holding_time}")
                return True

            # 2. 새로운 시간 기반 청산 규칙 적용
            time_rules = self.config.get('time_exit_rules', {})

            # 수익 중 시간 청산 (3시간 후)
            profit_exit_hours = time_rules.get('profit_exit_hours', 3)
            min_profit = time_rules.get('min_profit_for_time_exit', 0.5) / 100

            if (holding_time >= timedelta(hours=profit_exit_hours) and
                position.performance.pnl_pct and
                position.performance.pnl_pct >= min_profit):
                logger.info(f"⏰ {position.stock_code} 시간 기반 수익 청산: {holding_time}")
                return True

            return False

        except Exception as e:
            logger.error(f"시간 청산 체크 오류: {e}")
            return False

    # 🗑️ DEPRECATED: risk_manager.update_trailing_stop 사용
    # 동적 목표/손절 조정 로직은 risk_manager로 이동됨

    # 🗑️ DEPRECATED: signal_analyzer.analyze_realtime_patterns 사용

    def _get_pattern_strength_tier(self, strength: int) -> str:
        """패턴 강도를 티어로 분류"""
        if strength >= 90:
            return 'ULTRA_STRONG'  # 15% 목표, 4% 손절
        elif strength >= 80:
            return 'STRONG'        # 12% 목표, 3% 손절
        elif strength >= 70:
            return 'MEDIUM'        # 8% 목표, 3% 손절
        elif strength >= 60:
            return 'WEAK'          # 5% 목표, 2% 손절
        else:
            return 'VERY_WEAK'     # 3% 목표, 1.5% 손절

    # 🗑️ DEPRECATED: risk_manager.calculate_profit_adjustments 사용

    # 🗑️ DEPRECATED: risk_manager.calculate_trend_adjustments 사용

    # 🗑️ DEPRECATED: risk_manager.apply_dynamic_adjustments 사용

    def _get_pattern_tier_targets(self, strength_tier: str) -> Tuple[float, float]:
        """패턴 강도 티어별 목표/손절 퍼센트 반환"""
        tier_settings = {
            'ULTRA_STRONG': (15.0, 4.0),   # 15% 목표, 4% 손절
            'STRONG': (12.0, 3.0),         # 12% 목표, 3% 손절
            'MEDIUM': (8.0, 3.0),          # 8% 목표, 3% 손절
            'WEAK': (5.0, 2.0),            # 5% 목표, 2% 손절
            'VERY_WEAK': (3.0, 1.5)        # 3% 목표, 1.5% 손절
        }
        return tier_settings.get(strength_tier, (5.0, 2.0))

    def _fallback_trailing_stop(self, position: CandleTradeCandidate, current_price: float):
        """기존 방식 추적 손절 (폴백용)"""
        try:
            trailing_pct = position.risk_management.trailing_stop_pct / 100
            new_trailing_stop = current_price * (1 - trailing_pct)

            # 기존 손절가보다 높을 때만 업데이트
            if new_trailing_stop > position.risk_management.stop_loss_price:
                position.risk_management.stop_loss_price = new_trailing_stop
                logger.debug(f"📈 {position.stock_code} 기본 추적손절 업데이트: {new_trailing_stop:,.0f}원")

        except Exception as e:
            logger.error(f"기본 추적 손절 오류: {e}")

    async def _initialize_trading_day(self):
        """거래일 초기화"""
        try:
            logger.info("📅 거래일 초기화 시작")

            # 일일 통계 초기화
            self.daily_stats = {
                'trades_count': 0,
                'successful_trades': 0,
                'failed_trades': 0,
                'total_profit_loss': 0.0,
            }

            # 기존 완료된 거래 정리
            self.stock_manager.auto_cleanup()

            logger.info("✅ 거래일 초기화 완료")

        except Exception as e:
            logger.error(f"거래일 초기화 오류: {e}")

    def _log_status(self):
        """현재 상태 로깅"""
        try:
            stats = self.stock_manager.get_summary_stats()

            if self._last_scan_time:
                last_scan = (datetime.now() - self._last_scan_time).total_seconds()
                logger.info(f"📊 상태: 관찰{stats['total_stocks']}개 "
                           f"포지션{stats['active_positions']}개 "
                           f"마지막스캔{last_scan:.0f}초전")

        except Exception as e:
            logger.debug(f"상태 로깅 오류: {e}")

    # ========== 공개 인터페이스 (간소화) ==========

    def get_current_status(self) -> Dict[str, Any]:
        """현재 상태 조회"""
        try:
            stats = self.stock_manager.get_summary_stats()

            return {
                'is_running': self.is_running,
                'last_scan_time': self._last_scan_time.strftime('%H:%M:%S') if self._last_scan_time else None,
                'stock_counts': {
                    'total': stats['total_stocks'],
                    'active_positions': stats['active_positions'],
                    'buy_ready': stats.get('buy_ready', 0),
                    'sell_ready': stats.get('sell_ready', 0)
                },
                'daily_stats': self.daily_stats,
                'config': self.config
            }

        except Exception as e:
            logger.error(f"상태 조회 오류: {e}")
            return {'error': str(e)}

    def get_top_candidates(self, limit: int = 15) -> List[Dict[str, Any]]:
        """상위 매수 후보 조회"""
        try:
            candidates = self.stock_manager.get_top_buy_candidates(limit)
            return [candidate.to_dict() for candidate in candidates]
        except Exception as e:
            logger.error(f"상위 후보 조회 오류: {e}")
            return []

    def get_active_positions(self) -> List[Dict[str, Any]]:
        """활성 포지션 조회"""
        try:
            positions = self.stock_manager.get_active_positions()
            return [position.to_dict() for position in positions]
        except Exception as e:
            logger.error(f"활성 포지션 조회 오류: {e}")
            return []

    async def _save_candle_position_to_db(self, strategy_data: Dict) -> Optional[int]:
        """캔들 전략 포지션 정보를 DB에 저장"""
        try:
            if not self.trade_db:
                return None

            # 🆕 임시로 기존 DB 함수 활용 (나중에 전용 함수로 교체 가능)
            position_id = self.trade_db.record_candle_candidate(
                stock_code=strategy_data['stock_code'],
                stock_name=strategy_data['stock_name'],
                current_price=strategy_data['entry_price'],
                pattern_type=strategy_data['pattern_type'],
                pattern_strength=strategy_data['pattern_strength'],
                signal_strength='HIGH' if strategy_data['signal_strength'] >= 80 else 'MEDIUM',
                entry_reason=f"캔들전략매수:{strategy_data['pattern_type']}",
                risk_score=strategy_data['strategy_config'].get('risk_score', 50),
                target_price=strategy_data['target_price'],
                stop_loss_price=strategy_data['stop_loss_price']
            )

            # 추가 전략 정보를 메타데이터로 저장 (JSON 형태)
            import json
            metadata = {
                'entry_quantity': strategy_data['entry_quantity'],
                'position_amount': strategy_data['position_amount'],
                'trailing_stop_pct': strategy_data.get('trailing_stop_pct', 1.0),
                'max_holding_hours': strategy_data.get('max_holding_hours', 24),
                'strategy_config': strategy_data['strategy_config']
            }
            # metadata 저장은 추후 구현 또는 기존 함수 활용

            return position_id

        except Exception as e:
            logger.error(f"캔들 포지션 DB 저장 오류: {e}")
            return None

    async def _analyze_existing_holding_patterns(self, stock_code: str, stock_name: str, current_price: float) -> Optional[Dict]:
        """🔄 기존 보유 종목의 실시간 캔들 패턴 분석"""
        try:
            logger.debug(f"🔄 {stock_code} 실시간 캔들 패턴 분석 시작")

            # OHLCV 데이터 조회
            from ..api.kis_market_api import get_inquire_daily_itemchartprice
            ohlcv_data = get_inquire_daily_itemchartprice(
                output_dv="2",  # 일자별 차트 데이터 배열
                itm_no=stock_code,
                period_code="D",  # 일봉
                adj_prc="1"
            )

            if ohlcv_data is None or ohlcv_data.empty:
                logger.debug(f"❌ {stock_code} OHLCV 데이터 조회 실패")
                return None

            # 캔들 패턴 분석
            pattern_result = self.pattern_detector.analyze_stock_patterns(stock_code, ohlcv_data)

            if not pattern_result or len(pattern_result) == 0:
                logger.debug(f"❌ {stock_code} 캔들 패턴 감지 실패")
                return None

            # 가장 강한 패턴 선택
            strongest_pattern = max(pattern_result, key=lambda p: p.strength)

            # 매매 신호 생성
            trade_signal, signal_strength = self._generate_trade_signal_from_patterns(pattern_result)

            result = {
                'patterns_detected': True,
                'patterns': pattern_result,
                'strongest_pattern': {
                    'type': strongest_pattern.pattern_type.value,
                    'strength': strongest_pattern.strength,
                    'confidence': strongest_pattern.confidence,
                    'description': strongest_pattern.description
                },
                'trade_signal': trade_signal,
                'signal_strength': signal_strength,
                'analysis_time': datetime.now().isoformat()
            }

            logger.info(f"✅ {stock_code} 캔들 패턴 분석 완료: {strongest_pattern.pattern_type.value} "
                       f"(강도: {strongest_pattern.strength}, 신뢰도: {strongest_pattern.confidence:.2f})")

            return result

        except Exception as e:
            logger.error(f"❌ {stock_code} 캔들 패턴 분석 오류: {e}")
            return None

    def _generate_trade_signal_from_patterns(self, patterns: List) -> Tuple:
        """패턴 목록에서 매매 신호 생성"""
        try:
            if not patterns:
                return 'HOLD', 0

            # 가장 강한 패턴 기준으로 신호 생성
            strongest_pattern = max(patterns, key=lambda p: p.strength)

            from .candle_trade_candidate import PatternType, TradeSignal

            # 강세 패턴들
            bullish_patterns = {
                PatternType.HAMMER, PatternType.INVERTED_HAMMER,
                PatternType.BULLISH_ENGULFING, PatternType.MORNING_STAR,
                PatternType.RISING_THREE_METHODS
            }

            if strongest_pattern.pattern_type in bullish_patterns:
                # 더 엄격한 기준 적용
                if strongest_pattern.confidence >= 0.9 and strongest_pattern.strength >= 95:
                    return TradeSignal.STRONG_BUY, strongest_pattern.strength
                elif strongest_pattern.confidence >= 0.8 and strongest_pattern.strength >= 85:
                    return TradeSignal.BUY, strongest_pattern.strength
                else:
                    return TradeSignal.HOLD, strongest_pattern.strength
            else:
                return TradeSignal.HOLD, strongest_pattern.strength

        except Exception as e:
            logger.error(f"패턴 신호 생성 오류: {e}")
            return 'HOLD', 0

    # 🗑️ DEPRECATED: signal_analyzer.periodic_evaluation 사용
    # 주기적 신호 재평가 시스템은 signal_analyzer로 이동됨

    # 🗑️ DEPRECATED: signal_analyzer.batch_evaluate 사용

    # 🗑️ DEPRECATED: signal_analyzer.batch_evaluate_exit 사용

    # 🗑️ DEPRECATED: signal_analyzer.comprehensive_analysis 사용
    # 종합 신호 분석은 signal_analyzer로 이동됨

    # 🗑️ DEPRECATED: signal_analyzer.analyze_patterns 사용

    # 🗑️ DEPRECATED: signal_analyzer.analyze_technical 사용

    # 🗑️ DEPRECATED: risk_manager.analyze_time_conditions 사용

    # 🗑️ DEPRECATED: risk_manager.analyze_risk_conditions 사용

    def _analyze_exit_conditions(self, candidate: CandleTradeCandidate, current_price: float, ohlcv_data: Optional[Any] = None) -> Dict:
        """🎯 매도 조건 분석 (진입한 종목용)"""
        try:
            # 기본 매도 조건들
            should_exit = False
            exit_reasons = []

            # 🆕 패턴별 설정 가져오기
            target_profit_pct, stop_loss_pct, max_hours, pattern_based = self._get_pattern_based_target(candidate)

            # 1. 수익률 기반 매도 (패턴별 설정 사용)
            if candidate.performance.entry_price:
                pnl_pct = ((current_price - candidate.performance.entry_price) / candidate.performance.entry_price) * 100

                if pnl_pct >= target_profit_pct:
                    should_exit = True
                    exit_reasons.append(f'{target_profit_pct}% 수익 달성')
                elif pnl_pct <= -stop_loss_pct:
                    should_exit = True
                    exit_reasons.append(f'{stop_loss_pct}% 손절')

            # 2. 목표가/손절가 도달
            if current_price >= candidate.risk_management.target_price:
                should_exit = True
                exit_reasons.append('목표가 도달')
            elif current_price <= candidate.risk_management.stop_loss_price:
                should_exit = True
                exit_reasons.append('손절가 도달')

            # 3. 시간 청산 (패턴별)
            if self._should_time_exit_pattern_based(candidate, max_hours):
                should_exit = True
                exit_reasons.append(f'{max_hours}시간 청산')

            signal = 'strong_sell' if should_exit else 'hold'

            return {
                'signal': signal,
                'should_exit': should_exit,
                'exit_reasons': exit_reasons
            }

        except Exception as e:
            logger.debug(f"매도 조건 분석 오류: {e}")
            return {'signal': 'hold', 'should_exit': False, 'exit_reasons': []}

    def _analyze_entry_conditions_simple(self, candidate: CandleTradeCandidate, current_price: float) -> Dict:
        """🎯 진입 조건 간단 분석 (관찰 중인 종목용)"""
        try:
            # 기본 진입 조건 체크
            can_enter = True
            entry_reasons = []

            # 가격대 체크
            if not (self.config['min_price'] <= current_price <= self.config['max_price']):
                can_enter = False
            else:
                entry_reasons.append('가격대 적정')

            # 거래 시간 체크
            if self._is_trading_time():
                entry_reasons.append('거래 시간')
            else:
                can_enter = False

            signal = 'buy_ready' if can_enter else 'wait'

            return {
                'signal': signal,
                'can_enter': can_enter,
                'entry_reasons': entry_reasons
            }

        except Exception as e:
            logger.debug(f"진입 조건 분석 오류: {e}")
            return {'signal': 'wait', 'can_enter': False, 'entry_reasons': []}

    def _calculate_comprehensive_signal(self, pattern_signals: Dict, technical_signals: Dict,
                                      time_signals: Dict, risk_signals: Dict, position_signals: Dict,
                                      focus_on_exit: bool = False) -> Tuple[TradeSignal, int]:
        """🧮 종합 신호 계산"""
        try:
            # 가중치 설정
            if focus_on_exit:
                # 매도 신호 중심
                weights = {
                    'risk': 0.4,        # 리스크 조건이 가장 중요
                    'position': 0.3,    # 포지션 분석
                    'time': 0.2,        # 시간 조건
                    'technical': 0.1,   # 기술적 지표
                    'pattern': 0.0      # 패턴은 매도시 덜 중요
                }
            else:
                # 매수 신호 중심
                weights = {
                    'pattern': 0.4,     # 패턴이 가장 중요
                    'technical': 0.3,   # 기술적 지표
                    'position': 0.2,    # 진입 조건
                    'risk': 0.1,        # 리스크 조건
                    'time': 0.0         # 시간은 덜 중요
                }

            # 각 신호의 점수 계산 (0~100)
            pattern_score = self._get_signal_score(pattern_signals.get('signal', 'neutral'), 'pattern')
            technical_score = self._get_signal_score(technical_signals.get('signal', 'neutral'), 'technical')
            time_score = self._get_signal_score(time_signals.get('signal', 'normal'), 'time')
            risk_score = self._get_signal_score(risk_signals.get('signal', 'neutral'), 'risk')
            position_score = self._get_signal_score(position_signals.get('signal', 'wait'), 'position')

            # 가중 평균 계산
            total_score = (
                pattern_score * weights['pattern'] +
                technical_score * weights['technical'] +
                time_score * weights['time'] +
                risk_score * weights['risk'] +
                position_score * weights['position']
            )

            # 신호 결정
            if focus_on_exit:
                # 매도 신호
                if total_score >= 80:
                    return TradeSignal.STRONG_SELL, int(total_score)
                elif total_score >= 60:
                    return TradeSignal.SELL, int(total_score)
                else:
                    return TradeSignal.HOLD, int(total_score)
            else:
                # 매수 신호
                if total_score >= 80:
                    return TradeSignal.STRONG_BUY, int(total_score)
                elif total_score >= 60:
                    return TradeSignal.BUY, int(total_score)
                else:
                    return TradeSignal.HOLD, int(total_score)

        except Exception as e:
            logger.debug(f"종합 신호 계산 오류: {e}")
            return TradeSignal.HOLD, 50

    def _get_signal_score(self, signal: str, signal_type: str) -> float:
        """신호를 점수로 변환 (0~100)"""
        try:
            if signal_type == 'pattern':
                scores = {
                    'bullish': 80, 'bearish': 20, 'neutral': 50
                }
            elif signal_type == 'technical':
                scores = {
                    'oversold_bullish': 85, 'overbought_bearish': 15,
                    'oversold': 70, 'overbought': 30, 'neutral': 50
                }
            elif signal_type == 'time':
                scores = {
                    'time_exit': 80, 'time_caution': 60, 'normal': 50, 'closed_market': 30
                }
            elif signal_type == 'risk':
                scores = {
                    'target_reached': 90, 'stop_loss': 90, 'profit_target': 85,
                    'loss_limit': 85, 'profit_zone': 60, 'loss_zone': 40, 'neutral': 50
                }
            elif signal_type == 'position':
                scores = {
                    'strong_sell': 90, 'buy_ready': 80, 'hold': 50, 'wait': 30
                }
            else:
                scores = {'neutral': 50}

            return scores.get(signal, 50)

        except Exception as e:
            logger.debug(f"신호 점수 변환 오류: {e}")
            return 50

    def _should_update_signal(self, candidate: CandleTradeCandidate, analysis_result: Dict) -> bool:
        """신호 업데이트 필요 여부 판단"""
        try:
            new_signal = analysis_result['new_signal']
            new_strength = analysis_result['signal_strength']

            # 신호가 변경되었거나 강도가 크게 변했을 때만 업데이트
            signal_changed = new_signal != candidate.trade_signal
            strength_changed = abs(new_strength - candidate.signal_strength) >= 20  # 20점 이상 차이

            return signal_changed or strength_changed

        except Exception as e:
            logger.debug(f"신호 업데이트 판단 오류: {e}")
            return False

    def _should_update_exit_signal(self, candidate: CandleTradeCandidate, analysis_result: Dict) -> bool:
        """매도 신호 업데이트 필요 여부 판단"""
        try:
            new_signal = analysis_result['new_signal']

            # 매도 신호로 변경되었을 때만 업데이트
            return new_signal in [TradeSignal.SELL, TradeSignal.STRONG_SELL] and candidate.trade_signal != new_signal

        except Exception as e:
            logger.debug(f"매도 신호 업데이트 판단 오류: {e}")
            return False

    # ========== 포지션 관리 ==========
