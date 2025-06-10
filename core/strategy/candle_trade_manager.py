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
from .candle_analyzer import CandleAnalyzer
from .market_scanner import MarketScanner
from core.data.hybrid_data_manager import SimpleHybridDataManager
from core.trading.trade_executor import TradeExecutor
from core.websocket.kis_websocket_manager import KISWebSocketManager
import pandas as pd

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

                # 캔들 관련 매니저들 (중복 제거)
        self.stock_manager = CandleStockManager(max_watch_stocks=100, max_positions=20)
        self.pattern_detector = CandlePatternDetector()

        # 내부 상태
        self._last_scan_time: Optional[datetime] = None  # datetime 타입으로 명시
        self._scan_interval = 60  # 1분
        self.is_running = False

        # 실행 상태
        self.running = False
        self.scan_interval = 30  # 🆕 스캔 간격 (초)

        # 데이터 수집 및 분석 도구들

        # ========== 설정값 ==========
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

            # 🆕 기술적 지표 임계값 설정
            'rsi_oversold_threshold': 30,      # RSI 과매도 기준
            'rsi_overbought_threshold': 70,    # RSI 과매수 기준

            # 🆕 안전성 검증 설정
            'max_day_change_pct': 15.0,        # 최대 일일 변동률 (급등락 차단)
            'max_signal_age_seconds': 300,     # 신호 유효시간 (5분)
            'min_order_interval_seconds': 300, # 최소 주문 간격 (5분)

            # 🆕 우선순위 기반 투자금액 조정
            'max_priority_multiplier': 1.5,    # 최대 우선순위 배수
            'base_priority_multiplier': 0.5,   # 기본 우선순위 배수
            'max_single_investment_ratio': 0.4, # 단일 종목 최대 투자 비율 (40%)

            # 리스크 관리 설정 (캔들패턴에 맞게 조정)
            'max_position_size_pct': 30,       # 최대 포지션 크기 (%)
            'default_stop_loss_pct': 2.0,      # 기본 손절 비율 (%) - 2%로 조정
            'default_target_profit_pct': 2.0,  # 기본 목표 수익률 (%) - 2%로 조정 (캔들패턴은 더 큰 목표)
            'max_holding_hours': 48,           # 최대 보유 시간 - 48시간(2일)로 확장

            # 🆕 최소 보유시간 설정 (캔들패턴 전략에 맞게 하루 기준)
            'min_holding_minutes': 1440,       # 최소 보유시간 24시간(1440분) - 캔들패턴은 최소 하루
            'emergency_stop_loss_pct': 5.0,    # 긴급 손절 기준 (최소 보유시간 무시) - 5%로 확대
            'min_holding_override_conditions': {
                'high_profit_target': 3.0,     # 3% 이상 수익시 즉시 매도 허용
                'market_crash': -7.0,          # 시장 급락시 (-7%) 최소시간 무시
                'individual_limit_down': -15.0, # 개별 종목 큰 하락시 (-15%) 즉시 매도
            },

            # 패턴별 세부 목표 설정 (캔들패턴 이론에 맞게 하루 이상 보유)
            'pattern_targets': {
                'hammer': {'target': 3.0, 'stop': 2.0, 'max_hours': 48, 'min_minutes': 1440},           # 망치형: 최소 1일
                'inverted_hammer': {'target': 2.5, 'stop': 2.0, 'max_hours': 36, 'min_minutes': 1440},  # 역망치형: 최소 1일
                'bullish_engulfing': {'target': 3.0, 'stop': 2.5, 'max_hours': 48, 'min_minutes': 1440}, # 장악형: 최소 1일
                'morning_star': {'target': 3.25, 'stop': 2.5, 'max_hours': 72, 'min_minutes': 1440},     # 샛별형: 최소 1일 (강한 패턴)
                'rising_three': {'target': 3.5, 'stop': 3.0, 'max_hours': 96, 'min_minutes': 1440},    # 삼법형: 최소 1일 (지속성 패턴)
                'doji': {'target': 2.0, 'stop': 1.5, 'max_hours': 24, 'min_minutes': 720},             # 도지: 최소 12시간 (신중한 패턴)
            },

            # 시간 기반 청산 설정 (캔들패턴에 맞게 조정)
            'time_exit_rules': {
                'profit_exit_hours': 24,        # 24시간 후 수익중이면 청산 고려
                'min_profit_for_time_exit': 2.0,  # 시간 청산 최소 수익률 2%
                'market_close_exit_minutes': 30,  # 장 마감 30분 전 청산
                'overnight_avoid': False,      # 오버나이트 포지션 허용 (캔들패턴은 하루 이상 보유)
            },

            # 🆕 매수체결시간 기반 캔들전략 설정
            'execution_time_strategy': {
                'use_execution_time': True,     # 매수체결시간 활용 여부
                'min_holding_from_execution': 1440,  # 체결시간 기준 최소 보유시간 (24시간)
                'early_morning_bonus_hours': 2,      # 장 시작 2시간 내 매수시 추가 보유시간
                'late_trading_penalty_hours': -4,    # 장 종료 전 매수시 보유시간 단축
                'weekend_gap_consideration': True,    # 주말 갭 고려
            },

            # 🆕 투자금액 계산 설정
            'investment_calculation': {
                'available_amount_ratio': 0.9,    # 🎯 KIS API 매수가능금액 사용 비율 (90%)
                'cash_usage_ratio': 0.8,          # 현금잔고 사용 비율 (80%) - 폴백용
                'portfolio_usage_ratio': 0.2,     # 총평가액 사용 비율 (20%) - 사용하지 않음
                'min_cash_threshold': 500_000,    # 현금 우선 사용 최소 기준 (50만원)
                'max_portfolio_limit': 3_000_000, # 평가액 기준 최대 제한 (300만원)
                'default_investment': 1_000_000,  # 기본 투자 금액 (100만원)
                'min_investment': 100_000,        # 최소 투자 금액 (10만원)
            },
        }

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

        # 🆕 캔들 분석기 초기화 (config와 korea_tz 설정 후)
        self.candle_analyzer = CandleAnalyzer(
            pattern_detector=self.pattern_detector,
            config=self.config,
            korea_tz=self.korea_tz
        )

        # 🆕 시장 스캐너 초기화
        self.market_scanner = MarketScanner(candle_trade_manager=self)

        # 🆕 매수 기회 평가자 초기화
        from .buy_opportunity_evaluator import BuyOpportunityEvaluator
        self.buy_evaluator = BuyOpportunityEvaluator(self)

        # 🆕 매도 포지션 관리자 초기화
        from .sell_position_manager import SellPositionManager
        self.sell_manager = SellPositionManager(self)

        # 🆕 주문 타임아웃 콜백 등록 (OrderExecutionManager와 연동)
        self._register_order_timeout_callback()

        logger.info("✅ CandleTradeManager 초기화 완료")

    def _register_order_timeout_callback(self):
        """🆕 주문 타임아웃 콜백 등록"""
        try:
            if (hasattr(self.trade_executor, 'execution_manager') and
                self.trade_executor.execution_manager):

                # 타임아웃 콜백 함수 등록
                self.trade_executor.execution_manager.add_execution_callback(self._handle_order_timeout)
                logger.info("✅ 주문 타임아웃 콜백 등록 완료")
            else:
                logger.warning("⚠️ OrderExecutionManager 없음 - 타임아웃 콜백 등록 실패")
        except Exception as e:
            logger.error(f"❌ 타임아웃 콜백 등록 오류: {e}")

    def _handle_order_timeout(self, timeout_data: Dict):
        """🆕 주문 타임아웃 처리 - 종목 상태 복원"""
        try:
            if timeout_data.get('action') != 'order_timeout':
                return  # 타임아웃 이벤트가 아님

            stock_code = timeout_data.get('stock_code', '')
            order_type = timeout_data.get('order_type', '')
            elapsed_seconds = timeout_data.get('elapsed_seconds', 0)

            logger.warning(f"⏰ {stock_code} 주문 타임아웃 처리: {order_type} (경과: {elapsed_seconds:.0f}초)")

            # _all_stocks에서 해당 종목 찾기
            candidate = self.stock_manager._all_stocks.get(stock_code)
            if not candidate:
                logger.debug(f"📋 {stock_code} _all_stocks에 없음 - 타임아웃 처리 스킵")
                return

            # 주문 타입별 상태 복원
            if order_type.upper() == 'BUY':
                # 매수 주문 타임아웃 - BUY_READY 상태로 복원
                candidate.clear_pending_order('buy')
                candidate.status = CandleStatus.BUY_READY
                logger.info(f"🔄 {stock_code} 매수 타임아웃 - PENDING_ORDER → BUY_READY 복원")

            elif order_type.upper() == 'SELL':
                # 매도 주문 타임아웃 - ENTERED 상태로 복원
                candidate.clear_pending_order('sell')
                candidate.status = CandleStatus.ENTERED
                logger.info(f"🔄 {stock_code} 매도 타임아웃 - PENDING_ORDER → ENTERED 복원")

            # stock_manager에 상태 변경 반영
            self.stock_manager.update_candidate(candidate)

            # 메타데이터에 타임아웃 이력 기록
            timeout_history = candidate.metadata.get('timeout_history', [])
            timeout_history.append({
                'timeout_time': datetime.now().isoformat(),
                'order_type': order_type,
                'elapsed_seconds': elapsed_seconds,
                'restored_status': candidate.status.value
            })
            candidate.metadata['timeout_history'] = timeout_history

            logger.info(f"✅ {stock_code} 주문 타임아웃 복원 완료: {candidate.status.value}")

        except Exception as e:
            logger.error(f"❌ 주문 타임아웃 처리 오류: {e}")


    # 🆕 기존 보유 종목 웹소켓 구독 관리 (간소화)
    async def setup_existing_holdings_monitoring(self):
        """기존 보유 종목 웹소켓 모니터링 설정 - 메인 컨트롤러"""
        try:
            logger.debug("📊 기존 보유 종목 웹소켓 모니터링 설정 시작")

            # 1. 기존 보유 종목 조회
            existing_stocks = await self._fetch_existing_holdings()
            if not existing_stocks:
                logger.info("📊 보유 종목이 없습니다.")
                return True

            logger.debug(f"📈 보유 종목 {len(existing_stocks)}개 발견")

            # 2. 각 종목별 처리
            subscription_success_count = 0
            added_to_all_stocks_count = 0

            for i, stock_info in enumerate(existing_stocks):
                try:
                    stock_code = stock_info.get('stock_code', 'unknown')
                    stock_name = stock_info.get('stock_name', 'unknown')
                    logger.debug(f"🔍 종목 {i+1}/{len(existing_stocks)} 처리 시작: {stock_code}({stock_name})")

                    success_sub, success_add = await self._process_single_holding(stock_info)

                    logger.debug(f"🔍 종목 {i+1} 처리 결과: 구독={success_sub}, 추가={success_add}")

                    if success_sub:
                        subscription_success_count += 1
                    if success_add:
                        added_to_all_stocks_count += 1

                except Exception as e:
                    #stock_code = stock_info.get('stock_code', 'unknown') if stock_info else 'unknown'
                    logger.error(f"❌ 종목 {i+1} ({stock_code}) 처리 오류: {e}")
                    continue

            # 3. 결과 보고
            logger.info(f"📊 기존 보유 종목 웹소켓 구독 완료: {subscription_success_count}/{len(existing_stocks)}개")
            logger.info(f"🔄 _all_stocks에 기존 보유 종목 추가: {added_to_all_stocks_count}개")

            # 🔍 _all_stocks 상태 요약
            all_stocks_summary = {}
            entered_stocks = []

            for stock_code, candidate in self.stock_manager._all_stocks.items():
                status = candidate.status.value
                all_stocks_summary[status] = all_stocks_summary.get(status, 0) + 1

                if candidate.status.value == 'entered':
                    entered_stocks.append(f"{stock_code}({candidate.stock_name})")

            logger.info(f"🔍 _all_stocks 최종 상태: {all_stocks_summary}")
            logger.info(f"🔍 ENTERED 상태 종목들: {', '.join(entered_stocks)}")

            return subscription_success_count > 0

        except Exception as e:
            logger.error(f"기존 보유 종목 모니터링 설정 오류: {e}")
            return False

        """기존 보유 종목 조회"""
    async def _fetch_existing_holdings(self) -> List[Dict]:
        try:
            from ..api.kis_market_api import get_existing_holdings
            holdings = get_existing_holdings()

            # 🔍 디버깅: 조회된 보유 종목 상세 정보
            logger.info(f"🔍 계좌 보유 종목 조회 결과: {len(holdings) if holdings else 0}개")

            if holdings:
                for i, stock in enumerate(holdings):
                    stock_code = stock.get('stock_code', 'unknown')
                    stock_name = stock.get('stock_name', 'unknown')
                    quantity = stock.get('quantity', 0)
                    current_price = stock.get('current_price', 0)
                    logger.info(f"   {i+1}. {stock_code}({stock_name}): {quantity}주, {current_price:,}원")
            else:
                logger.warning("⚠️ 보유 종목 조회 결과가 비어있음")

            return holdings

        except Exception as e:
            logger.error(f"계좌 잔고 조회 오류: {e}")
            return []

    async def _process_single_holding(self, stock_info: Dict) -> Tuple[bool, bool]:
        """개별 보유 종목 처리"""
        try:
            # 기본 정보 추출
            stock_code = stock_info['stock_code']
            stock_name = stock_info['stock_name']
            current_price = stock_info.get('current_price', 0)
            buy_price = stock_info.get('avg_price', 0)
            quantity = stock_info.get('quantity', 0)
            profit_rate = stock_info.get('profit_loss_rate', 0.0)

            # 기본 정보 로깅
            logger.info(f"📈 {stock_code}({stock_name}): {current_price:,}원, 수익률: {profit_rate:+.1f}%")

            # CandleTradeCandidate 생성 및 설정 (패턴 분석 포함)
            success_add = await self._create_and_analyze_holding_candidate(
                stock_code, stock_name, current_price, buy_price, quantity
            )

            # 웹소켓 구독
            success_sub = await self._subscribe_holding_websocket(stock_code, stock_name)

            return success_sub, success_add

        except Exception as e:
            logger.error(f"개별 종목 처리 오류 ({stock_info.get('stock_code', 'unknown')}): {e}")
            return False, False


    async def _create_and_analyze_holding_candidate(self, stock_code: str, stock_name: str, current_price: float,
                                                  buy_price: float, quantity: int) -> bool:
        """보유 종목 CandleTradeCandidate 생성, 패턴 분석, 설정 통합 처리"""
        try:
            # 🔍 디버깅: 입력 데이터 검증
            logger.debug(f"🔍 {stock_code} 입력 데이터: 현재가={current_price}, 매수가={buy_price}, 수량={quantity}")

            # 이미 _all_stocks에 있는지 확인
            if stock_code in self.stock_manager._all_stocks:
                logger.info(f"⚠️ {stock_code} 이미 _all_stocks에 존재 - 중복 추가 방지")
                return False

            # 1. CandleTradeCandidate 객체 생성
            existing_candidate = self._create_holding_candidate_object(stock_code, stock_name, current_price)

            # 2. 진입 정보 설정
            if buy_price > 0 and quantity > 0:
                logger.debug(f"🔍 {stock_code} 진입 정보 설정 중...")

                existing_candidate.enter_position(float(buy_price), int(quantity))
                existing_candidate.update_price(float(current_price))
                existing_candidate.performance.entry_price = float(buy_price)

                # 3. _all_stocks에 먼저 추가 (패턴 분석에서 캐싱 가능하도록)
                self.stock_manager._all_stocks[stock_code] = existing_candidate
                logger.info(f"✅ {stock_code} _all_stocks에 기존 보유 종목으로 추가 완료")

                # 4. 캔들 패턴 분석
                #logger.debug(f"🔍 {stock_code} 캔들 패턴 분석 시작...")
                candle_analysis_result = await self._analyze_existing_holding_patterns(stock_code, stock_name, current_price)

                # 5. 리스크 관리 설정 (패턴 분석 결과 반영)
                #logger.debug(f"🔍 {stock_code} 리스크 관리 설정 중...")
                self.sell_manager.setup_holding_risk_management(existing_candidate, buy_price, current_price, candle_analysis_result)

                # 6. 메타데이터 설정
                #logger.debug(f"🔍 {stock_code} 메타데이터 설정 중...")
                self._setup_holding_metadata(existing_candidate, candle_analysis_result)

                # 6. 🆕 기존 보유 종목 매수 시간 추정 설정
                self._setup_buy_execution_time(existing_candidate)

                # 7. 설정 완료 로그
                self._log_holding_setup_completion(existing_candidate)

                #logger.info(f"✅ {stock_code} 기존 보유 종목 설정 완료")
                return True
            else:
                logger.warning(f"❌ {stock_code} 진입 정보 부족: 매수가={buy_price}, 수량={quantity}")
                return False

        except Exception as e:
            logger.error(f"❌ 보유 종목 후보 생성 및 분석 오류 ({stock_code}): {e}")
            import traceback
            logger.error(f"❌ 상세 오류: {traceback.format_exc()}")
            return False

    async def _analyze_existing_holding_patterns(self, stock_code: str, stock_name: str, current_price: float) -> Optional[Dict]:
        """🔄 기존 보유 종목의 실시간 캔들 패턴 분석 (🆕 캐싱 활용)"""
        try:
            logger.debug(f"🔄 {stock_code} 실시간 캔들 패턴 분석 시작")

            # 🆕 기존 _all_stocks에서 캐시된 데이터 확인
            ohlcv_data = None
            if stock_code in self.stock_manager._all_stocks:
                candidate = self.stock_manager._all_stocks[stock_code]
                ohlcv_data = candidate.get_ohlcv_data()
                if ohlcv_data is not None:
                    logger.debug(f"📄 {stock_code} 캐시된 일봉 데이터 사용")

            # 캐시에 없으면 API 호출
            if ohlcv_data is None:
                from ..api.kis_market_api import get_inquire_daily_itemchartprice
                ohlcv_data = get_inquire_daily_itemchartprice(
                    output_dv="2",  # 일자별 차트 데이터 배열
                    itm_no=stock_code,
                    period_code="D",  # 일봉
                    adj_prc="1"
                )

                # 🆕 조회 성공시 캐싱 (candidate가 있다면)
                if ohlcv_data is not None and not ohlcv_data.empty and stock_code in self.stock_manager._all_stocks:
                    self.stock_manager._all_stocks[stock_code].cache_ohlcv_data(ohlcv_data)
                    logger.debug(f"📥 {stock_code} 일봉 데이터 조회 및 캐싱 완료")

            if ohlcv_data is None or ohlcv_data.empty:
                logger.debug(f"❌ {stock_code} OHLCV 데이터 조회 실패")
                return None

            # 캔들 패턴 분석
            pattern_result : List[CandlePatternInfo] = self.pattern_detector.analyze_stock_patterns(stock_code, ohlcv_data)

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

    def _generate_trade_signal_from_patterns(self, patterns: List[CandlePatternInfo]) -> Tuple:
        """패턴 목록에서 매매 신호 생성 - candle_analyzer로 위임"""
        try:
            return self.candle_analyzer.generate_trade_signal_from_patterns(patterns)

        except Exception as e:
            logger.error(f"패턴 신호 생성 오류: {e}")
            return TradeSignal.HOLD, 0

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
                    await self._periodic_signal_evaluation()

                    # 진입 기회 평가 = 매수 준비 종목 평가
                    await self._evaluate_entry_opportunities()

                    # 기존 포지션 관리 - 매도 시그널 체크
                    await self._manage_existing_positions()

                    # 🆕 미체결 주문 관리 (1분마다)
                    if hasattr(self, '_last_stale_check_time'):
                        if (datetime.now() - self._last_stale_check_time).total_seconds() >= 60:
                            await self.check_and_cancel_stale_orders()
                            self._last_stale_check_time = datetime.now()
                    else:
                        self._last_stale_check_time = datetime.now()

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
        """종목 스캔 및 패턴 감지 (MarketScanner 위임)"""
        try:
            # MarketScanner에 위임
            await self.market_scanner.scan_and_detect_patterns()

            # 스캔 시간 업데이트
            self._last_scan_time = datetime.now()

        except Exception as e:
            logger.error(f"종목 스캔 오류: {e}")


    def _should_exit_position(self, candidate: CandleTradeCandidate) -> bool:
        """포지션 매도 조건 확인 (간단한 버전) - SellPositionManager에 위임"""
        try:
            return self.sell_manager._get_pattern_based_target(candidate)[0] > 0  # 간단한 체크로 대체
        except Exception as e:
            logger.debug(f"매도 조건 확인 오류: {e}")
            return False

    # ========== 진입 기회 평가 ==========
    # 진입 기회 평가 = 매수 준비 종목 평가
    async def _evaluate_entry_opportunities(self):
        """진입 기회 평가 및 매수 실행 - BuyOpportunityEvaluator에 위임"""
        try:
            await self.buy_evaluator.evaluate_entry_opportunities()
        except Exception as e:
            logger.error(f"❌ 진입 기회 평가 오류: {e}")

    # ========== 포지션 관리 ==========

    async def _manage_existing_positions(self):
        """기존 포지션 관리 (손절/익절/추적손절) - SellPositionManager에 위임"""
        try:
            await self.sell_manager.manage_existing_positions()

            # 🧹 주기적으로 조정 이력 정리 (1시간마다)
            self.sell_manager.cleanup_adjustment_history()

        except Exception as e:
            logger.error(f"❌ 포지션 관리 오류: {e}")

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

    def _create_holding_candidate_object(self, stock_code: str, stock_name: str, current_price: float) -> CandleTradeCandidate:
        """보유 종목 CandleTradeCandidate 객체 생성"""
        return CandleTradeCandidate(
            stock_code=stock_code,
            stock_name=stock_name,
            current_price=float(current_price) if current_price else 0.0,
            market_type="KOSPI",  # 기본값, 나중에 조회 가능
            status=CandleStatus.ENTERED,  # 이미 진입한 상태
            trade_signal=TradeSignal.HOLD,  # 보유 중
            created_at=datetime.now()
        )

    def _setup_holding_metadata(self, candidate: CandleTradeCandidate, candle_analysis_result: Optional[Dict]):
        """보유 종목 메타데이터 설정"""
        try:
            # 기본 메타데이터
            candidate.metadata['is_existing_holding'] = True

            # 🔧 프로그램을 통해 매수한 종목은 기본적으로 캔들 전략으로 분류
            # (사용자 요구사항: 모든 보유 종목은 프로그램을 통해 매수함)
            candidate.metadata['original_entry_source'] = 'candle_strategy'

            # 패턴 분석 결과가 있으면 추가 정보 저장
            if candle_analysis_result and candle_analysis_result.get('patterns_detected'):
                candidate.metadata['pattern_analysis_success'] = True
                candidate.metadata['pattern_analysis_time'] = datetime.now().isoformat()
                logger.debug(f"✅ {candidate.stock_code} 패턴 분석 성공 - 캔들 전략으로 분류")
            else:
                candidate.metadata['pattern_analysis_success'] = False
                candidate.metadata['pattern_analysis_failure_reason'] = 'no_patterns_detected'
                logger.debug(f"⚠️ {candidate.stock_code} 패턴 분석 실패하지만 캔들 전략으로 분류")

        except Exception as e:
            logger.error(f"메타데이터 설정 오류: {e}")

    def _setup_buy_execution_time(self, candidate: CandleTradeCandidate):
        """🆕 기존 보유 종목의 매수 체결 시간 설정 (실제 시간 조회 우선, 실패시 추정)"""
        try:
            current_time = datetime.now(self.korea_tz)

            # 1. 🎯 실제 매수 시간 조회 시도 (우선)
            actual_buy_time = self._get_actual_buy_execution_time_safe(candidate.stock_code)

            if actual_buy_time:
                # 실제 매수 시간을 찾은 경우
                candidate.performance.entry_time = actual_buy_time
                candidate.performance.buy_execution_time = actual_buy_time
                candidate.metadata['buy_execution_time_estimated'] = False
                candidate.metadata['buy_execution_time_source'] = 'kis_api_order_history'
                candidate.metadata['original_buy_execution_time'] = actual_buy_time.isoformat()
                logger.debug(f"✅ {candidate.stock_code} 실제 매수 시간 조회 성공: {actual_buy_time.strftime('%Y-%m-%d %H:%M:%S')}")
                return

            # 2. 실제 매수 기록을 찾지 못한 경우에만 추정 시간 사용 (폴백)
            logger.debug(f"⚠️ {candidate.stock_code} 실제 매수 기록 없음 - 추정 시간으로 폴백")

            # 추정 시간 계산 (프로그램 시작 시간 기준)
            today_9am = current_time.replace(hour=9, minute=0, second=0, microsecond=0)

            # 현재 시간이 오늘 9시 이후라면 오늘 9시를 매수 시간으로 추정
            # 9시 이전이라면 어제 장시간 중 매수했다고 추정
            if current_time.time() >= today_9am.time():
                estimated_buy_time = today_9am
            else:
                # 어제 15시로 추정
                yesterday_3pm = (current_time - timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)
                estimated_buy_time = yesterday_3pm

            # 추정 시간 설정
            candidate.performance.buy_execution_time = estimated_buy_time
            candidate.metadata['buy_execution_time_estimated'] = True
            candidate.metadata['buy_execution_time_source'] = 'program_start_estimation'
            candidate.metadata['original_buy_execution_time'] = estimated_buy_time.isoformat()
            logger.debug(f"📅 {candidate.stock_code} 추정 매수 시간 설정: {estimated_buy_time.strftime('%Y-%m-%d %H:%M:%S')}")

        except Exception as e:
            logger.error(f"❌ 매수 시간 설정 오류 ({candidate.stock_code}): {e}")
            # 오류시 현재 시간을 매수 시간으로 설정
            candidate.performance.buy_execution_time = datetime.now(self.korea_tz)
            candidate.metadata['buy_execution_time_estimated'] = True
            candidate.metadata['buy_execution_time_source'] = 'fallback_current_time'

    def _get_actual_buy_execution_time_safe(self, stock_code: str) -> Optional[datetime]:
        """🆕 안전한 실제 매수 체결 시간 조회 (오류시 None 반환)"""
        try:
            from datetime import datetime, timedelta
            from ..api.kis_order_api import get_inquire_daily_ccld_lst

            # 최근 2일간만 조회해서 API 부하 최소화
            end_date = datetime.now()
            start_date = end_date - timedelta(days=4)

            start_date_str = start_date.strftime("%Y%m%d")
            end_date_str = end_date.strftime("%Y%m%d")

            logger.debug(f"🔍 {stock_code} 매수 기록 조회: {start_date_str} ~ {end_date_str}")

            # 체결된 주문만 조회
            order_history = get_inquire_daily_ccld_lst(
                dv="01",                    # 3개월 이내
                inqr_strt_dt=start_date_str,
                inqr_end_dt=end_date_str,
                ccld_dvsn="01"              # 체결된 주문만
            )

            if order_history is None or order_history.empty:
                logger.debug(f"📋 {stock_code} 주문 기록이 없습니다")
                return None

            # 해당 종목의 매수 주문 필터링
            buy_orders = order_history[
                (order_history['pdno'] == stock_code) &           # 종목코드 일치
                (order_history['sll_buy_dvsn_cd'] == '02') &      # 매수 주문 (02)
                (order_history['tot_ccld_qty'].astype(int) > 0)   # 체결수량 > 0
            ]

            if buy_orders.empty:
                logger.debug(f"📋 {stock_code} 매수 체결 기록이 없습니다")
                return None

            # 가장 최근 매수 주문 선택 (ord_dt, ord_tmd 기준 내림차순)
            buy_orders = buy_orders.sort_values(['ord_dt', 'ord_tmd'], ascending=False)
            latest_buy = buy_orders.iloc[0]

            # 매수 시간 파싱
            order_date = latest_buy['ord_dt']  # YYYYMMDD
            order_time = latest_buy['ord_tmd']  # HHMMSS

            if order_date and order_time and len(str(order_time)) >= 6:
                # 시간 문자열 정규화 (6자리 맞춤)
                time_str = str(order_time).zfill(6)
                datetime_str = f"{order_date} {time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"

                # 한국 시간대로 파싱
                buy_datetime = datetime.strptime(datetime_str, "%Y%m%d %H:%M:%S")
                buy_datetime_kst = buy_datetime.replace(tzinfo=self.korea_tz)

                # 추가 정보 로깅
                order_qty = int(latest_buy.get('ord_qty', 0))
                filled_qty = int(latest_buy.get('tot_ccld_qty', 0))
                order_price = int(latest_buy.get('ord_unpr', 0))

                logger.debug(f"📅 {stock_code} 실제 매수 기록 발견: "
                           f"{buy_datetime_kst.strftime('%Y-%m-%d %H:%M:%S')} "
                           f"({filled_qty}/{order_qty}주, {order_price:,}원)")

                return buy_datetime_kst
            else:
                logger.debug(f"⚠️ {stock_code} 매수 시간 파싱 실패: date={order_date}, time={order_time}")
                return None

        except Exception as e:
            logger.debug(f"❌ {stock_code} 실제 매수 시간 조회 오류: {e}")
            return None

    def _log_holding_setup_completion(self, candidate: CandleTradeCandidate):
        """보유 종목 설정 완료 로그"""
        try:
            if candidate.performance.entry_price and candidate.performance.pnl_pct is not None:
                source_info = candidate.metadata.get('risk_management_source', 'unknown')

                logger.info(f"📊 {candidate.stock_code} 기존 보유 종목 설정 완료 ({source_info}):")
                logger.info(f"   - 진입가: {candidate.performance.entry_price:,.0f}원")
                logger.info(f"   - 수량: {candidate.performance.entry_quantity:,}주")
                logger.info(f"   - 현재가: {candidate.current_price:,.0f}원")
                logger.info(f"   - 수익률: {candidate.performance.pnl_pct:+.2f}%")
                logger.info(f"   - 목표가: {candidate.risk_management.target_price:,.0f}원")
                logger.info(f"   - 손절가: {candidate.risk_management.stop_loss_price:,.0f}원")
            else:
                logger.warning(f"⚠️ {candidate.stock_code} PerformanceTracking 설정 미완료 - 재확인 필요")

        except Exception as e:
            logger.error(f"설정 완료 로그 오류: {e}")

    async def _subscribe_holding_websocket(self, stock_code: str, stock_name: str) -> bool:
        """보유 종목 웹소켓 구독"""
        try:
            callback = self._create_existing_holding_callback(stock_code, stock_name)
            success = await self._subscribe_existing_holding(stock_code, callback)

            if success:
                self.existing_holdings_callbacks[stock_code] = callback

            return success

        except Exception as e:
            logger.error(f"웹소켓 구독 오류 ({stock_code}): {e}")
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
        logger.info("🧹 기존 보유 종목 웹소켓 모니터링 정리")
        self.existing_holdings_callbacks.clear()

    # ========== 🆕 체결 확인 처리 ==========

    async def handle_execution_confirmation(self, execution_data):
        """🎯 웹소켓 체결 통보 처리 - 매수/매도 체결 확인 후 상태 업데이트"""
        try:
            # 🚨 타입 안전성 검사 (문자열로 전달된 경우 처리)
            if isinstance(execution_data, str):
                logger.warning(f"⚠️ execution_data가 문자열로 전달됨: {execution_data}")
                # 문자열 파싱 시도하거나 빈 딕셔너리로 처리
                execution_data = {}

            if not isinstance(execution_data, dict):
                logger.error(f"❌ execution_data 타입 오류: {type(execution_data)}")
                return

            # 체결 데이터에서 주요 정보 추출
            stock_code = execution_data.get('stock_code', '')
            order_type = execution_data.get('order_type', '')  # 매수/매도 구분
            executed_quantity = int(execution_data.get('executed_quantity', 0))
            executed_price = float(execution_data.get('executed_price', 0))
            order_no = execution_data.get('order_no', '')

            # 체결 통보 파싱 성공 여부 확인
            if not execution_data.get('parsed_success', False):
                logger.debug(f"📝 체결통보 파싱 실패 또는 미파싱 데이터: {execution_data.get('raw_data', '')[:50]}...")
                return

            if not stock_code or not order_type:
                logger.warning(f"⚠️ 체결 통보 데이터 부족: {execution_data}")
                return

            logger.info(f"🎯 체결 확인: {stock_code} {order_type} {executed_quantity}주 {executed_price:,.0f}원 (주문번호: {order_no})")

            # _all_stocks에서 해당 종목 찾기
            candidate = self.stock_manager._all_stocks.get(stock_code)
            if not candidate:
                logger.debug(f"📋 {stock_code} _all_stocks에 없음 - 다른 전략의 거래일 수 있음")
                return

            # 매수 체결 처리
            if order_type.lower() in ['buy', '매수', '01']:
                await self._handle_buy_execution(candidate, executed_price, executed_quantity, order_no, execution_data)

            # 매도 체결 처리
            elif order_type.lower() in ['sell', '매도', '02']:
                await self._handle_sell_execution(candidate, executed_price, executed_quantity, order_no, execution_data)

            else:
                logger.warning(f"⚠️ 알 수 없는 주문 타입: {order_type}")

        except Exception as e:
            logger.error(f"❌ 체결 확인 처리 오류: {e}")

    async def _handle_buy_execution(self, candidate: CandleTradeCandidate, executed_price: float,
                                  executed_quantity: int, order_no: str, execution_data: Dict):
        """💰 매수 체결 처리"""
        try:
            # 1. PENDING_ORDER 상태 확인
            if candidate.status != CandleStatus.PENDING_ORDER:
                logger.warning(f"⚠️ {candidate.stock_code} PENDING_ORDER 상태가 아님: {candidate.status.value}")

            # 2. 대기 중인 매수 주문 확인
            pending_buy_order = candidate.get_pending_order_no('buy')
            if not pending_buy_order:
                logger.debug(f"📋 {candidate.stock_code} 대기 중인 매수 주문 없음 - 다른 시스템 주문일 수 있음")

            # 3. 주문번호 일치 확인 (가능한 경우)
            if order_no and pending_buy_order and pending_buy_order != order_no:
                logger.debug(f"📋 {candidate.stock_code} 주문번호 불일치: 대기({pending_buy_order}) vs 체결({order_no})")

            # 4. 포지션 진입 처리
            candidate.enter_position(executed_price, executed_quantity)

            # 5. 🆕 매수 체결 시간 기록
            execution_time = datetime.now(self.korea_tz)
            candidate.performance.buy_execution_time = execution_time

            # 6. 주문 완료 처리 및 상태 업데이트
            candidate.complete_order(order_no, 'buy')
            candidate.status = CandleStatus.ENTERED

            # 7. 메타데이터 업데이트
            candidate.metadata['execution_confirmed'] = {
                'executed_price': executed_price,
                'executed_quantity': executed_quantity,
                'execution_time': execution_time.isoformat(),
                'order_no': order_no,
                'execution_data': execution_data
            }

            # 7. stock_manager 업데이트
            self.stock_manager.update_candidate(candidate)

            # 8. 통계 업데이트
            self.daily_stats['successful_trades'] = self.daily_stats.get('successful_trades', 0) + 1

            logger.info(f"✅ {candidate.stock_code} 매수 체결 완료: "
                       f"PENDING_ORDER → ENTERED "
                       f"{executed_quantity}주 {executed_price:,.0f}원")

        except Exception as e:
            logger.error(f"❌ 매수 체결 처리 오류 ({candidate.stock_code}): {e}")

    async def _handle_sell_execution(self, candidate: CandleTradeCandidate, executed_price: float,
                                   executed_quantity: int, order_no: str, execution_data: Dict):
        """💸 매도 체결 처리"""
        try:
            # 1. PENDING_ORDER 상태 확인
            if candidate.status != CandleStatus.PENDING_ORDER:
                logger.warning(f"⚠️ {candidate.stock_code} PENDING_ORDER 상태가 아님: {candidate.status.value}")

            # 2. 대기 중인 매도 주문 확인
            pending_sell_order = candidate.get_pending_order_no('sell')
            if not pending_sell_order:
                logger.debug(f"📋 {candidate.stock_code} 대기 중인 매도 주문 없음 - 다른 시스템 주문일 수 있음")

            # 3. 주문번호 일치 확인
            if order_no and pending_sell_order and pending_sell_order != order_no:
                logger.debug(f"📋 {candidate.stock_code} 주문번호 불일치: 대기({pending_sell_order}) vs 체결({order_no})")

            # 4. 수익률 계산
            if candidate.performance.entry_price:
                profit_pct = ((executed_price - candidate.performance.entry_price) / candidate.performance.entry_price) * 100
            else:
                profit_pct = 0.0

            # 5. 🆕 매도 체결 시간 기록
            execution_time = datetime.now(self.korea_tz)
            candidate.performance.sell_execution_time = execution_time

            # 6. 포지션 종료 처리
            candidate.exit_position(executed_price, "체결 확인")

            # 7. 주문 완료 처리 및 상태 업데이트
            candidate.complete_order(order_no, 'sell')
            candidate.status = CandleStatus.EXITED

            # 8. 메타데이터 업데이트
            candidate.metadata['sell_execution'] = {
                'executed_price': executed_price,
                'executed_quantity': executed_quantity,
                'execution_time': execution_time.isoformat(),
                'order_no': order_no,
                'profit_pct': profit_pct,
                'execution_data': execution_data
            }

            # 8. stock_manager 업데이트
            self.stock_manager.update_candidate(candidate)

            # 9. 통계 업데이트
            if profit_pct > 0:
                self.daily_stats['successful_trades'] = self.daily_stats.get('successful_trades', 0) + 1
            else:
                self.daily_stats['failed_trades'] = self.daily_stats.get('failed_trades', 0) + 1

            self.daily_stats['total_profit_loss'] = self.daily_stats.get('total_profit_loss', 0.0) + profit_pct

            logger.info(f"✅ {candidate.stock_code} 매도 체결 완료: "
                       f"PENDING_ORDER → EXITED "
                       f"{executed_quantity}주 {executed_price:,.0f}원 (수익률: {profit_pct:.2f}%)")

        except Exception as e:
            logger.error(f"❌ 매도 체결 처리 오류 ({candidate.stock_code}): {e}")

    # ========== 🆕 주기적 신호 재평가 시스템 ==========

    async def _periodic_signal_evaluation(self):
        """🔄 주기적 신호 재평가 - 30초마다 실행"""
        try:
            # 🎯 현재 모든 종목 상태별 분류 (PENDING_ORDER 제외)
            all_candidates = [
                candidate for candidate in self.stock_manager._all_stocks.values()
                if candidate.status != CandleStatus.PENDING_ORDER  # 주문 대기 중인 종목 제외
            ]

            if not all_candidates:
                logger.debug("📊 평가할 종목이 없습니다")
                return

            # 상태별 분류
            watching_candidates = [c for c in all_candidates if c.status == CandleStatus.WATCHING or c.status == CandleStatus.BUY_READY or c.status == CandleStatus.SCANNING]
            entered_candidates = [c for c in all_candidates if c.status == CandleStatus.ENTERED]

            logger.info(f"🔄 신호 재평가: 관찰{len(watching_candidates)}개, 진입{len(entered_candidates)}개 "
                       f"(PENDING_ORDER 제외)")

            # 🎯 1. 관찰 중인 종목들 재평가 (우선순위 높음)
            if watching_candidates:
                logger.debug(f"📊 관찰 중인 종목 재평가: {len(watching_candidates)}개")

                # 1-1. 신호(TradeSignal) 업데이트
                signal_updated = await self._batch_update_signals_for_watching_stocks(watching_candidates)
                #logger.debug(f"✅ 관찰 종목 신호 업데이트: {signal_updated}개")

                # 1-2. 상태(CandleStatus) 전환 검토
                status_changed = await self._batch_evaluate_status_transitions(watching_candidates)
                #logger.debug(f"✅ 관찰 종목 상태 전환: {status_changed}개")

                watch_updated = signal_updated + status_changed

            # 🎯 2. 진입한 종목들 재평가 (매도 신호 중심)
            if entered_candidates:
                logger.debug(f"💰 진입 종목 재평가: {len(entered_candidates)}개")
                enter_updated = await self._batch_evaluate_entered_stocks(entered_candidates)
                logger.debug(f"✅ 진입 종목 신호 업데이트: {enter_updated}개")

        except Exception as e:
            logger.error(f"주기적 신호 재평가 오류: {e}")

    async def _batch_update_signals_for_watching_stocks(self, candidates: List[CandleTradeCandidate]) -> int:
        """관찰 중인 종목들 신호(TradeSignal) 업데이트"""
        try:
            updated_count = 0

            # API 호출 제한을 위해 5개씩 배치 처리
            batch_size = 5
            for i in range(0, len(candidates), batch_size):
                batch = candidates[i:i + batch_size]

                for candidate in batch:
                    try:
                        # 다각도 종합 분석 수행
                        analysis_result = await self.candle_analyzer.comprehensive_signal_analysis(candidate)

                        if analysis_result and self._should_update_signal(candidate, analysis_result):
                            # 신호 업데이트
                            old_signal = candidate.trade_signal
                            candidate.trade_signal = analysis_result['new_signal']
                            candidate.signal_strength = analysis_result['signal_strength']
                            candidate.signal_updated_at = datetime.now(self.korea_tz)

                            # 우선순위 재계산
                            candidate.entry_priority = self.candle_analyzer.calculate_entry_priority(candidate)

                            # stock_manager 업데이트
                            self.stock_manager.update_candidate(candidate)

                            #logger.info(f"🔄 {candidate.stock_code} 신호 업데이트: "
                            #           f"{old_signal.value} → {candidate.trade_signal.value} "
                            #           f"(강도:{candidate.signal_strength})")
                            updated_count += 1

                    except Exception as e:
                        logger.debug(f"종목 재평가 오류 ({candidate.stock_code}): {e}")
                        continue

                # API 호출 간격 조절
                if i + batch_size < len(candidates):
                    await asyncio.sleep(0.5)  # 0.5초 대기

            return updated_count

        except Exception as e:
            logger.error(f"관찰 종목 신호 업데이트 오류: {e}")
            return 0

    async def _batch_evaluate_status_transitions(self, candidates: List[CandleTradeCandidate]) -> int:
        """관찰 중인 종목들 상태(CandleStatus) 전환 검토"""
        try:
            # 🔧 매수 신호 재평가가 필요한 모든 상태 포함 (WATCHING, SCANNING, BUY_READY)
            eligible_candidates = [
                c for c in candidates
                if c.status in [CandleStatus.WATCHING, CandleStatus.SCANNING, CandleStatus.BUY_READY]
            ]

            if not eligible_candidates:
                logger.debug(f"📊 매수 신호 재평가 대상 없음 (입력: {len(candidates)}개)")
                return 0

            logger.info(f"🔍 매수 신호 재평가 대상: {len(eligible_candidates)}개 (WATCHING/SCANNING/BUY_READY)")

            # 매수 신호 재평가 및 상태 전환 검토 (BuyOpportunityEvaluator 위임)
            buy_ready_count = await self.buy_evaluator.evaluate_watching_stocks_for_entry(eligible_candidates)
            if buy_ready_count > 0:
                logger.info(f"🎯 BUY_READY 전환: {buy_ready_count}개 종목")

            return buy_ready_count

        except Exception as e:
            logger.error(f"상태 전환 검토 오류: {e}")
            return 0

    async def _batch_evaluate_entered_stocks(self, candidates: List[CandleTradeCandidate]) -> int:
        """진입한 종목들 배치 재평가 (매도 신호 중심) - 5개씩 병렬 처리"""
        try:
            updated_count = 0
            batch_size = 5

            # 5개씩 배치로 나누어 병렬 처리
            for i in range(0, len(candidates), batch_size):
                batch = candidates[i:i + batch_size]

                # 배치 내 모든 종목을 동시에 분석
                analysis_tasks = [
                    self.candle_analyzer.comprehensive_signal_analysis(candidate, focus_on_exit=True)
                    for candidate in batch
                ]

                # 병렬로 실행하고 결과 받기
                analysis_results = await asyncio.gather(*analysis_tasks, return_exceptions=True)

                # 결과 처리
                for candidate, analysis_result in zip(batch, analysis_results):
                    try:
                        # 예외 처리
                        if isinstance(analysis_result, Exception):
                            logger.debug(f"진입 종목 재평가 오류 ({candidate.stock_code}): {analysis_result}")
                            continue

                        # 타입 확인 후 처리
                        if analysis_result and isinstance(analysis_result, dict) and self._should_update_exit_signal(candidate, analysis_result):
                            # 매도 신호 업데이트
                            old_signal = candidate.trade_signal
                            candidate.trade_signal = analysis_result['new_signal']
                            candidate.signal_strength = analysis_result['signal_strength']
                            candidate.signal_updated_at = datetime.now(self.korea_tz)

                            logger.info(f"🔄 {candidate.stock_code} 매도신호 업데이트: "
                                       f"{old_signal.value} → {candidate.trade_signal.value} "
                                       f"(강도:{candidate.signal_strength})")
                            updated_count += 1

                            # 강한 매도 신호시 즉시 매도 검토
                            if candidate.trade_signal in [TradeSignal.STRONG_SELL, TradeSignal.SELL]:
                                logger.info(f"🎯 {candidate.stock_code} 강한 매도 신호 - 즉시 매도 검토")
                                # _manage_existing_positions에서 처리되도록 함

                    except Exception as e:
                        logger.debug(f"진입 종목 재평가 결과 처리 오류 ({candidate.stock_code}): {e}")
                        continue

                # 배치 간 짧은 간격 (API 부하 방지)
                if i + batch_size < len(candidates):
                    await asyncio.sleep(0.2)

            return updated_count

        except Exception as e:
            logger.error(f"진입 종목 배치 재평가 오류: {e}")
            return 0

    def _should_update_signal(self, candidate: CandleTradeCandidate, analysis_result: Dict) -> bool:
        """신호 업데이트 필요 여부 판단 (개선된 버전)"""
        try:
            new_signal = analysis_result['new_signal']
            new_strength = analysis_result['signal_strength']

            # 1. 신호 종류가 변경된 경우 (가장 중요)
            signal_changed = new_signal != candidate.trade_signal
            if signal_changed:
                logger.debug(f"🔄 {candidate.stock_code} 신호 변경: {candidate.trade_signal.value} → {new_signal.value}")
                return True

            # 2. 강도 변화 체크 (더 민감하게)
            strength_diff = abs(new_strength - candidate.signal_strength)

            # 강한 신호일수록 더 민감하게 반응
            if new_signal in [TradeSignal.STRONG_BUY, TradeSignal.STRONG_SELL]:
                threshold = 10  # 강한 신호는 10점 차이
            elif new_signal in [TradeSignal.BUY, TradeSignal.SELL]:
                threshold = 15  # 일반 신호는 15점 차이
            else:
                threshold = 20  # HOLD 신호는 20점 차이

            strength_changed = strength_diff >= threshold

            if strength_changed:
                logger.debug(f"🔄 {candidate.stock_code} 강도 변화: {candidate.signal_strength} → {new_strength} (차이:{strength_diff:.1f})")
                return True

            # 3. 중요한 임계점 통과 체크
            critical_thresholds = [30, 50, 70, 80]  # 중요한 강도 구간
            old_range = self._get_strength_range(candidate.signal_strength, critical_thresholds)
            new_range = self._get_strength_range(new_strength, critical_thresholds)

            if old_range != new_range:
                logger.debug(f"🔄 {candidate.stock_code} 강도 구간 변화: {old_range} → {new_range}")
                return True

            return False

        except Exception as e:
            logger.debug(f"신호 업데이트 판단 오류: {e}")
            return False

    def _get_strength_range(self, strength: float, thresholds: List[int]) -> str:
        """강도 구간 계산"""
        for i, threshold in enumerate(sorted(thresholds)):
            if strength <= threshold:
                return f"range_{i}"
        return f"range_{len(thresholds)}"

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
                scanner_status = f"구독{len(self.subscribed_stocks)}개 " if hasattr(self, 'subscribed_stocks') else ""
                logger.info(f"📊 상태: 관찰{stats['total_stocks']}개 "
                           f"포지션{stats['active_positions']}개 "
                           f"{scanner_status}"
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
                'market_scanner': self.market_scanner.get_scan_status() if hasattr(self, 'market_scanner') else None,
                'daily_stats': self.daily_stats,
                'config': self.config
            }

        except Exception as e:
            logger.error(f"상태 조회 오류: {e}")
            return {'error': str(e)}

    def get_active_positions(self) -> List[Dict[str, Any]]:
        """활성 포지션 조회"""
        try:
            positions = self.stock_manager.get_active_positions()
            return [position.to_dict() for position in positions]
        except Exception as e:
            logger.error(f"활성 포지션 조회 오류: {e}")
            return []

    # ========== 🆕 미체결 주문 관리 ==========

    async def check_and_cancel_stale_orders(self):
        """🕐 미체결 주문 자동 취소 체크 (5분 이상 미체결)"""
        try:
            stale_order_timeout = 300  # 5분 (300초)
            current_time = datetime.now()

            # ========== 1. 웹소켓 관리 종목들의 PENDING_ORDER 상태 체크 ==========
            pending_candidates = [
                candidate for candidate in self.stock_manager._all_stocks.values()
                if candidate.status == CandleStatus.PENDING_ORDER
            ]

            if pending_candidates:
                logger.debug(f"🕐 웹소켓 관리 종목 미체결 주문 체크: {len(pending_candidates)}개")

                for candidate in pending_candidates:
                    try:
                        # 주문 경과 시간 확인
                        order_age = candidate.get_pending_order_age_seconds()
                        if order_age is None or order_age < stale_order_timeout:
                            continue

                        # 5분 이상 미체결 주문 취소 처리
                        await self._cancel_stale_order(candidate, order_age)

                    except Exception as e:
                        logger.error(f"❌ {candidate.stock_code} 미체결 주문 처리 오류: {e}")

            # ========== 2. 🆕 KIS API로 전체 미체결 주문 조회 및 취소 ==========
            logger.debug("🔍 KIS API를 통한 전체 미체결 주문 조회 시작")
            from ..api import kis_order_api
            await kis_order_api.check_and_cancel_external_orders(self.kis_api_manager)

        except Exception as e:
            logger.error(f"❌ 미체결 주문 체크 오류: {e}")



    async def _cancel_stale_order(self, candidate: CandleTradeCandidate, order_age: float):
        """개별 미체결 주문 취소 처리"""
        try:
            minutes_elapsed = order_age / 60

            # 매수 주문 취소
            if candidate.has_pending_order('buy'):
                buy_order_no = candidate.get_pending_order_no('buy')
                if not buy_order_no:
                    logger.warning(f"⚠️ {candidate.stock_code} 매수 주문번호가 없음 - 상태만 복원")
                    candidate.clear_pending_order('buy')
                    candidate.status = CandleStatus.BUY_READY
                    return

                logger.warning(f"⏰ {candidate.stock_code} 매수 주문 {minutes_elapsed:.1f}분 미체결 - 취소 시도")

                # KIS API를 통한 주문 취소 실행
                cancel_result = self.kis_api_manager.cancel_order(
                    order_no=buy_order_no,
                    ord_orgno="",           # 주문조직번호 (공백)
                    ord_dvsn="01",          # 주문구분 (기본값: 지정가)
                    qty_all_ord_yn="Y"      # 전량 취소
                )

                if cancel_result and cancel_result.get('status') == 'success':
                    logger.info(f"✅ {candidate.stock_code} 매수 주문 취소 성공 (주문번호: {buy_order_no})")

                    # 주문 정보 해제 및 상태 복원
                    candidate.clear_pending_order('buy')
                    candidate.status = CandleStatus.BUY_READY  # 매수 준비 상태로 복원

                    logger.info(f"🔄 {candidate.stock_code} BUY_READY 상태 복원")
                else:
                    error_msg = cancel_result.get('message', 'Unknown error') if cancel_result else 'API call failed'
                    logger.error(f"❌ {candidate.stock_code} 매수 주문 취소 실패: {error_msg}")

                    # 취소 실패해도 상태는 복원 (수동 처리 필요)
                    candidate.clear_pending_order('buy')
                    candidate.status = CandleStatus.BUY_READY
                    logger.warning(f"⚠️ {candidate.stock_code} 취소 실패했지만 상태 복원 - 수동 확인 필요")

            # 매도 주문 취소
            elif candidate.has_pending_order('sell'):
                sell_order_no = candidate.get_pending_order_no('sell')
                if not sell_order_no:
                    logger.warning(f"⚠️ {candidate.stock_code} 매도 주문번호가 없음 - 상태만 복원")
                    candidate.clear_pending_order('sell')
                    candidate.status = CandleStatus.ENTERED
                    return

                logger.warning(f"⏰ {candidate.stock_code} 매도 주문 {minutes_elapsed:.1f}분 미체결 - 취소 시도")

                # KIS API를 통한 주문 취소 실행
                cancel_result = self.kis_api_manager.cancel_order(
                    order_no=sell_order_no,
                    ord_orgno="",           # 주문조직번호 (공백)
                    ord_dvsn="01",          # 주문구분 (기본값: 지정가)
                    qty_all_ord_yn="Y"      # 전량 취소
                )

                if cancel_result and cancel_result.get('status') == 'success':
                    logger.info(f"✅ {candidate.stock_code} 매도 주문 취소 성공 (주문번호: {sell_order_no})")

                    # 주문 정보 해제 및 상태 복원
                    candidate.clear_pending_order('sell')
                    candidate.status = CandleStatus.ENTERED  # 진입 상태로 복원

                    logger.info(f"🔄 {candidate.stock_code} ENTERED 상태 복원")
                else:
                    error_msg = cancel_result.get('message', 'Unknown error') if cancel_result else 'API call failed'
                    logger.error(f"❌ {candidate.stock_code} 매도 주문 취소 실패: {error_msg}")

                    # 취소 실패해도 상태는 복원 (수동 처리 필요)
                    candidate.clear_pending_order('sell')
                    candidate.status = CandleStatus.ENTERED
                    logger.warning(f"⚠️ {candidate.stock_code} 취소 실패했지만 상태 복원 - 수동 확인 필요")

            # stock_manager 업데이트
            self.stock_manager.update_candidate(candidate)

        except Exception as e:
            logger.error(f"❌ {candidate.stock_code} 주문 취소 처리 오류: {e}")
