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
        # 🆕 외부 설정 파일 로드
        self.config = self._load_trading_config()

        # API 관리자들 설정
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

        # 🆕 스캔 간격 (초)
        self.scan_interval = self.config.get('scan_interval_seconds', 120)
        self.signal_evaluation_interval = self.config.get('signal_evaluation_interval', 20)

        # 데이터 수집 및 분석 도구들
        self.pattern_detector = CandlePatternDetector()
        self.stock_manager = CandleStockManager(
            max_watch_stocks=self.config.get('max_scan_stocks', 50),
            max_positions=self.config.get('max_positions', 15)
        )

        # 스캔 및 신호 평가 이벤트
        self.scan_event = asyncio.Event()
        self.signal_event = asyncio.Event()

        # 🆕 스캔 간격 (초)

        # 데이터 수집 및 분석 도구들

        # ========== 상태 관리 ==========
        self.is_running = False  # 🆕 실행 상태 추가
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

        # 🆕 시장 상황 분석기 초기화
        from .market_condition_analyzer import MarketConditionAnalyzer
        self.market_analyzer = MarketConditionAnalyzer()

        # 🆕 주문 타임아웃 콜백 등록 (OrderExecutionManager와 연동)
        self._register_order_timeout_callback()

        # 🆕 스캔 관련 속성 초기화
        self._last_scan_time = None
        self._last_pattern_scan_time = None

        logger.info("✅ CandleTradeManager 초기화 완료")


    # ========== 메인 실행 루프 ==========
    async def start_trading(self):
        """🕯️ 캔들 기반 매매 시작 - 패턴의 특성에 맞춘 최적화"""
        try:
            logger.info("🕯️ 캔들 기반 매매 시스템 시작")

            # 기존 보유 종목 웹소켓 모니터링 설정
            await self.setup_existing_holdings_monitoring()

            # 거래일 초기화
            await self._initialize_trading_day()

            # 🆕 캔들패턴 전용 스캔 타이머 초기화
            self._last_pattern_scan_time = None
            self._pattern_scan_interval = self.scan_interval
            self._signal_evaluation_interval = self.signal_evaluation_interval

            # 🎯 초기 패턴 스캔 (시작시 한번)
            logger.info("🔍 초기 캔들패턴 스캔 시작...")
            await self._scan_and_detect_patterns()
            self._last_pattern_scan_time = datetime.now()
            logger.info("✅ 초기 패턴 스캔 완료")

            # 메인 트레이딩 루프 시작
            self.is_running = True
            self._log_status()

            while self.is_running:
                try:
                    current_time = datetime.now()

                    #🕯️ 1. 새로운 종목 패턴 스캔
                    if self._should_scan_new_patterns(current_time):
                        logger.info("🔍 정기 캔들패턴 스캔 시작...")
                        await self._scan_and_detect_patterns()
                        self._last_pattern_scan_time = current_time
                        logger.info("✅ 정기 패턴 스캔 완료")

                    # 🌍 2. 시장 상황 분석 (5분마다)
                    if self.market_analyzer.should_update():
                        await self.market_analyzer.analyze_market_condition()

                    # 🔄 3. 기존 종목 신호 재평가 (30초 간격 - 실시간 모니터링)
                    await self._periodic_signal_evaluation()

                    # 💰 4. 진입 기회 평가 및 매수 실행 (시장상황 반영)
                    await self.buy_evaluator.evaluate_entry_opportunities()

                    # 📈 4. 기존 포지션 관리 - 매도 시그널 체크
                    await self.sell_manager.manage_existing_positions()
                    self.sell_manager.cleanup_adjustment_history()

                    # 🧹 5. 미체결 주문 관리 (1분마다)
                    if hasattr(self, '_last_stale_check_time'):
                        if (current_time - self._last_stale_check_time).total_seconds() >= 30:
                            await self.check_and_cancel_stale_orders()
                            self._last_stale_check_time = current_time
                    else:
                        self._last_stale_check_time = current_time

                    # 🧹 6. EXITED 종목 정리 (5분마다)
                    if hasattr(self, '_last_cleanup_time'):
                        if (current_time - self._last_cleanup_time).total_seconds() >= 300:  # 5분
                            await self.cleanup_exited_positions()
                            self._last_cleanup_time = current_time
                    else:
                        self._last_cleanup_time = current_time

                    # 🧹 7. 이미 매도 완료된 종목 정리 (10분마다)
                    if hasattr(self, '_last_position_cleanup_time'):
                        if (current_time - self._last_position_cleanup_time).total_seconds() >= 600:  # 10분
                            await self.sell_manager.cleanup_completed_positions()
                            self._last_position_cleanup_time = current_time
                    else:
                        self._last_position_cleanup_time = current_time

                    # 📊 8. 상태 업데이트
                    self._log_status()

                    # ⏰ 9. 대기 시간 (기본 30초 - 기존 종목 모니터링 중심)
                    await asyncio.sleep(self._signal_evaluation_interval)

                except Exception as e:
                    logger.error(f"매매 루프 오류: {e}")
                    await asyncio.sleep(10)  # 오류시 10초 대기 후 재시도

        except Exception as e:
            logger.error(f"캔들 매매 시작 오류: {e}")
            self.is_running = False

    def _load_trading_config(self) -> Dict:
        """🆕 거래 설정 로드 (외부 파일 우선, 폴백 기본값)"""
        try:
            import json
            import os

            # 1. 외부 설정 파일 시도
            config_path = os.path.join('config', 'candle_strategy_config.json')
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    external_config = json.load(f)
                logger.info(f"✅ 외부 설정 파일 로드: {config_path}")
                return external_config

            # 2. 폴백: 기본 설정 반환
            logger.warning("⚠️ 외부 설정 파일 없음 - 기본 설정 사용")
            return {}  # 빈 딕셔너리 반환 (기본값들은 get() 메소드로 처리)

        except Exception as e:
            logger.warning(f"⚠️ 설정 파일 로드 실패: {e} - 기본 설정 사용")
            return {}  # 빈 딕셔너리 반환



    def _register_order_timeout_callback(self):
        """🆕 주문 타임아웃 콜백 등록 및 웹소켓 연결 강화"""
        try:
            if (hasattr(self.trade_executor, 'execution_manager') and
                self.trade_executor.execution_manager):

                # 타임아웃 콜백 함수 등록
                self.trade_executor.execution_manager.add_execution_callback(self._handle_order_timeout)

                # 🆕 웹소켓 매니저에 execution_manager 설정
                if self.websocket_manager and hasattr(self.websocket_manager, 'message_handler'):
                    if hasattr(self.websocket_manager.message_handler, 'set_execution_manager'):
                        self.websocket_manager.message_handler.set_execution_manager(self.trade_executor.execution_manager)
                        logger.info("✅ 웹소켓 매니저에 OrderExecutionManager 설정 완료")
                    else:
                        # 직접 설정 시도
                        self.websocket_manager.message_handler.execution_manager = self.trade_executor.execution_manager
                        logger.info("✅ 웹소켓 매니저에 OrderExecutionManager 직접 설정 완료")

                logger.info("✅ 주문 타임아웃 콜백 등록 및 웹소켓 연결 완료")
            else:
                logger.warning("⚠️ OrderExecutionManager 없음 - 타임아웃 콜백 등록 실패")
        except Exception as e:
            logger.error(f"❌ 타임아웃 콜백 등록 오류: {e}")

    def _handle_order_timeout(self, timeout_data: Dict, execution_info: Optional[Dict] = None):
        """🆕 주문 타임아웃 처리 - 종목 상태 복원"""
        try:
            # 정상 체결 콜백인 경우 무시 (타임아웃 전용 함수)
            if execution_info is not None:
                return  # 정상 체결 시에는 처리하지 않음

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

    async def _fetch_existing_holdings(self) -> List[Dict]:
        """기존 보유 종목 조회"""
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
        """🆕 기존 보유 종목을 CandleTradeCandidate로 생성 및 분석"""
        try:
            # 1. CandleTradeCandidate 객체 생성
            candidate = self._create_holding_candidate_object(stock_code, stock_name, current_price)

            # 2. 포지션 정보 설정 (이미 진입한 상태)
            candidate.enter_position(buy_price, quantity)

            # 3. 캔들 패턴 분석 수행
            candle_analysis_result = await self._analyze_holding_candle_patterns(stock_code, stock_name, current_price)

            # 4. 메타데이터 설정
            self._setup_holding_metadata(candidate, candle_analysis_result)

            # 5. 매수 체결 시간 설정
            self._setup_buy_execution_time(candidate)

            # 6. stock_manager에 추가
            success = self.stock_manager.add_candidate(candidate)

            if success:
                logger.info(f"✅ {stock_code} 기존 보유 종목 CandleTradeCandidate 생성 완료")
                return True
            else:
                logger.warning(f"⚠️ {stock_code} stock_manager 추가 실패")
                return False

        except Exception as e:
            logger.error(f"❌ 기존 보유 종목 CandleTradeCandidate 생성 실패 ({stock_code}): {e}")
            return False

    async def _analyze_holding_candle_patterns(self, stock_code: str, stock_name: str, current_price: float) -> Optional[Dict]:
        """🔄 기존 보유 종목의 패턴 정보 분석 - DB에서 읽어오거나 기본값 반환"""
        try:
            logger.debug(f"🔄 {stock_code} 보유 종목 패턴 정보 분석 시작")

            # 🆕 1단계: DB에서 기존 매수 패턴 정보 조회
            db_pattern_info = await self._get_pattern_info_from_db(stock_code)
            if db_pattern_info:
                pattern_type = db_pattern_info.get('strongest_pattern', {}).get('type', 'UNKNOWN')
                confidence = db_pattern_info.get('strongest_pattern', {}).get('confidence', 0.5)
                logger.info(f"📚 {stock_code} DB에서 패턴 정보 복원: {pattern_type} "
                           f"(신뢰도: {confidence:.2f})")
                return db_pattern_info

            # 🆕 2단계: DB에 정보가 없으면 OHLCV 데이터 캐싱만 수행
            logger.debug(f"📊 {stock_code} DB에 패턴 정보 없음 - 캐싱 후 기본값 반환")

            # 🆕 기존 _all_stocks에서 캐시된 데이터 확인
            ohlcv_data = None
            if stock_code in self.stock_manager._all_stocks:
                candidate = self.stock_manager._all_stocks[stock_code]
                ohlcv_data = candidate.get_ohlcv_data()
                if ohlcv_data is not None:
                    logger.debug(f"📄 {stock_code} 캐시된 일봉 데이터 사용")

            # 캐시에 없으면 API 호출해서 캐싱만 수행
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

            # 🆕 3단계: 기본값 반환 (실시간 분석 생략)
            logger.info(f"⚠️ {stock_code} 패턴 정보 없음 - 기본값 반환")
            return self._get_default_pattern_info(stock_code, stock_name, current_price)

        except Exception as e:
            logger.error(f"❌ {stock_code} 패턴 정보 분석 오류: {e}")
            # 오류시에도 기본값 반환
            return self._get_default_pattern_info(stock_code, stock_name, current_price)

    async def _get_pattern_info_from_db(self, stock_code: str) -> Optional[Dict]:
        """🏛️ DB에서 종목의 매수 패턴 정보 조회"""
        try:
            # 최근 매수 거래에서 패턴 정보 조회
            recent_buy_trade = self.trade_db.get_trade_history(
                stock_code=stock_code, 
                days=5,  # 최근 5일 
                trade_type='BUY'
            )
            
            if not recent_buy_trade:
                return None
            
            # 가장 최근 매수 거래
            latest_trade = recent_buy_trade[0]
            
            # 패턴 정보가 있는지 확인
            pattern_type = latest_trade.get('pattern_type')
            if not pattern_type:
                return None
            
            # 패턴 정보 구성
            from .candle_trade_candidate import TradeSignal
            
            result = {
                'patterns_detected': True,
                'patterns': [],  # 실제 패턴 객체는 없지만 호환성 유지
                'strongest_pattern': {
                    'type': pattern_type,
                    'strength': latest_trade.get('pattern_strength', 70),
                    'confidence': latest_trade.get('pattern_confidence', 0.7),
                    'description': f"DB에서 복원된 {pattern_type} 패턴"
                },
                'trade_signal': TradeSignal.BUY,  # 이미 매수한 상태
                'signal_strength': latest_trade.get('pattern_strength', 70),
                'analysis_time': latest_trade.get('timestamp', datetime.now().isoformat()),
                'source': 'database_restore',
                'trade_id': latest_trade.get('id'),
                'rsi_value': latest_trade.get('rsi_value'),
                'macd_value': latest_trade.get('macd_value'),
                'volume_ratio': latest_trade.get('volume_ratio')
            }
            
            logger.info(f"📚 {stock_code} DB 패턴 정보 복원 성공: {pattern_type}")
            return result
            
        except Exception as e:
            logger.debug(f"DB 패턴 정보 조회 오류 ({stock_code}): {e}")
            return None

    def _get_default_pattern_info(self, stock_code: str, stock_name: str, current_price: float) -> Dict:
        """🔧 기본 패턴 정보 반환 (패턴 감지 실패시)"""
        try:
            from .candle_trade_candidate import TradeSignal
            
            # 기본 패턴 정보 생성
            default_result = {
                'patterns_detected': False,
                'patterns': [],
                'strongest_pattern': {
                    'type': 'UNKNOWN',
                    'strength': 50,
                    'confidence': 0.5,
                    'description': '패턴 정보 없음 - 기본 설정 적용'
                },
                'trade_signal': TradeSignal.HOLD,
                'signal_strength': 50,
                'analysis_time': datetime.now().isoformat(),
                'source': 'default_fallback'
            }
            
            logger.info(f"🔧 {stock_code} 기본 패턴 정보 적용")
            return default_result
            
        except Exception as e:
            logger.error(f"기본 패턴 정보 생성 오류: {e}")
            # 최소한의 정보라도 반환
            return {
                'patterns_detected': False,
                'patterns': [],
                'strongest_pattern': {'type': 'UNKNOWN', 'strength': 50, 'confidence': 0.5},
                'source': 'error_fallback'
            }

    def _generate_trade_signal_from_patterns(self, patterns: List[CandlePatternInfo]) -> Tuple:
        """패턴 목록에서 매매 신호 생성 - candle_analyzer로 위임"""
        try:
            return self.candle_analyzer.generate_trade_signal_from_patterns(patterns)

        except Exception as e:
            logger.error(f"패턴 신호 생성 오류: {e}")
            return TradeSignal.HOLD, 0


    def _should_scan_new_patterns(self, current_time: datetime) -> bool:
        """🕯️ 🚀 스마트 패턴 스캔 스케줄링 (장전 1회 + 장중 보완)"""
        try:
            current_hour = current_time.hour
            current_minute = current_time.minute
            
            # 🎯 1. 장전 전체 스캔 (08:30 - 08:50)
            if 8 <= current_hour < 9 and 30 <= current_minute <= 50:
                if not self._last_pattern_scan_time:
                    logger.info("🌅 장전 전체 KOSPI 스캔 시작")
                    return True
                
                # 장전에는 한 번만 실행
                last_scan_hour = self._last_pattern_scan_time.hour
                if last_scan_hour < 8 or last_scan_hour >= 9:
                    logger.info("🌅 장전 전체 KOSPI 스캔 (일일 1회)")
                    return True
            
            # 🎯 2. 장중에는 전체 스캔 금지 (09:00 - 15:30)
            elif 9 <= current_hour < 15 or (current_hour == 15 and current_minute <= 30):
                # 장중에는 급등/급증 종목만 추가 모니터링
                return False
            
            # 🎯 3. 장후에는 다음날 준비를 위한 스캔 허용 (15:30 이후)
            elif current_hour >= 15 and current_minute > 30:
                if not self._last_pattern_scan_time:
                    return True
                
                # 장후에는 2시간마다 한 번씩
                time_elapsed = (current_time - self._last_pattern_scan_time).total_seconds()
                return time_elapsed >= 7200  # 2시간
            
            return False

        except Exception as e:
            logger.error(f"패턴 스캔 판단 오류: {e}")
            return False

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
        """기존 보유 종목 모니터링 정리"""
        try:
            # 웹소켓 구독 해제는 자동으로 처리됨
            logger.info("✅ 기존 보유 종목 모니터링 정리 완료")
        except Exception as e:
            logger.error(f"기존 보유 종목 모니터링 정리 오류: {e}")

    async def cleanup_exited_positions(self):
        """🧹 EXITED 상태 종목들을 _all_stocks에서 제거 (메모리 정리)"""
        try:
            exited_stocks = [
                stock_code for stock_code, candidate in self.stock_manager._all_stocks.items()
                if candidate.status == CandleStatus.EXITED
            ]

            cleanup_count = 0
            for stock_code in exited_stocks:
                try:
                    # 메타데이터 보존하여 로깅
                    candidate = self.stock_manager._all_stocks[stock_code]
                    profit_info = ""
                    if candidate.performance and candidate.performance.pnl_pct is not None:
                        profit_info = f" (수익률: {candidate.performance.pnl_pct:+.2f}%)"

                    # _all_stocks에서 제거
                    del self.stock_manager._all_stocks[stock_code]
                    cleanup_count += 1

                    logger.debug(f"🧹 {stock_code} EXITED 종목 제거 완료{profit_info}")

                except Exception as e:
                    logger.warning(f"⚠️ {stock_code} EXITED 종목 제거 실패: {e}")

            if cleanup_count > 0:
                logger.info(f"🧹 EXITED 종목 정리 완료: {cleanup_count}개 제거 (메모리 절약)")

        except Exception as e:
            logger.error(f"❌ EXITED 종목 정리 오류: {e}")

    # ========== 🆕 체결 확인 처리 ==========

    async def handle_execution_confirmation(self, execution_data):
        """🎯 웹소켓 체결 통보 처리 - 매수/매도 체결 확인 후 상태 업데이트 (개선된 버전)"""
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
                logger.warning(f"⚠️ 체결 통보 데이터 부족: stock_code={stock_code}, order_type={order_type}")
                return

            # 🆕 체결가 정보 로깅 강화
            logger.info(f"🎯 체결 확인 (상세): {stock_code} {order_type} {executed_quantity}주 {executed_price:,.0f}원 "
                       f"(주문번호: {order_no})")

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
            # 🆕 상세 오류 정보 로깅
            import traceback
            logger.error(f"❌ 체결 확인 처리 상세 오류:\n{traceback.format_exc()}")

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

            # 🆕 8. candle_trades 테이블에 매수 기록 저장
            await self._save_candle_trade_to_db(candidate, 'ENTRY', executed_price, executed_quantity, order_no, '매수 체결 완료')

            # 9. stock_manager 업데이트
            self.stock_manager.update_candidate(candidate)

            # 🆕 체결 완료 플래그 설정 (중복 처리 방지)
            candidate.metadata['execution_processed'] = True
            candidate.metadata['last_execution_update'] = datetime.now().isoformat()

            # 10. 통계 업데이트
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
                profit_loss = (executed_price - candidate.performance.entry_price) * executed_quantity
            else:
                profit_pct = 0.0
                profit_loss = 0

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

            # 🆕 9. candle_trades 테이블에 매도 기록 저장
            await self._save_candle_trade_to_db(
                candidate, 'EXIT', executed_price, executed_quantity, order_no, 
                f'매도 체결 완료 (수익률: {profit_pct:.2f}%)',
                profit_loss=profit_loss, profit_rate=profit_pct
            )

            # 10. stock_manager 업데이트
            self.stock_manager.update_candidate(candidate)

            # 🆕 체결 완료 플래그 설정 (중복 처리 방지)
            candidate.metadata['execution_processed'] = True
            candidate.metadata['last_execution_update'] = datetime.now().isoformat()
            candidate.metadata['final_exit_confirmed'] = True  # 매도 완료 확정

            # 11. 통계 업데이트
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

    async def _save_candle_trade_to_db(self, candidate: CandleTradeCandidate, trade_type: str,
                                     executed_price: float, executed_quantity: int, order_no: str,
                                     decision_reason: str, profit_loss: int = 0, profit_rate: float = 0.0):
        """🆕 candle_trades 테이블에 거래 기록 저장"""
        try:
            if not self.trade_db:
                logger.debug(f"📚 {candidate.stock_code} DB 없음 - candle_trades 저장 스킵")
                return

            # candidate_id 찾기 (candle_candidates 테이블에서)
            candidate_id = candidate.metadata.get('db_id')
            if not candidate_id:
                # DB에서 찾기 시도
                candidates = self.trade_db.get_candle_candidates(status=None, days=7)
                for cand in candidates:
                    if cand['stock_code'] == candidate.stock_code:
                        candidate_id = cand['id']
                        candidate.metadata['db_id'] = candidate_id
                        break

            if not candidate_id:
                logger.warning(f"⚠️ {candidate.stock_code} candidate_id를 찾을 수 없음 - candle_trades 저장 실패")
                return

            # 패턴 정보 추출
            pattern_matched = None
            if candidate.detected_patterns and len(candidate.detected_patterns) > 0:
                pattern_matched = candidate.detected_patterns[0].pattern_type.value

            # 보유 시간 계산 (매도인 경우)
            hold_duration = 0
            if trade_type == 'EXIT' and candidate.performance.entry_time:
                entry_time = candidate.performance.entry_time
                current_time = datetime.now(self.korea_tz)
                if entry_time.tzinfo is None:
                    entry_time = entry_time.replace(tzinfo=self.korea_tz)
                hold_duration = int((current_time - entry_time).total_seconds() / 60)  # 분 단위

            # candle_trades 테이블에 저장
            trade_id = self.trade_db.record_candle_trade(
                candidate_id=candidate_id,
                trade_type=trade_type,
                stock_code=candidate.stock_code,
                stock_name=candidate.stock_name,
                quantity=executed_quantity,
                price=int(executed_price),
                total_amount=int(executed_price * executed_quantity),
                decision_reason=decision_reason,
                pattern_matched=pattern_matched,
                order_id=order_no,
                entry_price=int(candidate.performance.entry_price) if candidate.performance.entry_price else None,
                profit_loss=profit_loss,
                profit_rate=profit_rate,
                hold_duration=hold_duration,
                market_condition='NORMAL',  # 추후 시장 상황 분석 결과로 대체 가능
                rsi_value=candidate.metadata.get('technical_indicators', {}).get('rsi'),
                macd_value=candidate.metadata.get('technical_indicators', {}).get('macd'),
                volume_ratio=candidate.metadata.get('technical_indicators', {}).get('volume_ratio')
            )

            if trade_id > 0:
                logger.info(f"📚 {candidate.stock_code} candle_trades 저장 완료: {trade_type} (ID: {trade_id})")
                candidate.metadata['candle_trade_id'] = trade_id
            else:
                logger.warning(f"⚠️ {candidate.stock_code} candle_trades 저장 실패")

        except Exception as e:
            logger.error(f"❌ {candidate.stock_code} candle_trades 저장 오류: {e}")
            import traceback
            logger.error(f"❌ 상세 오류:\n{traceback.format_exc()}")

    # ========== 🆕 주기적 신호 재평가 시스템 ==========

    async def _periodic_signal_evaluation(self):
        """🔄 주기적 신호 재평가 - 30초마다 실행"""
        try:
            # 🎯 현재 모든 종목 상태별 분류 (PENDING_ORDER, EXITED 제외)
            all_candidates = [
                candidate for candidate in self.stock_manager._all_stocks.values()
                if candidate.status not in [CandleStatus.PENDING_ORDER, CandleStatus.EXITED]  # 주문 대기 중이거나 매도 완료된 종목 제외
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

                # 🆕 통합된 신호 업데이트 및 상태 전환 처리
                watch_updated = await self._batch_evaluate_watching_stocks(watching_candidates)
                logger.debug(f"✅ 관찰 종목 처리 완료: {watch_updated}개")

            # 🎯 2. 진입한 종목들 재평가 (매도 신호 중심)
            if entered_candidates:
                logger.debug(f"💰 진입 종목 재평가: {len(entered_candidates)}개")
                enter_updated = await self._batch_evaluate_entered_stocks(entered_candidates)
                logger.debug(f"✅ 진입 종목 신호 업데이트: {enter_updated}개")

        except Exception as e:
            logger.error(f"주기적 신호 재평가 오류: {e}")

    async def _batch_evaluate_watching_stocks(self, candidates: List[CandleTradeCandidate]) -> int:
        """🆕 관찰 중인 종목들 통합 처리 - 신호 업데이트 및 상태 전환"""
        try:
            signal_updated_count = 0
            status_changed_count = 0

            # 🔧 매수 신호 재평가가 필요한 모든 상태 포함 (WATCHING, SCANNING, BUY_READY)
            eligible_candidates = [
                c for c in candidates
                if c.status in [CandleStatus.WATCHING, CandleStatus.SCANNING, CandleStatus.BUY_READY]
            ]

            if not eligible_candidates:
                logger.debug(f"📊 매수 신호 재평가 대상 없음 (입력: {len(candidates)}개)")
                return 0

            logger.info(f"🔍 관찰 종목 통합 처리 대상: {len(eligible_candidates)}개 (WATCHING/SCANNING/BUY_READY)")

            # 🆕 Step 1: 배치별 가격 정보 조회 (API 호출 최적화)
            batch_size = 10
            current_data_dict = {}  # 종목별 current_data 저장

            for i in range(0, len(eligible_candidates), batch_size):
                batch = eligible_candidates[i:i + batch_size]

                # 🎯 배치 시작 시 가격 정보 조회
                for candidate in batch:
                    try:
                        from ..api.kis_market_api import get_inquire_price
                        current_data = get_inquire_price("J", candidate.stock_code)
                        if current_data is not None and not current_data.empty:
                            current_data_dict[candidate.stock_code] = current_data
                    except Exception as e:
                        logger.debug(f"가격 조회 오류 ({candidate.stock_code}): {e}")
                        continue

                # 🎯 Step 2: 신호(TradeSignal) 업데이트 (current_data 활용)
                for candidate in batch:
                    try:
                        stock_current_data = current_data_dict.get(candidate.stock_code)
                        if stock_current_data is None:
                            continue

                        # 다각도 종합 분석 수행 (current_data 전달)
                        analysis_result = await self.candle_analyzer.comprehensive_signal_analysis(
                            candidate, current_data=stock_current_data
                        )

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

                            logger.debug(f"🔄 {candidate.stock_code} 신호 업데이트: "
                                       f"{old_signal.value} → {candidate.trade_signal.value} "
                                       f"(강도:{candidate.signal_strength})")
                            signal_updated_count += 1

                    except Exception as e:
                        logger.debug(f"종목 신호 재평가 오류 ({candidate.stock_code}): {e}")
                        continue

                # API 호출 간격 조절
                if i + batch_size < len(eligible_candidates):
                    await asyncio.sleep(0.5)  # 0.5초 대기

            # 🎯 Step 3: 상태(CandleStatus) 전환 검토 (current_data_dict 전달)
            status_changed_count = await self.buy_evaluator.evaluate_watching_stocks_for_entry(
                eligible_candidates, current_data_dict
            )

            # 결과 로깅
            if signal_updated_count > 0:
                logger.debug(f"✅ 신호 업데이트: {signal_updated_count}개 종목")
            if status_changed_count > 0:
                logger.info(f"🎯 BUY_READY 전환: {status_changed_count}개 종목")

            return signal_updated_count + status_changed_count

        except Exception as e:
            logger.error(f"관찰 종목 통합 처리 오류: {e}")
            return 0

    async def _batch_evaluate_entered_stocks(self, candidates: List[CandleTradeCandidate]) -> int:
        """진입한 종목들 배치 재평가 (매도 신호 중심) - current_data 최적화"""
        try:
            updated_count = 0
            batch_size = 10

            # 5개씩 배치로 나누어 처리
            for i in range(0, len(candidates), batch_size):
                batch = candidates[i:i + batch_size]

                # 🆕 배치 시작 시 가격 정보 조회
                current_data_dict = {}
                for candidate in batch:
                    try:
                        from ..api.kis_market_api import get_inquire_price
                        current_data = get_inquire_price("J", candidate.stock_code)
                        if current_data is not None and not current_data.empty:
                            current_data_dict[candidate.stock_code] = current_data
                    except Exception as e:
                        logger.debug(f"가격 조회 오류 ({candidate.stock_code}): {e}")
                        continue

                # 배치 내 모든 종목을 동시에 분석 (current_data 전달)
                analysis_tasks = [
                    self.candle_analyzer.comprehensive_signal_analysis(
                        candidate,
                        focus_on_exit=True,
                        current_data=current_data_dict.get(candidate.stock_code)
                    )
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
                    await asyncio.sleep(0.5)

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
        """현재 상태 조회 (🆕 시장상황 포함)"""
        try:
            stats = self.stock_manager.get_summary_stats()

            # 🆕 시장상황 정보 포함
            market_condition = self.market_analyzer.get_current_condition()

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
                'config': self.config,
                # 🆕 시장 상황 정보 추가 (Dict 타입 처리)
                'market_condition': self._format_market_condition_dict(market_condition)
            }

        except Exception as e:
            logger.error(f"상태 조회 오류: {e}")
            return {'error': str(e)}

    def _format_market_condition_dict(self, market_condition) -> Dict[str, Any]:
        """시장 상황 정보를 Dict로 안전하게 포맷팅"""
        try:
            # Dict 타입인 경우 (analyze_market_condition 결과)
            if isinstance(market_condition, dict):
                investor_sentiment = market_condition.get('investor_sentiment', {})
                return {
                    'market_trend': market_condition.get('market_trend', 'neutral_market'),
                    'investor_sentiment': {
                        'foreign_buying': investor_sentiment.get('foreign_buying', False),
                        'institution_buying': investor_sentiment.get('institution_buying', False),
                        'overall_sentiment': investor_sentiment.get('overall_sentiment', 'neutral')
                    },
                    'volatility': market_condition.get('volatility', 'low_volatility'),
                    'market_strength_score': 50.0,  # 기본값
                    'market_risk_level': 'medium',  # 기본값
                    'last_updated': market_condition.get('timestamp', ''),
                    'data_quality': 'good',
                    'confidence_score': 0.7
                }
            
            # MarketCondition 객체인 경우 (기존 방식)
            else:
                return {
                    'kospi_trend': market_condition.kospi_trend.value,
                    'kosdaq_trend': market_condition.kosdaq_trend.value,
                    'kospi_change_pct': market_condition.kospi_change_pct,
                    'kosdaq_change_pct': market_condition.kosdaq_change_pct,
                    'volatility': market_condition.volatility.value,
                    'volume_condition': market_condition.volume_condition.value,
                    'foreign_flow': market_condition.foreign_flow.value,
                    'institution_flow': market_condition.institution_flow.value,
                    'market_strength_score': self.market_analyzer.get_market_strength_score(),
                    'market_risk_level': self.market_analyzer.get_market_risk_level(),
                    'last_updated': market_condition.last_updated.strftime('%H:%M:%S'),
                    'data_quality': market_condition.data_quality,
                    'confidence_score': market_condition.confidence_score
                }
        except Exception as e:
            logger.warning(f"⚠️ 시장 상황 포맷팅 오류: {e} - 기본값 반환")
            return {
                'market_trend': 'neutral_market',
                'volatility': 'low_volatility',
                'market_strength_score': 50.0,
                'market_risk_level': 'medium',
                'last_updated': datetime.now().strftime('%H:%M:%S'),
                'data_quality': 'limited',
                'confidence_score': 0.5
            }

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

                # 🆕 정정취소가능주문조회로 정확한 주문조직번호 획득
                from ..api.kis_order_api import get_inquire_psbl_rvsecncl_lst

                try:
                    cancelable_orders = get_inquire_psbl_rvsecncl_lst()
                    ord_orgno = ""
                    ord_dvsn = "01"  # 기본값

                    if cancelable_orders is not None and not cancelable_orders.empty:
                        # 해당 주문번호 찾기
                        for _, order in cancelable_orders.iterrows():
                            if order.get('odno', '') == buy_order_no:
                                ord_orgno = order.get('ord_orgno', '')
                                ord_dvsn = order.get('ord_dvsn', '01')
                                logger.debug(f"📋 {candidate.stock_code} 매수 주문정보 획득: 조직번호={ord_orgno}, 구분={ord_dvsn}")
                                break

                    if not ord_orgno:
                        logger.warning(f"⚠️ {candidate.stock_code} 매수 주문조직번호 획득 실패 - 상태만 복원")
                        candidate.clear_pending_order('buy')
                        candidate.status = CandleStatus.BUY_READY
                        return

                except Exception as e:
                    logger.error(f"❌ {candidate.stock_code} 정정취소가능주문조회 오류: {e}")
                    candidate.clear_pending_order('buy')
                    candidate.status = CandleStatus.BUY_READY
                    return

                # KIS API를 통한 주문 취소 실행
                cancel_result = self.kis_api_manager.cancel_order(
                    order_no=buy_order_no,
                    ord_orgno=ord_orgno,        # 🆕 정확한 주문조직번호 사용
                    ord_dvsn=ord_dvsn,          # 🆕 정확한 주문구분 사용
                    qty_all_ord_yn="Y"          # 전량 취소
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

                # 🆕 정정취소가능주문조회로 정확한 주문조직번호 획득
                from ..api.kis_order_api import get_inquire_psbl_rvsecncl_lst

                try:
                    cancelable_orders = get_inquire_psbl_rvsecncl_lst()
                    ord_orgno = ""
                    ord_dvsn = "01"  # 기본값

                    if cancelable_orders is not None and not cancelable_orders.empty:
                        # 해당 주문번호 찾기
                        for _, order in cancelable_orders.iterrows():
                            if order.get('odno', '') == sell_order_no:
                                ord_orgno = order.get('ord_orgno', '')
                                ord_dvsn = order.get('ord_dvsn', '01')
                                logger.debug(f"📋 {candidate.stock_code} 매도 주문정보 획득: 조직번호={ord_orgno}, 구분={ord_dvsn}")
                                break

                    if not ord_orgno:
                        logger.warning(f"⚠️ {candidate.stock_code} 매도 주문조직번호 획득 실패 - 상태만 복원")
                        candidate.clear_pending_order('sell')
                        candidate.status = CandleStatus.ENTERED
                        return

                except Exception as e:
                    logger.error(f"❌ {candidate.stock_code} 정정취소가능주문조회 오류: {e}")
                    candidate.clear_pending_order('sell')
                    candidate.status = CandleStatus.ENTERED
                    return

                # KIS API를 통한 주문 취소 실행
                cancel_result = self.kis_api_manager.cancel_order(
                    order_no=sell_order_no,
                    ord_orgno=ord_orgno,        # 🆕 정확한 주문조직번호 사용
                    ord_dvsn=ord_dvsn,          # 🆕 정확한 주문구분 사용
                    qty_all_ord_yn="Y"          # 전량 취소
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