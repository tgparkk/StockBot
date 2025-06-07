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
        self.stock_manager = CandleStockManager(max_watch_stocks=100, max_positions=15)
        self.pattern_detector = CandlePatternDetector()

        # 내부 상태
        self._all_stocks: Dict[str, CandleTradeCandidate] = {}
        self._existing_holdings: Dict[str, Dict] = {}
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

        logger.info("✅ CandleTradeManager 초기화 완료")


    # 🆕 기존 보유 종목 웹소켓 구독 관리 (간소화)
    async def setup_existing_holdings_monitoring(self):
        """기존 보유 종목 웹소켓 모니터링 설정 - 메인 컨트롤러"""
        try:
            logger.info("📊 기존 보유 종목 웹소켓 모니터링 설정 시작")

            # 1. 기존 보유 종목 조회
            existing_stocks = await self._fetch_existing_holdings()
            if not existing_stocks:
                logger.info("📊 보유 종목이 없습니다.")
                return True

            logger.info(f"📈 보유 종목 {len(existing_stocks)}개 발견")

            # 2. 각 종목별 처리
            subscription_success_count = 0
            added_to_all_stocks_count = 0

            for stock_info in existing_stocks:
                try:
                    success_sub, success_add = await self._process_single_holding(stock_info)
                    if success_sub:
                        subscription_success_count += 1
                    if success_add:
                        added_to_all_stocks_count += 1

                except Exception as e:
                    logger.error(f"종목 처리 오류: {e}")
                    continue

            # 3. 결과 보고
            logger.info(f"📊 기존 보유 종목 웹소켓 구독 완료: {subscription_success_count}/{len(existing_stocks)}개")
            logger.info(f"🔄 _all_stocks에 기존 보유 종목 추가: {added_to_all_stocks_count}개")
            return subscription_success_count > 0

        except Exception as e:
            logger.error(f"기존 보유 종목 모니터링 설정 오류: {e}")
            return False

    async def _fetch_existing_holdings(self) -> List[Dict]:
        """기존 보유 종목 조회"""
        try:
            from ..api.kis_market_api import get_existing_holdings
            return get_existing_holdings()

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

            # 1. 캔들 패턴 분석
            candle_analysis_result = await self._analyze_holding_patterns(stock_code, stock_name, current_price)

            # 2. CandleTradeCandidate 생성 및 설정
            success_add = await self._create_and_setup_holding_candidate(
                stock_code, stock_name, current_price, buy_price, quantity, candle_analysis_result
            )

            # 3. 웹소켓 구독
            success_sub = await self._subscribe_holding_websocket(stock_code, stock_name)

            return success_sub, success_add

        except Exception as e:
            logger.error(f"개별 종목 처리 오류 ({stock_info.get('stock_code', 'unknown')}): {e}")
            return False, False

    async def _analyze_holding_patterns(self, stock_code: str, stock_name: str, current_price: float) -> Optional[Dict]:
        """보유 종목 캔들 패턴 분석"""
        try:
            return await self._analyze_existing_holding_patterns(stock_code, stock_name, current_price)
        except Exception as e:
            logger.debug(f"패턴 분석 오류 ({stock_code}): {e}")
            return None

    async def _create_and_setup_holding_candidate(self, stock_code: str, stock_name: str, current_price: float,
                                                buy_price: float, quantity: int, candle_analysis_result: Optional[Dict]) -> bool:
        """보유 종목 CandleTradeCandidate 생성 및 설정"""
        try:
            # 이미 _all_stocks에 있는지 확인
            if stock_code in self._all_stocks:
                logger.debug(f"✅ {stock_code} 이미 _all_stocks에 존재")
                return False

            # CandleTradeCandidate 객체 생성
            existing_candidate = self._create_holding_candidate_object(stock_code, stock_name, current_price)

            # 진입 정보 설정
            if buy_price > 0 and quantity > 0:
                existing_candidate.enter_position(float(buy_price), int(quantity))
                existing_candidate.update_price(float(current_price))
                existing_candidate.performance.entry_price = float(buy_price)

                # 리스크 관리 설정
                self._setup_holding_risk_management(existing_candidate, buy_price, current_price, candle_analysis_result)

                # 메타데이터 설정
                self._setup_holding_metadata(existing_candidate, candle_analysis_result)

                # _all_stocks에 추가
                self._all_stocks[stock_code] = existing_candidate
                logger.debug(f"✅ {stock_code} _all_stocks에 기존 보유 종목으로 추가")

                # 설정 완료 로그
                self._log_holding_setup_completion(existing_candidate)

                return True

            return False

        except Exception as e:
            logger.error(f"보유 종목 후보 생성 오류 ({stock_code}): {e}")
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

    def _setup_holding_risk_management(self, candidate: CandleTradeCandidate, buy_price: float,
                                     current_price: float, candle_analysis_result: Optional[Dict]):
        """보유 종목 리스크 관리 설정"""
        try:
            from .candle_trade_candidate import RiskManagement

            entry_price = float(buy_price)
            current_price_float = float(current_price)

            if candle_analysis_result and candle_analysis_result.get('patterns_detected'):
                # 캔들 패턴 분석 성공 시
                target_price, stop_loss_price, trailing_stop_pct, max_holding_hours, position_size_pct, risk_score, source_info = \
                    self._calculate_pattern_based_risk_settings(entry_price, current_price_float, candle_analysis_result)

                # 패턴 정보 저장
                self._save_pattern_info_to_candidate(candidate, candle_analysis_result)
            else:
                # 패턴 감지 실패 시 기본 설정
                target_price, stop_loss_price, trailing_stop_pct, max_holding_hours, position_size_pct, risk_score, source_info = \
                    self._calculate_default_risk_settings(entry_price, current_price_float)

            # RiskManagement 객체 생성
            entry_quantity = candidate.performance.entry_quantity or 0
            candidate.risk_management = RiskManagement(
                position_size_pct=position_size_pct,
                position_amount=int(entry_price * entry_quantity),
                stop_loss_price=stop_loss_price,
                target_price=target_price,
                trailing_stop_pct=trailing_stop_pct,
                max_holding_hours=max_holding_hours,
                risk_score=risk_score
            )

            # 메타데이터에 설정 출처 저장
            candidate.metadata['risk_management_source'] = source_info

        except Exception as e:
            logger.error(f"리스크 관리 설정 오류: {e}")

    def _calculate_pattern_based_risk_settings(self, entry_price: float, current_price: float,
                                             candle_analysis_result: Dict) -> Tuple[float, float, float, int, float, int, str]:
        """패턴 기반 리스크 설정 계산"""
        try:
            patterns = candle_analysis_result['patterns']
            strongest_pattern = candle_analysis_result['strongest_pattern']

            logger.info(f"🔄 실시간 캔들 패턴 감지: {strongest_pattern['type']} (강도: {strongest_pattern['strength']})")

            # 패턴별 설정 적용
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
            trailing_stop_pct = stop_pct * 0.6
            position_size_pct = 20.0
            risk_score = 100 - strongest_pattern['confidence'] * 100

            source_info = f"실시간패턴분석({strongest_pattern['type']})"

            return target_price, stop_loss_price, trailing_stop_pct, max_holding_hours, position_size_pct, risk_score, source_info

        except Exception as e:
            logger.error(f"패턴 기반 설정 계산 오류: {e}")
            return self._calculate_default_risk_settings(entry_price, current_price)

    def _calculate_default_risk_settings(self, entry_price: float, current_price: float) -> Tuple[float, float, float, int, float, int, str]:
        """기본 리스크 설정 계산"""
        try:
            logger.info("🔧 캔들 패턴 감지 실패 - 기본 설정 적용")

            # 기본 3% 목표가, 2% 손절가 설정
            target_price = entry_price * 1.03  # 3% 익절
            stop_loss_price = entry_price * 0.98  # 2% 손절

            # 현재가가 진입가보다 높다면 목표가 조정
            if current_price > entry_price:
                current_profit_rate = (current_price - entry_price) / entry_price
                if current_profit_rate >= 0.02:  # 이미 2% 이상 수익
                    target_price = current_price * 1.01  # 현재가에서 1% 더
                    stop_loss_price = current_price * 0.985  # 현재가에서 1.5% 하락

            trailing_stop_pct = 1.0
            max_holding_hours = 24
            position_size_pct = 20.0
            risk_score = 50
            source_info = "기본설정(패턴미감지)"

            return target_price, stop_loss_price, trailing_stop_pct, max_holding_hours, position_size_pct, risk_score, source_info

        except Exception as e:
            logger.error(f"기본 설정 계산 오류: {e}")
            # 최소한의 안전 설정
            return entry_price * 1.03, entry_price * 0.98, 1.0, 24, 20.0, 50, "오류시기본값"

    def _save_pattern_info_to_candidate(self, candidate: CandleTradeCandidate, candle_analysis_result: Dict):
        """패턴 정보를 candidate에 저장"""
        try:
            patterns = candle_analysis_result['patterns']
            strongest_pattern = candle_analysis_result['strongest_pattern']

            # 메타데이터 저장
            candidate.metadata['original_pattern_type'] = strongest_pattern['type']
            candidate.metadata['original_pattern_strength'] = strongest_pattern['strength']
            candidate.metadata['pattern_confidence'] = strongest_pattern['confidence']

            # 감지된 패턴 정보 추가
            for pattern in patterns:
                candidate.add_pattern(pattern)

        except Exception as e:
            logger.error(f"패턴 정보 저장 오류: {e}")

    def _setup_holding_metadata(self, candidate: CandleTradeCandidate, candle_analysis_result: Optional[Dict]):
        """보유 종목 메타데이터 설정"""
        try:
            # 기본 메타데이터
            candidate.metadata['is_existing_holding'] = True

            # 진입 출처 설정
            if candle_analysis_result and candle_analysis_result.get('patterns_detected'):
                candidate.metadata['original_entry_source'] = 'realtime_pattern_analysis'
            else:
                candidate.metadata['original_entry_source'] = 'manual_or_app_purchase'

        except Exception as e:
            logger.error(f"메타데이터 설정 오류: {e}")

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
                    await self._periodic_signal_evaluation()

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

    # ========== 매매 신호 생성 ==========

    def _generate_trade_signal(self, candidate: CandleTradeCandidate,
                             patterns: List[CandlePatternInfo]) -> Tuple[TradeSignal, int]:
        """패턴 기반 매매 신호 생성"""
        try:
            if not patterns:
                return TradeSignal.HOLD, 0

            # 가장 강한 패턴 기준
            primary_pattern = max(patterns, key=lambda p: p.strength)

            # 패턴별 신호 맵핑
            bullish_patterns = {
                PatternType.HAMMER,
                PatternType.INVERTED_HAMMER,
                PatternType.BULLISH_ENGULFING,
                PatternType.MORNING_STAR,
                PatternType.RISING_THREE_METHODS
            }

            bearish_patterns = {
                PatternType.BEARISH_ENGULFING,
                PatternType.EVENING_STAR,
                PatternType.FALLING_THREE_METHODS
            }

            neutral_patterns = {
                PatternType.DOJI
            }

            # 신호 결정
            if primary_pattern.pattern_type in bullish_patterns:
                if primary_pattern.confidence >= 0.85 and primary_pattern.strength >= 90:
                    return TradeSignal.STRONG_BUY, primary_pattern.strength
                elif primary_pattern.confidence >= 0.70:
                    return TradeSignal.BUY, primary_pattern.strength
                else:
                    return TradeSignal.HOLD, primary_pattern.strength

            elif primary_pattern.pattern_type in bearish_patterns:
                if primary_pattern.confidence >= 0.85:
                    return TradeSignal.STRONG_SELL, primary_pattern.strength
                else:
                    return TradeSignal.SELL, primary_pattern.strength

            elif primary_pattern.pattern_type in neutral_patterns:
                return TradeSignal.HOLD, primary_pattern.strength

            else:
                return TradeSignal.HOLD, 0

        except Exception as e:
            logger.error(f"매매 신호 생성 오류: {e}")
            return TradeSignal.HOLD, 0

    async def _check_entry_conditions(self, candidate: CandleTradeCandidate,
                                    current_info: Dict, daily_data: Optional[pd.DataFrame] = None) -> EntryConditions:
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

                        # 2. 기술적 지표 조건 (RSI, MACD, 볼린저밴드 등) - 전달받은 daily_data 사용
            try:
                conditions.rsi_check = True  # 기본값
                conditions.technical_indicators = {}  # 🆕 기술적 지표 저장

                if daily_data is not None and not daily_data.empty and len(daily_data) >= 20:
                    from ..analysis.technical_indicators import TechnicalIndicators

                    # OHLCV 데이터 추출
                    ohlcv_data = []
                    for _, row in daily_data.iterrows():
                        try:
                            open_price = float(row.get('stck_oprc', 0))
                            high_price = float(row.get('stck_hgpr', 0))
                            low_price = float(row.get('stck_lwpr', 0))
                            close_price = float(row.get('stck_clpr', 0))
                            volume = int(row.get('acml_vol', 0))

                            if all(x > 0 for x in [open_price, high_price, low_price, close_price]):
                                ohlcv_data.append({
                                    'open': open_price,
                                    'high': high_price,
                                    'low': low_price,
                                    'close': close_price,
                                    'volume': volume
                                })
                        except (ValueError, TypeError):
                            continue

                    if len(ohlcv_data) >= 14:
                        close_prices = [x['close'] for x in ohlcv_data]
                        high_prices = [x['high'] for x in ohlcv_data]
                        low_prices = [x['low'] for x in ohlcv_data]
                        volumes = [x['volume'] for x in ohlcv_data]

                        # 🔥 1. RSI 계산 및 체크
                        rsi_values = TechnicalIndicators.calculate_rsi(close_prices)
                        current_rsi = rsi_values[-1] if rsi_values else 50.0
                        conditions.technical_indicators['rsi'] = current_rsi

                        # RSI 과매수 구간 (65 이상) 체크
                        conditions.rsi_check = current_rsi < 65  # 65 미만일 때 진입 허용
                        if not conditions.rsi_check:
                            conditions.fail_reasons.append(f"RSI 과매수 ({current_rsi:.1f})")

                        # 🔥 2. MACD 계산 및 추가 확인
                        try:
                            macd_line, macd_signal, macd_histogram = TechnicalIndicators.calculate_macd(close_prices)
                            if macd_line and macd_signal and macd_histogram:
                                current_macd = macd_line[-1]
                                current_signal = macd_signal[-1]
                                current_histogram = macd_histogram[-1]

                                conditions.technical_indicators['macd'] = float(current_macd)
                                conditions.technical_indicators['macd_signal'] = float(current_signal)
                                conditions.technical_indicators['macd_histogram'] = float(current_histogram)

                                # MACD가 상승 전환 중이면 가점 (RSI 과매수여도 진입 고려)
                                if float(current_macd) > float(current_signal) and float(current_histogram) > 0.0:
                                    if not conditions.rsi_check and current_rsi < 75:  # RSI가 75 미만이면 MACD 우선
                                        conditions.rsi_check = True
                                        conditions.fail_reasons = [r for r in conditions.fail_reasons if 'RSI' not in r]
                                        logger.debug(f"📊 {candidate.stock_code} MACD 상승전환으로 RSI 조건 완화")
                        except Exception as e:
                            logger.debug(f"📊 {candidate.stock_code} MACD 계산 오류: {e}")

                        # 🔥 3. 볼린저 밴드 계산 (추가 확인)
                        try:
                            bb_upper, bb_middle, bb_lower = TechnicalIndicators.calculate_bollinger_bands(close_prices, 20, 2)
                            if bb_upper and bb_middle and bb_lower:
                                current_price = float(close_prices[-1])
                                bb_position = (current_price - float(bb_lower[-1])) / (float(bb_upper[-1]) - float(bb_lower[-1]))

                                conditions.technical_indicators['bb_position'] = bb_position

                                # 볼린저 밴드 하단 근처(20% 이하)면 RSI 과매수 조건 완화
                                if bb_position <= 0.2 and not conditions.rsi_check and current_rsi < 70:
                                    conditions.rsi_check = True
                                    conditions.fail_reasons = [r for r in conditions.fail_reasons if 'RSI' not in r]
                                    logger.debug(f"📊 {candidate.stock_code} 볼린저밴드 하단으로 RSI 조건 완화")
                        except Exception as e:
                            logger.debug(f"📊 {candidate.stock_code} 볼린저밴드 계산 오류: {e}")

                        logger.debug(f"📊 {candidate.stock_code} 기술지표 - RSI:{current_rsi:.1f}, "
                                   f"MACD:{conditions.technical_indicators.get('macd_histogram', 0):.3f}, "
                                   f"BB위치:{conditions.technical_indicators.get('bb_position', 0.5):.2f}")

                    else:
                        conditions.rsi_check = True  # 데이터 부족시 통과
                        logger.debug(f"📊 {candidate.stock_code} 기술지표 데이터 부족 - 통과")
                else:
                    conditions.rsi_check = True  # 데이터 없을 시 통과
                    logger.debug(f"📊 {candidate.stock_code} 일봉 데이터 없음 - 기술지표 체크 통과")

            except Exception as e:
                logger.error(f"기술지표 계산 오류 ({candidate.stock_code}): {e}")
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

    def _calculate_risk_management(self, candidate: CandleTradeCandidate) -> RiskManagement:
        """리스크 관리 설정 계산"""
        try:
            current_price = candidate.current_price

            # 패턴별 포지션 크기 조정
            if candidate.primary_pattern:
                pattern_type = candidate.primary_pattern.pattern_type
                confidence = candidate.primary_pattern.confidence

                # 강한 패턴일수록 큰 포지션
                if pattern_type in [PatternType.MORNING_STAR, PatternType.BULLISH_ENGULFING]:
                    base_position_pct = min(30, self.config['max_position_size_pct'])
                elif pattern_type in [PatternType.HAMMER, PatternType.INVERTED_HAMMER]:
                    base_position_pct = 20
                else:
                    base_position_pct = 15

                # 신뢰도에 따른 조정
                position_size_pct = base_position_pct * confidence
            else:
                position_size_pct = 10

            # 손절가/목표가 계산 - 패턴별 세부 설정 적용
            stop_loss_pct = self.config['default_stop_loss_pct']
            target_profit_pct = self.config['default_target_profit_pct']

            # 🆕 패턴별 목표 설정 적용
            if candidate.primary_pattern:
                pattern_name = candidate.primary_pattern.pattern_type.value.lower()
                pattern_config = self.config['pattern_targets'].get(pattern_name)

                if pattern_config:
                    target_profit_pct = pattern_config['target']
                    stop_loss_pct = pattern_config['stop']
                    logger.debug(f"📊 {candidate.stock_code} 패턴별 목표 적용: {pattern_name} - 목표:{target_profit_pct}%, 손절:{stop_loss_pct}%")
                else:
                    # 패턴 강도에 따른 기본 조정
                    if candidate.primary_pattern.pattern_type == PatternType.MORNING_STAR:
                        target_profit_pct = 2.5  # 샛별형은 강한 반전 신호
                        stop_loss_pct = 1.5
                    elif candidate.primary_pattern.pattern_type == PatternType.BULLISH_ENGULFING:
                        target_profit_pct = 2.0  # 장악형도 강함
                        stop_loss_pct = 1.5
                    elif candidate.primary_pattern.pattern_type == PatternType.HAMMER:
                        target_profit_pct = 1.5  # 망치형은 보수적
                        stop_loss_pct = 1.5

            stop_loss_price = current_price * (1 - stop_loss_pct / 100)
            target_price = current_price * (1 + target_profit_pct / 100)

            # 추적 손절 설정
            trailing_stop_pct = stop_loss_pct * 0.6  # 손절의 60% 수준

            # 최대 보유 시간 (패턴별 조정)
            max_holding_hours = self.config['max_holding_hours']
            if candidate.primary_pattern:
                pattern_name = candidate.primary_pattern.pattern_type.value.lower()
                pattern_config = self.config['pattern_targets'].get(pattern_name)

                if pattern_config and 'max_hours' in pattern_config:
                    max_holding_hours = pattern_config['max_hours']
                    logger.debug(f"📊 {candidate.stock_code} 패턴별 보유 시간 적용: {pattern_name} - {max_holding_hours}시간")
                else:
                    # 패턴별 기본 조정 (백업)
                    if candidate.primary_pattern.pattern_type in [PatternType.RISING_THREE_METHODS]:
                        max_holding_hours = 12  # 추세 지속 패턴은 길게
                    elif candidate.primary_pattern.pattern_type == PatternType.MORNING_STAR:
                        max_holding_hours = 8   # 샛별형은 강력한 패턴
                    elif candidate.primary_pattern.pattern_type in [PatternType.HAMMER, PatternType.INVERTED_HAMMER]:
                        max_holding_hours = 4   # 망치형은 짧게
                    elif candidate.primary_pattern.pattern_type == PatternType.DOJI:
                        max_holding_hours = 2   # 도지는 매우 짧게

            # 위험도 점수 계산
            risk_score = self._calculate_risk_score({'stck_prpr': current_price})

            return RiskManagement(
                position_size_pct=position_size_pct,
                position_amount=0,  # 실제 투자금액은 진입시 계산
                stop_loss_price=stop_loss_price,
                target_price=target_price,
                trailing_stop_pct=trailing_stop_pct,
                max_holding_hours=max_holding_hours,
                risk_score=risk_score
            )

        except Exception as e:
            logger.error(f"리스크 관리 계산 오류: {e}")
            return RiskManagement(0, 0, 0, 0, 0, 8, 100)

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

# _update_trailing_stop, _analyze_realtime_pattern_changes, _get_pattern_strength_tier,
# _calculate_profit_based_adjustments, _calculate_trend_based_adjustments 메서드는 PositionManager로 이동됨

# _apply_dynamic_adjustments, _get_pattern_tier_targets, _fallback_trailing_stop 메서드는 PositionManager로 이동됨

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

    # ========== 🆕 주기적 신호 재평가 시스템 ==========

    async def _periodic_signal_evaluation(self):
        """🔄 모든 _all_stocks 종목에 대한 주기적 신호 재평가"""
        try:
            if not self._all_stocks:
                return

            logger.debug(f"🔄 주기적 신호 재평가 시작: {len(self._all_stocks)}개 종목")

            # 종목들을 상태별로 분류하여 처리
            watching_stocks = []
            entered_stocks = []

            for stock_code, candidate in self._all_stocks.items():
                if candidate.status == CandleStatus.WATCHING or candidate.status == CandleStatus.BUY_READY:
                    watching_stocks.append(candidate)
                elif candidate.status == CandleStatus.ENTERED or candidate.status == CandleStatus.SELL_READY:
                    entered_stocks.append(candidate)

            # 배치 처리로 API 호출 최적화
            updated_count = 0

            # 1. 관찰 중인 종목들 재평가 (우선순위 높음)
            if watching_stocks:
                updated_count += await self._batch_evaluate_watching_stocks(watching_stocks)

            # 2. 진입한 종목들 재평가 (매도 신호 중심)
            if entered_stocks:
                updated_count += await self._batch_evaluate_entered_stocks(entered_stocks)

            if updated_count > 0:
                logger.info(f"🔄 신호 재평가 완료: {updated_count}개 종목 업데이트")

        except Exception as e:
            logger.error(f"주기적 신호 재평가 오류: {e}")

    async def _batch_evaluate_watching_stocks(self, candidates: List[CandleTradeCandidate]) -> int:
        """관찰 중인 종목들 배치 재평가"""
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
                            candidate.entry_priority = self._calculate_entry_priority(candidate)

                            # stock_manager 업데이트
                            self.stock_manager.update_candidate(candidate)

                            logger.info(f"🔄 {candidate.stock_code} 신호 업데이트: "
                                       f"{old_signal.value} → {candidate.trade_signal.value} "
                                       f"(강도:{candidate.signal_strength})")
                            updated_count += 1

                    except Exception as e:
                        logger.debug(f"종목 재평가 오류 ({candidate.stock_code}): {e}")
                        continue

                # API 호출 간격 조절
                if i + batch_size < len(candidates):
                    await asyncio.sleep(0.5)  # 0.5초 대기

            return updated_count

        except Exception as e:
            logger.error(f"관찰 종목 배치 재평가 오류: {e}")
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
