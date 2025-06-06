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
        """특정 시장에서 패턴 스캔 - 배치 처리 방식"""
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

            # 2. 배치 처리로 종목 분석 (5개씩 병렬 처리)
            pattern_found_count = 0
            batch_size = 5  # 배치 크기 설정

            # 배치 단위로 종목들을 그룹화
            for batch_start in range(0, len(unique_candidates), batch_size):
                batch_end = min(batch_start + batch_size, len(unique_candidates))
                batch_stocks = unique_candidates[batch_start:batch_end]

                logger.debug(f"📊 배치 처리: {batch_start+1}-{batch_end}/{len(unique_candidates)} "
                           f"종목 ({len(batch_stocks)}개)")

                # 배치 내 종목들을 병렬로 처리
                batch_results = await self._process_stock_batch(batch_stocks, market_name)

                # 성공적으로 패턴이 감지된 종목들을 스톡 매니저에 추가
                for candidate in batch_results:
                    if candidate and candidate.detected_patterns:
                        if self.stock_manager.add_candidate(candidate):
                            pattern_found_count += 1

                # 배치 간 간격 (API 부하 방지)
                if batch_end < len(unique_candidates):
                    await asyncio.sleep(0.5)  # 500ms 대기

            logger.info(f"🎯 {market_name} 패턴 감지: {pattern_found_count}개 종목")

        except Exception as e:
            logger.error(f"시장 {market} 스캔 오류: {e}")

    async def _process_stock_batch(self, stock_codes: List[str], market_name: str) -> List[Optional[CandleTradeCandidate]]:
        """주식 배치 병렬 처리"""
        import asyncio

        try:
            # 배치 내 모든 종목을 비동기로 동시 처리
            tasks = [
                self._analyze_stock_for_patterns(stock_code, market_name)
                for stock_code in stock_codes
            ]

            # 모든 작업이 완료될 때까지 대기
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # 성공한 결과만 필터링
            valid_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.debug(f"종목 {stock_codes[i]} 분석 실패: {result}")
                    valid_results.append(None)
                else:
                    valid_results.append(result)

            return valid_results

        except Exception as e:
            logger.error(f"배치 처리 오류: {e}")
            return [None] * len(stock_codes)

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

            # 🆕 매매 신호 생성 및 설정
            trade_signal, signal_strength = self._generate_trade_signal(candidate, pattern_result)
            candidate.trade_signal = trade_signal
            candidate.signal_strength = signal_strength
            candidate.signal_updated_at = datetime.now()

            # 🆕 진입 우선순위 계산
            candidate.entry_priority = self._calculate_entry_priority(candidate)

            # 🆕 리스크 관리 설정
            candidate.risk_management = self._calculate_risk_management(candidate)

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
                trade_signal, signal_strength = self._generate_trade_signal(candidate, candidate.detected_patterns)

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
                if self._should_exit_position(candidate):
                    await self._execute_exit(candidate, candidate.current_price, "패턴변화")

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
        """진입 기회 평가 및 매수 실행"""
        try:
            # 1단계: 기본 매수 후보들 조회 (패턴 기반)
            potential_candidates = self.stock_manager.get_stocks_by_signal(TradeSignal.STRONG_BUY)
            potential_candidates.extend(self.stock_manager.get_stocks_by_signal(TradeSignal.BUY))

            if not potential_candidates:
                logger.debug("🔍 매수 신호 종목 없음")
                return

            logger.info(f"🎯 매수 신호 종목: {len(potential_candidates)}개 - 진입 조건 재평가 시작")

            # 2단계: 각 후보에 대해 실시간 진입 조건 체크 (5개씩 병렬 처리)
            validated_candidates : List[CandleTradeCandidate] = []
            batch_size = 5

            # 5개씩 배치로 나누어 병렬 처리
            for batch_start in range(0, len(potential_candidates), batch_size):
                batch_end = min(batch_start + batch_size, len(potential_candidates))
                batch_candidates = potential_candidates[batch_start:batch_end]

                # 배치 내 후보들을 병렬로 처리
                batch_results = await self._process_entry_validation_batch(batch_candidates)

                # 검증 결과를 validated_candidates에 추가
                for candidate, validation_result in zip(batch_candidates, batch_results):
                    if validation_result.get('passed', False):
                        validated_candidates.append(candidate)

                # 배치 간 간격 (API 부하 방지)
                if batch_end < len(potential_candidates):
                    await asyncio.sleep(0.3)

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

                    # 🆕 실제 매수 실행
                    success = await self._execute_entry(candidate, entry_params)
                    if success:
                        logger.info(f"✅ {candidate.stock_code} 매수 성공")
                    else:
                        logger.warning(f"❌ {candidate.stock_code} 매수 실패")

                except Exception as e:
                    logger.error(f"❌ {candidate.stock_code} 매수 실행 오류: {e}")

        except Exception as e:
            logger.error(f"❌ 진입 기회 평가 오류: {e}")

    async def _execute_entry(self, candidate: CandleTradeCandidate, entry_params: Dict) -> bool:
        """실제 매수 진입 실행 (간소화된 버전)"""
        try:
            # 매개변수 추출
            current_price = entry_params['current_price']
            position_amount = entry_params['position_amount']
            quantity = entry_params['quantity']
            signal = entry_params['signal']

            # 실제 매수 주문 실행 (TradeExecutor 사용)
            if hasattr(self, 'trade_executor') and self.trade_executor:
                try:
                    result = self.trade_executor.execute_buy_signal(signal)
                    if not result.success:
                        logger.error(f"❌ 매수 주문 실패: {candidate.stock_code} - {result.error_message}")
                        return False
                    logger.info(f"✅ 실제 매수 주문 성공: {candidate.stock_code} (주문번호: {result.order_no})")
                except Exception as e:
                    logger.error(f"❌ 매수 주문 실행 오류: {candidate.stock_code} - {e}")
                    return False
            else:
                # TradeExecutor가 없는 경우 로그만 출력 (테스트 모드)
                logger.info(f"📈 매수 주문 (테스트): {candidate.stock_code} {quantity}주 {current_price:,.0f}원 "
                           f"(총 {position_amount:,.0f}원)")

            # 포지션 진입 기록
            quantity_int = int(quantity) if isinstance(quantity, float) else quantity
            candidate.enter_position(current_price, quantity_int)
            candidate.risk_management.position_amount = position_amount

            # 🆕 _all_stocks 상태 업데이트 (WATCHING → ENTERED)
            if candidate.stock_code in self._all_stocks:
                self._all_stocks[candidate.stock_code].status = CandleStatus.ENTERED
                self._all_stocks[candidate.stock_code].enter_position(current_price, quantity_int)
                logger.debug(f"🔄 {candidate.stock_code} _all_stocks 상태 업데이트: → ENTERED")

            # 🆕 캔들 전략 매수 정보 DB 저장 (프로그램 재시작 시 복원용)
            try:
                if self.trade_db:
                    # 캔들 전략 데이터 준비
                    candle_strategy_data = {
                        'stock_code': candidate.stock_code,
                        'stock_name': candidate.stock_name,
                        'entry_price': current_price,
                        'entry_quantity': quantity_int,
                        'position_amount': position_amount,
                        'target_price': candidate.risk_management.target_price,
                        'stop_loss_price': candidate.risk_management.stop_loss_price,
                        'trailing_stop_pct': candidate.risk_management.trailing_stop_pct,
                        'max_holding_hours': candidate.risk_management.max_holding_hours,
                        'pattern_type': str(candidate.detected_patterns[0].pattern_type) if candidate.detected_patterns else 'unknown',
                        'pattern_strength': candidate.detected_patterns[0].strength if candidate.detected_patterns else 0,
                        'pattern_confidence': candidate.detected_patterns[0].confidence if candidate.detected_patterns else 0.0,
                        'signal_strength': candidate.signal_strength,
                        'entry_priority': candidate.entry_priority,
                        'strategy_config': {
                            'position_size_pct': candidate.risk_management.position_size_pct,
                            'risk_score': candidate.risk_management.risk_score,
                            'pattern_description': candidate.detected_patterns[0].description if candidate.detected_patterns else '',
                            'market_type': candidate.market_type,
                            'created_at': candidate.created_at.isoformat() if candidate.created_at else None
                        }
                    }

                    # DB에 캔들 전략 매수 기록 저장 (새로운 함수 사용)
                    saved_id = await self._save_candle_position_to_db(candle_strategy_data)
                    if saved_id:
                        candidate.metadata['db_position_id'] = saved_id
                        logger.info(f"📚 {candidate.stock_code} 캔들 전략 정보 DB 저장 완료 (ID: {saved_id})")
                    else:
                        logger.warning(f"⚠️ {candidate.stock_code} 캔들 전략 정보 DB 저장 실패")

            except Exception as db_error:
                logger.warning(f"⚠️ {candidate.stock_code} 캔들 전략 DB 저장 오류: {db_error}")
                # DB 저장 실패해도 거래는 계속 진행

            # 상태 업데이트
            self.stock_manager.update_candidate(candidate)

            # 통계 업데이트
            self.daily_stats['trades_count'] += 1

            return True

        except Exception as e:
            logger.error(f"❌ 매수 진입 실행 오류 ({candidate.stock_code}): {e}")
            return False

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
            #    return

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

            # 🔄 추적 손절 항상 실행 (수익시)
            if pnl_pct > 2:  # 2% 이상 수익시 추적 손절 활성화
                self._update_trailing_stop(position, current_price)

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

            # 🔚 청산 실행 (거래 시간 내에서만)
            if should_exit and exit_reason:
                success = await self._execute_exit(position, current_price, exit_reason)
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
                # 수동/앱 매수 종목: 큰 수익/손실 허용 (🎯 10% 목표, 5% 손절)
                logger.debug(f"📊 {position.stock_code} 수동 매수 종목 - 기본 설정 적용")
                return 2.0, 3.0, 24, False

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

    async def _execute_exit(self, position: CandleTradeCandidate, exit_price: float, reason: str) -> bool:
        """매도 청산 실행"""
        try:
            # 🕐 거래 시간 재확인 (매도 실행 직전 체크)
            current_time = datetime.now().time()
            trading_start = datetime.strptime(self.config['trading_start_time'], '%H:%M').time()
            trading_end = datetime.strptime(self.config['trading_end_time'], '%H:%M').time()

            is_trading_time = trading_start <= current_time <= trading_end
            if not is_trading_time:
                logger.warning(f"⏰ {position.stock_code} 거래 시간 외 매도 차단 - {reason}")
                logger.info(f"현재 시간: {current_time}, 거래 시간: {trading_start} ~ {trading_end}")
                return False

            # 🔍 실제 보유 여부 확인 (매도 전 필수 체크)
            try:
                from ..api.kis_market_api import get_account_balance
                account_info = get_account_balance()

                if account_info and 'stocks' in account_info:
                    # 실제 보유 종목에서 해당 종목 찾기
                    actual_holding = None
                    for stock in account_info['stocks']:
                        if stock.get('stock_code') == position.stock_code:
                            actual_holding = stock
                            break

                    if not actual_holding:
                        logger.warning(f"⚠️ {position.stock_code} 실제 보유하지 않는 종목 - 매도 취소")
                        return False

                    actual_quantity = actual_holding.get('quantity', 0)
                    if actual_quantity <= 0:
                        logger.warning(f"⚠️ {position.stock_code} 실제 보유 수량 없음 ({actual_quantity}주) - 매도 취소")
                        return False

                    # 매도할 수량을 실제 보유 수량으로 조정
                    quantity = min(position.performance.entry_quantity or 0, actual_quantity)
                    logger.info(f"✅ {position.stock_code} 보유 확인: 시스템{position.performance.entry_quantity}주 → 실제{actual_quantity}주 → 매도{quantity}주")
                else:
                    logger.warning(f"⚠️ {position.stock_code} 계좌 정보 조회 실패 - 매도 진행")
                    quantity = position.performance.entry_quantity

            except Exception as e:
                logger.warning(f"⚠️ {position.stock_code} 보유 확인 오류: {e} - 기존 수량으로 진행")
                quantity = position.performance.entry_quantity

            if not quantity or quantity <= 0:
                logger.warning(f"❌ {position.stock_code} 매도할 수량 없음 ({quantity}주)")
                return False

            # 🆕 안전한 매도가 계산 (현재가 직접 사용 금지)
            safe_sell_price = self._calculate_safe_sell_price(exit_price, reason)

            # 매도 신호 생성
            signal = {
                'stock_code': position.stock_code,
                'action': 'sell',
                'strategy': 'candle_pattern',
                'price': safe_sell_price,  # 🎯 계산된 안전한 매도가 사용
                'quantity': quantity,
                'total_amount': int(safe_sell_price * quantity),
                'reason': reason,
                'pattern_type': str(position.detected_patterns[0].pattern_type) if position.detected_patterns else 'unknown',
                'pre_validated': True  # 캔들 시스템에서 이미 검증 완료
            }

            # 실제 매도 주문 실행 (TradeExecutor 사용)
            if hasattr(self, 'trade_executor') and self.trade_executor:
                try:
                    result = self.trade_executor.execute_sell_signal(signal)
                    if not result.success:
                        logger.error(f"❌ 매도 주문 실패: {position.stock_code} - {result.error_message}")
                        return False
                    logger.info(f"✅ 실제 매도 주문 성공: {position.stock_code} "
                               f"현재가{exit_price:,.0f}원 → 주문가{safe_sell_price:,.0f}원 "
                               f"(주문번호: {result.order_no})")
                except Exception as e:
                    logger.error(f"❌ 매도 주문 실행 오류: {position.stock_code} - {e}")
                    return False
            else:
                # TradeExecutor가 없는 경우 로그만 출력 (테스트 모드)
                logger.info(f"📉 매도 주문 (테스트): {position.stock_code} {quantity}주 "
                           f"현재가{exit_price:,.0f}원 → 주문가{safe_sell_price:,.0f}원")

            # 포지션 청산 기록 (원래 exit_price로 기록)
            position.exit_position(exit_price, reason)

            # 🆕 _all_stocks 상태 업데이트 (ENTERED → EXITED)
            if position.stock_code in self._all_stocks:
                self._all_stocks[position.stock_code].status = CandleStatus.EXITED
                self._all_stocks[position.stock_code].exit_position(exit_price, reason)
                logger.debug(f"🔄 {position.stock_code} _all_stocks 상태 업데이트: → EXITED")

            # 🆕 캔들 전략 매도 정보 DB 업데이트 (프로그램 재시작 시 복원 방지)
            try:
                if self.trade_db and 'db_position_id' in position.metadata:
                    # 매도 정보 업데이트 (추후 DB 스키마에 따라 구현)
                    exit_data = {
                        'exit_price': exit_price,
                        'exit_reason': reason,
                        'realized_pnl': position.performance.realized_pnl or 0.0,
                        'exit_time': datetime.now().isoformat(),
                        'holding_duration': str(datetime.now() - position.performance.entry_time) if position.performance.entry_time else None
                    }

                    # 임시로 로그만 출력 (실제 DB 업데이트는 스키마 확인 후 구현)
                    logger.info(f"📚 {position.stock_code} 캔들 전략 매도 기록 업데이트 대상 (DB ID: {position.metadata['db_position_id']})")
                    logger.debug(f"매도 정보: {exit_data}")

                elif position.metadata.get('original_entry_source') == 'candle_strategy':
                    logger.debug(f"📚 {position.stock_code} 캔들 전략 종목이나 DB ID 없음 - 매도 기록 스킵")

            except Exception as db_error:
                logger.warning(f"⚠️ {position.stock_code} 캔들 전략 매도 DB 업데이트 오류: {db_error}")
                # DB 업데이트 실패해도 거래는 계속 진행

            # 일일 통계 업데이트
            if position.performance.realized_pnl:
                self.daily_stats['total_profit_loss'] += position.performance.realized_pnl

                if position.performance.realized_pnl > 0:
                    self.daily_stats['successful_trades'] += 1
                else:
                    self.daily_stats['failed_trades'] += 1

            return True

        except Exception as e:
            logger.error(f"매도 청산 실행 오류 ({position.stock_code}): {e}")
            return False

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

    def _update_trailing_stop(self, position: CandleTradeCandidate, current_price: float):
        """🔄 패턴 기반 동적 목표/손절 조정 시스템 (개선된 버전)"""
        try:
            # 🆕 OHLCV 데이터 한 번만 조회하여 재사용
            from ..api.kis_market_api import get_inquire_daily_itemchartprice
            ohlcv_data = get_inquire_daily_itemchartprice(
                output_dv="2",
                itm_no=position.stock_code,
                period_code="D",
                adj_prc="1"
            )

            # 🆕 1단계: 실시간 캔들 패턴 재분석 (OHLCV 데이터 전달)
            pattern_update = self._analyze_realtime_pattern_changes(position.stock_code, current_price, ohlcv_data)

            # 🆕 2단계: 수익률 기반 동적 조정
            profit_based_update = self._calculate_profit_based_adjustments(position, current_price)

            # 🆕 3단계: 추세 강도 기반 조정 (OHLCV 데이터 전달)
            trend_based_update = self._calculate_trend_based_adjustments(position, current_price, ohlcv_data)

            # 🆕 4단계: 종합 판단 및 업데이트
            self._apply_dynamic_adjustments(position, current_price, pattern_update, profit_based_update, trend_based_update)

        except Exception as e:
            logger.error(f"동적 목표/손절 조정 오류 ({position.stock_code}): {e}")
            # 기존 방식으로 폴백
            self._fallback_trailing_stop(position, current_price)

    def _analyze_realtime_pattern_changes(self, stock_code: str, current_price: float, ohlcv_data: Optional[Any] = None) -> Dict:
        """🔄 실시간 캔들 패턴 변화 분석"""
        try:
            # OHLCV 데이터가 전달되지 않은 경우에만 새로 조회
            if ohlcv_data is None:
                from ..api.kis_market_api import get_inquire_daily_itemchartprice
                ohlcv_data = get_inquire_daily_itemchartprice(
                    output_dv="2",
                    itm_no=stock_code,
                    period_code="D",
                    adj_prc="1"
                )

            if ohlcv_data is None or ohlcv_data.empty:
                return {'pattern_strength_changed': False, 'new_patterns': []}

            # 현재 패턴 분석
            current_patterns = self.pattern_detector.analyze_stock_patterns(stock_code, ohlcv_data)

            if not current_patterns:
                return {'pattern_strength_changed': False, 'new_patterns': []}

            # 가장 강한 패턴 선택
            strongest_pattern = max(current_patterns, key=lambda p: p.strength)

            # 패턴 강도 변화 분석
            pattern_strength_tier = self._get_pattern_strength_tier(strongest_pattern.strength)

            return {
                'pattern_strength_changed': True,
                'new_patterns': current_patterns,
                'strongest_pattern': strongest_pattern,
                'strength_tier': pattern_strength_tier,
                'pattern_type': strongest_pattern.pattern_type.value,
                'confidence': strongest_pattern.confidence
            }

        except Exception as e:
            logger.debug(f"실시간 패턴 분석 오류 ({stock_code}): {e}")
            return {'pattern_strength_changed': False, 'new_patterns': []}

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

    def _calculate_profit_based_adjustments(self, position: CandleTradeCandidate, current_price: float) -> Dict:
        """💰 수익률 기반 동적 조정 계산 (마이너스 상황 포함)"""
        try:
            if not position.performance.entry_price:
                return {'target_multiplier': 1.0, 'stop_tightening': 1.0}

            # 현재 수익률 계산
            pnl_pct = ((current_price - position.performance.entry_price) / position.performance.entry_price) * 100

            # 🆕 수익률별 동적 조정 (마이너스 구간 추가)
            if pnl_pct >= 5.0:
                # 5% 이상 수익: 목표 1.5배 확장, 손절 50% 강화
                return {'target_multiplier': 1.5, 'stop_tightening': 0.5, 'reason': '고수익구간'}
            elif pnl_pct >= 3.0:
                # 3% 이상 수익: 목표 1.3배 확장, 손절 70% 강화
                return {'target_multiplier': 1.3, 'stop_tightening': 0.7, 'reason': '수익구간'}
            elif pnl_pct >= 1.0:
                # 1% 이상 수익: 목표 1.1배 확장, 손절 80% 강화
                return {'target_multiplier': 1.1, 'stop_tightening': 0.8, 'reason': '소폭수익'}
            elif pnl_pct >= -1.0:
                # 소폭 마이너스(-1% 이내): 기본 설정 유지
                return {'target_multiplier': 1.0, 'stop_tightening': 1.0, 'reason': '소폭손실'}
            elif pnl_pct >= -3.0:
                # 🆕 중간 마이너스(-3% 이내): 패턴 강화시 기회 확대
                return {'target_multiplier': 1.0, 'stop_relaxation': 1.2, 'allow_lower_stop': True, 'reason': '중간손실_회복대기'}
            elif pnl_pct >= -5.0:
                # 🆕 큰 마이너스(-5% 이내): 강한 반전 패턴시에만 기다림
                return {'target_multiplier': 1.0, 'stop_relaxation': 1.5, 'allow_lower_stop': True, 'reason': '큰손실_반전대기'}
            else:
                # 🆕 매우 큰 마이너스(-5% 초과): 매우 강한 패턴에서만 추가 대기
                return {'target_multiplier': 1.0, 'stop_relaxation': 1.8, 'allow_lower_stop': True, 'reason': '심각손실_특수패턴대기'}

        except Exception as e:
            logger.debug(f"수익률 기반 조정 계산 오류: {e}")
            return {'target_multiplier': 1.0, 'stop_tightening': 1.0}

    def _calculate_trend_based_adjustments(self, position: CandleTradeCandidate, current_price: float, ohlcv_data: Optional[Any] = None) -> Dict:
        """📈 추세 강도 기반 조정 계산"""
        try:
            # OHLCV 데이터가 전달되지 않은 경우에만 새로 조회
            if ohlcv_data is None:
                from ..api.kis_market_api import get_inquire_daily_itemchartprice
                daily_data = get_inquire_daily_itemchartprice(
                    output_dv="2",
                    itm_no=position.stock_code,
                    period_code="D"
                )
            else:
                daily_data = ohlcv_data

            if daily_data is None or daily_data.empty or len(daily_data) < 5:
                return {'trend_strength': 'NEUTRAL', 'trend_multiplier': 1.0}

            # 최근 5일 종가 추출
            recent_closes = []
            for _, row in daily_data.head(5).iterrows():
                try:
                    close_price = float(row.get('stck_clpr', 0))
                    if close_price > 0:
                        recent_closes.append(close_price)
                except (ValueError, TypeError):
                    continue

            if len(recent_closes) < 3:
                return {'trend_strength': 'NEUTRAL', 'trend_multiplier': 1.0}

            # 추세 강도 계산 (최신가 vs 과거가 비교)
            trend_pct = ((recent_closes[0] - recent_closes[-1]) / recent_closes[-1]) * 100

            if trend_pct >= 10:
                return {'trend_strength': 'VERY_STRONG_UP', 'trend_multiplier': 1.4, 'reason': '강한상승추세'}
            elif trend_pct >= 5:
                return {'trend_strength': 'STRONG_UP', 'trend_multiplier': 1.2, 'reason': '상승추세'}
            elif trend_pct >= 2:
                return {'trend_strength': 'WEAK_UP', 'trend_multiplier': 1.1, 'reason': '약한상승'}
            elif trend_pct <= -5:
                return {'trend_strength': 'STRONG_DOWN', 'trend_multiplier': 0.8, 'reason': '하락추세'}
            else:
                return {'trend_strength': 'NEUTRAL', 'trend_multiplier': 1.0, 'reason': '중립'}

        except Exception as e:
            logger.debug(f"추세 분석 오류: {e}")
            return {'trend_strength': 'NEUTRAL', 'trend_multiplier': 1.0}

    def _apply_dynamic_adjustments(self, position: CandleTradeCandidate, current_price: float,
                                 pattern_update: Dict, profit_update: Dict, trend_update: Dict):
        """🎯 동적 조정 적용 (마이너스 상황 특수 로직 포함)"""
        try:
            entry_price = position.performance.entry_price
            if not entry_price:
                return

            # 🆕 현재 목표가/손절가 백업
            original_target = position.risk_management.target_price
            original_stop = position.risk_management.stop_loss_price

            # 🆕 1단계: 패턴 기반 기본 목표/손절 재계산
            if pattern_update.get('pattern_strength_changed'):
                new_target_pct, new_stop_pct = self._get_pattern_tier_targets(pattern_update['strength_tier'])
            else:
                # 기존 설정 유지를 위한 역계산
                new_target_pct = ((original_target - entry_price) / entry_price) * 100
                new_stop_pct = ((entry_price - original_stop) / entry_price) * 100

            # 🆕 2단계: 수익률 기반 조정 적용 (마이너스 로직 추가)
            target_multiplier = profit_update.get('target_multiplier', 1.0)

            # 마이너스 상황에서의 특수 처리
            if profit_update.get('stop_relaxation'):
                # 손절 완화 적용 (마이너스 상황)
                stop_relaxation = profit_update.get('stop_relaxation', 1.0)
                adjusted_stop_pct = new_stop_pct * stop_relaxation
                allow_lower_stop = profit_update.get('allow_lower_stop', False)
            else:
                # 기존 로직 (수익 상황)
                stop_tightening = profit_update.get('stop_tightening', 1.0)
                adjusted_stop_pct = new_stop_pct * stop_tightening
                allow_lower_stop = False

            adjusted_target_pct = new_target_pct * target_multiplier

            # 🆕 3단계: 추세 기반 조정 적용
            trend_multiplier = trend_update.get('trend_multiplier', 1.0)
            final_target_pct = adjusted_target_pct * trend_multiplier

            # 🆕 4단계: 새로운 목표가/손절가 계산
            new_target_price = entry_price * (1 + final_target_pct / 100)
            new_stop_price = entry_price * (1 - adjusted_stop_pct / 100)

            # 🆕 5단계: 패턴 강도 기반 마이너스 특수 조건 검사
            strong_reversal_pattern = False
            if pattern_update.get('pattern_strength_changed'):
                strongest_pattern_obj = pattern_update.get('strongest_pattern')
                pattern_tier = pattern_update.get('strength_tier', '')

                # CandlePatternInfo 객체에서 직접 속성 접근
                if strongest_pattern_obj:
                    pattern_strength = strongest_pattern_obj.strength

                    # 강한 반전 패턴 감지 (STRONG 이상)
                    if pattern_tier in ['ULTRA_STRONG', 'STRONG'] and pattern_strength >= 80:
                        strong_reversal_pattern = True

            # 🆕 6단계: 안전성 검증 및 적용 (마이너스 로직 추가)
            # 목표가 업데이트
            if new_target_price > original_target:
                position.risk_management.target_price = new_target_price
                target_updated = True
            else:
                target_updated = False

            # 🆕 손절가 업데이트 (마이너스 상황에서 조건부 하향 허용)
            if allow_lower_stop and strong_reversal_pattern:
                # 🎯 마이너스 + 강한 반전 패턴: 손절가 하향 조정 허용
                if new_stop_price != original_stop:  # 변경이 있을 때만
                    position.risk_management.stop_loss_price = new_stop_price
                    stop_updated = True
                    logger.info(f"🔄 {position.stock_code} 마이너스 특수조정: 강한 반전패턴으로 손절가 완화")
                else:
                    stop_updated = False
            else:
                # 기존 로직: 상향만 허용
                if new_stop_price > original_stop:
                    position.risk_management.stop_loss_price = new_stop_price
                    stop_updated = True
                else:
                    stop_updated = False

            # 🆕 6단계: 변경사항 로깅
            if target_updated or stop_updated:
                pnl_pct = ((current_price - entry_price) / entry_price) * 100

                logger.info(f"🔄 {position.stock_code} 동적 조정 적용 (수익률: {pnl_pct:+.1f}%):")

                if target_updated:
                    logger.info(f"   📈 목표가: {original_target:,.0f}원 → {new_target_price:,.0f}원 "
                               f"({((new_target_price - entry_price) / entry_price * 100):+.1f}%)")

                if stop_updated:
                    logger.info(f"   🛡️ 손절가: {original_stop:,.0f}원 → {new_stop_price:,.0f}원 "
                               f"({((entry_price - new_stop_price) / entry_price * 100):+.1f}%)")

                # 조정 사유 로깅
                reasons = []
                if pattern_update.get('pattern_strength_changed'):
                    reasons.append(f"패턴강도: {pattern_update['strength_tier']}")
                if profit_update.get('reason'):
                    reasons.append(f"수익: {profit_update['reason']}")
                if trend_update.get('reason'):
                    reasons.append(f"추세: {trend_update['reason']}")

                if reasons:
                    logger.info(f"   📋 조정사유: {', '.join(reasons)}")

        except Exception as e:
            logger.error(f"동적 조정 적용 오류 ({position.stock_code}): {e}")

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
                        analysis_result = await self._comprehensive_signal_analysis(candidate)

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
                    self._comprehensive_signal_analysis(candidate, focus_on_exit=True)
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

    async def _comprehensive_signal_analysis(self, candidate: CandleTradeCandidate, focus_on_exit: bool = False) -> Optional[Dict]:
        """🔍 다각도 종합 신호 분석"""
        try:
            stock_code = candidate.stock_code

            # 1. 최신 가격 정보 조회
            from ..api.kis_market_api import get_inquire_price
            current_data = get_inquire_price("J", stock_code)

            if current_data is None or current_data.empty:
                return None

            current_price = float(current_data.iloc[0].get('stck_prpr', 0))
            if current_price <= 0:
                return None

            # 가격 업데이트
            old_price = candidate.current_price
            candidate.update_price(current_price)

            # 🆕 OHLCV 데이터 한 번만 조회 (하위 함수들에서 공유 사용)
            from ..api.kis_market_api import get_inquire_daily_itemchartprice
            ohlcv_data = get_inquire_daily_itemchartprice(
                output_dv="2",
                itm_no=stock_code,
                period_code="D",
                adj_prc="1"
            )

            # 2. 📊 최신 캔들 패턴 재분석 (OHLCV 데이터 전달)
            pattern_signals = await self._analyze_current_patterns(stock_code, current_price, ohlcv_data)

            # 3. 📈 기술적 지표 분석 (OHLCV 데이터 전달)
            technical_signals = await self._analyze_technical_indicators(stock_code, current_price, ohlcv_data)

            # 4. ⏰ 시간 기반 조건 분석
            time_signals = self._analyze_time_conditions(candidate)

            # 5. 💰 리스크 조건 분석
            risk_signals = self._analyze_risk_conditions(candidate, current_price)

            # 6. 🎯 포지션 상태별 특화 분석
            if focus_on_exit:
                position_signals = self._analyze_exit_conditions(candidate, current_price, ohlcv_data)
            else:
                position_signals = self._analyze_entry_conditions_simple(candidate, current_price)

            # 7. 종합 신호 계산
            final_signal, signal_strength = self._calculate_comprehensive_signal(
                pattern_signals, technical_signals, time_signals,
                risk_signals, position_signals, focus_on_exit
            )

            return {
                'new_signal': final_signal,
                'signal_strength': signal_strength,
                'price_change_pct': ((current_price - old_price) / old_price * 100) if old_price > 0 else 0,
                'pattern_signals': pattern_signals,
                'technical_signals': technical_signals,
                'time_signals': time_signals,
                'risk_signals': risk_signals,
                'position_signals': position_signals,
                'analysis_time': datetime.now()
            }

        except Exception as e:
            logger.debug(f"종합 신호 분석 오류 ({candidate.stock_code}): {e}")
            return None

    async def _analyze_current_patterns(self, stock_code: str, current_price: float, ohlcv_data: Optional[Any]) -> Dict:
        """📊 최신 캔들 패턴 분석"""
        try:
            # 전달받은 OHLCV 데이터 사용
            if ohlcv_data is None or ohlcv_data.empty:
                return {'signal': 'neutral', 'strength': 0, 'patterns': []}

            # 패턴 감지
            pattern_result = self.pattern_detector.analyze_stock_patterns(stock_code, ohlcv_data)

            if not pattern_result:
                return {'signal': 'neutral', 'strength': 0, 'patterns': []}

            # 가장 강한 패턴 기준
            strongest_pattern = max(pattern_result, key=lambda p: p.strength)

            # 패턴 기반 신호 생성
            if strongest_pattern.pattern_type in [PatternType.HAMMER, PatternType.INVERTED_HAMMER,
                                                PatternType.BULLISH_ENGULFING, PatternType.MORNING_STAR]:
                signal = 'bullish'
            elif strongest_pattern.pattern_type in [PatternType.BEARISH_ENGULFING, PatternType.EVENING_STAR]:
                signal = 'bearish'
            else:
                signal = 'neutral'

            return {
                'signal': signal,
                'strength': strongest_pattern.strength,
                'confidence': strongest_pattern.confidence,
                'patterns': pattern_result,
                'primary_pattern': strongest_pattern.pattern_type.value
            }

        except Exception as e:
            logger.debug(f"패턴 분석 오류 ({stock_code}): {e}")
            return {'signal': 'neutral', 'strength': 0, 'patterns': []}

    async def _analyze_technical_indicators(self, stock_code: str, current_price: float, ohlcv_data: Optional[Any]) -> Dict:
        """📈 기술적 지표 분석"""
        try:
            # 전달받은 OHLCV 데이터 사용
            if ohlcv_data is None or ohlcv_data.empty or len(ohlcv_data) < 20:
                return {'signal': 'neutral', 'rsi': 50.0, 'trend': 'neutral'}

            # 종가 데이터 추출
            close_prices = []
            for _, row in ohlcv_data.head(20).iterrows():  # 최근 20일
                try:
                    close_price = float(row.get('stck_clpr', 0))
                    if close_price > 0:
                        close_prices.append(close_price)
                except (ValueError, TypeError):
                    continue

            if len(close_prices) < 14:
                return {'signal': 'neutral', 'rsi': 50.0, 'trend': 'neutral'}

            # RSI 계산
            from ..analysis.technical_indicators import TechnicalIndicators
            rsi_values = TechnicalIndicators.calculate_rsi(close_prices)
            current_rsi = rsi_values[-1] if rsi_values else 50.0

            # 이동평균 추세
            if len(close_prices) >= 5:
                ma_5 = sum(close_prices[:5]) / 5
                ma_20 = sum(close_prices[:20]) / 20 if len(close_prices) >= 20 else ma_5

                if current_price > ma_5 > ma_20:
                    trend = 'uptrend'
                elif current_price < ma_5 < ma_20:
                    trend = 'downtrend'
                else:
                    trend = 'neutral'
            else:
                trend = 'neutral'

            # 종합 신호
            if current_rsi < 30 and trend in ['uptrend', 'neutral']:
                signal = 'oversold_bullish'
            elif current_rsi > 70 and trend in ['downtrend', 'neutral']:
                signal = 'overbought_bearish'
            elif current_rsi < 30:
                signal = 'oversold'
            elif current_rsi > 70:
                signal = 'overbought'
            else:
                signal = 'neutral'

            return {
                'signal': signal,
                'rsi': current_rsi,
                'trend': trend,
                'ma_5': ma_5 if 'ma_5' in locals() else current_price,
                'ma_20': ma_20 if 'ma_20' in locals() else current_price
            }

        except Exception as e:
            logger.debug(f"기술적 지표 분석 오류 ({stock_code}): {e}")
            return {'signal': 'neutral', 'rsi': 50.0, 'trend': 'neutral'}

    def _analyze_time_conditions(self, candidate: CandleTradeCandidate) -> Dict:
        """⏰ 시간 기반 조건 분석"""
        try:
            current_time = datetime.now()

            # 거래 시간 체크
            trading_hours = self._is_trading_time()

            # 보유 시간 분석 (진입한 종목의 경우)
            holding_duration = None
            time_pressure = 'none'

            if candidate.status == CandleStatus.ENTERED and candidate.performance.entry_time:
                holding_duration = current_time - candidate.performance.entry_time
                max_holding = timedelta(hours=candidate.risk_management.max_holding_hours)

                if holding_duration >= max_holding * 0.8:  # 80% 경과
                    time_pressure = 'high'
                elif holding_duration >= max_holding * 0.5:  # 50% 경과
                    time_pressure = 'medium'
                else:
                    time_pressure = 'low'

            # 시간 기반 신호
            if not trading_hours:
                signal = 'closed_market'
            elif time_pressure == 'high':
                signal = 'time_exit'
            elif time_pressure == 'medium':
                signal = 'time_caution'
            else:
                signal = 'normal'

            return {
                'signal': signal,
                'trading_hours': trading_hours,
                'holding_duration': str(holding_duration) if holding_duration else None,
                'time_pressure': time_pressure
            }

        except Exception as e:
            logger.debug(f"시간 조건 분석 오류: {e}")
            return {'signal': 'normal', 'trading_hours': True, 'time_pressure': 'none'}

    def _analyze_risk_conditions(self, candidate: CandleTradeCandidate, current_price: float) -> Dict:
        """💰 리스크 조건 분석"""
        try:
            if not candidate.risk_management:
                return {'signal': 'neutral', 'risk_level': 'medium'}

            # 현재 수익률 계산
            if candidate.performance.entry_price:
                pnl_pct = ((current_price - candidate.performance.entry_price) / candidate.performance.entry_price) * 100
            else:
                pnl_pct = 0.0

            # 손절/익절 조건 체크
            target_price = candidate.risk_management.target_price
            stop_loss_price = candidate.risk_management.stop_loss_price

            if current_price >= target_price:
                signal = 'target_reached'
                risk_level = 'profit_secure'
            elif current_price <= stop_loss_price:
                signal = 'stop_loss'
                risk_level = 'high_loss'
            elif pnl_pct >= 3.0:  # 3% 수익 (사용자 요구사항)
                signal = 'profit_target'
                risk_level = 'secure_profit'
            elif pnl_pct <= -3.0:  # 3% 손실
                signal = 'loss_limit'
                risk_level = 'high_loss'
            elif pnl_pct >= 1.0:
                signal = 'profit_zone'
                risk_level = 'low'
            elif pnl_pct <= -1.0:
                signal = 'loss_zone'
                risk_level = 'medium'
            else:
                signal = 'neutral'
                risk_level = 'medium'

            return {
                'signal': signal,
                'risk_level': risk_level,
                'pnl_pct': pnl_pct,
                'target_distance': ((target_price - current_price) / current_price * 100) if target_price else 0,
                'stop_distance': ((current_price - stop_loss_price) / current_price * 100) if stop_loss_price else 0
            }

        except Exception as e:
            logger.debug(f"리스크 조건 분석 오류: {e}")
            return {'signal': 'neutral', 'risk_level': 'medium'}

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

    async def _process_entry_validation_batch(self, candidates: List[CandleTradeCandidate]) -> List[Dict]:
        """진입 조건 검증 배치 병렬 처리"""
        try:
            # 각 후보에 대해 검증 작업을 정의
            validation_tasks = [
                self._validate_single_entry_candidate(candidate)
                for candidate in candidates
            ]

            # 병렬로 실행
            results = await asyncio.gather(*validation_tasks, return_exceptions=True)

            # 결과 정리
            processed_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.debug(f"진입 검증 오류 ({candidates[i].stock_code}): {result}")
                    processed_results.append({'passed': False, 'error': str(result)})
                else:
                    processed_results.append(result)

            return processed_results

        except Exception as e:
            logger.error(f"진입 검증 배치 처리 오류: {e}")
            return [{'passed': False, 'error': str(e)}] * len(candidates)

    async def _validate_single_entry_candidate(self, candidate: CandleTradeCandidate) -> Dict:
        """개별 후보 진입 조건 검증"""
        try:
            # 최신 가격 정보 조회
            from ..api.kis_market_api import get_inquire_price
            current_data = get_inquire_price("J", candidate.stock_code)

            if current_data is None or current_data.empty:
                logger.debug(f"⏸️ {candidate.stock_code} 현재가 조회 실패")
                return {'passed': False, 'reason': '현재가 조회 실패'}

            current_price = float(current_data.iloc[0].get('stck_prpr', 0))
            if current_price <= 0:
                return {'passed': False, 'reason': '가격 정보 없음'}

            # 가격 업데이트
            candidate.update_price(current_price)

            # 일봉 데이터 조회 (RSI 계산용)
            daily_data = None
            try:
                from ..api.kis_market_api import get_inquire_daily_itemchartprice
                daily_data = get_inquire_daily_itemchartprice(
                    output_dv="2",
                    itm_no=candidate.stock_code
                )
                logger.debug(f"📊 {candidate.stock_code} 일봉 데이터 조회 완료: {len(daily_data) if daily_data is not None else 0}건")
            except Exception as e:
                logger.warning(f"📊 {candidate.stock_code} 일봉 데이터 조회 실패: {e}")

            # 실시간 진입 조건 재평가
            entry_conditions = await self._check_entry_conditions(
                candidate,
                current_data.iloc[0].to_dict(),
                daily_data
            )
            candidate.entry_conditions = entry_conditions

            # 결과 처리
            if entry_conditions.overall_passed:
                candidate.status = CandleStatus.BUY_READY
                logger.info(f"✅ {candidate.stock_code} 진입 조건 통과 - 매수 준비")

                # stock_manager 업데이트
                self.stock_manager.update_candidate(candidate)

                return {'passed': True, 'candidate': candidate}
            else:
                candidate.status = CandleStatus.WATCHING
                fail_reasons = ', '.join(entry_conditions.fail_reasons)
                logger.debug(f"⏸️ {candidate.stock_code} 진입 조건 미통과: {fail_reasons}")

                # stock_manager 업데이트
                self.stock_manager.update_candidate(candidate)

                return {'passed': False, 'reason': fail_reasons}

        except Exception as e:
            logger.error(f"❌ {candidate.stock_code} 진입 조건 체크 오류: {e}")
            return {'passed': False, 'error': str(e)}
