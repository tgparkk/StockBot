"""
캔들 기반 매매 전략 통합 관리자
"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from utils.logger import setup_logger

from .candle_trade_candidate import (
    CandleTradeCandidate, CandleStatus, TradeSignal, PatternType,
    CandlePatternInfo, EntryConditions, RiskManagement, PerformanceTracking
)
from .candle_stock_manager import CandleStockManager
from .candle_pattern_detector import CandlePatternDetector

logger = setup_logger(__name__)


class CandleTradeManager:
    """캔들 기반 매매 전략 통합 관리자"""

    def __init__(self, kis_api_manager, data_manager, trade_executor, websocket_manager):
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
        self.stock_manager = CandleStockManager(max_watch_stocks=100, max_positions=10)
        self.pattern_detector = CandlePatternDetector()

        # 내부 상태
        self._all_stocks: Dict[str, CandleTradeCandidate] = {}
        self._existing_holdings: Dict[str, Dict] = {}
        self._last_scan_time: Optional[datetime] = None  # datetime 타입으로 명시
        self._scan_interval = 60  # 1분
        self.is_running = False

        # ========== 설정값 ==========
        self.config = {
            # 기본 스캔 설정
            'scan_interval_seconds': 60,      # 1분마다 스캔
            'max_positions': 10,                # 최대 포지션 수
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
            'default_stop_loss_pct': 2,        # 기본 손절 비율 (%) - 2%로 조정
            'default_target_profit_pct': 3,    # 기본 목표 수익률 (%) - 3%로 조정 (현실적)
            'max_holding_hours': 6,            # 최대 보유 시간 - 6시간으로 조정 (단기 트레이딩)

            # 패턴별 세부 목표 설정 (더 현실적으로)
            'pattern_targets': {
                'hammer': {'target': 1.5, 'stop': 1.5, 'max_hours': 4},           # 망치형: 4시간
                'inverted_hammer': {'target': 1.2, 'stop': 1.5, 'max_hours': 4},  # 역망치형: 4시간
                'bullish_engulfing': {'target': 2.0, 'stop': 1.5, 'max_hours': 6}, # 장악형: 6시간
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

        # 🆕 웹소켓 구독 상태 관리 (중복 구독 방지)
        self.subscribed_stocks = set()  # 현재 구독 중인 종목 코드들

        logger.info("✅ CandleTradeManager 초기화 완료")

    # ==========================================
    # 🆕 기존 보유 종목 웹소켓 구독 관리
    # ==========================================

    async def setup_existing_holdings_monitoring(self):
        """기존 보유 종목 웹소켓 모니터링 설정 (캔들 시스템 통합)"""
        try:
            logger.info("📊 기존 보유 종목 웹소켓 모니터링 설정 시작")

            # KIS API로 직접 보유 종목 조회
            from ..api.kis_market_api import get_account_balance
            account_balance = get_account_balance()

            if not account_balance or account_balance['total_stocks'] == 0:
                logger.info("📊 보유 종목이 없습니다.")
                return True

            existing_stocks = account_balance['stocks']
            logger.info(f"📈 보유 종목 {len(existing_stocks)}개 발견 - 상세 분석 시작")

            # 각 보유 종목에 대해 웹소켓 구독
            subscription_success_count = 0
            for stock_info in existing_stocks:
                try:
                    stock_code = stock_info['stock_code']
                    stock_name = stock_info['stock_name']

                    # 🆕 데이터베이스에서 매수 정보 조회
                    holding_analysis = await self._analyze_holding_position(stock_info)

                    if holding_analysis:
                        # 데이터베이스 기반 상세 정보 출력
                        self._log_detailed_holding_info(stock_code, stock_name, stock_info, holding_analysis)
                    else:
                        # 기본 정보만 출력
                        self._log_basic_holding_info(stock_code, stock_name, stock_info)

                    # 콜백 함수 생성
                    callback = self._create_existing_holding_callback(stock_code, stock_name)

                    # 웹소켓 구독 시도
                    if await self._subscribe_existing_holding(stock_code, callback):
                        subscription_success_count += 1
                        self.existing_holdings_callbacks[stock_code] = callback
                        logger.info(f"📡 {stock_code}({stock_name}) 실시간 모니터링 등록 성공")
                    else:
                        logger.warning(f"⚠️ {stock_code}({stock_name}) 실시간 모니터링 등록 실패")

                    # API 부하 방지
                    await asyncio.sleep(0.2)

                except Exception as e:
                    logger.error(f"종목 구독 오류: {e}")
                    continue

            logger.info(f"📊 기존 보유 종목 웹소켓 구독 완료: {subscription_success_count}/{len(existing_stocks)}개")
            return subscription_success_count > 0

        except Exception as e:
            logger.error(f"기존 보유 종목 모니터링 설정 오류: {e}")
            return False

    def _log_detailed_holding_info(self, stock_code: str, stock_name: str, stock_info: dict, analysis: dict):
        """🆕 데이터베이스 기반 상세 보유 종목 정보 로그"""
        try:
            current_price = stock_info.get('current_price', 0)
            quantity = stock_info.get('quantity', 0)
            market_value = stock_info.get('market_value', 0)
            pnl_amount = stock_info.get('pnl_amount', 0)
            pnl_rate = stock_info.get('pnl_rate', 0.0)

            logger.info(f"📈 보유종목 상세분석 [{stock_code}({stock_name})]:")
            logger.info(f"   💰 매수정보: {analysis.get('buy_price', 0):,}원 x {quantity:,}주 = {analysis.get('buy_amount', 0):,}원")
            logger.info(f"   📊 현재상황: {current_price:,}원 | 평가금액: {market_value:,}원")
            logger.info(f"   💹 손익현황: {pnl_amount:+,}원 ({pnl_rate:+.2f}%)")

            # 매수 사유 (실제 데이터베이스 기반)
            entry_reason = analysis.get('entry_reason', '정보 없음')
            pattern_type = analysis.get('pattern_type', '패턴 불명')
            buy_date = analysis.get('buy_date', '날짜 불명')
            logger.info(f"   📝 매수사유: {entry_reason}")
            logger.info(f"   🎯 감지패턴: {pattern_type} (매수일: {buy_date})")

            # 목표가 및 손절가 (데이터베이스 설정값 또는 계산값)
            target_price = analysis.get('target_price')
            stop_loss_price = analysis.get('stop_loss_price')
            buy_price = analysis.get('buy_price', current_price)

            if target_price and stop_loss_price:
                target_pct = ((target_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
                stop_pct = ((stop_loss_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
                logger.info(f"   🎯 목표가: {target_price:,}원 ({target_pct:+.1f}%)")
                logger.info(f"   🛑 손절가: {stop_loss_price:,}원 ({stop_pct:.1f}%)")
            else:
                # 기본 목표가/손절가 계산
                default_target = int(buy_price * 1.05)  # 5% 익절
                default_stop = int(buy_price * 0.95)    # 5% 손절
                logger.info(f"   🎯 목표가: {default_target:,}원 (+5% 기본설정)")
                logger.info(f"   🛑 손절가: {default_stop:,}원 (-5% 기본설정)")

            # 현재 추천 사항
            recommendation = analysis.get('recommendation', '관찰')
            risk_level = analysis.get('risk_level', 'MEDIUM')
            logger.info(f"   💡 현재추천: {recommendation} (리스크: {risk_level})")

            # 보유 기간
            holding_days = analysis.get('holding_days', 0)
            logger.info(f"   ⏰ 보유기간: {holding_days}일")

        except Exception as e:
            logger.error(f"상세 보유 정보 로그 오류 ({stock_code}): {e}")
            # 오류 시 기본 정보로 대체
            self._log_basic_holding_info(stock_code, stock_name, stock_info)

    def _log_basic_holding_info(self, stock_code: str, stock_name: str, stock_info: dict):
        """🆕 기본 보유 종목 정보 로그 (데이터베이스 정보가 없을 때)"""
        try:
            current_price = stock_info.get('current_price', 0)
            buy_price = stock_info.get('buy_price', 0)
            quantity = stock_info.get('quantity', 0)
            market_value = stock_info.get('market_value', 0)
            pnl_amount = stock_info.get('pnl_amount', 0)
            pnl_rate = stock_info.get('pnl_rate', 0.0)

            logger.info(f"📈 보유종목 기본정보 [{stock_code}({stock_name})]:")
            logger.info(f"   💰 매수가: {buy_price:,}원 | 현재가: {current_price:,}원 | 수량: {quantity:,}주")
            logger.info(f"   📊 평가금액: {market_value:,}원 | 손익: {pnl_amount:+,}원 ({pnl_rate:+.2f}%)")

            if buy_price > 0:
                target_price_5 = int(buy_price * 1.05)  # 5% 익절
                target_price_10 = int(buy_price * 1.10)  # 10% 익절
                stop_loss_price = int(buy_price * 0.95)  # 5% 손절

                logger.info(f"   🎯 목표가: {target_price_5:,}원(+5%) / {target_price_10:,}원(+10%)")
                logger.info(f"   🛑 손절가: {stop_loss_price:,}원(-5%)")

                # 보유 이유 추론 (매우 기본적)
                if pnl_rate > 3:
                    hold_reason = "수익 실현 대기 중"
                elif pnl_rate < -2:
                    hold_reason = "손절 기준 검토 필요"
                else:
                    hold_reason = "추가 관찰 중"

                logger.info(f"   📝 추정사유: {hold_reason} (DB정보 없음)")

        except Exception as e:
            logger.error(f"기본 보유 정보 로그 오류 ({stock_code}): {e}")

    async def _subscribe_existing_holding(self, stock_code: str, callback) -> bool:
        """기존 보유 종목 웹소켓 구독 (내부 메서드) - 🆕 중복 구독 방지"""
        try:
            # 🆕 중복 구독 체크
            if stock_code in self.subscribed_stocks:
                logger.debug(f"📡 {stock_code} 이미 구독 중 - 건너뛰기")
                return True

            if self.websocket_manager:
                # 웹소켓 매니저를 통한 구독
                success = await self.websocket_manager.subscribe_stock(stock_code, callback)
                if success:
                    self.subscribed_stocks.add(stock_code)  # 구독 성공시 추가
                    logger.debug(f"📡 {stock_code} 웹소켓 구독 성공")
                return success
            elif self.data_manager:
                # 데이터 매니저를 통한 백업 구독
                from core.data.data_priority import DataPriority
                success = self.data_manager.add_stock_request(
                    stock_code=stock_code,
                    priority=DataPriority.HIGH,
                    strategy_name="existing_holding_candle",
                    callback=callback
                )
                if success:
                    self.subscribed_stocks.add(stock_code)  # 구독 성공시 추가
                    logger.debug(f"📡 {stock_code} 데이터매니저 구독 성공")
                return success
            else:
                logger.warning("웹소켓 매니저와 데이터 매니저 모두 없음")
                return False

        except Exception as e:
            if "ALREADY IN SUBSCRIBE" in str(e):
                logger.debug(f"📡 {stock_code} 이미 구독됨 - 정상 처리")
                self.subscribed_stocks.add(stock_code)  # 이미 구독되어 있으므로 추가
                return True
            else:
                logger.error(f"기존 보유 종목 구독 오류 ({stock_code}): {e}")
                return False

    def _create_existing_holding_callback(self, stock_code: str, stock_name: str):
        """기존 보유 종목용 콜백 함수 생성 (캔들 시스템 통합)"""
        def existing_holding_callback(data_type: str, received_stock_code: str, data: Dict, source: str = 'websocket') -> None:
            """기존 보유 종목 실시간 가격 콜백 (캔들 시스템에서 처리)"""
            try:
                if data_type == 'price' and 'stck_prpr' in data:
                    current_price = int(data.get('stck_prpr', 0))
                    if current_price > 0:
                        # 가격 정보 로깅
                        logger.debug(f"📊 {stock_code}({stock_name}) 실시간 가격: {current_price:,}원")

                        # 🆕 캔들 패턴 분석에 가격 정보 반영
                        self._update_existing_holding_price(stock_code, current_price)

                        # 🆕 필요시 매도 시그널 체크 (기존 보유 종목용)
                        asyncio.create_task(self._check_existing_holding_exit_signal(stock_code, current_price))

            except Exception as e:
                logger.error(f"기존 보유 종목 콜백 오류 ({stock_code}): {e}")

        return existing_holding_callback

    def _update_existing_holding_price(self, stock_code: str, current_price: int):
        """기존 보유 종목 가격 업데이트 (stock_manager의 메서드 사용)"""
        try:
            # 캔들 후보에 있으면 가격 업데이트 (stock_manager의 실제 메서드 사용)
            watching_stocks = self.stock_manager.get_watching_stocks()
            for candidate in watching_stocks:
                if candidate.stock_code == stock_code:
                    candidate.update_price(current_price)

            # 포지션에 있으면 가격 업데이트 (stock_manager의 실제 메서드 사용)
            active_positions = self.stock_manager.get_active_positions()
            for position in active_positions:
                if position.stock_code == stock_code:
                    position.update_price(current_price)

        except Exception as e:
            logger.debug(f"기존 보유 종목 가격 업데이트 오류 ({stock_code}): {e}")

    async def _check_existing_holding_exit_signal(self, stock_code: str, current_price: int):
        """기존 보유 종목 매도 시그널 체크"""
        try:
            # 포지션이 있는지 확인 (stock_manager의 실제 메서드 사용)
            active_positions = self.stock_manager.get_active_positions()

            for position in active_positions:
                if position.stock_code == stock_code:
                    # 매도 조건 체크
                    if self._should_exit_existing_position(position, current_price):
                        logger.info(f"🚨 {stock_code} 기존 보유 종목 매도 시그널 감지")
                        await self._execute_exit(position, exit_price=float(current_price), reason="기존보유_패턴분석")

        except Exception as e:
            logger.debug(f"기존 보유 종목 매도 시그널 체크 오류 ({stock_code}): {e}")

    def _should_exit_existing_position(self, position, current_price: int) -> bool:
        """기존 보유 종목 매도 조건 확인 (간단한 버전)"""
        try:
            # 기본적인 손절/익절 조건만 확인
            if hasattr(position, 'buy_price') and hasattr(position, 'risk_management'):
                buy_price = position.buy_price

                # 손절 조건 (3% 하락)
                loss_pct = ((current_price - buy_price) / buy_price) * 100
                if loss_pct <= -3.0:
                    return True

                # 익절 조건 (5% 상승)
                if loss_pct >= 5.0:
                    return True

            return False

        except Exception as e:
            logger.debug(f"매도 조건 확인 오류: {e}")
            return False

    def cleanup_existing_holdings_monitoring(self):
        """기존 보유 종목 모니터링 정리 - 🆕 구독 상태도 정리"""
        try:
            logger.info("📊 기존 보유 종목 모니터링 정리 시작")

            # 콜백 정리
            cleanup_count = len(self.existing_holdings_callbacks)
            self.existing_holdings_callbacks.clear()

            # 🆕 구독 상태 정리
            subscribed_count = len(self.subscribed_stocks)
            self.subscribed_stocks.clear()

            logger.info(f"📊 기존 보유 종목 모니터링 정리 완료: {cleanup_count}개 콜백, {subscribed_count}개 구독 정리")

        except Exception as e:
            logger.error(f"기존 보유 종목 모니터링 정리 오류: {e}")

    # ==========================================
    # 기존 메서드들 유지...
    # ==========================================

    # ========== 메인 실행 루프 ==========

    async def start_trading(self):
        """캔들 전략 거래 시작"""
        try:
            if self.is_running:
                logger.warning("캔들 전략이 이미 실행 중입니다")
                return False

            self.is_running = True
            logger.info("🎯 캔들 기반 매매 전략 시작")

            # 초기화
            await self._initialize_trading_day()

            # 메인 루프 실행
            while self.is_running:
                try:
                    # 1. 시장 시간 체크
                    if not self._is_trading_time():
                        await asyncio.sleep(60)  # 1분 대기
                        continue

                    # 2. 종목 스캔 및 패턴 감지
                    await self._scan_and_detect_patterns()

                    # 3. 진입 기회 평가
                    await self._evaluate_entry_opportunities()

                    # 4. 기존 포지션 관리
                    await self._manage_existing_positions()

                    # 5. 자동 정리
                    self.stock_manager.auto_cleanup()

                    # 6. 상태 로깅
                    self._log_status()

                    # 다음 스캔까지 대기
                    await asyncio.sleep(self.config['scan_interval_seconds'])

                except Exception as e:
                    logger.error(f"거래 루프 오류: {e}")
                    await asyncio.sleep(30)  # 오류 시 30초 대기

            logger.info("🎯 캔들 전략 거래 종료")
            return True

        except Exception as e:
            logger.error(f"캔들 전략 시작 오류: {e}")
            self.is_running = False
            return False

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
            if (self._last_scan_time and
                (current_time - self._last_scan_time).total_seconds() < self._scan_interval):
                return

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
                    if i % 10 == 0 and i > 0:
                        await asyncio.sleep(1)

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
                if self.websocket_manager and stock_code not in self._all_stocks:
                    await self.websocket_manager.subscribe_stock(stock_code)  # ✅ 메서드명 수정
                    logger.info(f"📡 {stock_code} 웹소켓 구독 추가")
            except Exception as ws_error:
                logger.warning(f"⚠️ {stock_code} 웹소켓 구독 실패: {ws_error}")

            logger.info(f"✅ {stock_code}({stock_name}) 패턴 감지: {strongest_pattern.pattern_type.value} "
                       f"신뢰도:{strongest_pattern.confidence:.2f} "
                       f"강도:{strongest_pattern.strength}점 "
                       f"패턴:{strongest_pattern.pattern_type.value}")

            return candidate

        except Exception as e:
            logger.error(f"❌ {stock_code} 패턴 분석 오류: {e}")
            return None

    async def _subscribe_new_candidate(self, candidate: CandleTradeCandidate) -> bool:
        """새 후보 종목 웹소켓 구독 - 🆕 중복 구독 방지"""
        try:
            stock_code = candidate.stock_code

            # 🆕 중복 구독 체크
            if stock_code in self.subscribed_stocks:
                logger.debug(f"📡 {stock_code} 이미 구독 중 - 콜백만 업데이트")
                return True

            if self.websocket_manager:
                callback = self._create_candidate_callback(candidate)
                success = await self.websocket_manager.subscribe_stock(stock_code, callback)
                if success:
                    self.subscribed_stocks.add(stock_code)  # 구독 성공시 추가
                    logger.info(f"📡 {stock_code} 신규 후보 웹소켓 구독 성공")
                return success
            elif self.data_manager:
                from core.data.data_priority import DataPriority
                callback = self._create_candidate_callback(candidate)
                success = self.data_manager.add_stock_request(
                    stock_code=stock_code,
                    priority=DataPriority.HIGH,
                    strategy_name="candle_trading",
                    callback=callback
                )
                if success:
                    self.subscribed_stocks.add(stock_code)  # 구독 성공시 추가
                    logger.info(f"📡 {stock_code} 신규 후보 데이터매니저 구독 성공")
                return success
            else:
                logger.warning(f"웹소켓/데이터 매니저 없음 ({stock_code})")
                return False

        except Exception as e:
            if "ALREADY IN SUBSCRIBE" in str(e):
                logger.debug(f"📡 {stock_code} 이미 구독됨 - 정상 처리")
                self.subscribed_stocks.add(stock_code)  # 이미 구독되어 있으므로 추가
                return True
            else:
                logger.error(f"신규 후보 구독 오류 ({stock_code}): {e}")
                return False

    def _create_candidate_callback(self, candidate: CandleTradeCandidate):
        """새 후보 종목용 콜백 함수 생성"""
        def candidate_callback(data_type: str, received_stock_code: str, data: Dict, source: str = 'websocket') -> None:
            """새 후보 종목 실시간 가격 콜백"""
            try:
                if data_type == 'price' and 'stck_prpr' in data:
                    current_price = float(data.get('stck_prpr', 0))
                    if current_price > 0:
                        # 캔들 매니저의 종목 가격 업데이트
                        stored_candidate = self.stock_manager.get_stock(candidate.stock_code)
                        if stored_candidate:
                            stored_candidate.update_price(current_price)

                            # 상태 변화 체크 (관찰→매수준비, 매수준비→진입 등)
                            asyncio.create_task(self._check_candidate_status_change(stored_candidate))

                        logger.debug(f"📊 후보종목 {candidate.stock_code} 실시간 가격: {current_price:,}원")

            except Exception as e:
                logger.error(f"후보 종목 콜백 오류 ({candidate.stock_code}): {e}")

        return candidate_callback

    async def _check_candidate_status_change(self, candidate: CandleTradeCandidate):
        """후보 종목 상태 변화 체크"""
        try:
            old_status = candidate.status

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
            if old_status != candidate.status:
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

            # 손절 체크
            if current_price <= candidate.risk_management.stop_loss_price:
                return True

            # 익절 체크
            if current_price >= candidate.risk_management.target_price:
                return True

            # 시간 청산 체크
            if self._should_time_exit(candidate):
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
                # RSI 계산은 별도 구현 필요하거나 기존 기술적 지표 활용
                conditions.rsi_check = True  # 임시로 통과
            except:
                conditions.rsi_check = True

            # 3. 시간대 조건
            current_time = datetime.now().time()
            trading_start = datetime.strptime(self.config['trading_start_time'], '%H:%M').time()
            trading_end = datetime.strptime(self.config['trading_end_time'], '%H:%M').time()

            conditions.time_check = trading_start <= current_time <= trading_end
            if not conditions.time_check:
                conditions.fail_reasons.append("거래 시간 외")

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

    # ========== 진입 기회 평가 ==========

    async def _evaluate_entry_opportunities(self):
        """진입 기회 평가 및 매수 실행"""
        try:
            # 매수 준비된 종목들 조회
            buy_candidates = self.stock_manager.get_top_buy_candidates(limit=5)

            if not buy_candidates:
                return

            logger.info(f"🎯 매수 후보 평가: {len(buy_candidates)}개 종목")

            # 현재 포지션 수 체크
            active_positions = self.stock_manager.get_active_positions()
            max_positions = self.stock_manager.max_positions

            available_slots = max_positions - len(active_positions)

            if available_slots <= 0:
                logger.info("최대 포지션 수 도달 - 신규 진입 제한")
                return

            # 상위 후보들 평가
            for candidate in buy_candidates[:available_slots]:
                try:
                    success = await self._execute_entry(candidate)
                    if success:
                        logger.info(f"✅ {candidate.stock_code} 진입 성공")
                    else:
                        logger.warning(f"❌ {candidate.stock_code} 진입 실패")

                except Exception as e:
                    logger.error(f"진입 실행 오류 ({candidate.stock_code}): {e}")

        except Exception as e:
            logger.error(f"진입 기회 평가 오류: {e}")

    async def _execute_entry(self, candidate: CandleTradeCandidate, available_amount: Optional[int] = None) -> bool:
        """
        실제 매수 진입 실행

        Args:
            candidate: 매수 후보 종목
            available_amount: 구매 가능 금액 (None이면 자동 계산)
        """
        try:
            # 최신 가격 확인
            from ..api.kis_market_api import get_inquire_price
            current_data = get_inquire_price("J", candidate.stock_code)

            if current_data is None or current_data.empty:
                return False

            current_price = float(current_data.iloc[0].get('stck_prpr', 0))

            if current_price <= 0:
                return False

            # 가격 업데이트
            candidate.update_price(current_price)

            # 🆕 투자 금액 계산 (파라미터 또는 자동 계산)
            if available_amount is not None:
                # 명시적으로 제공된 구매 가능 금액 사용
                total_available = available_amount
                logger.info(f"📊 명시된 구매 가능 금액 사용: {total_available:,}원")
            else:
                # 자동 계산: 계좌 잔고 조회 또는 기본값 사용
                try:
                    from ..api.kis_market_api import get_account_balance
                    balance_info = get_account_balance()
                    if balance_info and balance_info.get('total_value', 0) > 0:
                        # 계좌 평가액의 10%를 기본 투자 금액으로 설정
                        total_available = int(balance_info['total_value'] * 0.1)
                        logger.info(f"📊 계좌 평가액 기반 구매 금액: {total_available:,}원 (총 평가액의 10%)")
                    else:
                        # 백업: 기본 투자 금액
                        total_available = 1_000_000  # 100만원 기본값
                        logger.warning(f"📊 잔고 조회 실패 - 기본 투자 금액 사용: {total_available:,}원")
                except Exception as e:
                    logger.warning(f"📊 잔고 조회 오류 - 기본 투자 금액 사용: {e}")
                    total_available = 1_000_000  # 100만원 기본값

            # 포지션 크기에 따른 실제 투자 금액 계산
            position_amount = int(total_available * candidate.risk_management.position_size_pct / 100)
            quantity = position_amount // current_price

            if quantity <= 0:
                logger.warning(f"{candidate.stock_code}: 계산된 수량이 0 이하 (가용금액: {total_available:,}, 포지션크기: {candidate.risk_management.position_size_pct}%)")
                return False

            logger.info(f"💰 매수 계획: {candidate.stock_code} - 가용금액: {total_available:,}원, "
                       f"포지션크기: {candidate.risk_management.position_size_pct}%, "
                       f"투자금액: {position_amount:,}원, 수량: {quantity:,}주")

            # 🆕 간소화된 매수 신호 (검증은 이미 완료됨)
            signal = {
                'stock_code': candidate.stock_code,
                'action': 'buy',
                'strategy': 'candle_pattern',
                'price': current_price,
                'quantity': quantity,
                'total_amount': position_amount,
                'pattern_type': str(candidate.detected_patterns[0].pattern_type) if candidate.detected_patterns else 'unknown',
                # 🆕 캔들 시스템에서 이미 검증 완료됨을 표시
                'pre_validated': True,
                'validation_source': 'candle_system'
            }

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

            # 상태 업데이트
            self.stock_manager.update_candidate(candidate)

            # 통계 업데이트
            self.daily_stats['trades_count'] += 1

            return True

        except Exception as e:
            logger.error(f"매수 진입 실행 오류 ({candidate.stock_code}): {e}")
            return False

    # ========== 포지션 관리 ==========

    async def _manage_existing_positions(self):
        """기존 포지션 관리 (손절/익절/추적손절)"""
        try:
            active_positions = self.stock_manager.get_active_positions()

            if not active_positions:
                return

            logger.debug(f"📊 포지션 관리: {len(active_positions)}개 포지션")

            for position in active_positions:
                try:
                    await self._manage_single_position(position)
                except Exception as e:
                    logger.error(f"포지션 관리 오류 ({position.stock_code}): {e}")

        except Exception as e:
            logger.error(f"포지션 관리 오류: {e}")

    async def _manage_single_position(self, position: CandleTradeCandidate):
        """개별 포지션 관리"""
        try:
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

            # 청산 조건 체크
            exit_reason = None
            should_exit = False

            # 1. 손절 체크
            if current_price <= position.risk_management.stop_loss_price:
                exit_reason = "손절"
                should_exit = True

            # 2. 익절 체크
            elif current_price >= position.risk_management.target_price:
                exit_reason = "목표가 도달"
                should_exit = True

            # 3. 시간 청산 체크
            elif self._should_time_exit(position):
                exit_reason = "시간 청산"
                should_exit = True

            # 4. 추적 손절 조정
            elif pnl_pct > 5:  # 5% 이상 수익시 추적 손절 활성화
                self._update_trailing_stop(position, current_price)

            # 청산 실행
            if should_exit and exit_reason:
                success = await self._execute_exit(position, current_price, exit_reason)
                if success:
                    logger.info(f"🔚 {position.stock_code} 청산: {exit_reason} "
                               f"(수익률 {pnl_pct:+.1f}%)")

            # 포지션 상태 업데이트
            self.stock_manager.update_candidate(position)

        except Exception as e:
            logger.error(f"개별 포지션 관리 오류 ({position.stock_code}): {e}")

    async def _execute_exit(self, position: CandleTradeCandidate, exit_price: float, reason: str) -> bool:
        """매도 청산 실행"""
        try:
            quantity = position.performance.entry_quantity
            if not quantity:
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
                logger.info(f"⏰ {position.stock_code} 시간 기반 수익 청산: {holding_time}, 수익률: {position.performance.pnl_pct:.2%}")
                return True

            # 3. 장 마감 전 청산 (오버나이트 회피)
            if time_rules.get('overnight_avoid', True):
                current_time = datetime.now()
                market_close_minutes = time_rules.get('market_close_exit_minutes', 30)

                # 장 마감 30분 전부터 청산 고려
                if (current_time.hour == 15 and
                    current_time.minute >= (30 - market_close_minutes) and
                    position.performance.pnl_pct and
                    position.performance.pnl_pct > -0.01):  # -1% 이상이면 청산
                    logger.info(f"⏰ {position.stock_code} 장 마감 전 청산: 수익률 {position.performance.pnl_pct:.2%}")
                    return True

            return False

        except Exception as e:
            logger.error(f"시간 청산 체크 오류: {e}")
            return False

    def _update_trailing_stop(self, position: CandleTradeCandidate, current_price: float):
        """추적 손절가 업데이트"""
        try:
            trailing_pct = position.risk_management.trailing_stop_pct / 100
            new_trailing_stop = current_price * (1 - trailing_pct)

            # 기존 손절가보다 높을 때만 업데이트
            if new_trailing_stop > position.risk_management.stop_loss_price:
                position.risk_management.stop_loss_price = new_trailing_stop
                position.risk_management.current_trailing_stop = new_trailing_stop
                position.risk_management.last_trailing_update = datetime.now()

                logger.debug(f"📈 {position.stock_code} 추적손절 업데이트: {new_trailing_stop:,.0f}원")

        except Exception as e:
            logger.error(f"추적 손절 업데이트 오류: {e}")

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

    # ========== 공개 인터페이스 ==========

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

    def get_top_candidates(self, limit: int = 10) -> List[Dict[str, Any]]:
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

    async def analyze_existing_holdings(self):
        """🆕 기존 보유 종목 분석 및 DB 저장"""
        try:
            logger.info("🔍 기존 보유 종목 분석 시작")

            # KIS API를 통해 보유 종목 조회
            from ..api.kis_market_api import get_account_balance
            balance_info = get_account_balance()

            if not balance_info or not balance_info.get('stocks'):
                logger.info("📊 보유 종목 없음")
                return {
                    'analyzed_count': 0,
                    'total_value': 0,
                    'recommendations': [],
                    'analysis_time': datetime.now().isoformat()
                }

            analysis_results = []
            total_value = balance_info.get('total_eval_amount', 0)

            for stock in balance_info['stocks']:
                stock_code = stock['stock_code']
                stock_name = stock['stock_name']

                logger.info(f"📊 {stock_code}({stock_name}) 분석 중...")

                # 현재 분석 수행
                analysis = await self._analyze_holding_position(stock)
                if analysis:
                    # stock 정보도 추가
                    analysis.update({
                        'stock_name': stock_name,
                        'quantity': stock['quantity'],
                        'avg_price': stock['avg_price'],
                        'current_price': stock['current_price'],
                        'profit_loss': stock['profit_loss'],
                        'profit_rate': stock['profit_loss_rate']
                    })
                    analysis_results.append(analysis)

                    # 🆕 DB에 분석 결과 저장
                    try:
                        if self.trade_db:
                            self.trade_db.record_existing_holdings_analysis(
                                stock_code=stock_code,
                                stock_name=stock_name,
                                quantity=stock['quantity'],
                                avg_price=int(stock['avg_price']),
                                current_price=int(stock['current_price']),
                                total_value=stock['eval_amount'],
                                profit_loss=stock['profit_loss'],
                                profit_rate=stock['profit_loss_rate'],
                                recommendation=analysis['recommendation'],
                                recommendation_reasons=analysis['reasons'],
                                risk_level=analysis['risk_level'],
                                current_pattern=analysis.get('pattern'),
                                pattern_strength=analysis.get('pattern_strength', 0.0),
                                technical_indicators=analysis.get('technical_indicators', {}),
                                suggested_action=analysis.get('action', 'HOLD'),
                                target_sell_price=analysis.get('target_sell_price'),
                                stop_loss_price=analysis.get('stop_loss_price')
                            )
                            logger.info(f"🗄️ {stock_code} 보유종목 분석 DB 저장 완료")
                    except Exception as db_error:
                        logger.error(f"❌ {stock_code} 보유종목 DB 저장 실패: {db_error}")

                await asyncio.sleep(0.1)  # API 제한 방지

            logger.info(f"✅ 기존 보유 종목 분석 완료: {len(analysis_results)}개")

            # 딕셔너리 형태로 반환
            return {
                'analyzed_count': len(analysis_results),
                'total_value': total_value,
                'recommendations': analysis_results,
                'analysis_time': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"❌ 기존 보유 종목 분석 오류: {e}")
            return {
                'analyzed_count': 0,
                'total_value': 0,
                'recommendations': [],
                'error': str(e),
                'analysis_time': datetime.now().isoformat()
            }

    async def _analyze_holding_position(self, holding_info: dict) -> Optional[dict]:
        """🆕 강화된 개별 보유 종목 분석 - 데이터베이스 기반"""
        try:
            stock_code = holding_info['stock_code']
            current_price = holding_info['current_price']
            avg_price = holding_info['avg_price']
            profit_rate = holding_info['profit_loss_rate']

            # 🆕 1. 데이터베이스에서 매수 정보 조회
            buy_info = self._get_buy_info_from_db(stock_code)

            if buy_info:
                # 데이터베이스 기반 분석
                return self._analyze_with_db_info(holding_info, buy_info)
            else:
                # 메모리에서 활성 포지션 정보 확인
                memory_info = self._get_buy_info_from_memory(stock_code)

                if memory_info:
                    return self._analyze_with_memory_info(holding_info, memory_info)
                else:
                    # 기본 분석 (정보 없음)
                    return self._analyze_basic(holding_info)

        except Exception as e:
            logger.error(f"보유 종목 분석 오류 ({stock_code}): {e}")
            return None

    def _get_buy_info_from_db(self, stock_code: str) -> Optional[dict]:
        """🆕 데이터베이스에서 매수 정보 조회"""
        try:
            if not self.trade_db:
                return None

            # 캔들 거래 테이블에서 매수 기록 조회 (최근 30일)
            candle_trades = self.trade_db.get_candle_trades(stock_code=stock_code, days=30, trade_type='BUY')

            if candle_trades:
                # 가장 최근 매수 기록
                latest_buy = candle_trades[0]

                # 후보 정보도 함께 조회
                candidate_info = None
                if latest_buy.get('candidate_id'):
                    try:
                        candidates = self.trade_db.get_candle_candidates(days=30)
                        candidate_info = next(
                            (c for c in candidates if c['id'] == latest_buy['candidate_id']),
                            None
                        )
                    except Exception as e:
                        logger.debug(f"후보 정보 조회 오류: {e}")

                return {
                    'buy_price': latest_buy['price'],
                    'buy_amount': latest_buy['total_amount'],
                    'buy_date': latest_buy['timestamp'][:10],  # YYYY-MM-DD
                    'entry_reason': latest_buy['decision_reason'],
                    'pattern_type': latest_buy.get('pattern_matched', '패턴 불명'),
                    'strategy_type': 'candle_trading',
                    'target_price': candidate_info.get('target_price') if candidate_info else None,
                    'stop_loss_price': candidate_info.get('stop_loss_price') if candidate_info else None,
                    'signal_strength': candidate_info.get('signal_strength') if candidate_info else 'UNKNOWN',
                    'risk_score': candidate_info.get('risk_score') if candidate_info else 50
                }

            # 일반 거래 테이블에서 매수 기록 조회 (백업)
            general_trades = self.trade_db.get_trade_history(stock_code=stock_code, days=30, trade_type='BUY')

            if general_trades:
                latest_buy = general_trades[0]
                return {
                    'buy_price': latest_buy['price'],
                    'buy_amount': latest_buy['total_amount'],
                    'buy_date': latest_buy['timestamp'][:10],
                    'entry_reason': latest_buy.get('strategy_type', '일반 매수'),
                    'pattern_type': '일반 거래',
                    'strategy_type': latest_buy.get('strategy_type', 'manual'),
                    'target_price': None,
                    'stop_loss_price': None,
                    'signal_strength': 'MANUAL',
                    'risk_score': 30
                }

            return None

        except Exception as e:
            logger.error(f"DB 매수 정보 조회 오류 ({stock_code}): {e}")
            return None

    def _get_buy_info_from_memory(self, stock_code: str) -> Optional[dict]:
        """🆕 메모리(실시간 후보)에서 매수 정보 조회"""
        try:
            if stock_code in self._all_stocks:
                candidate = self._all_stocks[stock_code]

                # performance.entry_price 사용 (buy_price 대신)
                if hasattr(candidate, 'performance') and candidate.performance.entry_price:
                    pattern_type = 'UNKNOWN'
                    entry_reason = '실시간 매수'

                    if candidate.detected_patterns:
                        pattern_type = candidate.detected_patterns[0].pattern_type.value
                        entry_reason = candidate.detected_patterns[0].description

                    return {
                        'buy_price': candidate.performance.entry_price,
                        'buy_amount': candidate.performance.entry_price * getattr(candidate, 'quantity', 0),
                        'buy_date': datetime.now().strftime('%Y-%m-%d'),
                        'entry_reason': entry_reason,
                        'pattern_type': pattern_type,
                        'strategy_type': 'candle_trading',
                        'target_price': getattr(candidate.risk_management, 'target_price', None) if hasattr(candidate, 'risk_management') else None,
                        'stop_loss_price': getattr(candidate.risk_management, 'stop_loss_price', None) if hasattr(candidate, 'risk_management') else None,
                        'signal_strength': candidate.trade_signal.value if hasattr(candidate, 'trade_signal') else 'MEDIUM',
                        'risk_score': getattr(candidate, 'risk_score', 50)
                    }

            return None

        except Exception as e:
            logger.error(f"메모리 매수 정보 조회 오류 ({stock_code}): {e}")
            return None

    def _analyze_with_db_info(self, holding_info: dict, buy_info: dict) -> dict:
        """🆕 데이터베이스 정보 기반 분석"""
        stock_code = holding_info['stock_code']
        current_price = holding_info['current_price']
        avg_price = holding_info['avg_price']
        profit_rate = holding_info['profit_loss_rate']

        # 보유 기간 계산
        buy_date_str = buy_info['buy_date']
        try:
            buy_date = datetime.strptime(buy_date_str, '%Y-%m-%d')
            holding_days = (datetime.now() - buy_date).days
        except:
            holding_days = 0

        # 실제 매수 정보 기반 추천
        reasons = []
        risk_level = 'MEDIUM'

        # 패턴별 맞춤 전략
        pattern_type = buy_info['pattern_type']
        signal_strength = buy_info.get('signal_strength', 'MEDIUM')

        if 'bullish_engulfing' in pattern_type.lower():
            # 상승 장악형: 강한 상승 패턴
            if profit_rate > 8:
                recommendation = 'PARTIAL_SELL'
                reasons.append(f'상승장악형 목표 달성 ({profit_rate:.1f}%)')
                action = 'PARTIAL_SELL'
            elif profit_rate < -3:
                recommendation = 'REVIEW_SELL'
                reasons.append(f'상승장악형 신호 실패 ({profit_rate:.1f}%)')
                action = 'REVIEW'
                risk_level = 'HIGH'
            else:
                recommendation = 'HOLD'
                reasons.append(f'상승장악형 진행 중 ({profit_rate:.1f}%)')
                action = 'HOLD'

        elif 'hammer' in pattern_type.lower():
            # 망치형: 반전 신호
            if profit_rate > 5:
                recommendation = 'HOLD'
                reasons.append(f'망치형 반전 성공 ({profit_rate:.1f}%)')
                action = 'HOLD'
            elif profit_rate < -5:
                recommendation = 'SELL'
                reasons.append(f'망치형 반전 실패 ({profit_rate:.1f}%)')
                action = 'FULL_SELL'
                risk_level = 'HIGH'
            else:
                recommendation = 'HOLD'
                reasons.append(f'망치형 관찰 중 ({profit_rate:.1f}%)')
                action = 'HOLD'

        else:
            # 기본 전략
            if profit_rate > 10:
                recommendation = 'PARTIAL_SELL'
                reasons.append(f'목표 수익 달성 ({profit_rate:.1f}%)')
                action = 'PARTIAL_SELL'
            elif profit_rate < -8:
                recommendation = 'SELL'
                reasons.append(f'손절 기준 도달 ({profit_rate:.1f}%)')
                action = 'FULL_SELL'
                risk_level = 'HIGH'
            else:
                recommendation = 'HOLD'
                reasons.append(f'정상 범위 ({profit_rate:.1f}%)')
                action = 'HOLD'

        # 보유 기간 고려
        if holding_days > 10:
            reasons.append(f'장기 보유 {holding_days}일')
            if profit_rate > 3:
                recommendation = 'PARTIAL_SELL'
                action = 'PARTIAL_SELL'

        return {
            'stock_code': stock_code,
            'buy_price': buy_info['buy_price'],
            'buy_amount': buy_info['buy_amount'],
            'buy_date': buy_info['buy_date'],
            'entry_reason': buy_info['entry_reason'],
            'pattern_type': buy_info['pattern_type'],
            'strategy_type': buy_info['strategy_type'],
            'target_price': buy_info.get('target_price'),
            'stop_loss_price': buy_info.get('stop_loss_price'),
            'holding_days': holding_days,
            'recommendation': recommendation,
            'reasons': reasons,
            'risk_level': risk_level,
            'action': action,
            'signal_strength': signal_strength,
            'analysis_time': datetime.now().isoformat()
        }

    def _analyze_with_memory_info(self, holding_info: dict, memory_info: dict) -> dict:
        """🆕 메모리 정보 기반 분석"""
        # DB 정보와 유사하지만 메모리 기반
        result = self._analyze_with_db_info(holding_info, memory_info)
        result['data_source'] = 'memory'
        return result

    def _analyze_basic(self, holding_info: dict) -> dict:
        """🆕 기본 분석 - 시장 지표 기반 분석으로 업그레이드"""
        return self._analyze_basic_with_market_indicators(holding_info)

    def _analyze_basic_with_market_indicators(self, holding_info: dict) -> dict:
        """🆕 시장 지표 기반 기본 분석 (매수 정보 없는 증권사 앱 매수 종목용)"""
        stock_code = holding_info['stock_code']
        current_price = holding_info['current_price']
        avg_price = holding_info['avg_price']
        profit_rate = holding_info['profit_loss_rate']

        # 시장 지표 분석
        market_indicators = self._get_market_indicators(stock_code)

        # 기본 추천 로직 + 시장 지표 보정
        reasons = []
        risk_level = 'MEDIUM'

        # 🎯 1단계: 이격도 기반 분석
        disparity_signal = market_indicators.get('disparity_signal', 'NEUTRAL')

        # 🎯 2단계: RSI 기반 분석
        rsi_value = market_indicators.get('rsi', 50)
        rsi_signal = 'OVERBOUGHT' if rsi_value > 75 else 'OVERSOLD' if rsi_value < 25 else 'NEUTRAL'

        # 🎯 3단계: 거래량 분석
        volume_signal = market_indicators.get('volume_signal', 'NORMAL')

        # 🎯 통합 분석 - 시장 지표 + 수익률 기반
        if disparity_signal == 'STRONG_SELL' or (rsi_value > 80 and profit_rate > 10):
            recommendation = 'SELL'
            reasons.append(f'과매수 구간 ({rsi_value:.0f} RSI, {profit_rate:.1f}% 수익)')
            action = 'PARTIAL_SELL'
            risk_level = 'HIGH'
        elif disparity_signal == 'SELL' or (profit_rate > 15 and rsi_value > 70):
            recommendation = 'SELL'
            reasons.append(f'고수익+과열 ({profit_rate:.1f}%, RSI {rsi_value:.0f})')
            action = 'PARTIAL_SELL'
        elif profit_rate < -10 and disparity_signal != 'BUY':
            recommendation = 'SELL'
            reasons.append(f'손실 확대 ({profit_rate:.1f}%) + 시장 악화')
            action = 'FULL_SELL'
            risk_level = 'HIGH'
        elif disparity_signal == 'STRONG_BUY' and profit_rate > -5:
            recommendation = 'HOLD'
            reasons.append(f'과매도 반등 기대 (RSI {rsi_value:.0f}, 이격도 신호)')
            action = 'HOLD'
            risk_level = 'LOW'
        elif profit_rate > 5 and volume_signal == 'HIGH':
            recommendation = 'HOLD'
            reasons.append(f'거래량 증가 중 ({profit_rate:.1f}% 수익)')
            action = 'HOLD'
        elif rsi_value < 30 and profit_rate > -8:
            recommendation = 'HOLD'
            reasons.append(f'과매도 구간 (RSI {rsi_value:.0f})')
            action = 'HOLD'
            risk_level = 'MEDIUM'
        else:
            recommendation = 'HOLD'
            reasons.append(f'적정 범위 (RSI {rsi_value:.0f}, 수익률 {profit_rate:.1f}%)')
            action = 'HOLD'
            risk_level = 'LOW'

        # 목표가/손절가 설정 (시장 지표 반영)
        if disparity_signal in ['STRONG_SELL', 'SELL']:
            # 과열 구간 - 보수적 목표
            target_sell_price = int(current_price * 1.03)  # 3% 목표
            stop_loss_price = int(avg_price * 0.92)        # 8% 손절
        elif disparity_signal in ['STRONG_BUY', 'BUY']:
            # 과매도 구간 - 적극적 목표
            target_sell_price = int(current_price * 1.12)  # 12% 목표
            stop_loss_price = int(avg_price * 0.97)        # 3% 손절
        else:
            # 일반 구간 - 기본 목표
            target_sell_price = int(current_price * 1.08)  # 8% 목표
            stop_loss_price = int(avg_price * 0.95)        # 5% 손절

        return {
            'stock_code': stock_code,
            'buy_price': avg_price,  # 평균매수가 사용
            'buy_amount': 0,  # 불명
            'buy_date': '날짜 불명',
            'entry_reason': '매수 정보 없음 (증권사 앱 매수 추정)',
            'pattern_type': '패턴 불명',
            'strategy_type': 'market_indicator_analysis',
            'target_price': target_sell_price,
            'stop_loss_price': stop_loss_price,
            'holding_days': 0,
            'recommendation': recommendation,
            'reasons': reasons,
            'risk_level': risk_level,
            'action': action,
            'signal_strength': 'UNKNOWN',
            'data_source': 'market_indicator_analysis',
            'analysis_time': datetime.now().isoformat(),
            # 🆕 시장 지표 정보 추가
            'market_indicators': {
                'rsi': rsi_value,
                'disparity_signal': disparity_signal,
                'volume_signal': volume_signal,
                'market_sentiment': market_indicators.get('market_sentiment', 'NEUTRAL')
            }
        }

    def _get_market_indicators(self, stock_code: str) -> dict:
        """🆕 종목별 시장 지표 조회"""
        try:
            indicators = {
                'rsi': 50,  # 기본값
                'disparity_signal': 'NEUTRAL',
                'volume_signal': 'NORMAL',
                'market_sentiment': 'NEUTRAL'
            }

            # 🎯 1. 이격도 분석
            from ..api.kis_market_api import get_disparity_rank

            # 20일 이격도 상위 조회
            upper_disparity = get_disparity_rank(
                fid_input_iscd="0000",
                fid_rank_sort_cls_code="0",  # 상위
                fid_hour_cls_code="20"
            )

            # 20일 이격도 하위 조회
            lower_disparity = get_disparity_rank(
                fid_input_iscd="0000",
                fid_rank_sort_cls_code="1",  # 하위
                fid_hour_cls_code="20"
            )

            # 해당 종목이 이격도 순위에 있는지 확인
            if upper_disparity is not None and not upper_disparity.empty:
                matching_upper = upper_disparity[upper_disparity['mksc_shrn_iscd'] == stock_code]
                if not matching_upper.empty:
                    d20_val = float(matching_upper.iloc[0].get('d20_dsrt', 100))
                    if d20_val >= 120:
                        indicators['disparity_signal'] = 'STRONG_SELL'
                    elif d20_val >= 110:
                        indicators['disparity_signal'] = 'SELL'

            if lower_disparity is not None and not lower_disparity.empty:
                matching_lower = lower_disparity[lower_disparity['mksc_shrn_iscd'] == stock_code]
                if not matching_lower.empty:
                    d20_val = float(matching_lower.iloc[0].get('d20_dsrt', 100))
                    if d20_val <= 80:
                        indicators['disparity_signal'] = 'STRONG_BUY'
                    elif d20_val <= 90:
                        indicators['disparity_signal'] = 'BUY'

            # 🎯 2. RSI 계산 (technical_indicators 활용)
            try:
                # calculate_rsi 함수를 직접 구현하거나 기존 함수 활용
                from ..api.kis_market_api import get_inquire_daily_itemchartprice

                # 일봉 데이터 조회 (RSI 계산용)
                daily_data = get_inquire_daily_itemchartprice(
                    output_dv="2",  # 차트 데이터
                    itm_no=stock_code
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
                        # 🆕 간단한 RSI 계산 (별도 함수로 분리)
                        rsi_value = self._calculate_simple_rsi(close_prices)
                        if rsi_value is not None:
                            indicators['rsi'] = float(rsi_value)

            except Exception as e:
                logger.debug(f"RSI 계산 오류 ({stock_code}): {e}")

            # 🎯 3. 거래량 분석
            try:
                from ..api.kis_market_api import get_inquire_price

                current_data = get_inquire_price("J", stock_code)
                if current_data is not None and not current_data.empty:
                    current_info = current_data.iloc[0]
                    current_volume = int(current_info.get('acml_vol', 0))
                    avg_volume = int(current_info.get('avrg_vol', 0))

                    if avg_volume > 0:
                        volume_ratio = current_volume / avg_volume
                        if volume_ratio >= 3.0:
                            indicators['volume_signal'] = 'VERY_HIGH'
                        elif volume_ratio >= 2.0:
                            indicators['volume_signal'] = 'HIGH'
                        elif volume_ratio <= 0.5:
                            indicators['volume_signal'] = 'LOW'
                        else:
                            indicators['volume_signal'] = 'NORMAL'

            except Exception as e:
                logger.debug(f"거래량 분석 오류 ({stock_code}): {e}")

            logger.debug(f"📊 {stock_code} 시장지표: RSI={indicators['rsi']:.1f}, "
                        f"이격도={indicators['disparity_signal']}, "
                        f"거래량={indicators['volume_signal']}")

            return indicators

        except Exception as e:
            logger.error(f"시장 지표 조회 오류 ({stock_code}): {e}")
            return {
                'rsi': 50,
                'disparity_signal': 'NEUTRAL',
                'volume_signal': 'NORMAL',
                'market_sentiment': 'NEUTRAL'
            }

    def _calculate_simple_rsi(self, prices: list, period: int = 14) -> Optional[float]:
        """🆕 간단한 RSI 계산"""
        try:
            if len(prices) < period + 1:
                return None

            # 가격 변화량 계산
            price_changes = []
            for i in range(1, len(prices)):
                change = prices[i] - prices[i-1]
                price_changes.append(change)

            if len(price_changes) < period:
                return None

            # 최근 period개 변화량
            recent_changes = price_changes[-period:]

            # 상승/하락 평균 계산
            gains = [change for change in recent_changes if change > 0]
            losses = [-change for change in recent_changes if change < 0]

            avg_gain = sum(gains) / period if gains else 0
            avg_loss = sum(losses) / period if losses else 0

            if avg_loss == 0:
                return 100  # 모두 상승

            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

            return rsi

        except Exception as e:
            logger.debug(f"RSI 계산 오류: {e}")
            return None

    # =============================================================================
    # 🆕 거래 실행 시 DB 저장
    # =============================================================================

    def record_trade_execution(self, stock_code: str, trade_type: str,
                              quantity: int, price: int, total_amount: int,
                              decision_reason: str, order_id: Optional[str] = None,
                              **additional_data) -> Optional[int]:
            """🆕 거래 실행 시 DB 기록"""
            try:
                if not self.trade_db:
                    return None

                # 후보 정보 찾기
                candidate = None
                candidate_id = None

                if stock_code in self._all_stocks:
                    candidate = self._all_stocks[stock_code]
                    candidate_id = getattr(candidate, 'db_id', None)

                # DB에서 후보 찾기 (백업)
                if not candidate_id:
                    db_candidate = self.trade_db.find_candidate_by_stock_code(stock_code, 'WATCHING')
                    if db_candidate:
                        candidate_id = db_candidate['id']

                # candidate_id가 여전히 None이면 기본값 설정
                if candidate_id is None:
                    candidate_id = 0  # 기본값으로 0 설정

                stock_name = candidate.stock_name if candidate else additional_data.get('stock_name', f'종목{stock_code}')
                # ✅ pattern_type 속성 오류 수정 - detected_patterns 사용
                if candidate and candidate.detected_patterns:
                    pattern_matched = str(candidate.detected_patterns[0].pattern_type.value)
                else:
                    pattern_matched = additional_data.get('pattern_type', 'UNKNOWN')

                # 거래 기록
                trade_id = self.trade_db.record_candle_trade(
                    candidate_id=candidate_id,
                    trade_type=trade_type,
                    stock_code=stock_code,
                    stock_name=stock_name,
                    quantity=quantity,
                    price=price,
                    total_amount=total_amount,
                    decision_reason=decision_reason,
                    pattern_matched=pattern_matched,
                    order_id=order_id or "",  # ✅ None이면 빈 문자열로 변환
                    **additional_data
                )

                logger.info(f"🗄️ {stock_code} {trade_type} 거래 DB 기록 완료 (ID: {trade_id})")
                return trade_id

            except Exception as e:
                logger.error(f"❌ {stock_code} 거래 DB 기록 실패: {e}")
                return None

    def record_market_scan_results(self, market_type: str, scan_start_time: datetime,
                                  total_scanned: int, candidates_found: int,
                                  patterns_detected: int) -> Optional[int]:
            """🆕 시장 스캔 결과 기록"""
            try:
                if not self.trade_db:
                    return None

                scan_duration = int((datetime.now() - scan_start_time).total_seconds())

                scan_id = self.trade_db.record_market_scan(
                    market_type=market_type,
                    scan_duration=scan_duration,
                    total_stocks_scanned=total_scanned,
                    candidates_found=candidates_found,
                    patterns_detected=patterns_detected,
                    market_sentiment='NEUTRAL',  # 향후 개선
                    volatility_level='MEDIUM',   # 향후 개선
                    scan_config=self.config
                )

                logger.info(f"🗄️ {market_type} 시장 스캔 DB 기록 완료 (ID: {scan_id})")
                return scan_id

            except Exception as e:
                logger.error(f"❌ 시장 스캔 DB 기록 실패: {e}")
                return None

    # =============================================================================
    # 🆕 DB 조회 메서드들
    # =============================================================================

    def get_candle_trading_history(self, days: int = 7) -> dict:
        """�� 캔들 트레이딩 히스토리 조회"""
        try:
            if not self.trade_db:
                return {'error': 'Database not available'}

            # 후보 종목들
            candidates = self.trade_db.get_candle_candidates(days=days)

            # 거래 기록들
            trades = self.trade_db.get_candle_trades(days=days)

            # 성과 통계
            performance = self.trade_db.get_candle_performance_stats(days=days)

            return {
                'period_days': days,
                'candidates': candidates,
                'trades': trades,
                'performance': performance,
                'summary': {
                    'total_candidates': len(candidates),
                    'total_trades': len(trades),
                    'win_rate': performance.get('win_rate', 0),
                    'total_profit_loss': performance.get('total_profit_loss', 0)
                }
            }

        except Exception as e:
            logger.error(f"❌ 캔들 트레이딩 히스토리 조회 오류: {e}")
            return {'error': str(e)}

    def get_trading_insights(self) -> dict:
        """🆕 트레이딩 인사이트 제공"""
        try:
            if not self.trade_db:
                return {'error': 'Database not available'}

            # 최근 성과
            performance_7d = self.trade_db.get_candle_performance_stats(days=7)
            performance_30d = self.trade_db.get_candle_performance_stats(days=30)

            # 현재 후보들
            watching_candidates = self.trade_db.get_candle_candidates(status='WATCHING', days=1)

            # 패턴별 성과 분석
            best_patterns = []
            if performance_30d.get('pattern_performance'):
                best_patterns = sorted(
                    performance_30d['pattern_performance'],
                    key=lambda x: x['avg_return'],
                    reverse=True
                )[:3]

            return {
                'current_status': {
                    'watching_candidates': len(watching_candidates),
                    'active_positions': len(self._existing_holdings)
                },
                'performance_comparison': {
                    '7_days': {
                        'trades': performance_7d.get('total_trades', 0),
                        'win_rate': performance_7d.get('win_rate', 0),
                        'profit_loss': performance_7d.get('total_profit_loss', 0)
                    },
                    '30_days': {
                        'trades': performance_30d.get('total_trades', 0),
                        'win_rate': performance_30d.get('win_rate', 0),
                        'profit_loss': performance_30d.get('total_profit_loss', 0)
                    }
                },
                'best_patterns': best_patterns,
                'recommendations': self._generate_trading_recommendations(performance_30d)
            }

        except Exception as e:
            logger.error(f"❌ 트레이딩 인사이트 조회 오류: {e}")
            return {'error': str(e)}

    def _generate_trading_recommendations(self, performance_data: dict) -> List[str]:
        """트레이딩 추천사항 생성"""
        recommendations = []

        try:
            win_rate = performance_data.get('win_rate', 0)
            total_trades = performance_data.get('total_trades', 0)

            if total_trades == 0:
                recommendations.append("아직 거래 데이터가 충분하지 않습니다")
            elif win_rate < 40:
                recommendations.append("승률이 낮습니다. 진입 조건을 더 까다롭게 설정해보세요")
            elif win_rate > 70:
                recommendations.append("좋은 성과입니다! 현재 전략을 유지하세요")
            else:
                recommendations.append("적정한 성과를 보이고 있습니다")

            # 패턴별 분석
            if performance_data.get('pattern_performance'):
                best_pattern = max(
                    performance_data['pattern_performance'],
                    key=lambda x: x.get('avg_return', 0)
                )
                if best_pattern:
                    recommendations.append(f"가장 좋은 패턴: {best_pattern.get('pattern_type', 'UNKNOWN')}")

        except Exception:
            recommendations.append("추천사항 생성 중 오류 발생")

        return recommendations
