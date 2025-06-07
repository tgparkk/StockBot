"""
시장 스캔 및 캔들 패턴 감지 전용 클래스
종목 스캔, 패턴 감지, 후보 생성 등을 담당
"""
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, TYPE_CHECKING
import pandas as pd

from .candle_trade_candidate import (
    CandleTradeCandidate, CandleStatus, TradeSignal, PatternType,
    CandlePatternInfo, RiskManagement
)
from utils.logger import setup_logger

# 순환 import 방지를 위한 TYPE_CHECKING 사용
if TYPE_CHECKING:
    from .candle_trade_manager import CandleTradeManager

logger = setup_logger(__name__)


class MarketScanner:
    """시장 스캔 및 캔들 패턴 감지 전용 클래스"""

    def __init__(self, candle_trade_manager: "CandleTradeManager"):
        """
        Args:
            candle_trade_manager: CandleTradeManager 인스턴스 (의존성 주입)
        """
        self.manager = candle_trade_manager

        # 편의를 위한 속성 참조
        self.pattern_detector = candle_trade_manager.pattern_detector
        self.stock_manager = candle_trade_manager.stock_manager
        self.config = candle_trade_manager.config
        self.trade_db = candle_trade_manager.trade_db
        self.websocket_manager = candle_trade_manager.websocket_manager
        self.subscribed_stocks = candle_trade_manager.subscribed_stocks
        self.korea_tz = candle_trade_manager.korea_tz

        self._last_scan_time: Optional[datetime] = None
        self._scan_interval = 60  # 1분

        logger.info("✅ MarketScanner 초기화 완료")

    async def scan_and_detect_patterns(self):
        """종목 스캔 및 패턴 감지 - 메인 진입점"""
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
                await self.scan_market_for_patterns(market)

            self._last_scan_time = current_time
            logger.info("✅ 종목 스캔 완료")

        except Exception as e:
            logger.error(f"종목 스캔 오류: {e}")

    async def scan_market_for_patterns(self, market: str):
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
                batch_results = await self.process_stock_batch(batch_stocks, market_name)

                # 성공적으로 패턴이 감지된 종목들을 스톡 매니저에 추가
                for candidate in batch_results:
                    if candidate and candidate.detected_patterns:
                        if self.stock_manager.add_candidate(candidate):
                            pattern_found_count += 1

                # 배치 간 간격 (API 부하 방지)
                if batch_end < len(unique_candidates):
                    await asyncio.sleep(0.3)  # 300ms 대기

            logger.info(f"🎯 {market_name} 패턴 감지: {pattern_found_count}개 종목")

        except Exception as e:
            logger.error(f"시장 {market} 스캔 오류: {e}")

    async def process_stock_batch(self, stock_codes: List[str], market_name: str) -> List[Optional[CandleTradeCandidate]]:
        """주식 배치 병렬 처리"""
        import asyncio

        try:
            # 배치 내 모든 종목을 비동기로 동시 처리
            tasks = [
                self.analyze_stock_for_patterns(stock_code, market_name)
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

    async def analyze_stock_for_patterns(self, stock_code: str, market_name: str) -> Optional[CandleTradeCandidate]:
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

    # 상태 조회 메서드
    def get_scan_status(self) -> Dict[str, Any]:
        """스캔 상태 조회"""
        return {
            'last_scan_time': self._last_scan_time.strftime('%H:%M:%S') if self._last_scan_time else None,
            'scan_interval': self._scan_interval,
            'subscribed_stocks_count': len(self.subscribed_stocks)
        }
