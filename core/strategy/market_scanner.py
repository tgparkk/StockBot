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
    CandlePatternInfo, EntryConditions, RiskManagement
)
from .price_position_filter import PricePositionFilter
from .pattern_manager import PatternManager
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

        # 🆕 PatternManager 초기화 (시간대별 전략 자동 전환)
        self.pattern_manager = PatternManager()

        self._last_scan_time: Optional[datetime] = None
        self._scan_interval = 30  # 30초
        
        # 🆕 가격 위치 필터 초기화
        self.price_position_filter = PricePositionFilter(self.config)

        logger.info("✅ MarketScanner 초기화 완료 (PatternManager 포함)")

    def _get_current_strategy_source(self) -> str:
        """🆕 현재 시간대에 따른 전략 소스 결정"""
        try:
            current_time = datetime.now().time()
            
            # 08:00-08:59: 장전 전략
            if current_time >= datetime.strptime("08:00", "%H:%M").time() and current_time <= datetime.strptime("08:59", "%H:%M").time():
                return "premarket"
            
            # 09:00-15:30: 실시간 전략
            elif current_time >= datetime.strptime("09:00", "%H:%M").time() and current_time <= datetime.strptime("15:30", "%H:%M").time():
                return "realtime"
            
            # 15:31-07:59: 장전 전략 (다음날 준비)
            else:
                return "premarket"
                
        except Exception as e:
            logger.error(f"전략 소스 결정 오류: {e}")
            return "premarket"  # 기본값

    async def scan_and_detect_patterns(self):
        """🚀 스마트 종목 스캔 - 장전 전체 스캔 vs 장중 급등/급증 모니터링"""
        try:
            current_time = datetime.now()
            current_hour = current_time.hour
            current_minute = current_time.minute

            # 🎯 1. 장전 전체 스캔 (08:30 - 08:50)
            if 8 <= current_hour < 9 and 30 <= current_minute <= 50:
                logger.info("🌅 장전 전체 KOSPI 스캔 시작")
                await self.scan_market_for_patterns("0001")  # KOSPI만
                
            # 🎯 2. 장중 급등/급증 종목 모니터링 (09:00 - 15:30)
            elif 9 <= current_hour < 15 or (current_hour == 15 and current_minute <= 30):
                logger.debug("📈 장중 급등/급증 종목 모니터링")
                await self.scan_intraday_movers("0001")  # 새로운 함수
                
            # 🎯 3. 장후에는 스캔 안함 (15:30 이후)
            else:
                logger.debug("🌙 장후 시간 - 스캔 생략")
                await self.scan_intraday_movers("0001")  # 새로운 함수
                return

            self._last_scan_time = current_time
            logger.debug("✅ 종목 스캔 완료")

        except Exception as e:
            logger.error(f"종목 스캔 오류: {e}")

    async def scan_intraday_movers(self, market: str):
        """🆕 장중 급등/급증 종목 모니터링 (기존 API 활용)"""
        try:
            market_name = "코스피" if market == "0001" else "코스닥"
            logger.debug(f"📈 {market_name} 장중 급등/급증 종목 모니터링")

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
            unique_candidates = list(set(candidates))[:50]  # 최대 50개

            if not unique_candidates:
                logger.debug(f"📊 {market_name} 장중 급등/급증 종목 없음")
                return

            logger.info(f"📊 {market_name} 장중 급등/급증 후보: {len(unique_candidates)}개")

            # 2. 후보 종목들에 대해 빠른 패턴 분석
            new_candidates_count = 0
            for stock_code in unique_candidates:
                try:
                    # 🚨 이미 보유/주문 중인 종목은 스캔에서 제외 (중복 매수 방지)
                    skip_analysis = False

                    if stock_code in self.manager.stock_manager._all_stocks:
                        
                        existing_candidate = self.manager.stock_manager._all_stocks[stock_code]
                        
                        # 🚨 이미 보유/주문 중인 종목은 스캔에서 완전 제외 (중복 매수 방지)
                        if existing_candidate.status in [CandleStatus.ENTERED, CandleStatus.PENDING_ORDER]:
                            logger.debug(f"🚫 {stock_code} 이미 보유/주문 중 - 스캔 제외 ({existing_candidate.status.value})")
                            skip_analysis = True
                        
                        # 🔧 EXITED 상태도 스캔에서 제외 (당일 재매수 방지)
                        elif existing_candidate.status == CandleStatus.EXITED:
                            logger.debug(f"🚫 {stock_code} 당일 매도 완료 종목 - 스캔 제외 (재매수 방지)")
                            skip_analysis = True
                        
                        # 🔄 WATCHING, SCANNING, BUY_READY 상태는 신호 업데이트를 위해 분석 계속
                        else:
                            logger.debug(f"🔄 {stock_code} 기존 관리 종목 신호 업데이트: {existing_candidate.status.value}")
                        
                    
                    if skip_analysis:
                        continue

                    # 빠른 패턴 분석
                    candidate = await self.analyze_stock_for_patterns(stock_code, market_name)
                    
                    # 🆕 현재 시간대에 따른 전략 소스 결정
                    strategy_source = self._get_current_strategy_source()
                    
                    if candidate and self.stock_manager.add_candidate(candidate, strategy_source=strategy_source):
                        new_candidates_count += 1
                        logger.debug(f"✅ 장중 신규 후보: {candidate.stock_code}({candidate.stock_name}) - 전략:{strategy_source}")

                except Exception as e:
                    logger.debug(f"장중 종목 분석 오류 ({stock_code}): {e}")
                    continue

            logger.info(f"🎯 {market_name} 장중 신규 후보: {new_candidates_count}개 추가")

        except Exception as e:
            logger.error(f"장중 급등/급증 모니터링 오류: {e}")

    async def scan_market_for_patterns(self, market: str):
        """🆕 전체 KOSPI 종목 대상 캔들 패턴 스캔 - 새로운 방식"""
        try:
            # 🆕 KOSPI만 지원 (코스닥은 추후 확장)
            if market != "0001":
                logger.info(f"⏩ {market} 시장은 현재 지원하지 않습니다. KOSPI만 지원.")
                return

            market_name = "코스피"
            logger.info(f"📊 {market_name} 전체 종목 캔들 패턴 스캔 시작")

            # 🆕 1. 전체 KOSPI 종목 리스트 로드
            from ..utils.stock_list_loader import load_kospi_stocks
            all_kospi_stocks = load_kospi_stocks()

            if not all_kospi_stocks:
                logger.error("❌ KOSPI 종목 리스트 로드 실패")
                return

            logger.info(f"📋 전체 KOSPI 종목: {len(all_kospi_stocks)}개")

            # 🆕 2. 성능 최적화된 종목 스크리닝 (30분 → 10분)
            candidates_with_scores = []
            processed_count = 0
            batch_size = 20  # 🚀 배치 크기 증가 (10 → 20)

            # 배치 단위로 처리
            for batch_start in range(0, len(all_kospi_stocks), batch_size):
                batch_end = min(batch_start + batch_size, len(all_kospi_stocks))
                batch_stocks = all_kospi_stocks[batch_start:batch_end]

                logger.debug(f"📊 배치 처리: {batch_start+1}-{batch_end}/{len(all_kospi_stocks)} "
                           f"종목 ({len(batch_stocks)}개)")

                # 배치 내 종목들 병렬 처리
                batch_results = await self.process_full_screening_batch(batch_stocks, market_name)

                # 패턴이 감지된 종목들 수집
                for result in batch_results:
                    if result and result['candidate'] and result['pattern_score'] > 0:
                        candidates_with_scores.append(result)

                processed_count += len(batch_stocks)

                # 진행률 로깅 (100개마다)
                if processed_count % 100 == 0:
                    logger.info(f"🔄 진행률: {processed_count}/{len(all_kospi_stocks)} "
                               f"({processed_count/len(all_kospi_stocks)*100:.1f}%) "
                               f"- 현재 후보: {len(candidates_with_scores)}개")

                # 🚀 API 대기 시간 최적화 (300ms → 100ms)
                if batch_end < len(all_kospi_stocks):
                    await asyncio.sleep(0.1)  # 100ms 대기 (초당 20회 제한 준수)

            # 🆕 3. 패턴 점수 기준으로 상위 50개 선별
            candidates_with_scores.sort(key=lambda x: x['pattern_score'], reverse=True)
            top_candidates = candidates_with_scores[:50]  # 상위 50개만

            logger.info(f"🎯 {market_name} 패턴 분석 완료: "
                       f"전체 {len(candidates_with_scores)}개 중 상위 {len(top_candidates)}개 선별")

            # 🆕 4. 선별된 후보들을 스톡 매니저에 추가
            pattern_found_count = 0
            strategy_source = self._get_current_strategy_source()  # 🆕 전략 소스 결정
            
            for result in top_candidates:
                candidate = result['candidate']
                if self.stock_manager.add_candidate(candidate, strategy_source=strategy_source):
                    pattern_found_count += 1
                    logger.debug(f"✅ {candidate.stock_code}({candidate.stock_name}) "
                               f"패턴점수: {result['pattern_score']:.2f} - 전략:{strategy_source}")

            logger.info(f"🏆 {market_name} 최종 후보: {pattern_found_count}개 종목 추가 (전략:{strategy_source})")

        except Exception as e:
            logger.error(f"시장 {market} 전체 스캔 오류: {e}")
            import traceback
            traceback.print_exc()

    async def process_full_screening_batch(self, stock_codes: List[str], market_name: str) -> List[Optional[Dict]]:
        """🆕 전체 스크리닝 배치 처리 (기본 필터링 + 패턴 분석)"""
        import asyncio

        try:
            # 배치 내 모든 종목을 비동기로 동시 처리
            tasks = [
                self.analyze_stock_with_full_screening(stock_code, market_name)
                for stock_code in stock_codes
            ]

            # 모든 작업이 완료될 때까지 대기
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # 성공한 결과만 필터링
            valid_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.debug(f"종목 {stock_codes[i]} 전체 스크리닝 실패: {result}")
                    valid_results.append(None)
                else:
                    valid_results.append(result)

            return valid_results

        except Exception as e:
            logger.error(f"전체 스크리닝 배치 처리 오류: {e}")
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
            stock_name = current_info.iloc[0].get('prdt_name', f'{stock_code}')

            if current_price <= 0:
                return None

            # 2. 기본 필터링
            if not self._passes_basic_filters(current_price, current_info.iloc[0].to_dict()):
                return None

            # 🆕 3. 🚀 고성능 OHLCV 데이터 준비 (캐시 우선 + 에러 핸들링)
            ohlcv_data = None
            use_cached_data = False

            # 🚀 candle_trade_manager의 stock_manager._all_stocks에서 캐시된 데이터 우선 확인
            if stock_code in self.manager.stock_manager._all_stocks:
                existing_candidate = self.manager.stock_manager._all_stocks[stock_code]

                # 🔧 중요한 상태(ENTERED, PENDING_ORDER)는 스캔에서 제외
                if existing_candidate.status in [CandleStatus.ENTERED, CandleStatus.PENDING_ORDER]:
                    return None  # 로깅 제거로 성능 향상

                # 🔄 다른 상태는 캐시된 데이터 사용해서 패턴 업데이트 진행
                ohlcv_data = existing_candidate.get_ohlcv_data()
                if ohlcv_data is not None and not ohlcv_data.empty:
                    use_cached_data = True

                    
            # 캐시에 없으면 API 호출 (timeout 설정으로 성능 향상)
            if ohlcv_data is None or ohlcv_data.empty:
                try:
                    from ..api.kis_market_api import get_inquire_daily_itemchartprice
                    ohlcv_data = get_inquire_daily_itemchartprice(
                        output_dv="2",  # ✅ output2 데이터 (일자별 차트 데이터 배열) 조회
                        itm_no=stock_code,
                        period_code="D",  # 일봉
                        adj_prc="1"
                    )
                except Exception as e:
                    # 🚀 API 오류 시 빠른 실패로 성능 확보
                    return None

                # 🆕 API 조회 성공시 로그
                if ohlcv_data is not None and not ohlcv_data.empty:
                    logger.debug(f"📥 {stock_code} API로 일봉 데이터 조회 완료")
                else:
                    logger.debug(f"❌ {stock_code} 일봉 데이터 조회 실패")

            # ✅ DataFrame ambiguous 오류 해결
            if ohlcv_data is None or ohlcv_data.empty:
                logger.debug(f"{stock_code}: OHLCV 데이터 없음")
                return None

            # 4. 🆕 PatternManager를 통한 시간대별 패턴 분석
            current_strategy_source = self._get_current_strategy_source()
            
            # 분봉 데이터 준비 (실시간 전략인 경우)
            minute_data = None
            if current_strategy_source == "realtime":
                try:
                    from ..api.kis_market_api import get_inquire_time_itemchartprice
                    from datetime import datetime, timedelta

                    # 🔧 현실적 제한: 최대 30분봉만 조회 가능
                    now = datetime.now()
                    thirty_minutes_ago = now - timedelta(minutes=30)
                    input_hour = thirty_minutes_ago.strftime("%H%M%S")

                    minute_data = get_inquire_time_itemchartprice(
                        output_dv="2",              # 분봉 데이터 배열
                        div_code="J",               # 주식
                        itm_no=stock_code,
                        input_hour=input_hour,      # 30분 전부터 조회
                        past_data_yn="Y",           # 과거데이터포함
                        etc_cls_code=""             # 기타구분코드
                    )
                    if minute_data is not None and not minute_data.empty:
                        # 최신순 정렬
                        minute_data = minute_data.sort_values('stck_cntg_hour', ascending=False).reset_index(drop=True)
                        logger.debug(f"📊 {stock_code} 분봉 데이터 조회 성공: {len(minute_data)}개 (최대 30분)")
                    else:
                        logger.debug(f"📊 {stock_code} 분봉 데이터 없음")
                        minute_data = None
                except Exception as e:
                    logger.debug(f"📊 {stock_code} 분봉 데이터 조회 실패: {e}")
                    minute_data = None
            
            # PatternManager 통합 분석
            pattern_analysis = self.pattern_manager.analyze_patterns(
                stock_code=stock_code,
                current_price=current_price,
                daily_ohlcv=ohlcv_data,
                minute_data=minute_data,
                mode=current_strategy_source
            )
            
            pattern_result = pattern_analysis.get('patterns', [])
            if not pattern_result or len(pattern_result) == 0:
                return None

            # 5. 가장 강한 패턴 선택
            strongest_pattern = max(pattern_result, key=lambda p: p.strength)
            
            logger.debug(f"🔍 {stock_code} 패턴 분석 완료: {pattern_analysis.get('mode')} 모드, "
                        f"{len(pattern_result)}개 패턴, 감지기: {pattern_analysis.get('detector_used')}")

            # 6. 후보 생성
            candidate = CandleTradeCandidate(
                stock_code=stock_code,
                stock_name=stock_name,
                current_price=int(current_price),
                market_type=market_name  # 시장 타입 추가
            )

            # 🆕 조회한 일봉 데이터를 새로운 candidate에 캐싱
            if ohlcv_data is not None:
                candidate.cache_ohlcv_data(ohlcv_data)

            # 🆕 분봉 데이터도 candidate에 캐싱 (실시간 전략용)
            if minute_data is not None and not minute_data.empty:
                # 분봉 데이터를 메타데이터에 저장
                if not hasattr(candidate, 'metadata') or candidate.metadata is None:
                    candidate.metadata = {}
                candidate.metadata['minute_data_cached'] = True
                candidate.metadata['minute_data_count'] = len(minute_data)
                
                # 🆕 분봉 데이터 캐싱 메서드 추가 (CandleTradeCandidate에 필요)
                if hasattr(candidate, 'cache_minute_data'):
                    candidate.cache_minute_data(minute_data)
                else:
                    # 임시로 메타데이터에 저장
                    candidate.metadata['minute_ohlcv'] = minute_data.to_dict('records')
                
                logger.debug(f"📊 {stock_code} 분봉 데이터 캐싱 완료: {len(minute_data)}개")

            # 패턴 정보 추가
            for pattern in pattern_result:
                candidate.add_pattern(pattern)

            # 🆕 PatternManager 결과에서 매매 신호 가져오기
            trade_signal = pattern_analysis.get('trade_signal', TradeSignal.HOLD)
            signal_strength = pattern_analysis.get('signal_strength', 0)
            
            candidate.trade_signal = trade_signal
            candidate.signal_strength = signal_strength
            candidate.signal_updated_at = datetime.now()
            
            # 🆕 매수 신호 발생시 상세 로깅 추가
            if trade_signal in [TradeSignal.STRONG_BUY, TradeSignal.BUY]:
                #logger.info(f"🚀 {stock_code} 매수 신호 발생! 신호:{trade_signal.value}, 강도:{signal_strength}, "
                #           f"패턴:{strongest_pattern.pattern_type.value}, 신뢰도:{strongest_pattern.confidence:.2f}")
                
                # 진입 조건 사전 체크 로깅
                logger.info(f"📋 {stock_code} 진입 조건 사전 체크:")
                logger.info(f"   - 현재 상태: {candidate.status.value}")
                logger.info(f"   - 매매 신호: {candidate.trade_signal.value}")
                logger.info(f"   - 신호 강도: {candidate.signal_strength}")
                logger.info(f"   - 패턴 수: {len(candidate.detected_patterns)}")
                logger.info(f"   - 전략 소스: {current_strategy_source}")

            # 🆕 전략 소스 메타데이터 추가
            if not hasattr(candidate, 'metadata') or candidate.metadata is None:
                candidate.metadata = {}
            candidate.metadata.update({
                'strategy_source': current_strategy_source,
                'detector_used': pattern_analysis.get('detector_used'),
                'analysis_mode': pattern_analysis.get('mode')
            })

            # 🆕 진입 우선순위 계산
            candidate.entry_priority = self.manager.candle_analyzer.calculate_entry_priority(candidate)

            # 🆕 리스크 관리 설정
            candidate.risk_management = self._calculate_risk_management(candidate)

            # 🆕 신호 정보 메타데이터에 저장 (신호 고정용)
            if not hasattr(candidate, 'metadata') or candidate.metadata is None:
                candidate.metadata = {}
            
            candidate.metadata.update({
                'pattern_detected_signal': candidate.trade_signal.value,
                'pattern_detected_strength': candidate.signal_strength,
                'pattern_detected_time': datetime.now().isoformat(),
                'pattern_detected_price': candidate.current_price,
                'signal_locked': True,  # 🔒 신호 고정 플래그
                'lock_reason': f'패턴감지시점_신호고정_{strongest_pattern.pattern_type.value}'
            })

            # 🎯 패턴 감지 성공 - 후보 종목으로 등록
            success = self.manager.stock_manager.add_candidate(candidate, strategy_source=current_strategy_source)
            
            if success:
                logger.info(f"✅ {stock_code}({stock_name}) 패턴 감지: {strongest_pattern.description} 흐름: {strongest_pattern.pattern_type.value} 신뢰도:{strongest_pattern.confidence:.2f} 강도:{strongest_pattern.strength}점")
                return candidate
            else:
                logger.warning(f"⚠️ {stock_code}({stock_name}) 패턴 감지했으나 후보 등록 실패: {strongest_pattern.description}")
                # 🆕 등록 실패 이유 상세 분석
                existing_candidate = self.manager.stock_manager.get_stock(stock_code)
                if existing_candidate:
                    existing_status = existing_candidate.status.value
                    existing_source = existing_candidate.metadata.get('strategy_source', 'unknown') if existing_candidate.metadata else 'unknown'
                    logger.warning(f"   📋 기존 종목 정보: 상태={existing_status}, 전략소스={existing_source}")
                    
                    # 중요 상태인지 확인
                    if existing_candidate.status in [CandleStatus.ENTERED, CandleStatus.PENDING_ORDER]:
                        logger.warning(f"   🚨 중요 상태 보호로 인한 등록 거부")
                    
                    # 전략 소스 충돌인지 확인
                    if current_strategy_source != existing_source:
                        logger.warning(f"   🔄 전략 소스 충돌: {existing_source} → {current_strategy_source}")
                else:
                    logger.warning(f"   📊 관찰 한도 초과 또는 품질 기준 미달로 추정")
                
                return None

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

    def _generate_trade_signal(self, patterns: List[CandlePatternInfo]) -> Tuple[TradeSignal, int]:
        """패턴 기반 매매 신호 생성 - candle_analyzer로 위임"""
        return self.manager.candle_analyzer.generate_trade_signal_from_patterns(patterns)

    # _calculate_entry_priority 함수는 candle_analyzer.py로 이동됨

    def _calculate_risk_management(self, candidate: CandleTradeCandidate) -> RiskManagement:
        """리스크 관리 설정 계산"""
        try:
            current_price = candidate.current_price

            # 패턴별 포지션 크기 조정
            if candidate.primary_pattern:
                pattern_type = candidate.primary_pattern.pattern_type
                confidence = candidate.primary_pattern.confidence

                # 강한 패턴일수록 큰 포지션
                if pattern_type in [PatternType.BULLISH_ENGULFING]:
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
            # 기본값 설정 (패턴이 없는 경우)
            stop_loss_pct = 3.0  # 기본 3% 손절
            target_profit_pct = 3.0  # 기본 3% 목표

            # 🆕 패턴별 목표 설정 적용 (우선순위)
            if candidate.primary_pattern:
                pattern_name = candidate.primary_pattern.pattern_type.value.lower()
                pattern_config = self.config['pattern_targets'].get(pattern_name)

                if pattern_config:
                    target_profit_pct = pattern_config['target']
                    stop_loss_pct = pattern_config['stop']
                    logger.debug(f"📊 {candidate.stock_code} 패턴별 목표 적용: {pattern_name} - 목표:{target_profit_pct}%, 손절:{stop_loss_pct}%")
                else:
                    logger.debug(f"📊 {candidate.stock_code} 패턴별 설정 없음, 기본값 사용: 목표:{target_profit_pct}%, 손절:{stop_loss_pct}%")
            else:
                logger.debug(f"📊 {candidate.stock_code} 패턴 없음, 기본값 사용: 목표:{target_profit_pct}%, 손절:{stop_loss_pct}%")

            stop_loss_price = current_price * (1 - stop_loss_pct / 100)
            target_price = current_price * (1 + target_profit_pct / 100)

            # 추적 손절 설정
            trailing_stop_pct = stop_loss_pct * 0.6  # 손절의 60% 수준

            # 최대 보유 시간 (패턴별만 사용)
            max_holding_hours = 24  # 기본값 (패턴이 없는 경우)
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
                    elif candidate.primary_pattern.pattern_type == PatternType.BULLISH_ENGULFING:
                        max_holding_hours = 8   # 샛별형은 강력한 패턴
                    elif candidate.primary_pattern.pattern_type in [PatternType.HAMMER, PatternType.INVERTED_HAMMER]:
                        max_holding_hours = 4   # 망치형은 짧게
                    elif candidate.primary_pattern.pattern_type == PatternType.DOJI:
                        max_holding_hours = 2   # 도지는 매우 짧게
                    logger.debug(f"📊 {candidate.stock_code} 패턴별 기본 보유시간: {pattern_name} - {max_holding_hours}시간")
            else:
                logger.debug(f"📊 {candidate.stock_code} 패턴 없음, 기본 보유시간: {max_holding_hours}시간")

            # 위험도 점수 계산
            risk_score = self.manager.candle_analyzer.calculate_risk_score({'stck_prpr': current_price})

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

    # _calculate_risk_score 함수는 candle_analyzer.py로 이동됨

    async def analyze_stock_with_full_screening(self, stock_code: str, market_name: str) -> Optional[Dict]:
        """🆕 🚀 고성능 개별 종목 전체 스크리닝 (빠른 실패 + 캐시 활용)"""
        try:
            # 🚀 1. 엑셀에서 종목 기본 정보 조회 (빠른 실패)
            from ..utils.stock_list_loader import get_stock_info_from_excel
            stock_excel_info = get_stock_info_from_excel(stock_code)
            
            if not stock_excel_info:
                return None

            stock_name = stock_excel_info['stock_name_short']
            listed_shares = stock_excel_info['listed_shares']

            # 🚀 2. 현재가 조회 (timeout 처리)
            try:
                from ..api.kis_market_api import get_inquire_price
                current_info = get_inquire_price(itm_no=stock_code)
                
                if current_info is None or current_info.empty:
                    return None

                current_price = float(current_info.iloc[0].get('stck_prpr', 0))
                volume = int(current_info.iloc[0].get('acml_vol', 0))
                trading_value = int(current_info.iloc[0].get('acml_tr_pbmn', 0))
                
                if current_price <= 0:
                    return None

            except Exception:
                return None  # 빠른 실패

            # 🚀 3. 기본 필터링 조건 체크 (빠른 제외)
            if not self._passes_enhanced_basic_filters(
                current_price, volume, trading_value, listed_shares, stock_code
            ):
                return None

            # 🚀 4. 캐시 우선 일봉 데이터 조회
            ohlcv_data = None
            use_cached = False

            # 캐시 확인 (기존 candidate에서 OHLCV 데이터 재사용)
            if hasattr(self.manager, 'stock_manager') and hasattr(self.manager.stock_manager, '_all_stocks'):
                if stock_code in self.manager.stock_manager._all_stocks:
                    existing_candidate = self.manager.stock_manager._all_stocks[stock_code]
                    # 중요 상태 제외
                    if existing_candidate.status not in [CandleStatus.ENTERED, CandleStatus.PENDING_ORDER]:
                        cached_ohlcv = existing_candidate.get_ohlcv_data()
                        if cached_ohlcv is not None and not cached_ohlcv.empty and len(cached_ohlcv) >= 20:
                            ohlcv_data = cached_ohlcv
                            use_cached = True

            # 캐시 없으면 API 호출
            if ohlcv_data is None:
                try:
                    from ..api.kis_market_api import get_inquire_daily_itemchartprice
                    from datetime import datetime, timedelta
                    
                    # 시작일 (30거래일 전 approximate)
                    start_date = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d")
                    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")  # 당일 제외
                    
                    ohlcv_data = get_inquire_daily_itemchartprice(
                        output_dv="2",  # 일봉 데이터 배열
                        itm_no=stock_code,
                        inqr_strt_dt=start_date,
                        inqr_end_dt=end_date,
                        period_code="D",  # 일봉
                        adj_prc="1"       # 원주가
                    )
                except Exception:
                    return None  # 빠른 실패

            if ohlcv_data is None or ohlcv_data.empty or len(ohlcv_data) < 10:
                return None

            # 🚀 5. 거래량 필터링 (빠른 체크)
            if not self._check_recent_volume_filter(ohlcv_data):
                return None

            # 🆕 6. 가격 위치 안전성 체크 (고점 매수 방지)
            price_position_check = self.price_position_filter.check_price_position_safety(
                stock_code, current_price, ohlcv_data, {'rsi_value': None}
            )
            
            if not price_position_check['is_safe']:
                risk_factors = ', '.join(price_position_check['risk_factors'])
                logger.debug(f"🚫 {stock_code} 가격위치 필터링: {risk_factors}")
                return None
            elif price_position_check['risk_factors']:
                # 위험 요소가 있지만 통과한 경우 로깅
                position_summary = self.price_position_filter.get_position_summary(
                    price_position_check['position_scores']
                )
                logger.debug(f"⚠️ {stock_code} 가격위치 주의: {position_summary}")

            # 🚀 7. 캔들 패턴 분석 (에러 처리 강화)
            try:
                pattern_result = self.pattern_detector.analyze_stock_patterns(stock_code, ohlcv_data)
                
                if not pattern_result or len(pattern_result) == 0:
                    return None
            except Exception:
                return None  # 빠른 실패

            # 🚀 8. 패턴 점수 계산 (최적화)
            pattern_score = self._calculate_enhanced_pattern_score(pattern_result, ohlcv_data)
            
            if pattern_score < 0.3:  # 최소 점수 기준
                return None

            # 🚀 9. 후보 생성 (필수 데이터만)
            candidate = CandleTradeCandidate(
                stock_code=stock_code,
                stock_name=stock_name,
                current_price=current_price,
                market_type=market_name
            )

            # 패턴 정보 추가
            for pattern in pattern_result:
                candidate.add_pattern(pattern)

            # 일봉 데이터 캐싱
            candidate.cache_ohlcv_data(ohlcv_data)

            # 매매 신호 생성
            trade_signal, signal_strength = self._generate_trade_signal(pattern_result)
            candidate.trade_signal = trade_signal
            candidate.signal_strength = signal_strength
            candidate.signal_updated_at = datetime.now()

            # 진입 우선순위 계산
            candidate.entry_priority = self.manager.candle_analyzer.calculate_entry_priority(candidate)

            # 리스크 관리 설정
            candidate.risk_management = self._calculate_risk_management(candidate)

            # 🆕 신호 정보 메타데이터에 저장 (신호 고정용)
            if not hasattr(candidate, 'metadata') or candidate.metadata is None:
                candidate.metadata = {}
            
            candidate.metadata.update({
                'pattern_detected_signal': candidate.trade_signal.value,
                'pattern_detected_strength': candidate.signal_strength,
                'pattern_detected_time': datetime.now().isoformat(),
                'pattern_detected_price': candidate.current_price,
                'signal_locked': True,  # 🔒 신호 고정 플래그
                'lock_reason': f'패턴감지시점_신호고정_{strongest_pattern.pattern_type.value}'
            })

            # 🎯 패턴 감지 성공 - 후보 종목으로 등록
            success = self.manager.stock_manager.add_candidate(candidate, strategy_source=current_strategy_source)
            
            if success:
                logger.info(f"✅ {stock_code}({stock_name}) 패턴 감지: {strongest_pattern.description} 흐름: {strongest_pattern.pattern_type.value} 신뢰도:{strongest_pattern.confidence:.2f} 강도:{strongest_pattern.strength}점")
                return candidate
            else:
                logger.warning(f"⚠️ {stock_code}({stock_name}) 패턴 감지했으나 후보 등록 실패: {strongest_pattern.description}")
                # 🆕 등록 실패 이유 상세 분석
                existing_candidate = self.manager.stock_manager.get_stock(stock_code)
                if existing_candidate:
                    existing_status = existing_candidate.status.value
                    existing_source = existing_candidate.metadata.get('strategy_source', 'unknown') if existing_candidate.metadata else 'unknown'
                    logger.warning(f"   📋 기존 종목 정보: 상태={existing_status}, 전략소스={existing_source}")
                    
                    # 중요 상태인지 확인
                    if existing_candidate.status in [CandleStatus.ENTERED, CandleStatus.PENDING_ORDER]:
                        logger.warning(f"   🚨 중요 상태 보호로 인한 등록 거부")
                    
                    # 전략 소스 충돌인지 확인
                    if current_strategy_source != existing_source:
                        logger.warning(f"   🔄 전략 소스 충돌: {existing_source} → {current_strategy_source}")
                else:
                    logger.warning(f"   📊 관찰 한도 초과 또는 품질 기준 미달로 추정")
                
                return None

        except Exception as e:
            logger.error(f"❌ {stock_code} 패턴 분석 오류: {e}")
            return None

    def _passes_enhanced_basic_filters(self, current_price: float, volume: int, 
                                     trading_value: int, listed_shares: int, stock_code: str) -> bool:
        """🆕 강화된 기본 필터링 (시가총액, 거래량, 가격대 등)"""
        try:
            # 1. 가격대 필터 (1,000원 ~ 300,000원)
            if not (1000 <= current_price <= 300000):
                return False

            # 2. 일일 거래량 필터 (최소 10,000주)
            if volume < 10000:
                return False

            # 3. 일일 거래대금 필터 (최소 1억원)
            if trading_value < 100_000_000:
                return False

            # 4. 시가총액 필터 (1,000억 ~ 10조)
            if listed_shares > 0:
                market_cap = current_price * listed_shares
                if not (100_000_000_000 <= market_cap <= 10_000_000_000_000):
                    return False
            else:
                return False  # 상장주식수 정보 없으면 제외

            # 5. 상장주식수 필터 (최소 1,000만주)
            if listed_shares < 10_000_000:
                return False

            return True

        except Exception as e:
            logger.debug(f"❌ {stock_code} 기본 필터링 오류: {e}")
            return False

    def _check_recent_volume_filter(self, ohlcv_data) -> bool:
        """🆕 최근 5일 평균 거래량 필터링 (50,000주 이상)"""
        try:
            if len(ohlcv_data) < 5:
                return False

            # 최근 5일 거래량 (stck_vol 또는 acml_vol 컬럼)
            volume_col = 'stck_vol' if 'stck_vol' in ohlcv_data.columns else 'acml_vol'
            
            if volume_col not in ohlcv_data.columns:
                return False

            recent_5_days = ohlcv_data.head(5)  # 최신 데이터가 앞에 있다고 가정
            avg_volume = recent_5_days[volume_col].astype(int).mean()

            return avg_volume >= 50000

        except Exception as e:
            logger.debug(f"거래량 필터링 오류: {e}")
            return False

    def _calculate_enhanced_pattern_score(self, patterns: List[CandlePatternInfo], ohlcv_data) -> float:
        """🆕 강화된 패턴 점수 계산 (과거 흐름 + 최근 패턴)"""
        try:
            if not patterns or len(ohlcv_data) < 10:
                return 0.0

            # 1. 기본 패턴 점수 (기존 로직)
            strongest_pattern = max(patterns, key=lambda p: p.strength)
            base_score = strongest_pattern.strength / 100.0  # 0.0 ~ 1.0

            # 2. 패턴 신뢰도 가중치
            confidence_weight = strongest_pattern.confidence  # 0.0 ~ 1.0

            # 3. 최근 2일 패턴 완성도 체크
            recent_completion_score = self._check_recent_pattern_completion(ohlcv_data)

            # 4. 추세 일관성 점수 (과거 28일 vs 최근 2일)
            trend_consistency_score = self._check_trend_consistency(ohlcv_data)

            # 5. 거래량 증가 점수
            volume_increase_score = self._check_volume_increase_pattern(ohlcv_data)

            # 6. 종합 점수 계산 (가중 평균)
            final_score = (
                base_score * 0.4 +
                confidence_weight * 0.2 +
                recent_completion_score * 0.2 +
                trend_consistency_score * 0.1 +
                volume_increase_score * 0.1
            )

            return min(1.0, max(0.0, final_score))

        except Exception as e:
            logger.debug(f"패턴 점수 계산 오류: {e}")
            return 0.0

    def _check_recent_pattern_completion(self, ohlcv_data) -> float:
        """최근 2일 패턴 완성도 체크"""
        try:
            if len(ohlcv_data) < 3:
                return 0.0

            # 최근 3일 데이터 (비교용)
            recent_3 = ohlcv_data.head(3)

            # 가격 컬럼 확인
            price_col = 'stck_clpr' if 'stck_clpr' in recent_3.columns else 'close'
            if price_col not in recent_3.columns:
                return 0.0

            prices = recent_3[price_col].astype(float).tolist()

            # 상승 패턴 완성도 체크 (최근 2일 연속 상승)
            if len(prices) >= 3:
                if prices[0] > prices[1] > prices[2]:  # 2일 연속 상승
                    return 0.8
                elif prices[0] > prices[1]:  # 1일 상승
                    return 0.5

            return 0.2

        except Exception as e:
            logger.debug(f"최근 패턴 완성도 체크 오류: {e}")
            return 0.0

    def _check_trend_consistency(self, ohlcv_data) -> float:
        """추세 일관성 체크 (과거 28일 vs 최근 2일)"""
        try:
            if len(ohlcv_data) < 30:
                return 0.5  # 기본값

            # 가격 컬럼 확인
            price_col = 'stck_clpr' if 'stck_clpr' in ohlcv_data.columns else 'close'
            if price_col not in ohlcv_data.columns:
                return 0.5

            prices = ohlcv_data[price_col].astype(float)

            # 과거 28일 추세 (장기)
            long_term_start = prices.iloc[-28]
            long_term_end = prices.iloc[-3]  # 최근 2일 제외
            long_term_trend = (long_term_end - long_term_start) / long_term_start

            # 최근 2일 추세 (단기)
            short_term_start = prices.iloc[-2]
            short_term_end = prices.iloc[0]
            short_term_trend = (short_term_end - short_term_start) / short_term_start

            # 추세 일관성 (같은 방향이면 높은 점수)
            if long_term_trend > 0 and short_term_trend > 0:  # 둘 다 상승
                return 0.8
            elif long_term_trend < 0 and short_term_trend > 0:  # 반전 패턴
                return 0.6
            else:
                return 0.3

        except Exception as e:
            logger.debug(f"추세 일관성 체크 오류: {e}")
            return 0.5

    def _check_volume_increase_pattern(self, ohlcv_data) -> float:
        """거래량 증가 패턴 체크"""
        try:
            if len(ohlcv_data) < 10:
                return 0.5

            # 거래량 컬럼 확인
            volume_col = 'stck_vol' if 'stck_vol' in ohlcv_data.columns else 'volume'
            if volume_col not in ohlcv_data.columns:
                return 0.5

            volumes = ohlcv_data[volume_col].astype(int)

            # 최근 2일 평균 vs 과거 8일 평균 비교
            recent_avg = volumes.head(2).mean()
            past_avg = volumes.iloc[2:10].mean()

            volume_ratio = recent_avg / past_avg if past_avg > 0 else 1.0

            if volume_ratio >= 1.5:  # 50% 이상 증가
                return 0.8
            elif volume_ratio >= 1.2:  # 20% 이상 증가
                return 0.6
            else:
                return 0.3

        except Exception as e:
            logger.debug(f"거래량 증가 패턴 체크 오류: {e}")
            return 0.5

    # 상태 조회 메서드
    def get_scan_status(self) -> Dict[str, Any]:
        """스캔 상태 조회"""
        return {
            'last_scan_time': self._last_scan_time.strftime('%H:%M:%S') if self._last_scan_time else None,
            'scan_interval': self._scan_interval,
            'subscribed_stocks_count': len(self.subscribed_stocks)
        }
