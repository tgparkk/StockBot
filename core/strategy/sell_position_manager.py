"""
매도 포지션 관리자
캔들 기반 매매 전략의 기존 포지션 관리를 담당
"""
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, TYPE_CHECKING
from utils.logger import setup_logger

if TYPE_CHECKING:
    from .candle_trade_manager import CandleTradeManager

from .candle_trade_candidate import CandleTradeCandidate, CandleStatus, RiskManagement, TradeSignal

logger = setup_logger(__name__)


def calculate_business_hours(start_time: datetime, end_time: datetime) -> float:
    """🕒 주말을 제외한 영업시간 계산 (시간 단위)"""
    try:
        # 시작시간이 종료시간보다 늦으면 0 반환
        if start_time >= end_time:
            return 0.0

        total_hours = 0.0
        current = start_time

        # 하루씩 계산하면서 주말 제외
        while current < end_time:
            # 현재 날짜의 요일 확인 (0=월요일, 6=일요일)
            weekday = current.weekday()

            # 주말(토요일=5, 일요일=6) 제외
            if weekday < 5:  # 월~금요일만
                # 하루의 끝 시간 계산
                day_end = current.replace(hour=23, minute=59, second=59, microsecond=999999)

                # 이 날에서 계산할 시간 범위
                day_start = current
                day_finish = min(day_end, end_time)

                # 이 날의 시간 추가
                day_hours = (day_finish - day_start).total_seconds() / 3600
                total_hours += day_hours

            # 다음 날로 이동
            current = (current + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

        return total_hours

    except Exception as e:
        logger.error(f"❌ 영업시간 계산 오류: {e}")
        # 오류시 기존 방식으로 폴백
        return (end_time - start_time).total_seconds() / 3600


def calculate_business_minutes(start_time: datetime, end_time: datetime) -> float:
    """🕒 주말을 제외한 영업시간 계산 (분 단위)"""
    return calculate_business_hours(start_time, end_time) * 60


class SellPositionManager:
    """매도 포지션 관리 및 매도 실행 관리자"""

    def __init__(self, candle_trade_manager: 'CandleTradeManager'):
        """
        Args:
            candle_trade_manager: CandleTradeManager 인스턴스 (필수)
        """
        if not candle_trade_manager:
            raise ValueError("candle_trade_manager는 필수 인자입니다.")

        self.manager = candle_trade_manager

        # 🚨 연속 조정 방지를 위한 이력 추적
        self._adjustment_history = {}  # {stock_code: {'last_adjustment_time', 'last_direction', 'adjustment_count'}}
        self._min_adjustment_interval = 300  # 최소 5분 간격
        self._max_consecutive_adjustments = 3  # 최대 연속 조정 횟수

        logger.info("✅ SellPositionManager 초기화 완료")

    async def manage_existing_positions(self):
        """기존 포지션 관리 (손절/익절/추적손절) - _all_stocks 통합 버전"""
        try:
            # 🆕 _all_stocks에서 ENTERED 상태인 모든 종목 관리 (기존 보유 + 새로 매수)
            # 🔧 더 강화된 필터링: 실제로 관리가 필요한 종목만 선별
            entered_positions = []
            for stock in self.manager.stock_manager._all_stocks.values():
                # 기본 상태 체크
                if stock.status != CandleStatus.ENTERED:
                    continue
                
                # 매도 체결 확인 완료된 종목 제외
                if stock.metadata.get('final_exit_confirmed', False):
                    continue
                
                # 자동 종료된 종목 제외
                if stock.metadata.get('auto_exit_reason'):
                    continue
                
                # PENDING_ORDER 상태로 변경된 종목 제외 (매도 주문 대기 중)
                if stock.status == CandleStatus.PENDING_ORDER:
                    continue
                
                entered_positions.append(stock)

            if not entered_positions:
                return

            logger.debug(f"📊 포지션 관리: {len(entered_positions)}개 포지션 (_all_stocks 통합, 필터링 강화)")

            for position in entered_positions:
                try:
                    await self._manage_single_position(position)
                except Exception as e:
                    logger.error(f"포지션 관리 오류 ({position.stock_code}): {e}")

        except Exception as e:
            logger.error(f"포지션 관리 오류: {e}")

    async def _manage_single_position(self, position: CandleTradeCandidate):
        """🆕 개별 포지션 관리 - candle_analyzer로 통합 위임"""
        try:
            # 🆕 EXITED나 PENDING_ORDER 상태 종목 스킵 (체결 통보 처리 완료된 종목)
            if position.status in [CandleStatus.EXITED, CandleStatus.PENDING_ORDER]:
                logger.debug(f"⏭️ {position.stock_code} {position.status.value} 상태 - 포지션 관리 생략")
                return

            # 🆕 체결 완료 확인된 종목 스킵 (추가 안전장치)
            if position.metadata.get('final_exit_confirmed', False):
                logger.debug(f"⏭️ {position.stock_code} 매도 체결 확인 완료 - 포지션 관리 생략")
                return

            # 🆕 자동 종료된 종목 스킵 (실제 보유 없음으로 인한 자동 종료)
            if position.metadata.get('auto_exit_reason'):
                logger.debug(f"⏭️ {position.stock_code} 자동 종료됨 ({position.metadata['auto_exit_reason']}) - 포지션 관리 생략")
                return

            # 🆕 실제 보유 여부 사전 체크 (매번 API 호출하지 않고 캐시 활용)
            if hasattr(position, '_last_holding_check'):
                last_check_time = position._last_holding_check.get('time', datetime.min)
                if (datetime.now() - last_check_time).total_seconds() < 60:  # 1분 이내 체크했으면 스킵
                    if not position._last_holding_check.get('has_holding', True):
                        logger.debug(f"⏭️ {position.stock_code} 최근 보유 확인 실패 - 포지션 관리 생략")
                        return

            current_price = position.current_price
            
            # 🆕 업데이트된 매도 신호 확인 (중복 분석 방지)
            trade_signal = position.trade_signal
            should_exit = trade_signal in [TradeSignal.STRONG_SELL, TradeSignal.SELL]
            
            if should_exit:
                # 신호 강도에 따른 매도 사유 결정
                if trade_signal == TradeSignal.STRONG_SELL:
                    exit_reason = f"강한 매도 신호 (강도: {position.signal_strength})"
                    exit_priority = "high"
                else:
                    exit_reason = f"매도 신호 (강도: {position.signal_strength})"
                    exit_priority = "normal"
                
                # 🔧 실시간 수익률 재계산하여 정확한 로깅
                entry_price = position.performance.entry_price
                if entry_price and entry_price > 0:
                    real_pnl_pct = ((current_price - entry_price) / entry_price) * 100
                    logger.info(f"📉 {position.stock_code} 매도 신호 감지 ({exit_priority}): {exit_reason} "
                               f"(실제수익률: {real_pnl_pct:+.2f}%, 현재가: {current_price:,.0f}원)")
                else:
                    logger.info(f"📉 {position.stock_code} 매도 신호 감지 ({exit_priority}): {exit_reason} "
                               f"(수익률계산불가, 현재가: {current_price:,.0f}원)")
                
                await self._execute_exit(position, current_price, exit_reason)
            else:
                # 🆕 동적 추적 손절 업데이트 (매도하지 않을 때만)
                if position.performance.entry_price:
                    #self._update_trailing_stop(position, current_price)
                    pass

        except Exception as e:
            logger.error(f"개별 포지션 관리 오류 ({position.stock_code}): {e}")

    async def _execute_exit(self, position: CandleTradeCandidate, exit_price: float, reason: str) -> bool:
        """매도 청산 실행 - 간소화된 버전"""
        try:
            # 🆕 사전 체크: 이미 EXITED 상태이거나 체결 완료 확인된 종목은 스킵
            if position.status == CandleStatus.EXITED or position.metadata.get('final_exit_confirmed', False):
                logger.debug(f"⏭️ {position.stock_code} 이미 매도 완료 - 실행 생략")
                return False
            
            # 🆕 실제 보유 여부 사전 체크 (API 오류 방지)
            try:
                from ..api.kis_market_api import get_existing_holdings
                holdings = get_existing_holdings()
                
                actual_holding = False
                actual_quantity = 0
                
                if holdings:
                    for holding in holdings:
                        if holding.get('stock_code') == position.stock_code:
                            actual_quantity = holding.get('quantity', 0)
                            if actual_quantity > 0:
                                actual_holding = True
                            break
                
                if not actual_holding or actual_quantity <= 0:
                    logger.warning(f"⚠️ {position.stock_code} 실제 보유 없음 (보유수량: {actual_quantity}) - EXITED 상태로 변경")
                    position.status = CandleStatus.EXITED
                    position.metadata['auto_exit_reason'] = '실제보유없음_자동종료'
                    position.metadata['final_exit_confirmed'] = True
                    self.manager.stock_manager.update_candidate(position)
                    return False
                    
                # 수량 불일치 확인
                system_quantity = position.performance.entry_quantity or 0
                if actual_quantity != system_quantity:
                    logger.warning(f"⚠️ {position.stock_code} 수량 불일치: 시스템={system_quantity}주, 실제={actual_quantity}주")
                    # 실제 수량으로 업데이트
                    position.performance.entry_quantity = actual_quantity
                    
            except Exception as e:
                logger.debug(f"실제 보유 확인 오류 ({position.stock_code}): {e}")
                # API 오류시에도 계속 진행 (기존 로직 유지)

            # 🕐 거래 시간 체크
            current_time = datetime.now().time()
            trading_start = datetime.strptime(self.manager.config['trading_start_time'], '%H:%M').time()
            trading_end = datetime.strptime(self.manager.config['trading_end_time'], '%H:%M').time()

            if not (trading_start <= current_time <= trading_end):
                logger.warning(f"⏰ {position.stock_code} 거래 시간 외 매도 차단 - {reason}")
                return False

            # 🔍 파라미터에서 직접 정보 추출 (검증 로직 간소화)
            stock_code = position.stock_code
            quantity = position.performance.entry_quantity or 0
            
            if quantity <= 0:
                logger.warning(f"❌ {position.stock_code} 매도할 수량 없음 ({quantity}주)")
                # 수량이 없으면 EXITED 상태로 변경
                position.status = CandleStatus.EXITED
                position.metadata['auto_exit_reason'] = '매도수량_없음'
                position.metadata['final_exit_confirmed'] = True
                self.manager.stock_manager.update_candidate(position)
                return False

            # 🆕 안전한 매도가 계산
            safe_sell_price = self._calculate_safe_sell_price(exit_price, reason)

            # 매도 신호 생성
            signal = {
                'stock_code': stock_code,
                'action': 'sell',
                'strategy': 'candle_pattern',
                'price': safe_sell_price,
                'quantity': quantity,
                'total_amount': int(safe_sell_price * quantity),
                'reason': reason,
                'pattern_type': str(position.detected_patterns[0].pattern_type) if position.detected_patterns else 'unknown',
                'pre_validated': True
            }

            # 🚀 매도 주문 실행
            if hasattr(self.manager, 'trade_executor') and self.manager.trade_executor:
                try:
                    result = self.manager.trade_executor.execute_sell_signal(signal)
                    if not result.success:
                        logger.error(f"❌ 매도 주문 실패: {stock_code} - {result.error_message}")
                        
                        # 🆕 특정 오류 코드에 대한 자동 처리
                        error_msg = result.error_message or ""
                        if "주문 가능한 수량을 초과" in error_msg or "APBK0400" in error_msg:
                            logger.warning(f"⚠️ {stock_code} 이미 매도 완료된 것으로 추정 - EXITED 상태로 변경")
                            position.status = CandleStatus.EXITED
                            position.metadata['auto_exit_reason'] = '주문수량초과오류_자동종료'
                            position.metadata['final_exit_confirmed'] = True
                            self.manager.stock_manager.update_candidate(position)
                        
                        return False

                    # 매도 주문 성공시 PENDING_ORDER 상태로 변경
                    order_no = getattr(result, 'order_no', None)
                    position.set_pending_order(order_no or f"sell_{datetime.now().strftime('%H%M%S')}", 'sell')

                    # 로깅
                    logger.info(f"📉 매도 주문 제출 성공: {stock_code}")
                    logger.info(f"   💰 기준가: {exit_price:,.0f}원 → 주문가: {safe_sell_price:,.0f}원")
                    logger.info(f"   📦 수량: {quantity:,}주 | 🆔 주문번호: {order_no}")
                    logger.info(f"   📋 매도사유: {reason}")

                    return True

                except Exception as e:
                    logger.error(f"❌ 매도 주문 실행 오류: {stock_code} - {e}")
                    return False

            logger.error(f"❌ TradeExecutor 없음: {stock_code}")
            return False

        except Exception as e:
            logger.error(f"❌ 매도 실행 오류 ({position.stock_code}): {e}")
            return False

    def _calculate_safe_sell_price(self, current_price: float, reason: str) -> int:
        """안전한 매도가 계산 (틱 단위 맞춤) - 개선된 버전"""
        try:
            # 매도 이유별 할인율 적용 (목표가 도달시 할인 최소화)
            if reason == "손절":
                discount_pct = 0.008  # 0.8% 할인 (빠른 체결 우선)
            elif reason in ["목표가 도달", "익절"]:
                discount_pct = 0.002  # 🎯 0.2% 할인으로 최소화 (수익 보호)
            elif reason == "시간 청산":
                discount_pct = 0.005  # 0.5% 할인 (중간 속도)
            else:
                discount_pct = 0.003  # 기본 0.3% 할인

            # 할인된 가격 계산
            target_price = int(current_price * (1 - discount_pct))

            # 틱 단위 맞춤
            tick_unit = self._get_tick_unit(target_price)
            safe_price = (target_price // tick_unit) * tick_unit

            # 🆕 목표가 도달시 최소 가격 보정 강화 (현재가의 99% 이상)
            if reason in ["목표가 도달", "익절"]:
                min_price = int(current_price * 0.99)  # 현재가의 99% 이상
            else:
                min_price = int(current_price * 0.97)  # 기본 97% 이상

            safe_price = max(safe_price, min_price)

            logger.debug(f"💰 매도가 계산 (개선): 현재가{current_price:,.0f}원 → 주문가{safe_price:,.0f}원 "
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

    def _update_trailing_stop(self, position: CandleTradeCandidate, current_price: float):
        """🔄 패턴 기반 동적 목표/손절 조정 시스템 (개선된 버전)"""
        try:
            # 🆕 캐시된 OHLCV 데이터 사용 (API 호출 제거)
            ohlcv_data = position.get_ohlcv_data()
            if ohlcv_data is None:
                logger.debug(f"📄 {position.stock_code} 캐시된 일봉 데이터 없음 - 기본 trailing stop 적용")
                # 캐시된 데이터가 없으면 기존 방식으로 폴백
                self._fallback_trailing_stop(position, current_price)
                return

            # 🆕 1단계: 실시간 캔들 패턴 재분석 (OHLCV 데이터 전달)
            pattern_update = self._analyze_realtime_pattern_changes(position.stock_code, current_price, ohlcv_data)

            # 🆕 2단계: 수익률 기반 동적 조정
            profit_based_update = self._calculate_profit_based_adjustments(position, current_price)

            # 🆕 3단계: 추세 강도 기반 조정 (OHLCV 데이터 전달)
            trend_based_update = self._calculate_trend_based_adjustments(position, current_price, ohlcv_data)

            # 🆕 4단계: 종합 판단 및 업데이트
            # self._apply_dynamic_adjustments(position, current_price, pattern_update, profit_based_update, trend_based_update)

        except Exception as e:
            logger.error(f"동적 목표/손절 조정 오류 ({position.stock_code}): {e}")
            # 기존 방식으로 폴백
            self._fallback_trailing_stop(position, current_price)

    def _analyze_realtime_pattern_changes(self, stock_code: str, current_price: float, ohlcv_data: Optional[Any] = None) -> Dict:
        """🔄 실시간 캔들 패턴 변화 분석"""
        try:
            # 🆕 전달받은 OHLCV 데이터만 사용 (API 호출 제거)
            if ohlcv_data is None or ohlcv_data.empty:
                logger.debug(f"📄 {stock_code} OHLCV 데이터 없음 - 패턴 분석 불가")
                return {'pattern_strength_changed': False, 'new_patterns': []}

            # 현재 패턴 분석
            current_patterns = self.manager.pattern_detector.analyze_stock_patterns(stock_code, ohlcv_data)

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
            # 🆕 전달받은 OHLCV 데이터만 사용 (API 호출 제거)
            if ohlcv_data is None or ohlcv_data.empty or len(ohlcv_data) < 5:
                logger.debug(f"📄 {position.stock_code} OHLCV 데이터 부족 - 추세 분석 불가")
                return {'trend_strength': 'NEUTRAL', 'trend_multiplier': 1.0}

            # 최근 5일 종가 추출
            recent_closes = []
            for _, row in ohlcv_data.head(5).iterrows():
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
        """🎯 동적 조정 적용 (연속 조정 방지 포함)"""
        try:
            entry_price = position.performance.entry_price
            if not entry_price:
                return

            # 🆕 현재 목표가/손절가 백업
            original_target = position.risk_management.target_price
            original_stop = position.risk_management.stop_loss_price

            # 🚨 1단계: 연속 조정 방지 검증
            target_multiplier = profit_update.get('target_multiplier', 1.0)
            trend_multiplier = trend_update.get('trend_multiplier', 1.0)

            # 조정 방향 결정
            will_increase_target = (target_multiplier > 1.0) or (trend_multiplier > 1.0)
            will_decrease_target = (target_multiplier < 1.0) or (trend_multiplier < 1.0)

            adjustment_direction = None
            if will_increase_target:
                adjustment_direction = "UP"
            elif will_decrease_target:
                adjustment_direction = "DOWN"
            else:
                adjustment_direction = "NEUTRAL"

            # 🚨 연속 조정 방지 검증
            if adjustment_direction != "NEUTRAL":
                if not self._can_apply_adjustment(position.stock_code, adjustment_direction):
                    logger.info(f"🛑 {position.stock_code} 연속 조정 방지 - 동적 조정 건너뜀")
                    return

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

            # 🆕 6단계: 변경사항 로깅 및 이력 기록
            if target_updated or stop_updated:
                pnl_pct = ((current_price - entry_price) / entry_price) * 100

                logger.info(f"🔄 {position.stock_code} 동적 조정 적용 (수익률: {pnl_pct:+.1f}%):")

                if target_updated:
                    target_change_pct = ((new_target_price - original_target) / original_target * 100) if original_target > 0 else 0
                    target_profit_pct = ((new_target_price - entry_price) / entry_price * 100) if entry_price > 0 else 0
                    logger.info(f"   📈 목표가: {original_target:,.0f}원 → {new_target_price:,.0f}원 "
                               f"(변화: {target_change_pct:+.1f}%, 목표수익: {target_profit_pct:+.1f}%)")

                if stop_updated:
                    stop_change_pct = ((new_stop_price - original_stop) / original_stop * 100) if original_stop > 0 else 0
                    stop_loss_pct = ((entry_price - new_stop_price) / entry_price * 100) if entry_price > 0 else 0
                    logger.info(f"   🛡️ 손절가: {original_stop:,.0f}원 → {new_stop_price:,.0f}원 "
                               f"(변화: {stop_change_pct:+.1f}%, 손절범위: {stop_loss_pct:+.1f}%)")

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

                # 🚨 조정 이력 기록
                if adjustment_direction != "NEUTRAL":
                    self._record_adjustment(position.stock_code, adjustment_direction)

        except Exception as e:
            logger.error(f"동적 조정 적용 오류 ({position.stock_code}): {e}")

    def _get_pattern_tier_targets(self, strength_tier: str) -> Tuple[float, float]:
        """패턴 강도 티어별 목표/손절 퍼센트 반환"""
        tier_settings = {
            'ULTRA_STRONG': (8.0, 4.0),    # 8% 목표, 4% 손절
            'STRONG': (6.0, 3.0),          # 6% 목표, 3% 손절
            'MEDIUM': (4.0, 3.0),          # 4% 목표, 3% 손절
            'WEAK': (2.0, 2.0),            # 2% 목표, 2% 손절
            'VERY_WEAK': (2.0, 1.5)        # 2% 목표, 1.5% 손절
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

    def _can_apply_adjustment(self, stock_code: str, adjustment_direction: str) -> bool:
        """🚨 연속 조정 방지 검증"""
        try:
            from datetime import datetime, timedelta

            current_time = datetime.now()

            # 이력이 없으면 허용
            if stock_code not in self._adjustment_history:
                return True

            history = self._adjustment_history[stock_code]
            last_time = history.get('last_adjustment_time')
            last_direction = history.get('last_direction')
            adjustment_count = history.get('adjustment_count', 0)

            # 시간 간격 체크 (최소 5분)
            if last_time and (current_time - last_time).total_seconds() < self._min_adjustment_interval:
                logger.warning(f"⏰ {stock_code} 조정 간격 부족 - 대기 중 (최소 {self._min_adjustment_interval}초)")
                return False

            # 연속 조정 방향 체크
            if last_direction == adjustment_direction:
                if adjustment_count >= self._max_consecutive_adjustments:
                    logger.warning(f"🔄 {stock_code} 연속 조정 한도 초과 ({adjustment_direction}) - 차단")
                    return False

            return True

        except Exception as e:
            logger.error(f"조정 검증 오류 ({stock_code}): {e}")
            return False

    def _record_adjustment(self, stock_code: str, adjustment_direction: str):
        """🚨 조정 이력 기록"""
        try:
            from datetime import datetime

            current_time = datetime.now()

            if stock_code not in self._adjustment_history:
                self._adjustment_history[stock_code] = {
                    'last_adjustment_time': current_time,
                    'last_direction': adjustment_direction,
                    'adjustment_count': 1
                }
            else:
                history = self._adjustment_history[stock_code]
                last_direction = history.get('last_direction')

                # 같은 방향이면 카운트 증가, 다른 방향이면 카운트 리셋
                if last_direction == adjustment_direction:
                    history['adjustment_count'] = history.get('adjustment_count', 0) + 1
                else:
                    history['adjustment_count'] = 1

                history['last_adjustment_time'] = current_time
                history['last_direction'] = adjustment_direction

            logger.debug(f"📝 {stock_code} 조정 이력 기록: {adjustment_direction} "
                        f"(연속: {self._adjustment_history[stock_code]['adjustment_count']}회)")

        except Exception as e:
            logger.error(f"조정 이력 기록 오류 ({stock_code}): {e}")

    def cleanup_adjustment_history(self):
        """🧹 오래된 조정 이력 정리 (1시간 이상 된 이력 제거)"""
        try:
            from datetime import datetime, timedelta

            current_time = datetime.now()
            cutoff_time = current_time - timedelta(hours=1)

            stocks_to_remove = []
            for stock_code, history in self._adjustment_history.items():
                last_time = history.get('last_adjustment_time')
                if last_time and last_time < cutoff_time:
                    stocks_to_remove.append(stock_code)

            for stock_code in stocks_to_remove:
                del self._adjustment_history[stock_code]
                logger.debug(f"🧹 {stock_code} 조정 이력 정리 완료")

            if stocks_to_remove:
                logger.info(f"🧹 조정 이력 정리: {len(stocks_to_remove)}개 종목")
                
        except Exception as e:
            logger.error(f"조정 이력 정리 오류: {e}")

    def _check_min_holding_time(self, position: CandleTradeCandidate, stop_loss_pct: float) -> Dict:
        """🆕 최소 보유시간 체크 (매수체결시간 기반 + 캔들전략 설정 + 주말 제외)"""
        try:
            # 🎯 매수체결시간 우선 사용, 없으면 entry_time 사용
            reference_time = position.performance.buy_execution_time or position.performance.entry_time

            if not reference_time:
                logger.warning(f"⚠️ {position.stock_code} 매수시간 정보 없음 - 매도 허용")
                return {'can_exit': True, 'reason': 'no_buy_time_info'}

            # 🔧 timezone 통일: 현재 시간을 한국 시간대로 설정
            current_time = datetime.now(self.manager.korea_tz)

            # reference_time이 naive datetime인 경우 한국 시간대로 변환
            if reference_time.tzinfo is None:
                reference_time = reference_time.replace(tzinfo=self.manager.korea_tz)

            # 🆕 현재 보유 시간 계산 (주말 제외)
            holding_hours = calculate_business_hours(reference_time, current_time)
            holding_minutes = holding_hours * 60

            # 🆕 2. 매수체결시간 기반 캔들전략 적용
            execution_strategy = self.manager.config.get('execution_time_strategy', {})
            if execution_strategy.get('use_execution_time', False) and position.performance.buy_execution_time:
                adjusted_min_minutes = self._calculate_execution_time_based_holding(position, reference_time)
                logger.debug(f"🕐 {position.stock_code} 매수체결시간 기반 최소시간: {adjusted_min_minutes}분")
            else:
                # 기존 패턴 기반 최소시간
                adjusted_min_minutes = self._get_pattern_min_holding_time(position)

            # 3. 최소 보유시간 체크 (영업일 기준)
            if holding_minutes < adjusted_min_minutes:
                remaining_minutes = adjusted_min_minutes - holding_minutes
                remaining_hours = remaining_minutes / 60

                time_source = "체결시간" if position.performance.buy_execution_time else "진입시간"
                logger.debug(f"⏰ {position.stock_code} 최소 보유시간 미달 ({time_source} 기준, 주말제외): "
                           f"{holding_hours:.1f}시간/{adjusted_min_minutes/60:.1f}시간 "
                           f"(남은시간: {remaining_hours:.1f}시간)")

                return {
                    'can_exit': False,
                    'reason': f'min_holding_time_business_days',
                    'detail': f'{holding_hours:.1f}h/{adjusted_min_minutes/60:.1f}h 보유 ({time_source}, 주말제외)',
                    'remaining_hours': remaining_hours,
                    'time_source': time_source,
                    'business_hours_only': True
                }

            # 4. 최소 보유시간 충족
            time_source = "체결시간" if position.performance.buy_execution_time else "진입시간"
            logger.debug(f"✅ {position.stock_code} 최소 보유시간 충족 ({time_source} 기준, 주말제외): "
                       f"{holding_hours:.1f}시간 (기준: {adjusted_min_minutes/60:.1f}시간)")
            return {'can_exit': True, 'reason': 'min_time_satisfied', 'time_source': time_source, 'business_hours_only': True}

        except Exception as e:
            logger.error(f"❌ 최소 보유시간 체크 오류 ({position.stock_code}): {e}")
            # 오류시 안전하게 매도 허용
            return {'can_exit': True, 'reason': 'error_fallback'}

    def _calculate_execution_time_based_holding(self, position: CandleTradeCandidate, buy_execution_time: datetime) -> float:
        """🕐 매수체결시간 기반 최소 보유시간 계산 (분 단위)"""
        try:
            execution_strategy = self.manager.config.get('execution_time_strategy', {})
            base_min_minutes = execution_strategy.get('min_holding_from_execution', 1440)  # 기본 24시간

            # 매수체결시간 분석
            buy_time = buy_execution_time.time()
            buy_hour = buy_time.hour
            buy_minute = buy_time.minute

            # 🌅 장 시작 시간 보너스 (09:00-11:00 매수시 추가 보유시간)
            early_bonus_hours = execution_strategy.get('early_morning_bonus_hours', 2)
            if 9 <= buy_hour <= 11:
                bonus_minutes = early_bonus_hours * 60
                base_min_minutes += bonus_minutes
                logger.debug(f"🌅 {position.stock_code} 장 시작 시간 매수 보너스: +{early_bonus_hours}시간")

            # 🌆 장 마감 시간 페널티 (14:00-15:20 매수시 보유시간 단축)
            late_penalty_hours = execution_strategy.get('late_trading_penalty_hours', -4)
            if (buy_hour == 14) or (buy_hour == 15 and buy_minute <= 20):
                penalty_minutes = abs(late_penalty_hours) * 60
                base_min_minutes = max(base_min_minutes - penalty_minutes, 720)  # 최소 12시간은 보장
                logger.debug(f"🌆 {position.stock_code} 장 마감 시간 매수 페널티: {late_penalty_hours}시간")

            # 📅 주말 갭 고려
            if execution_strategy.get('weekend_gap_consideration', True):
                buy_weekday = buy_execution_time.weekday()  # 0=월요일, 4=금요일

                # 금요일 매수시 주말을 고려해서 보유시간 연장
                if buy_weekday == 4:  # 금요일
                    weekend_bonus = 24 * 60  # 24시간 추가
                    base_min_minutes += weekend_bonus
                    logger.debug(f"📅 {position.stock_code} 금요일 매수 - 주말 갭 고려: +24시간")

            # 패턴별 최소시간과 비교해서 더 큰 값 사용
            pattern_min_minutes = self._get_pattern_min_holding_time(position)
            final_min_minutes = max(base_min_minutes, pattern_min_minutes)

            logger.debug(f"🕐 {position.stock_code} 매수체결시간 기반 최소시간 계산: "
                       f"기본{base_min_minutes/60:.1f}h vs 패턴{pattern_min_minutes/60:.1f}h → "
                       f"최종{final_min_minutes/60:.1f}h")

            return final_min_minutes

        except Exception as e:
            logger.error(f"❌ 매수체결시간 기반 최소시간 계산 오류: {e}")
            # 오류시 기본 패턴별 최소시간 반환
            return self._get_pattern_min_holding_time(position)

    def _get_pattern_min_holding_time(self, position: CandleTradeCandidate) -> float:
        """패턴별 최소 보유시간 가져오기 (분 단위)"""
        try:
            # 기본 최소 보유시간 (패턴이 없는 경우만 사용)
            default_min_minutes = 1440  # 24시간

            # 캔들 전략 종목인지 확인
            is_candle_strategy = (
                position.metadata.get('restored_from_db', False) or
                position.metadata.get('original_entry_source') == 'candle_strategy' or
                len(position.detected_patterns) > 0
            )

            if not is_candle_strategy:
                # 수동/앱 매수 종목: 기본 설정 사용
                logger.debug(f"📊 {position.stock_code} 패턴 미발견 - 기본 최소시간: {default_min_minutes/60:.1f}시간")
                return default_min_minutes

            # 패턴별 설정 조회
            pattern_name = None
            if position.detected_patterns and len(position.detected_patterns) > 0:
                strongest_pattern = max(position.detected_patterns, key=lambda p: p.strength)
                pattern_name = strongest_pattern.pattern_type.value.lower()
            elif 'original_pattern_type' in position.metadata:
                pattern_name = position.metadata['original_pattern_type'].lower()

            if pattern_name:
                pattern_config = self.manager.config['pattern_targets'].get(pattern_name, {})
                pattern_min_minutes = pattern_config.get('min_minutes', default_min_minutes)
                logger.debug(f"📊 {position.stock_code} 패턴 '{pattern_name}' 최소시간: {pattern_min_minutes/60:.1f}시간")
                return pattern_min_minutes

            # 패턴 정보 없으면 기본값
            logger.debug(f"📊 {position.stock_code} 패턴정보 없음 - 기본 최소시간: {default_min_minutes/60:.1f}시간")
            return default_min_minutes

        except Exception as e:
            logger.error(f"❌ 패턴별 최소시간 조회 오류 ({position.stock_code}): {e}")
            return 1440  # 오류시 기본 24시간

    # ========== 🆕 보유 종목 리스크 관리 함수들 ==========

    def setup_holding_risk_management(self, candidate: CandleTradeCandidate, buy_price: float,
                                     current_price: float, candle_analysis_result: Optional[Dict[str, Any]]) -> None:
        """🆕 보유 종목 리스크 관리 설정"""
        try:
            entry_price: float = float(buy_price)
            current_price_float: float = float(current_price)

            if candle_analysis_result and candle_analysis_result.get('patterns_detected'):
                # 캔들 패턴 분석 성공 시
                risk_settings: Tuple[float, float, float, int, float, int, str] = \
                    self.calculate_pattern_based_risk_settings(entry_price, current_price_float, candle_analysis_result)

                target_price, stop_loss_price, trailing_stop_pct, max_holding_hours, position_size_pct, risk_score, source_info = risk_settings

                # 패턴 정보 저장
                self._save_pattern_info_to_candidate(candidate, candle_analysis_result)

                strongest_pattern: Dict[str, Any] = candle_analysis_result['strongest_pattern']
                logger.info(f"✅ {candidate.stock_code} 패턴 분석 성공: {strongest_pattern['type']} "
                           f"(강도: {strongest_pattern['strength']}, "
                           f"신뢰도: {strongest_pattern['confidence']:.2f})")
            else:
                # 패턴 감지 실패 시 기본 설정
                risk_settings: Tuple[float, float, float, int, float, int, str] = \
                    self.calculate_default_risk_settings(entry_price, current_price_float)

                target_price, stop_loss_price, trailing_stop_pct, max_holding_hours, position_size_pct, risk_score, source_info = risk_settings

            # RiskManagement 객체 생성
            entry_quantity: int = candidate.performance.entry_quantity or 0
            position_amount: int = int(entry_price * entry_quantity)

            candidate.risk_management = RiskManagement(
                position_size_pct=float(position_size_pct),
                position_amount=position_amount,
                stop_loss_price=float(stop_loss_price),
                target_price=float(target_price),
                trailing_stop_pct=float(trailing_stop_pct),
                max_holding_hours=int(max_holding_hours),
                risk_score=int(risk_score)
            )

            # 메타데이터에 설정 출처 저장
            candidate.metadata['risk_management_source'] = str(source_info)

        except Exception as e:
            logger.error(f"리스크 관리 설정 오류: {e}")

    def calculate_pattern_based_risk_settings(self, entry_price: float, current_price: float,
                                            candle_analysis_result: Dict[str, Any]) -> Tuple[float, float, float, int, float, int, str]:
        """🆕 패턴 기반 리스크 설정 계산"""
        try:
            patterns: List[Any] = candle_analysis_result['patterns']
            strongest_pattern: Dict[str, Any] = candle_analysis_result['strongest_pattern']

            pattern_type: str = str(strongest_pattern['type'])
            pattern_strength: int = int(strongest_pattern['strength'])
            pattern_confidence: float = float(strongest_pattern['confidence'])

            #logger.info(f"🔄 실시간 캔들 패턴 감지: {pattern_type} (강도: {pattern_strength})")

            # 패턴별 설정 적용
            pattern_config: Optional[Dict[str, Any]] = self.manager.config['pattern_targets'].get(pattern_type.lower())
            if pattern_config:
                target_pct: float = float(pattern_config['target'])
                stop_pct: float = float(pattern_config['stop'])
                max_holding_hours: int = int(pattern_config['max_hours'])
            else:
                # 패턴 강도별 기본 설정
                if pattern_strength >= 90:
                    target_pct, stop_pct, max_holding_hours = 5.0, 4.0, 8
                elif pattern_strength >= 80:
                    target_pct, stop_pct, max_holding_hours = 4.0, 3.0, 6
                elif pattern_strength >= 70:
                    target_pct, stop_pct, max_holding_hours = 3.0, 3.0, 4
                else:
                    target_pct, stop_pct, max_holding_hours = 2.0, 2.0, 2

            target_price: float = entry_price * (1 + target_pct / 100)
            stop_loss_price: float = entry_price * (1 - stop_pct / 100)
            trailing_stop_pct: float = stop_pct * 0.6
            position_size_pct: float = 20.0
            risk_score: int = int(100 - pattern_confidence * 100)

            source_info: str = f"실시간패턴분석({pattern_type})"

            return target_price, stop_loss_price, trailing_stop_pct, max_holding_hours, position_size_pct, risk_score, source_info

        except Exception as e:
            logger.error(f"패턴 기반 설정 계산 오류: {e}")
            return self.calculate_default_risk_settings(entry_price, current_price)

    def calculate_default_risk_settings(self, entry_price: float, current_price: float) -> Tuple[float, float, float, int, float, int, str]:
        """🆕 기본 리스크 설정 계산"""
        try:
            logger.info("🔧 캔들 패턴 감지 실패 - 기본 설정 적용")

            # 기본 3% 목표가, 2% 손절가 설정
            target_price: float = entry_price * 1.03  # 3% 익절
            stop_loss_price: float = entry_price * 0.98  # 2% 손절

            # 현재가가 진입가보다 높다면 목표가 조정
            if current_price > entry_price:
                current_profit_rate: float = (current_price - entry_price) / entry_price
                if current_profit_rate >= 0.02:  # 이미 2% 이상 수익
                    target_price = current_price * 1.01  # 현재가에서 1% 더
                    stop_loss_price = current_price * 0.985  # 현재가에서 1.5% 하락

            trailing_stop_pct: float = 1.0
            max_holding_hours: int = 24
            position_size_pct: float = 20.0
            risk_score: int = 50
            source_info: str = "기본설정(패턴미감지)"

            return target_price, stop_loss_price, trailing_stop_pct, max_holding_hours, position_size_pct, risk_score, source_info

        except Exception as e:
            logger.error(f"기본 설정 계산 오류: {e}")
            # 최소한의 안전 설정
            return entry_price * 1.03, entry_price * 0.98, 1.0, 24, 20.0, 50, "오류시기본값"

    def _save_pattern_info_to_candidate(self, candidate: CandleTradeCandidate, candle_analysis_result: Dict[str, Any]) -> None:
        """🆕 패턴 정보를 candidate에 저장"""
        try:
            patterns: List[Any] = candle_analysis_result['patterns']
            strongest_pattern: Dict[str, Any] = candle_analysis_result['strongest_pattern']

            # 메타데이터 저장
            candidate.metadata['original_pattern_type'] = str(strongest_pattern['type'])
            candidate.metadata['original_pattern_strength'] = int(strongest_pattern['strength'])
            candidate.metadata['pattern_confidence'] = float(strongest_pattern['confidence'])

            # 감지된 패턴 정보 추가
            for pattern in patterns:
                candidate.add_pattern(pattern)

        except Exception as e:
            logger.error(f"패턴 정보 저장 오류: {e}")

    async def cleanup_completed_positions(self):
        """🧹 이미 매도 완료된 종목들을 정리 (시스템 관리 수량 기준)"""
        try:
            cleanup_count = 0
            all_stocks = list(self.manager.stock_manager._all_stocks.values())
            
            for position in all_stocks:
                # ENTERED 상태이지만 실제로는 보유하지 않는 종목들 정리
                if (position.status == CandleStatus.ENTERED and 
                    not position.metadata.get('final_exit_confirmed', False) and
                    not position.metadata.get('auto_exit_reason')):
                    
                    # 🆕 시스템 관리 수량 기준으로 정리 (API 호출 제거)
                    system_quantity = position.performance.entry_quantity or 0
                    
                    if system_quantity <= 0:
                        position.status = CandleStatus.EXITED
                        position.metadata['auto_exit_reason'] = '정리작업_시스템관리수량없음'
                        position.metadata['final_exit_confirmed'] = True
                        self.manager.stock_manager.update_candidate(position)
                        cleanup_count += 1
                        logger.info(f"🧹 {position.stock_code} 정리 완료: ENTERED → EXITED (시스템 관리 수량 없음)")
                        continue
                    
                    # 🆕 선택적으로만 실제 보유 확인 (설정으로 제어)
                    if self.manager.config.get('validate_actual_holding_before_sell', False):
                        try:
                            account_info = await self.manager.kis_api_manager.get_account_balance()
                            if account_info and 'stocks' in account_info:
                                actual_holding = None
                                for stock in account_info['stocks']:
                                    if stock.get('stock_code') == position.stock_code:
                                        actual_holding = stock
                                        break
                                
                                # 실제 보유하지 않는 종목 정리
                                if not actual_holding or actual_holding.get('quantity', 0) <= 0:
                                    position.status = CandleStatus.EXITED
                                    position.metadata['auto_exit_reason'] = '정리작업_실제보유없음'
                                    position.metadata['final_exit_confirmed'] = True
                                    self.manager.stock_manager.update_candidate(position)
                                    cleanup_count += 1
                                    logger.info(f"🧹 {position.stock_code} 정리 완료: ENTERED → EXITED (실제 보유 없음)")
                        
                        except Exception as e:
                            logger.debug(f"보유 확인 오류 ({position.stock_code}): {e}")
                            continue
            
            if cleanup_count > 0:
                logger.info(f"🧹 포지션 정리 완료: {cleanup_count}개 종목")
                
        except Exception as e:
            logger.error(f"포지션 정리 오류: {e}")


