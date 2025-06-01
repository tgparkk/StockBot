"""
고도화된 거래 신호 시스템
- 전문가급 기술적 분석
- 리스크 관리 통합
- 포지션 사이징
"""
import time
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, NamedTuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from utils.logger import setup_logger
from ..analysis.technical_indicators import TechnicalIndicators

logger = setup_logger(__name__)

@dataclass
class TechnicalAnalysis:
    """기술적 분석 결과"""
    rsi: float
    rsi_signal: str  # 'oversold', 'overbought', 'neutral'
    macd_line: float
    macd_signal: float
    macd_histogram: float
    macd_trend: str  # 'bullish', 'bearish', 'neutral'
    ma_5: float
    ma_20: float
    ma_60: float
    ma_signal: str  # 'golden_cross', 'death_cross', 'above_ma', 'below_ma'
    bb_upper: float
    bb_middle: float
    bb_lower: float
    bb_position: float  # 0-1 (볼린저 밴드 내 위치)
    support_level: float
    resistance_level: float
    trend_strength: float  # 0-1

@dataclass
class VolumeProfile:
    """거래량 프로파일 분석"""
    current_volume: int
    avg_volume_20: float
    volume_ratio: float
    volume_trend: str  # 'increasing', 'decreasing', 'stable'
    volume_breakout: bool
    volume_spike: bool
    relative_volume: float  # 대비 평상시

@dataclass
class RiskManagement:
    """리스크 관리"""
    stop_loss_price: float
    stop_loss_pct: float
    take_profit_price: float
    take_profit_pct: float
    position_size: float  # 권장 포지션 크기 (총 자본 대비 %)
    risk_reward_ratio: float
    max_risk_per_trade: float  # 거래당 최대 리스크 %

@dataclass
class AdvancedSignal:
    """고도화된 거래 신호"""
    stock_code: str
    signal_type: str  # 'BUY', 'SELL', 'HOLD'
    strategy: str
    confidence: float  # 0-1 신뢰도
    strength: float   # 0-1 신호 강도
    price: float
    target_price: float
    stop_loss: float
    position_size: float
    risk_reward: float
    technical_analysis: TechnicalAnalysis
    volume_profile: VolumeProfile
    risk_management: RiskManagement
    reason: str
    warnings: List[str]  # 주의사항
    timestamp: float

class AdvancedSignalGenerator:
    """고도화된 거래 신호 생성기"""

    def __init__(self, data_manager, trading_api, account_balance: float = 10000000):
        self.data_manager = data_manager
        self.trading_api = trading_api
        self.account_balance = account_balance

        # 리스크 관리 설정
        self.risk_config = {
            'max_risk_per_trade': 0.02,     # 거래당 최대 2% 리스크
            'max_portfolio_risk': 0.10,     # 전체 포트폴리오 최대 10% 리스크
            'min_risk_reward': 2.0,         # 최소 2:1 수익비
            'stop_loss_multiplier': 2.0,    # ATR 기반 손절 배수
        }

        # 기술적 분석 임계값
        self.tech_thresholds = {
            'rsi_oversold': 30,
            'rsi_overbought': 70,
            'rsi_strong_oversold': 20,
            'rsi_strong_overbought': 80,
            'volume_spike_threshold': 3.0,
            'volume_breakout_threshold': 2.0,
            'trend_strength_min': 0.6,
        }

        # 신호 가중치
        self.signal_weights = {
            'technical_score': 0.35,    # 기술적 지표
            'volume_score': 0.25,       # 거래량 분석
            'trend_score': 0.20,        # 트렌드 강도
            'risk_score': 0.20,         # 리스크 점수
        }

    def generate_advanced_signal(self, strategy_name: str, stock_code: str,
                                current_data: Dict) -> Optional[AdvancedSignal]:
        """고도화된 거래 신호 생성"""
        try:
            logger.info(f"🔬 고도화된 신호 분석 시작: {stock_code} ({strategy_name})")

            # 1. 기본 데이터 검증
            if not self._validate_input_data(current_data):
                return None

            # 2. 과거 데이터 조회
            historical_data = self._get_historical_data(stock_code)
            if historical_data is None or len(historical_data) < 60:
                logger.warning(f"⚠️ {stock_code}: 충분한 과거 데이터 없음 ({len(historical_data) if historical_data else 0}일)")
                return None

            # 3. 기술적 분석 수행
            tech_analysis = self._perform_technical_analysis(historical_data, current_data)

            # 4. 거래량 프로파일 분석
            volume_profile = self._analyze_volume_profile(historical_data, current_data)

            # 5. 종합 신호 점수 계산
            signal_scores = self._calculate_signal_scores(tech_analysis, volume_profile, strategy_name)

            # 6. 리스크 관리 계산
            risk_mgmt = self._calculate_risk_management(
                stock_code, current_data, tech_analysis, historical_data, strategy_name
            )

            # 7. 최종 신호 결정
            final_signal = self._make_final_decision(
                strategy_name, stock_code, current_data,
                tech_analysis, volume_profile, risk_mgmt, signal_scores
            )

            return final_signal

        except Exception as e:
            logger.error(f"❌ 고도화된 신호 생성 오류: {stock_code} - {e}")
            return None

    def _perform_technical_analysis(self, historical_data: pd.DataFrame,
                                   current_data: Dict) -> TechnicalAnalysis:
        """전문가급 기술적 분석"""
        try:
            df = historical_data.copy()
            current_price = current_data.get('current_price', 0)

            # 1. RSI 계산 및 분석
            rsi = self._calculate_rsi(df['close'], period=14)
            current_rsi = rsi.iloc[-1] if len(rsi) > 0 else 50

            rsi_signal = 'neutral'
            if current_rsi <= self.tech_thresholds['rsi_strong_oversold']:
                rsi_signal = 'strong_oversold'
            elif current_rsi <= self.tech_thresholds['rsi_oversold']:
                rsi_signal = 'oversold'
            elif current_rsi >= self.tech_thresholds['rsi_strong_overbought']:
                rsi_signal = 'strong_overbought'
            elif current_rsi >= self.tech_thresholds['rsi_overbought']:
                rsi_signal = 'overbought'

            # 2. MACD 계산 및 분석
            macd_data = self._calculate_macd(df['close'])
            macd_line = macd_data['macd'].iloc[-1]
            macd_signal_line = macd_data['signal'].iloc[-1]
            macd_histogram = macd_data['histogram'].iloc[-1]

            macd_trend = 'neutral'
            if macd_line > macd_signal_line and macd_histogram > 0:
                macd_trend = 'bullish'
            elif macd_line < macd_signal_line and macd_histogram < 0:
                macd_trend = 'bearish'

            # 3. 이동평균 계산 및 분석
            ma_5 = df['close'].rolling(5).mean().iloc[-1]
            ma_20 = df['close'].rolling(20).mean().iloc[-1]
            ma_60 = df['close'].rolling(60).mean().iloc[-1]

            ma_signal = 'neutral'
            if ma_5 > ma_20 > ma_60 and current_price > ma_5:
                ma_signal = 'strong_bullish'
            elif ma_5 > ma_20 and current_price > ma_5:
                ma_signal = 'bullish'
            elif ma_5 < ma_20 < ma_60 and current_price < ma_5:
                ma_signal = 'strong_bearish'
            elif ma_5 < ma_20 and current_price < ma_5:
                ma_signal = 'bearish'

            # 4. 볼린저 밴드 계산
            bb_data = self._calculate_bollinger_bands(df['close'])
            bb_upper = bb_data['upper'].iloc[-1]
            bb_middle = bb_data['middle'].iloc[-1]
            bb_lower = bb_data['lower'].iloc[-1]

            # 볼린저 밴드 내 위치 (0=하단, 1=상단)
            bb_position = (current_price - bb_lower) / (bb_upper - bb_lower) if bb_upper != bb_lower else 0.5
            bb_position = max(0, min(1, bb_position))

            # 5. 지지/저항 레벨 계산
            support_resistance = self._calculate_support_resistance(df)

            # 6. 트렌드 강도 계산
            trend_strength = self._calculate_trend_strength(df)

            return TechnicalAnalysis(
                rsi=current_rsi,
                rsi_signal=rsi_signal,
                macd_line=macd_line,
                macd_signal=macd_signal_line,
                macd_histogram=macd_histogram,
                macd_trend=macd_trend,
                ma_5=ma_5,
                ma_20=ma_20,
                ma_60=ma_60,
                ma_signal=ma_signal,
                bb_upper=bb_upper,
                bb_middle=bb_middle,
                bb_lower=bb_lower,
                bb_position=bb_position,
                support_level=support_resistance['support'],
                resistance_level=support_resistance['resistance'],
                trend_strength=trend_strength
            )

        except Exception as e:
            logger.error(f"기술적 분석 오류: {e}")
            # 기본값 반환
            current_price = current_data.get('current_price', 0)
            return TechnicalAnalysis(
                rsi=50, rsi_signal='neutral',
                macd_line=0, macd_signal=0, macd_histogram=0, macd_trend='neutral',
                ma_5=current_price, ma_20=current_price, ma_60=current_price, ma_signal='neutral',
                bb_upper=current_price*1.02, bb_middle=current_price, bb_lower=current_price*0.98, bb_position=0.5,
                support_level=current_price*0.95, resistance_level=current_price*1.05, trend_strength=0.5
            )

    def _analyze_volume_profile(self, historical_data: pd.DataFrame,
                               current_data: Dict) -> VolumeProfile:
        """거래량 프로파일 분석"""
        try:
            # 🔧 장 시간 체크 추가
            from datetime import datetime, time as dt_time
            now = datetime.now()

            # 장 시간: 평일 9:00~15:30
            market_open_time = dt_time(9, 0)
            market_close_time = dt_time(15, 30)
            is_weekday = now.weekday() < 5  # 월~금 (0~4)
            is_market_hours = market_open_time <= now.time() <= market_close_time
            is_market_open = is_weekday and is_market_hours

            # 🔧 장 시간이 아니면 현재 거래량을 0으로 처리
            if not is_market_open:
                current_volume = 0
                logger.debug(f"🕐 장마감/주말 - 현재 거래량을 0으로 처리 (요일:{now.weekday()}, 시간:{now.time()})")
            else:
                current_volume = current_data.get('volume', 0)
                logger.debug(f"📊 장 시간 - 실시간 거래량 사용: {current_volume:,}주")

            # 최근 20일 평균 거래량
            recent_volumes = historical_data['volume'].tail(20)
            avg_volume_20 = recent_volumes.mean()

            # 거래량 비율 (장마감 시에는 항상 0)
            if is_market_open:
                volume_ratio = current_volume / avg_volume_20 if avg_volume_20 > 0 else 0
            else:
                volume_ratio = 0.0  # 장마감 시 거래량 비율 0

            # 🔧 거래량 분석 상세 로깅 (장 상태 포함)
            market_status = "장중" if is_market_open else "장마감/주말"
            logger.debug(f"📊 거래량 분석 ({market_status}): 현재={current_volume:,}주, 20일평균={avg_volume_20:,.0f}주, 비율={volume_ratio:.1f}배")

            # 거래량 트렌드 분석
            volume_trend = 'stable'
            if len(recent_volumes) >= 5:
                recent_5 = recent_volumes.tail(5).mean()
                previous_5 = recent_volumes.head(5).mean()

                if recent_5 > previous_5 * 1.2:
                    volume_trend = 'increasing'
                elif recent_5 < previous_5 * 0.8:
                    volume_trend = 'decreasing'

                logger.debug(f"📈 거래량 트렌드: {volume_trend} (최근5일평균={recent_5:,.0f}, 이전5일평균={previous_5:,.0f})")

            # 🔧 거래량 돌파 및 급증 판단 (장마감 시에는 무조건 False)
            if is_market_open:
                volume_breakout = volume_ratio >= self.tech_thresholds['volume_breakout_threshold']
                volume_spike = volume_ratio >= self.tech_thresholds['volume_spike_threshold']
            else:
                volume_breakout = False
                volume_spike = False
                logger.debug(f"🚫 장마감/주말 - 거래량 신호 비활성화")

            # 🔧 거래량 임계값 로깅
            logger.debug(f"🚨 거래량 신호: 돌파={volume_breakout}(기준{self.tech_thresholds['volume_breakout_threshold']}배), 급증={volume_spike}(기준{self.tech_thresholds['volume_spike_threshold']}배)")

            # 상대적 거래량 (과거 90일 대비)
            if len(historical_data) >= 90:
                volume_90_percentile = historical_data['volume'].tail(90).quantile(0.8)
                if is_market_open:
                    relative_volume = current_volume / volume_90_percentile if volume_90_percentile > 0 else 1.0
                else:
                    relative_volume = 0.0  # 장마감 시 0
                logger.debug(f"📊 90일 상대거래량: {relative_volume:.1f}배 (90일 80%={volume_90_percentile:,.0f}주)")
            else:
                relative_volume = volume_ratio

            return VolumeProfile(
                current_volume=current_volume,
                avg_volume_20=avg_volume_20,
                volume_ratio=volume_ratio,
                volume_trend=volume_trend,
                volume_breakout=volume_breakout,
                volume_spike=volume_spike,
                relative_volume=relative_volume
            )

        except Exception as e:
            logger.error(f"거래량 프로파일 분석 오류: {e}")
            return VolumeProfile(
                current_volume=current_data.get('volume', 0),
                avg_volume_20=1000000,
                volume_ratio=1.0,
                volume_trend='stable',
                volume_breakout=False,
                volume_spike=False,
                relative_volume=1.0
            )

    def _calculate_signal_scores(self, tech_analysis: TechnicalAnalysis,
                               volume_profile: VolumeProfile,
                               strategy_name: str) -> Dict[str, float]:
        """종합 신호 점수 계산"""
        scores = {}

        # 1. 기술적 지표 점수
        tech_score = 0.0

        # RSI 점수
        if tech_analysis.rsi_signal == 'strong_oversold':
            tech_score += 0.4
        elif tech_analysis.rsi_signal == 'oversold':
            tech_score += 0.3
        elif tech_analysis.rsi_signal == 'strong_overbought':
            tech_score -= 0.4
        elif tech_analysis.rsi_signal == 'overbought':
            tech_score -= 0.3

        # MACD 점수
        if tech_analysis.macd_trend == 'bullish':
            tech_score += 0.3
        elif tech_analysis.macd_trend == 'bearish':
            tech_score -= 0.3

        # 이동평균 점수
        if tech_analysis.ma_signal == 'strong_bullish':
            tech_score += 0.3
        elif tech_analysis.ma_signal == 'bullish':
            tech_score += 0.2
        elif tech_analysis.ma_signal == 'strong_bearish':
            tech_score -= 0.3
        elif tech_analysis.ma_signal == 'bearish':
            tech_score -= 0.2

        scores['technical_score'] = max(0, min(1, tech_score + 0.5))

        # 2. 거래량 점수
        volume_score = 0.5  # 기본값

        if volume_profile.volume_spike:
            volume_score += 0.3
        elif volume_profile.volume_breakout:
            volume_score += 0.2

        if volume_profile.volume_trend == 'increasing':
            volume_score += 0.1
        elif volume_profile.volume_trend == 'decreasing':
            volume_score -= 0.1

        scores['volume_score'] = max(0, min(1, volume_score))

        # 3. 트렌드 점수
        scores['trend_score'] = tech_analysis.trend_strength

        # 4. 리스크 점수 (볼린저 밴드 위치 기반)
        risk_score = 0.5
        if tech_analysis.bb_position < 0.2:  # 하단 근처 (저위험)
            risk_score += 0.3
        elif tech_analysis.bb_position > 0.8:  # 상단 근처 (고위험)
            risk_score -= 0.3

        scores['risk_score'] = max(0, min(1, risk_score))

        return scores

    def _calculate_risk_management(self, stock_code: str, current_data: Dict,
                                 tech_analysis: TechnicalAnalysis,
                                 historical_data: pd.DataFrame, strategy_name: str) -> RiskManagement:
        """리스크 관리 계산"""
        try:
            current_price = current_data.get('current_price', 0)

            # 1. ATR 기반 손절가 계산
            atr = self._calculate_atr(historical_data)
            current_atr = atr.iloc[-1] if len(atr) > 0 else current_price * 0.02

            # 2. 손절가 설정 (현재가보다 낮아야 함) - 🔧 수정된 로직
            atr_stop = current_price - (current_atr * self.risk_config['stop_loss_multiplier'])

            # 지지선 기반 손절가 (현재가보다 낮게 제한)
            support_stop = min(tech_analysis.support_level * 0.98, current_price * 0.95)

            # 볼린저 밴드 기반 손절가 (현재가보다 낮게 제한)
            bb_stop = min(tech_analysis.bb_lower * 0.99, current_price * 0.97)

            # 🔧 가장 높은 손절가 선택하되 현재가보다는 낮게 제한 (매일 복리용 - 빠른 손절)
            stop_loss_price = min(
                max(atr_stop, support_stop, bb_stop),
                current_price * 0.985  # 최대 1.5% 손절 (매일 복리용)
            )

            # 🔧 최소 손절 거리 보장 (0.8% 이상 - 매일 복리용)
            min_stop_distance = current_price * 0.008  # 0.8%
            if current_price - stop_loss_price < min_stop_distance:
                stop_loss_price = current_price - min_stop_distance

            stop_loss_pct = (current_price - stop_loss_price) / current_price

            # 3. 🔧 목표가 설정 (현재가보다 반드시 높게, 2:1 비율 보장)
            risk_amount = current_price - stop_loss_price  # 이제 항상 양수

            # 🔧 최소 리스크-수익 비율 강화 (2.5:1로 상향)
            min_risk_reward = max(self.risk_config['min_risk_reward'], 2.5)
            base_target = current_price + (risk_amount * min_risk_reward)

            # 🔧 전략별 목표가 조정
            strategy_multipliers = {
                'momentum': 3.0,      # 모멘텀: 더 높은 목표
                'gap_trading': 2.5,   # 갭 거래: 중간 목표
                'volume_breakout': 3.5, # 거래량 돌파: 가장 높은 목표
                'default': 2.5        # 기본값
            }

            # 전략에 맞는 배수 적용
            strategy_key = 'default'
            for key in strategy_multipliers.keys():
                if key in strategy_name.lower():  # 🔧 매개변수의 strategy_name 사용
                    strategy_key = key
                    break

            strategy_multiplier = strategy_multipliers[strategy_key]
            strategy_target = current_price + (risk_amount * strategy_multiplier)

            # 더 높은 목표가 선택
            calculated_target = max(base_target, strategy_target)

            # 저항선 고려한 목표가 조정 (단, 너무 낮아지지 않도록)
            if (tech_analysis.resistance_level > current_price and
                calculated_target > tech_analysis.resistance_level):
                # 저항선이 너무 낮으면 무시하고 계산된 목표가 사용
                resistance_target = tech_analysis.resistance_level * 0.98
                if resistance_target >= current_price * 1.02:  # 최소 2% 이상 목표가
                    take_profit_price = resistance_target
                else:
                    take_profit_price = calculated_target  # 저항선 무시
                    logger.debug(f"🎯 저항선이 너무 낮아 무시: 저항선={tech_analysis.resistance_level:,}, 계산목표가={calculated_target:,}")
            else:
                take_profit_price = calculated_target

            # 🔧 최소 목표가 보장 강화 (현재가의 최소 2.5% 이상)
            min_target_pct = 0.025  # 2.5%
            min_target = current_price * (1 + min_target_pct)
            take_profit_price = max(take_profit_price, min_target)

            # 🔧 최대 목표가 제한 (매일 복리 수익 전략 - 초보수적)
            strategy_max_limits = {
                'momentum': 0.04,        # 모멘텀: 4% (매일 복리용)
                'gap_trading': 0.03,     # 갭 거래: 3% (매우 안전)
                'volume_breakout': 0.05, # 거래량 돌파: 5% (적당)
                'default': 0.025         # 기본값: 2.5% (매일 복리 최적)
            }

            # 전략에 맞는 최대 제한 선택
            max_limit_key = 'default'
            for key in strategy_max_limits.keys():
                if key in strategy_name.lower():
                    max_limit_key = key
                    break

            max_target_pct = strategy_max_limits[max_limit_key]
            max_target = current_price * (1 + max_target_pct)
            take_profit_price = min(take_profit_price, max_target)

            # 🔧 매일 복리 수익 최적화 로깅
            #daily_compound_info = f"💰 매일복리모드: {stock_code} 목표 {max_target_pct:.1%} (현재: {current_price:,}원 → 목표: {max_target:,}원)"
            #logger.info(daily_compound_info)

            # 📊 복리 수익 시뮬레이션 (참고용)
            if max_target_pct > 0:
                monthly_return = ((1 + max_target_pct) ** 20) - 1  # 월 20거래일 가정
                yearly_return = ((1 + max_target_pct) ** 240) - 1  # 연 240거래일 가정
                #logger.info(f"🚀 복리 예상: 월 {monthly_return:.1%}, 연 {yearly_return:.1%} (성공률 80% 가정)")

            take_profit_pct = (take_profit_price - current_price) / current_price

            # 4. 포지션 사이징 계산
            max_loss_per_trade = self.account_balance * self.risk_config['max_risk_per_trade']
            loss_per_share = current_price - stop_loss_price

            if loss_per_share > 0:
                max_shares = int(max_loss_per_trade / loss_per_share)
                position_value = max_shares * current_price
                position_size = position_value / self.account_balance
            else:
                position_size = 0.01  # 최소 포지션

            # 5. 🔧 리스크-수익 비율 계산 (항상 양수가 되도록)
            profit_amount = take_profit_price - current_price
            risk_reward_ratio = profit_amount / risk_amount if risk_amount > 0 else 2.0

            # 🔧 계산 검증 및 로깅
            logger.debug(f"💰 {stock_code} 가격계산: 현재={current_price:,}원, 목표={take_profit_price:,}원, 손절={stop_loss_price:,}원")
            logger.debug(f"📊 리스크 분석: 손실={risk_amount:.0f}원, 이익={profit_amount:.0f}원, 비율={risk_reward_ratio:.1f}:1")

            return RiskManagement(
                stop_loss_price=stop_loss_price,
                stop_loss_pct=stop_loss_pct,
                take_profit_price=take_profit_price,
                take_profit_pct=take_profit_pct,
                position_size=min(position_size, 0.02),  # 최대 2% 포지션 (초보자용)
                risk_reward_ratio=risk_reward_ratio,
                max_risk_per_trade=self.risk_config['max_risk_per_trade']
            )

        except Exception as e:
            logger.error(f"리스크 관리 계산 오류: {e}")
            current_price = current_data.get('current_price', 0)
            return RiskManagement(
                stop_loss_price=current_price * 0.95,
                stop_loss_pct=0.05,
                take_profit_price=current_price * 1.10,
                take_profit_pct=0.10,
                position_size=0.02,
                risk_reward_ratio=2.0,
                max_risk_per_trade=0.02
            )

    def _make_final_decision(self, strategy_name: str, stock_code: str, current_data: Dict,
                           tech_analysis: TechnicalAnalysis, volume_profile: VolumeProfile,
                           risk_mgmt: RiskManagement, signal_scores: Dict) -> Optional[AdvancedSignal]:
        """최종 신호 결정"""
        try:
            # 1. 종합 점수 계산
            total_score = 0.0
            for component, score in signal_scores.items():
                weight = self.signal_weights.get(component, 0)
                total_score += score * weight

            # 2. 신뢰도 계산
            confidence = self._calculate_confidence(tech_analysis, volume_profile)

            # 3. 신호 생성 조건 확인
            warnings = []

            # 기본 필터링
            if total_score < 0.6:
                return None

            if confidence < 0.5:
                warnings.append("낮은 신뢰도")

            if risk_mgmt.risk_reward_ratio < 1.5:
                warnings.append("낮은 리스크-수익 비율")

            if tech_analysis.rsi_signal in ['strong_overbought', 'overbought'] and strategy_name != 'contrarian':
                warnings.append("과매수 상태")

            if not volume_profile.volume_breakout and strategy_name == 'volume_breakout':
                return None

            # 4. 최종 신호 생성
            current_price = current_data.get('current_price', 0)

            # 신호 강도 조정
            strength = total_score
            if volume_profile.volume_spike:
                strength += 0.1
            if tech_analysis.rsi_signal == 'strong_oversold':
                strength += 0.1

            strength = min(1.0, strength)

            # 상세 이유 생성
            reason_parts = []
            if tech_analysis.rsi_signal in ['oversold', 'strong_oversold']:
                reason_parts.append(f"RSI과매도({tech_analysis.rsi:.1f})")
            if tech_analysis.macd_trend == 'bullish':
                reason_parts.append("MACD상승")
            if tech_analysis.ma_signal in ['bullish', 'strong_bullish']:
                reason_parts.append("이평선상승")
            if volume_profile.volume_breakout:
                reason_parts.append(f"거래량돌파({volume_profile.volume_ratio:.1f}x)")

            reason = f"고도화분석: {', '.join(reason_parts)} | 종합점수:{total_score:.2f}"

            return AdvancedSignal(
                stock_code=stock_code,
                signal_type='BUY',
                strategy=f"{strategy_name}_advanced",
                confidence=confidence,
                strength=strength,
                price=current_price,
                target_price=risk_mgmt.take_profit_price,
                stop_loss=risk_mgmt.stop_loss_price,
                position_size=risk_mgmt.position_size,
                risk_reward=risk_mgmt.risk_reward_ratio,
                technical_analysis=tech_analysis,
                volume_profile=volume_profile,
                risk_management=risk_mgmt,
                reason=reason,
                warnings=warnings,
                timestamp=time.time()
            )

        except Exception as e:
            logger.error(f"최종 신호 결정 오류: {e}")
            return None

    # === 기술적 지표 계산 메서드들 ===

    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """RSI 계산"""
        delta = prices.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def _calculate_macd(self, prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict:
        """MACD 계산"""
        ema_fast = prices.ewm(span=fast).mean()
        ema_slow = prices.ewm(span=slow).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal).mean()
        histogram = macd_line - signal_line

        return {
            'macd': macd_line,
            'signal': signal_line,
            'histogram': histogram
        }

    def _calculate_bollinger_bands(self, prices: pd.Series, period: int = 20, std: float = 2) -> Dict:
        """볼린저 밴드 계산"""
        middle = prices.rolling(window=period).mean()
        std_dev = prices.rolling(window=period).std()
        upper = middle + (std_dev * std)
        lower = middle - (std_dev * std)

        return {
            'upper': upper,
            'middle': middle,
            'lower': lower
        }

    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """ATR 계산"""
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())

        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = true_range.rolling(window=period).mean()
        return atr

    def _calculate_support_resistance(self, df: pd.DataFrame) -> Dict:
        """지지/저항 레벨 계산"""
        try:
            # 최근 60일 데이터 사용
            recent_data = df.tail(60)

            # 지지선: 최근 저점들의 평균
            lows = recent_data['low'].rolling(window=5).min()
            support = lows.quantile(0.2)

            # 저항선: 최근 고점들의 평균
            highs = recent_data['high'].rolling(window=5).max()
            resistance = highs.quantile(0.8)

            return {
                'support': support,
                'resistance': resistance
            }
        except:
            current_price = df['close'].iloc[-1]
            return {
                'support': current_price * 0.95,
                'resistance': current_price * 1.05
            }

    def _calculate_trend_strength(self, df: pd.DataFrame) -> float:
        """트렌드 강도 계산"""
        try:
            # 20일 이동평균의 기울기로 트렌드 강도 측정
            ma20 = df['close'].rolling(20).mean()
            if len(ma20) < 20:
                return 0.5

            # 최근 20일 이동평균의 변화율
            trend_change = (ma20.iloc[-1] - ma20.iloc[-20]) / ma20.iloc[-20]

            # 0-1 범위로 정규화
            trend_strength = max(0, min(1, 0.5 + trend_change * 10))
            return trend_strength
        except:
            return 0.5

    def _calculate_confidence(self, tech_analysis: TechnicalAnalysis,
                            volume_profile: VolumeProfile) -> float:
        """신호 신뢰도 계산"""
        confidence = 0.5

        # 기술적 지표 일치도
        bullish_signals = 0
        total_signals = 0

        # RSI 신호
        if tech_analysis.rsi_signal in ['oversold', 'strong_oversold']:
            bullish_signals += 1
        total_signals += 1

        # MACD 신호
        if tech_analysis.macd_trend == 'bullish':
            bullish_signals += 1
        total_signals += 1

        # 이동평균 신호
        if tech_analysis.ma_signal in ['bullish', 'strong_bullish']:
            bullish_signals += 1
        total_signals += 1

        # 거래량 확인
        if volume_profile.volume_breakout:
            bullish_signals += 1
        total_signals += 1

        # 신뢰도 계산
        if total_signals > 0:
            signal_consistency = bullish_signals / total_signals
            confidence = signal_consistency

        # 트렌드 강도에 따른 가중치
        confidence = confidence * 0.7 + tech_analysis.trend_strength * 0.3

        return max(0, min(1, confidence))

    def _get_historical_data(self, stock_code: str) -> Optional[pd.DataFrame]:
        """과거 데이터 조회"""
        try:
            # data_manager를 통해 일봉 데이터 조회
            daily_data = self.data_manager.collector.get_daily_prices(stock_code, "D", use_cache=True)

            if not daily_data:
                return None

            # DataFrame으로 변환
            if isinstance(daily_data, list):
                df_data = []
                for item in daily_data:
                    if isinstance(item, dict):
                        df_data.append({
                            'date': item.get('stck_bsop_date', ''),
                            'open': float(item.get('stck_oprc', 0)),
                            'high': float(item.get('stck_hgpr', 0)),
                            'low': float(item.get('stck_lwpr', 0)),
                            'close': float(item.get('stck_clpr', 0)),
                            'volume': int(item.get('acml_vol', 0))
                        })

                if df_data:
                    df = pd.DataFrame(df_data)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.set_index('date')
                    return df.sort_index()

            return None

        except Exception as e:
            logger.error(f"과거 데이터 조회 오류: {stock_code} - {e}")
            return None

    def _validate_input_data(self, data: Dict) -> bool:
        """입력 데이터 검증"""
        try:
            # 🔧 장 시간 체크
            from datetime import datetime, time as dt_time
            now = datetime.now()

            market_open_time = dt_time(9, 0)
            market_close_time = dt_time(15, 30)
            is_weekday = now.weekday() < 5
            is_market_hours = market_open_time <= now.time() <= market_close_time
            is_market_open = is_weekday and is_market_hours

            # 현재 가격은 항상 필수
            if 'current_price' not in data or data['current_price'] <= 0:
                logger.debug("❌ 유효하지 않은 현재가")
                return False

            # 🔧 장 시간에만 거래량 검증
            if is_market_open:
                if 'volume' not in data or data['volume'] <= 0:
                    logger.debug("❌ 장 시간인데 거래량이 없음")
                    return False
            else:
                # 장마감/주말에는 거래량이 없어도 OK
                logger.debug(f"✅ 장마감/주말 - 거래량 검증 스킵")

            return True

        except Exception as e:
            logger.error(f"데이터 검증 오류: {e}")
            return False

    def predict_bounce_potential(self, stock_code: str, current_price: float,
                               profit_rate: float, tech_analysis: TechnicalAnalysis,
                               volume_profile: VolumeProfile, historical_data: pd.DataFrame) -> Dict:
        """🎯 반등 가능성 예측 (-5% → +5% 시나리오 분석)"""
        try:
            bounce_score = 0.0
            reasons = []
            confidence = 0.0

            # 기본 조건: 손실 구간에서만 분석
            if profit_rate >= -1.0:
                return {'bounce_potential': 0.0, 'confidence': 0.0, 'reasons': []}

            logger.info(f"🔍 {stock_code} 반등 가능성 분석 시작 (현재손실: {profit_rate:.1f}%)")

            # 1. 🎯 이격도 기반 과매도 분석 (가장 강력한 지표)
            try:
                from ..api.kis_market_api import get_disparity_rank
                disparity_data = get_disparity_rank("0000", "20", "5000")

                if disparity_data is not None and not disparity_data.empty:
                    stock_row = disparity_data[disparity_data['mksc_shrn_iscd'] == stock_code]
                    if not stock_row.empty:
                        d5 = float(stock_row.iloc[0].get('d5_dsrt', 100))
                        d20 = float(stock_row.iloc[0].get('d20_dsrt', 100))
                        d60 = float(stock_row.iloc[0].get('d60_dsrt', 100))

                        # 극도 과매도: 강력한 반등 신호
                        if d20 <= 85 and d5 <= 90:
                            bounce_score += 40  # 최고 점수
                            reasons.append(f"극도과매도(D20:{d20:.1f}, D5:{d5:.1f})")
                        elif d20 <= 90 and d5 <= 95:
                            bounce_score += 25
                            reasons.append(f"과매도상태(D20:{d20:.1f}, D5:{d5:.1f})")

                        # 장기/단기 이격도 다이버전스
                        if d60 >= 105 and d20 <= 90:  # 장기 상승 + 단기 과매도
                            bounce_score += 20
                            reasons.append(f"다이버전스반등(D60:{d60:.1f}↑ D20:{d20:.1f}↓)")

            except Exception as e:
                logger.debug(f"이격도 분석 오류: {e}")

            # 2. 🎯 RSI 기반 과매도 분석
            if tech_analysis.rsi <= 20:  # 극도 과매도
                bounce_score += 25
                reasons.append(f"RSI극도과매도({tech_analysis.rsi:.1f})")
            elif tech_analysis.rsi <= 30:
                bounce_score += 15
                reasons.append(f"RSI과매도({tech_analysis.rsi:.1f})")

            # 3. 🎯 볼린저 밴드 이탈 분석
            if tech_analysis.bb_position <= 0.1:  # 하단 10% 이하
                bb_deviation = (current_price - tech_analysis.bb_lower) / tech_analysis.bb_lower
                if bb_deviation < -0.02:  # 하단 2% 이상 이탈
                    bounce_score += 20
                    reasons.append(f"볼린저하단이탈({bb_deviation:.1%})")
                elif bb_deviation < 0:
                    bounce_score += 10
                    reasons.append(f"볼린저하단접촉({bb_deviation:.1%})")

            # 4. 🎯 거래량 분석 (매도 압력 vs 매수 압력)
            if volume_profile.volume_ratio >= 2.0:  # 대량 거래량
                if profit_rate <= -3.0:  # 대량 거래량 + 하락 = 대량 매도 후 반등 가능성
                    bounce_score += 15
                    reasons.append(f"대량거래량반등({volume_profile.volume_ratio:.1f}배)")

            # 거래량 트렌드가 증가 중이면 관심 증가
            if volume_profile.volume_trend == 'increasing':
                bounce_score += 10
                reasons.append("거래량증가추세")

            # 5. 🎯 MACD 다이버전스 분석
            if tech_analysis.macd_trend == 'bullish' and profit_rate <= -2.0:
                # 가격은 하락했지만 MACD는 상승 = 다이버전스
                bounce_score += 15
                reasons.append("MACD다이버전스")

            # 6. 🎯 지지선 근접 분석
            support_distance = (current_price - tech_analysis.support_level) / current_price
            if 0 <= support_distance <= 0.02:  # 지지선 2% 이내
                bounce_score += 15
                reasons.append(f"지지선근접({support_distance:.1%})")
            elif support_distance < 0:  # 지지선 이탈
                if abs(support_distance) <= 0.01:  # 1% 이내 이탈은 일시적 가능성
                    bounce_score += 10
                    reasons.append(f"지지선일시이탈({support_distance:.1%})")

            # 7. 🎯 시장 전체 상황 고려
            try:
                # 코스피/코스닥 급락 중이면 개별주 반등 어려움
                # 시장 안정/상승 중이면 개별주 반등 가능성 증가
                # (실제 구현은 시장 지수 데이터 필요)
                pass
            except:
                pass

            # 8. 🎯 패턴 매칭 (과거 유사 사례)
            if len(historical_data) >= 60:
                try:
                    # 과거 60일 중 유사한 급락 후 반등 패턴 찾기
                    recent_closes = historical_data['close'].tail(60)
                    daily_changes = recent_closes.pct_change()

                    # -3% 이상 급락 후 반등한 횟수
                    sharp_drops = daily_changes <= -0.03
                    if sharp_drops.sum() > 0:
                        # 급락 다음날 반등 비율 계산
                        drop_indices = daily_changes[sharp_drops].index
                        bounce_count = 0
                        total_drops = 0

                        for drop_idx in drop_indices:
                            try:
                                next_idx = daily_changes.index.get_loc(drop_idx) + 1
                                if next_idx < len(daily_changes):
                                    next_change = daily_changes.iloc[next_idx]
                                    total_drops += 1
                                    if next_change > 0.01:  # 1% 이상 반등
                                        bounce_count += 1
                            except:
                                continue

                        if total_drops > 0:
                            bounce_ratio = bounce_count / total_drops
                            if bounce_ratio >= 0.6:  # 60% 이상 반등
                                bounce_score += 15
                                reasons.append(f"과거반등패턴({bounce_ratio:.0%})")
                            elif bounce_ratio >= 0.4:  # 40% 이상 반등
                                bounce_score += 8
                                reasons.append(f"과거반등패턴({bounce_ratio:.0%})")

                except Exception as e:
                    logger.debug(f"패턴 분석 오류: {e}")

            # 9. 🎯 신뢰도 계산
            max_score = 100
            confidence = min(bounce_score / max_score, 1.0)

            # 10. 🎯 종합 판단
            if bounce_score >= 60:
                prediction = "HIGH"  # 높은 반등 가능성
            elif bounce_score >= 40:
                prediction = "MEDIUM"  # 중간 반등 가능성
            elif bounce_score >= 20:
                prediction = "LOW"  # 낮은 반등 가능성
            else:
                prediction = "MINIMAL"  # 거의 없음

            logger.info(f"✅ {stock_code} 반등 분석 완료: {prediction} (점수:{bounce_score:.0f}, 신뢰도:{confidence:.2f})")
            logger.info(f"📋 반등 근거: {', '.join(reasons)}")

            return {
                'bounce_potential': bounce_score,
                'confidence': confidence,
                'prediction': prediction,
                'reasons': reasons,
                'recommended_action': self._get_bounce_recommendation(bounce_score, profit_rate)
            }

        except Exception as e:
            logger.error(f"반등 예측 분석 오류: {e}")
            return {'bounce_potential': 0.0, 'confidence': 0.0, 'reasons': []}

    def _get_bounce_recommendation(self, bounce_score: float, profit_rate: float) -> str:
        """반등 점수 기반 추천 액션"""
        if bounce_score >= 60:
            if profit_rate <= -4.0:
                return "HOLD_STRONG"  # 강력 보유 (손절 지연)
            else:
                return "HOLD"  # 보유
        elif bounce_score >= 40:
            if profit_rate <= -6.0:
                return "HOLD"  # 보유 (손절 완화)
            else:
                return "MONITOR"  # 모니터링
        elif bounce_score >= 20:
            return "CAREFUL"  # 신중 관찰
        else:
            return "NORMAL"  # 일반 손절 적용
