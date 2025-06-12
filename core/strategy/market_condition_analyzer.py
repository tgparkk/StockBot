"""
시장 상황 분석기
거시적/세부적 시장 상황을 분석하여 매매 전략에 활용
"""
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
import pandas as pd
from utils.logger import setup_logger
from core.api.kis_market_api import get_market_overview, get_index_data, get_investor_flow_data

logger = setup_logger(__name__)


class MarketTrend(Enum):
    """시장 추세"""
    BULL = "BULL"          # 상승장
    BEAR = "BEAR"          # 하락장
    NEUTRAL = "NEUTRAL"    # 중립/박스권


class MarketVolatility(Enum):
    """시장 변동성"""
    HIGH = "HIGH"          # 고변동성
    NORMAL = "NORMAL"      # 보통
    LOW = "LOW"            # 저변동성


class MarketVolume(Enum):
    """시장 거래량 상황"""
    ACTIVE = "ACTIVE"      # 활황
    NORMAL = "NORMAL"      # 보통
    WEAK = "WEAK"          # 침체


class ForeignFlow(Enum):
    """외국인 자금 흐름"""
    BUY = "BUY"           # 순매수
    SELL = "SELL"         # 순매도
    NEUTRAL = "NEUTRAL"   # 중립


class InstitutionFlow(Enum):
    """기관 자금 흐름"""
    BUY = "BUY"           # 순매수
    SELL = "SELL"         # 순매도
    NEUTRAL = "NEUTRAL"   # 중립


class MarketCondition:
    """시장 상황 데이터 클래스"""

    def __init__(self):
        # 거시적 시장 상황
        self.kospi_trend: MarketTrend = MarketTrend.NEUTRAL
        self.kosdaq_trend: MarketTrend = MarketTrend.NEUTRAL
        self.volatility: MarketVolatility = MarketVolatility.NORMAL
        self.volume_condition: MarketVolume = MarketVolume.NORMAL

        # 세부적 시장 상황
        self.foreign_flow: ForeignFlow = ForeignFlow.NEUTRAL
        self.institution_flow: InstitutionFlow = InstitutionFlow.NEUTRAL
        self.sector_leaders: List[Dict] = []
        self.sector_laggards: List[Dict] = []

        # 수치 데이터
        self.kospi_change_pct: float = 0.0
        self.kosdaq_change_pct: float = 0.0
        self.market_volatility_score: float = 0.0
        self.volume_ratio: float = 1.0
        self.foreign_net_buy_amount: float = 0.0
        self.institution_net_buy_amount: float = 0.0

        # 메타 정보
        self.last_updated: datetime = datetime.now()
        self.data_quality: str = "normal"  # high, normal, low
        self.confidence_score: float = 0.5  # 0.0 ~ 1.0

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리 변환"""
        return {
            'kospi_trend': self.kospi_trend.value,
            'kosdaq_trend': self.kosdaq_trend.value,
            'volatility': self.volatility.value,
            'volume_condition': self.volume_condition.value,
            'foreign_flow': self.foreign_flow.value,
            'institution_flow': self.institution_flow.value,
            'sector_leaders': self.sector_leaders,
            'sector_laggards': self.sector_laggards,
            'kospi_change_pct': self.kospi_change_pct,
            'kosdaq_change_pct': self.kosdaq_change_pct,
            'market_volatility_score': self.market_volatility_score,
            'volume_ratio': self.volume_ratio,
            'foreign_net_buy_amount': self.foreign_net_buy_amount,
            'institution_net_buy_amount': self.institution_net_buy_amount,
            'last_updated': self.last_updated.isoformat(),
            'data_quality': self.data_quality,
            'confidence_score': self.confidence_score
        }


class MarketConditionAnalyzer:
    """시장 상황 분석기"""

    def __init__(self):
        """초기화"""
        self.last_update = None
        self.market_data = None
        self.analysis_result = None
        
        # 설정 파일에서 update_interval_seconds 값을 읽어옴
        try:
            with open('config/candle_strategy_config.json', 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.update_interval = config.get('market_condition_adjustments', {}).get('update_interval_seconds', 300)
                logger.info(f"✅ 시장 상황 업데이트 주기 설정: {self.update_interval}초")
        except Exception as e:
            logger.warning(f"⚠️ 설정 파일 읽기 실패, 기본값(300초) 사용: {e}")
            self.update_interval = 300  # 기본값 5분

    def should_update(self) -> bool:
        """업데이트가 필요한지 확인"""
        if not self.last_update:
            return True

        time_diff = (datetime.now() - self.last_update).total_seconds()
        return time_diff >= self.update_interval

    def analyze_market_condition(self) -> Dict[str, Any]:
        """
        현재 시장 상황을 분석합니다.

        Returns:
            Dict: 시장 상황 분석 결과
        """
        try:
            # 마지막 업데이트로부터 일정 시간이 지났는지 확인
            if self.should_update():
                # 실제 KIS API를 통해 시장 데이터 조회
                self.market_data = get_market_overview()
                self.last_update = datetime.now()

                if self.market_data:
                    self.analysis_result = self._analyze_market_data()
                else:
                    logger.warning("⚠️ 시장 데이터 조회 실패")
                    return self._get_default_analysis()

            return self.analysis_result or self._get_default_analysis()

        except Exception as e:
            logger.error(f"❌ 시장 상황 분석 오류: {e}")
            return self._get_default_analysis()

    def get_current_condition(self) -> Dict[str, Any]:
        """
        현재 시장 상황을 반환합니다.

        Returns:
            Dict: 현재 시장 상황
        """
        return self.analyze_market_condition()

    def get_market_strength_score(self) -> float:
        """
        시장 강도 점수를 반환합니다.

        Returns:
            float: 시장 강도 점수 (0.0 ~ 1.0)
        """
        analysis = self.analyze_market_condition()
        score = 0.5  # 기본값

        # 시장 추세 반영
        market_trend = analysis.get('market_trend', 'neutral_market')
        if market_trend == 'bull_market':
            score += 0.2
        elif market_trend == 'bear_market':
            score -= 0.2

        # 투자자 심리 반영
        investor_sentiment = analysis.get('investor_sentiment', {})
        if investor_sentiment.get('overall_sentiment') == 'positive':
            score += 0.1
        elif investor_sentiment.get('overall_sentiment') == 'negative':
            score -= 0.1

        return max(0.0, min(1.0, score))

    def get_market_risk_level(self) -> str:
        """
        시장 리스크 수준을 반환합니다.

        Returns:
            str: 리스크 수준 ('low', 'medium', 'high')
        """
        analysis = self.analyze_market_condition()
        risk_score = 0

        # 변동성 반영
        volatility = analysis.get('volatility', 'low_volatility')
        if volatility == 'high_volatility':
            risk_score += 2
        elif volatility == 'low_volatility':
            risk_score -= 1

        # 시장 추세 반영
        market_trend = analysis.get('market_trend', 'neutral_market')
        if market_trend == 'bear_market':
            risk_score += 1

        # 투자자 심리 반영
        investor_sentiment = analysis.get('investor_sentiment', {})
        if investor_sentiment.get('overall_sentiment') == 'negative':
            risk_score += 1

        # 리스크 수준 결정
        if risk_score >= 3:
            return "high"
        elif risk_score <= 0:
            return "low"
        else:
            return "medium"

    def _analyze_market_data(self) -> Dict[str, Any]:
        """
        시장 데이터를 분석하여 시장 상황을 판단합니다.

        Returns:
            Dict: 분석 결과
        """
        try:
            if not self.market_data:
                return self._get_default_analysis()

            # 코스피/코스닥 지수 분석
            kospi_data = self.market_data.get('kospi', {})
            kosdaq_data = self.market_data.get('kosdaq', {})

            # 투자자 동향 분석
            investor_data = self.market_data.get('investor_flows', {})

            # 시장 상황 판단
            market_trend = self._analyze_market_trend(kospi_data, kosdaq_data)
            investor_sentiment = self._analyze_investor_sentiment(investor_data)
            volatility = self._analyze_volatility(kospi_data, kosdaq_data)

            # 종합 분석 결과
            analysis = {
                'market_trend': market_trend,
                'investor_sentiment': investor_sentiment,
                'volatility': volatility,
                'timestamp': datetime.now().isoformat(),
                'raw_data': self.market_data
            }

            logger.info(f"✅ 시장 상황 분석 완료: {analysis}")
            return analysis

        except Exception as e:
            logger.error(f"❌ 시장 데이터 분석 오류: {e}")
            return self._get_default_analysis()

    def _analyze_market_trend(self, kospi_data: Dict, kosdaq_data: Dict) -> str:
        """
        시장 추세를 분석합니다.

        Args:
            kospi_data: 코스피 지수 데이터
            kosdaq_data: 코스닥 지수 데이터

        Returns:
            str: 시장 추세 ('bull_market', 'bear_market', 'neutral_market')
        """
        try:
            # 코스피/코스닥 전일대비 등락률 확인
            kospi_change = float(kospi_data.get('prdy_vrss', 0))
            kosdaq_change = float(kosdaq_data.get('prdy_vrss', 0))

            # 종합 등락률 계산
            avg_change = (kospi_change + kosdaq_change) / 2

            if avg_change > 0.5:  # 0.5% 이상 상승
                return 'bull_market'
            elif avg_change < -0.5:  # 0.5% 이상 하락
                return 'bear_market'
            else:
                return 'neutral_market'

        except Exception as e:
            logger.error(f"❌ 시장 추세 분석 오류: {e}")
            return 'neutral_market'

    def _analyze_investor_sentiment(self, investor_data: Dict) -> Dict[str, Any]:
        """
        투자자 심리를 분석합니다.

        Args:
            investor_data: 투자자별 매매 현황 데이터

        Returns:
            Dict: 투자자 심리 분석 결과
        """
        try:
            investor_summary = investor_data.get('investor_summary', [])

            # 외국인/기관 순매수 여부 확인
            foreign_buying = False
            institution_buying = False

            for investor in investor_summary:
                if investor.get('invst_tycd') == 'F':  # 외국인
                    foreign_buying = float(investor.get('ntby_amt', 0)) > 0
                elif investor.get('invst_tycd') == 'I':  # 기관
                    institution_buying = float(investor.get('ntby_amt', 0)) > 0

            return {
                'foreign_buying': foreign_buying,
                'institution_buying': institution_buying,
                'overall_sentiment': 'positive' if (foreign_buying and institution_buying) else 'neutral'
            }

        except Exception as e:
            logger.error(f"❌ 투자자 심리 분석 오류: {e}")
            return {
                'foreign_buying': False,
                'institution_buying': False,
                'overall_sentiment': 'neutral'
            }

    def _analyze_volatility(self, kospi_data: Dict, kosdaq_data: Dict) -> str:
        """
        시장 변동성을 분석합니다.

        Args:
            kospi_data: 코스피 지수 데이터
            kosdaq_data: 코스닥 지수 데이터

        Returns:
            str: 변동성 수준 ('high_volatility', 'low_volatility')
        """
        try:
            # 코스피/코스닥 등락폭 확인
            kospi_change = abs(float(kospi_data.get('prdy_vrss', 0)))
            kosdaq_change = abs(float(kosdaq_data.get('prdy_vrss', 0)))

            # 종합 등락폭 계산
            avg_change = (kospi_change + kosdaq_change) / 2

            return 'high_volatility' if avg_change > 1.0 else 'low_volatility'

        except Exception as e:
            logger.error(f"❌ 변동성 분석 오류: {e}")
            return 'low_volatility'

    def _get_default_analysis(self) -> Dict[str, Any]:
        """
        기본 분석 결과를 반환합니다.

        Returns:
            Dict: 기본 분석 결과
        """
        return {
            'market_trend': 'neutral_market',
            'investor_sentiment': {
                'foreign_buying': False,
                'institution_buying': False,
                'overall_sentiment': 'neutral'
            },
            'volatility': 'low_volatility',
            'timestamp': datetime.now().isoformat(),
            'raw_data': None
        }

    def get_market_condition_adjustments(self) -> Dict[str, Any]:
        """
        현재 시장 상황에 따른 매매 전략 조정값을 반환합니다.

        Returns:
            Dict: 매매 전략 조정값
        """
        analysis = self.analyze_market_condition()

        # 기본 조정값
        adjustments = {
            'entry_threshold_multiplier': 1.0,
            'position_size_multiplier': 1.0,
            'pattern_weight_bonus': 0,
            'max_positions_bonus': 0,
            'stop_loss_tighter': 1.0,
            'target_profit_wider': 1.0,
            'position_size_reduction': 1.0,
            'bullish_pattern_bonus': 0,
            'entry_confidence_bonus': 0.0,
            'bearish_pattern_awareness': False,
            'entry_caution_multiplier': 1.0,
            'quick_exit_trigger': False
        }

        # 시장 추세에 따른 조정
        market_trend = analysis.get('market_trend', 'neutral_market')
        if market_trend == 'bull_market':
            adjustments.update({
                'entry_threshold_multiplier': 0.8,
                'position_size_multiplier': 1.2,
                'pattern_weight_bonus': 10,
                'max_positions_bonus': 5
            })
        elif market_trend == 'bear_market':
            adjustments.update({
                'entry_threshold_multiplier': 1.3,
                'position_size_multiplier': 0.7,
                'pattern_weight_penalty': -15,
                'max_positions_reduction': -10
            })

        # 변동성에 따른 조정
        volatility = analysis.get('volatility', 'low_volatility')
        if volatility == 'high_volatility':
            adjustments.update({
                'stop_loss_tighter': 0.7,
                'target_profit_wider': 1.3,
                'position_size_reduction': 0.8
            })

        # 투자자 심리에 따른 조정
        investor_sentiment = analysis.get('investor_sentiment', {})
        if investor_sentiment.get('foreign_buying'):
            adjustments.update({
                'bullish_pattern_bonus': 15,
                'entry_confidence_bonus': 0.1
            })
        if investor_sentiment.get('overall_sentiment') == 'negative':
            adjustments.update({
                'bearish_pattern_awareness': True,
                'entry_caution_multiplier': 1.2,
                'quick_exit_trigger': True
            })

        return adjustments
