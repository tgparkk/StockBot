"""
패턴 감지 통합 관리자 - 장전/실시간 패턴 감지기 통합 운영
"""
from datetime import datetime, time
from typing import Dict, List, Optional, Tuple, Any
from utils.logger import setup_logger

from .candle_pattern_detector import CandlePatternDetector
from .realtime_pattern_detector import RealtimePatternDetector
from .candle_trade_candidate import CandlePatternInfo, TradeSignal

logger = setup_logger(__name__)


class PatternManager:
    """🎯 패턴 감지 통합 관리자"""

    def __init__(self, premarket_detector: CandlePatternDetector, realtime_detector: RealtimePatternDetector):
        # 외부에서 생성된 감지기 인스턴스 사용
        self.premarket_detector = premarket_detector    # 장전 전용
        self.realtime_detector = realtime_detector      # 실시간 전용
        
        logger.info("🎯 PatternManager 초기화 완료 - 외부 감지기 인스턴스 연결")

    def analyze_patterns(self, stock_code: str, 
                        current_price: float,
                        daily_ohlcv: Any,
                        minute_data: Optional[Any] = None,
                        mode: str = "auto") -> Dict:
        """🔍 통합 패턴 분석 - 시간대별 자동 선택"""
        try:
            current_time = datetime.now().time()
            
            # 모드 자동 결정
            if mode == "auto":
                if self._is_premarket_time(current_time):
                    mode = "premarket"
                elif self._is_trading_time(current_time):
                    mode = "realtime"
                else:
                    mode = "premarket"  # 장후에는 다음날 준비
            
            # 모드별 분석 실행
            if mode == "premarket":
                return self._analyze_premarket_patterns(stock_code, daily_ohlcv)
            elif mode == "realtime":
                return self._analyze_realtime_patterns(stock_code, current_price, daily_ohlcv, minute_data)
            elif mode == "both":
                return self._analyze_both_patterns(stock_code, current_price, daily_ohlcv, minute_data)
            else:
                logger.warning(f"알 수 없는 모드: {mode}")
                return self._get_empty_result()

        except Exception as e:
            logger.error(f"통합 패턴 분석 오류 ({stock_code}): {e}")
            return self._get_empty_result()

    def _analyze_premarket_patterns(self, stock_code: str, daily_ohlcv: Any) -> Dict:
        """🌅 장전 패턴 분석"""
        try:
            patterns = self.premarket_detector.analyze_stock_patterns(stock_code, daily_ohlcv)
            
            # 매매 신호 생성 (기존 로직 사용)
            trade_signal = TradeSignal.HOLD
            signal_strength = 0
            
            if patterns:
                strongest_pattern = max(patterns, key=lambda p: p.strength)
                if strongest_pattern.pattern_type.value in ['hammer', 'bullish_engulfing', 'piercing_line', 'morning_star']:
                    trade_signal = TradeSignal.BUY
                    signal_strength = strongest_pattern.strength

            result = {
                'mode': 'premarket',
                'patterns': patterns,
                'trade_signal': trade_signal,
                'signal_strength': signal_strength,
                'analysis_time': datetime.now().isoformat(),
                'detector_used': 'CandlePatternDetector',
                'description': f"장전 분석 - {len(patterns)}개 패턴 감지"
            }

            if patterns:
                logger.info(f"🌅 {stock_code} 장전 패턴 분석: {len(patterns)}개 패턴")
            
            return result

        except Exception as e:
            logger.error(f"장전 패턴 분석 오류 ({stock_code}): {e}")
            return self._get_empty_result()

    def _analyze_realtime_patterns(self, stock_code: str, current_price: float, 
                                  daily_ohlcv: Any, minute_data: Optional[Any]) -> Dict:
        """🕐 실시간 패턴 분석"""
        try:
            patterns = self.realtime_detector.analyze_realtime_patterns(
                stock_code, current_price, daily_ohlcv, minute_data
            )
            
            # 실시간 매매 신호 생성
            trade_signal, signal_strength = self.realtime_detector.get_realtime_trading_signal(patterns)

            result = {
                'mode': 'realtime',
                'patterns': patterns,
                'trade_signal': trade_signal,
                'signal_strength': signal_strength,
                'current_price': current_price,
                'analysis_time': datetime.now().isoformat(),
                'detector_used': 'RealtimePatternDetector',
                'description': f"실시간 분석 - {len(patterns)}개 패턴 감지"
            }

            if patterns:
                logger.info(f"🕐 {stock_code} 실시간 패턴 분석: {len(patterns)}개 패턴")
            
            return result

        except Exception as e:
            logger.error(f"실시간 패턴 분석 오류 ({stock_code}): {e}")
            return self._get_empty_result()

    def _analyze_both_patterns(self, stock_code: str, current_price: float, 
                              daily_ohlcv: Any, minute_data: Optional[Any]) -> Dict:
        """🔄 장전+실시간 종합 분석"""
        try:
            # 두 분석기 모두 실행
            premarket_result = self._analyze_premarket_patterns(stock_code, daily_ohlcv)
            realtime_result = self._analyze_realtime_patterns(stock_code, current_price, daily_ohlcv, minute_data)
            
            # 결과 통합
            all_patterns = premarket_result.get('patterns', []) + realtime_result.get('patterns', [])
            
            # 종합 신호 결정 (실시간 우선)
            if realtime_result['trade_signal'] != TradeSignal.HOLD:
                final_signal = realtime_result['trade_signal']
                final_strength = realtime_result['signal_strength']
                primary_source = 'realtime'
            elif premarket_result['trade_signal'] != TradeSignal.HOLD:
                final_signal = premarket_result['trade_signal']
                final_strength = premarket_result['signal_strength'] 
                primary_source = 'premarket'
            else:
                final_signal = TradeSignal.HOLD
                final_strength = 0
                primary_source = 'none'

            result = {
                'mode': 'both',
                'patterns': all_patterns,
                'trade_signal': final_signal,
                'signal_strength': final_strength,
                'primary_source': primary_source,
                'premarket_patterns': len(premarket_result.get('patterns', [])),
                'realtime_patterns': len(realtime_result.get('patterns', [])),
                'analysis_time': datetime.now().isoformat(),
                'detector_used': 'Both',
                'description': f"종합 분석 - 장전:{len(premarket_result.get('patterns', []))}개, 실시간:{len(realtime_result.get('patterns', []))}개"
            }

            logger.info(f"🔄 {stock_code} 종합 패턴 분석: 총 {len(all_patterns)}개 패턴")
            
            return result

        except Exception as e:
            logger.error(f"종합 패턴 분석 오류 ({stock_code}): {e}")
            return self._get_empty_result()

    def get_recommended_mode(self, current_time: Optional[datetime] = None) -> str:
        """💡 현재 시간대에 권장되는 분석 모드"""
        if current_time is None:
            current_time = datetime.now().time()
        else:
            current_time = current_time.time()
        
        if self._is_premarket_time(current_time):
            return "premarket"
        elif self._is_trading_time(current_time):
            return "realtime"
        else:
            return "premarket"  # 장후 -> 다음날 준비

    def _is_premarket_time(self, current_time: time) -> bool:
        """장전 시간대 확인"""
        try:
            # 08:00 ~ 08:59
            return (time(8, 0) <= current_time <= time(8, 59))
        except:
            return False

    def _is_trading_time(self, current_time: time) -> bool:
        """거래 시간대 확인"""
        try:
            # 09:00 ~ 15:30
            return (time(9, 0) <= current_time <= time(15, 30))
        except:
            return False

    def _get_empty_result(self) -> Dict:
        """빈 결과 반환"""
        return {
            'mode': 'none',
            'patterns': [],
            'trade_signal': TradeSignal.HOLD,
            'signal_strength': 0,
            'analysis_time': datetime.now().isoformat(),
            'detector_used': 'None',
            'description': '패턴 분석 없음'
        }

    def get_detector_info(self) -> Dict:
        """감지기 정보 반환"""
        return {
            'premarket_detector': {
                'class': 'CandlePatternDetector',
                'purpose': '장전 패턴 분석 (완성된 캔들)',
                'patterns': list(self.premarket_detector.pattern_weights.keys()),
                'confidence_threshold': self.premarket_detector.thresholds.get('min_confidence', 0.6)
            },
            'realtime_detector': {
                'class': 'RealtimePatternDetector', 
                'purpose': '실시간 패턴 분석 (진행중인 캔들)',
                'patterns': list(self.realtime_detector.pattern_weights.keys()),
                'confidence_threshold': self.realtime_detector.thresholds.get('realtime_confidence_min', 0.55)
            }
        } 