import logging
from typing import Dict, List, Optional, Tuple
import pandas as pd
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class PricePositionFilter:
    """🎯 가격 위치 기반 필터링 시스템 (고점 매수 방지)"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.price_filters = config.get('price_position_filters', {})
        self.surge_protection = config.get('surge_protection', {})
        self.technical_filters = config.get('technical_position_filters', {})
        
    def check_price_position_safety(self, stock_code: str, current_price: float, 
                                  ohlcv_data: pd.DataFrame, current_data: Dict) -> Dict:
        """🔍 종합 가격 위치 안전성 체크"""
        try:
            result = {
                'is_safe': True,
                'risk_factors': [],
                'position_scores': {},
                'recommendations': []
            }
            
            if not self.price_filters.get('enabled', False):
                return result
                
            # 1. 고가 대비 현재가 위치 체크
            high_position_check = self._check_high_position(current_price, ohlcv_data)
            result['position_scores'].update(high_position_check)
            
            # 2. 급등 후 고점 체크
            surge_check = self._check_surge_protection(current_price, ohlcv_data)
            result['position_scores'].update(surge_check)
            
            # 3. 기술적 지표 기반 고점 체크
            technical_check = self._check_technical_position(current_price, ohlcv_data, current_data)
            result['position_scores'].update(technical_check)
            
            # 4. 종합 판정
            result = self._make_final_decision(result, stock_code)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ {stock_code} 가격 위치 체크 오류: {e}")
            return {'is_safe': True, 'risk_factors': ['check_error'], 'position_scores': {}}
    
    def _check_high_position(self, current_price: float, ohlcv_data: pd.DataFrame) -> Dict:
        """📊 고가 대비 현재가 위치 체크"""
        try:
            scores = {}
            
            if len(ohlcv_data) < 20:
                return {'high_position_check': 'insufficient_data'}
            
            # 20일 고가 대비 위치
            recent_20d = ohlcv_data.head(20)
            high_20d = recent_20d['stck_hgpr'].astype(float).max()
            position_vs_20d = (current_price / high_20d) * 100
            scores['position_vs_20d_high'] = position_vs_20d
            
            # 60일 고가 대비 위치 (데이터 있는 경우)
            if len(ohlcv_data) >= 60:
                recent_60d = ohlcv_data.head(60)
                high_60d = recent_60d['stck_hgpr'].astype(float).max()
                position_vs_60d = (current_price / high_60d) * 100
                scores['position_vs_60d_high'] = position_vs_60d
            
            # 장중 고가 대비 위치 (당일 데이터)
            if len(ohlcv_data) > 0:
                today_high = float(ohlcv_data.iloc[0].get('stck_hgpr', current_price))
                intraday_position = (current_price / today_high) * 100
                scores['intraday_high_ratio'] = intraday_position
            
            return scores
            
        except Exception as e:
            logger.debug(f"고가 위치 체크 오류: {e}")
            return {'high_position_check': 'error'}
    
    def _check_surge_protection(self, current_price: float, ohlcv_data: pd.DataFrame) -> Dict:
        """🚫 급등 후 고점 매수 방지"""
        try:
            scores = {}
            
            if not self.surge_protection.get('enabled', False) or len(ohlcv_data) < 5:
                return {'surge_protection': 'disabled_or_insufficient_data'}
            
            # 3일간 급등 체크
            if len(ohlcv_data) >= 3:
                price_3d_ago = float(ohlcv_data.iloc[2].get('stck_clpr', current_price))
                surge_3d = ((current_price - price_3d_ago) / price_3d_ago) * 100
                scores['surge_3d_pct'] = surge_3d
            
            # 5일간 급등 체크
            if len(ohlcv_data) >= 5:
                price_5d_ago = float(ohlcv_data.iloc[4].get('stck_clpr', current_price))
                surge_5d = ((current_price - price_5d_ago) / price_5d_ago) * 100
                scores['surge_5d_pct'] = surge_5d
            
            # 거래량 확인 (급등과 함께 거래량 증가했는지)
            if self.surge_protection.get('check_volume_confirmation', False) and len(ohlcv_data) >= 5:
                recent_volume = ohlcv_data.head(3)['stck_vol'].astype(int).mean()
                past_volume = ohlcv_data.iloc[3:8]['stck_vol'].astype(int).mean()
                volume_ratio = recent_volume / past_volume if past_volume > 0 else 1.0
                scores['surge_volume_ratio'] = volume_ratio
            
            return scores
            
        except Exception as e:
            logger.debug(f"급등 보호 체크 오류: {e}")
            return {'surge_protection': 'error'}
    
    def _check_technical_position(self, current_price: float, ohlcv_data: pd.DataFrame, 
                                current_data: Dict) -> Dict:
        """📈 기술적 지표 기반 고점 체크"""
        try:
            scores = {}
            
            if not self.technical_filters.get('enabled', False):
                return {'technical_position': 'disabled'}
            
            # RSI 체크 (이미 다른 곳에서 계산된 값 사용)
            rsi_value = current_data.get('rsi_value')
            if rsi_value:
                scores['current_rsi'] = rsi_value
            
            # 볼린저 밴드 위치 체크 (간단 구현)
            if len(ohlcv_data) >= 20:
                recent_closes = ohlcv_data.head(20)['stck_clpr'].astype(float)
                bb_middle = recent_closes.mean()
                bb_std = recent_closes.std()
                bb_upper = bb_middle + (bb_std * 2)
                bb_lower = bb_middle - (bb_std * 2)
                
                if bb_upper > bb_lower:
                    bb_position = ((current_price - bb_lower) / (bb_upper - bb_lower)) * 100
                    scores['bollinger_position'] = bb_position
            
            # 위꼬리 패턴 체크 (최근 캔들)
            if len(ohlcv_data) > 0:
                recent_candle = ohlcv_data.iloc[0]
                high = float(recent_candle.get('stck_hgpr', 0))
                close = float(recent_candle.get('stck_clpr', 0))
                open_price = float(recent_candle.get('stck_oprc', 0))
                
                if high > 0 and close > 0:
                    body_size = abs(close - open_price)
                    upper_shadow = high - max(close, open_price)
                    
                    if body_size > 0:
                        shadow_ratio = upper_shadow / body_size
                        scores['upper_shadow_ratio'] = shadow_ratio
            
            # 지지선 거리 체크 (간단 구현)
            if len(ohlcv_data) >= 10:
                recent_lows = ohlcv_data.head(10)['stck_lwpr'].astype(float)
                support_level = recent_lows.min()
                support_distance = ((current_price - support_level) / support_level) * 100
                scores['support_distance'] = support_distance
            
            return scores
            
        except Exception as e:
            logger.debug(f"기술적 위치 체크 오류: {e}")
            return {'technical_position': 'error'}
    
    def _make_final_decision(self, result: Dict, stock_code: str) -> Dict:
        """🎯 종합 판정 및 권고사항 생성"""
        try:
            scores = result['position_scores']
            risk_factors = []
            recommendations = []
            
            # 1. 고가 대비 위치 체크
            position_20d = scores.get('position_vs_20d_high', 0)
            if position_20d > self.price_filters.get('max_position_vs_20d_high', 90):
                risk_factors.append(f'20일고가대비_{position_20d:.1f}%')
                recommendations.append('20일 고가 근처 - 매수 신중')
            
            position_60d = scores.get('position_vs_60d_high', 0)
            if position_60d > self.price_filters.get('max_position_vs_60d_high', 95):
                risk_factors.append(f'60일고가대비_{position_60d:.1f}%')
                recommendations.append('60일 고가 근처 - 매수 위험')
            
            intraday_ratio = scores.get('intraday_high_ratio', 0)
            if intraday_ratio > self.price_filters.get('max_intraday_high_ratio', 95):
                risk_factors.append(f'장중고가대비_{intraday_ratio:.1f}%')
                recommendations.append('장중 고가 근처 - 매수 부적절')
            
            # 2. 급등 보호 체크
            surge_3d = scores.get('surge_3d_pct', 0)
            if surge_3d > self.surge_protection.get('max_3day_surge', 25):
                risk_factors.append(f'3일급등_{surge_3d:.1f}%')
                recommendations.append('3일간 급등 - 고점 매수 위험')
            
            surge_5d = scores.get('surge_5d_pct', 0)
            if surge_5d > self.surge_protection.get('max_5day_surge', 35):
                risk_factors.append(f'5일급등_{surge_5d:.1f}%')
                recommendations.append('5일간 급등 - 조정 대기 권장')
            
            # 3. 기술적 지표 체크
            rsi_value = scores.get('current_rsi', 50)
            if rsi_value > self.technical_filters.get('rsi_high_threshold', 65):
                risk_factors.append(f'RSI과매수_{rsi_value:.1f}')
                recommendations.append('RSI 과매수 구간 - 매수 신중')
            
            bb_position = scores.get('bollinger_position', 50)
            if bb_position > self.technical_filters.get('max_bollinger_position', 85):
                risk_factors.append(f'볼린저상단_{bb_position:.1f}%')
                recommendations.append('볼린저 밴드 상단 - 고점 위험')
            
            shadow_ratio = scores.get('upper_shadow_ratio', 0)
            if shadow_ratio > self.technical_filters.get('avoid_upper_shadow_ratio', 2.5):
                risk_factors.append(f'긴위꼬리_{shadow_ratio:.1f}배')
                recommendations.append('긴 위꼬리 - 매도 압력 존재')
            
            # 4. 최종 판정
            critical_risk_count = len([r for r in risk_factors if any(keyword in r for keyword in 
                                     ['20일고가대비', '3일급등', 'RSI과매수', '볼린저상단'])])
            
            if critical_risk_count >= 2:
                result['is_safe'] = False
                recommendations.append('⚠️ 복합 위험 요소 - 매수 금지')
            elif len(risk_factors) >= 3:
                result['is_safe'] = False
                recommendations.append('⚠️ 다중 위험 요소 - 매수 부적절')
            elif len(risk_factors) >= 1:
                recommendations.append('⚡ 일부 위험 요소 - 매수 신중')
            
            result['risk_factors'] = risk_factors
            result['recommendations'] = recommendations
            
            # 로깅
            if not result['is_safe']:
                logger.info(f"🚫 {stock_code} 가격위치 필터링: {', '.join(risk_factors)}")
            elif risk_factors:
                logger.debug(f"⚠️ {stock_code} 가격위치 주의: {', '.join(risk_factors)}")
            
            return result
            
        except Exception as e:
            logger.error(f"최종 판정 오류: {e}")
            result['is_safe'] = True  # 오류시 통과
            return result
    
    def get_position_summary(self, scores: Dict) -> str:
        """📊 위치 요약 정보 생성"""
        try:
            summary_parts = []
            
            if 'position_vs_20d_high' in scores:
                summary_parts.append(f"20일고가대비 {scores['position_vs_20d_high']:.1f}%")
            
            if 'surge_3d_pct' in scores:
                summary_parts.append(f"3일상승 {scores['surge_3d_pct']:.1f}%")
            
            if 'current_rsi' in scores:
                summary_parts.append(f"RSI {scores['current_rsi']:.1f}")
            
            if 'bollinger_position' in scores:
                summary_parts.append(f"BB위치 {scores['bollinger_position']:.1f}%")
            
            return " | ".join(summary_parts) if summary_parts else "위치정보없음"
            
        except Exception as e:
            logger.debug(f"위치 요약 생성 오류: {e}")
            return "요약오류" 