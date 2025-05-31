"""
KIS REST API 통합 관리자 (리팩토링 버전)
공식 문서 기반 + 모듈화
"""
import time
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Union, Any
from utils.logger import setup_logger

# KIS 모듈 import
from . import kis_auth as kis
from . import kis_order_api as order_api
from . import kis_account_api as account_api
from . import kis_market_api as market_api
from .kis_market_api import (
    get_gap_trading_candidates, get_volume_breakout_candidates, get_momentum_candidates,
    get_fluctuation_rank, get_volume_rank, get_volume_power_rank
)

# 데이터 모델은 필요할 때 지연 import (순환 import 방지)

# 설정 import (settings.py에서 .env 파일을 읽어서 제공)
from config.settings import (
    KIS_BASE_URL, APP_KEY, SECRET_KEY,
    ACCOUNT_NUMBER, HTS_ID
)

logger = setup_logger(__name__)


class KISRestAPIManager:
    """KIS REST API 통합 관리자 (간소화 버전)"""

    def __init__(self):
        """초기화"""
        # 인증 초기화
        svr = 'prod'
        if not kis.auth(svr):
            raise ValueError("KIS API 인증 실패")


    # === 인증 관련 ===

    def get_token_info(self) -> Dict:
        """토큰 정보 조회"""
        env = kis.getTREnv()
        if not env:
            return {"status": "error", "message": "인증되지 않음"}

        return {
            "status": "success",
            "app_key": env.my_app[:10] + "...",  # 일부만 표시
            "account": env.my_acct,
            "product": env.my_prod,
            "url": env.my_url
        }

    def force_token_refresh(self) -> bool:
        """토큰 강제 갱신"""
        svr = 'prod'
        return kis.auth(svr)

    # === 주문 관련 ===

    def buy_order(self, stock_code: str, quantity: int, price: int = 0) -> Dict:
        """매수 주문"""
        if price == 0:
            logger.warning("시장가 매수 주문")

        result = order_api.get_order_cash("buy", stock_code, quantity, price)

        if result is not None and not result.empty:
            return {
                "status": "success",
                "order_no": result.iloc[0].get('odno', ''),
                "message": "매수 주문 완료"
            }
        else:
            return {
                "status": "error",
                "message": "매수 주문 실패"
            }

    def sell_order(self, stock_code: str, quantity: int, price: int = 0) -> Dict:
        """매도 주문"""
        if price == 0:
            logger.warning("시장가 매도 주문")

        result = order_api.get_order_cash("sell", stock_code, quantity, price)

        if result is not None and not result.empty:
            return {
                "status": "success",
                "order_no": result.iloc[0].get('odno', ''),
                "message": "매도 주문 완료"
            }
        else:
            return {
                "status": "error",
                "message": "매도 주문 실패"
            }

    def cancel_order(self, order_no: str, stock_code: str, quantity: int) -> Dict:
        """주문 취소"""
        # 실제 주문 취소는 정정취소 API를 사용해야 하며 추가 정보가 필요
        logger.warning("주문 취소는 정정취소 API를 직접 사용하세요")
        return {
            "status": "error",
            "message": "주문 취소는 kis_order_api.get_order_rvsecncl() 사용 권장"
        }

    def get_today_orders(self) -> List[Dict]:
        """당일 주문 내역"""
        result = order_api.get_inquire_daily_ccld_lst()

        if result is not None and not result.empty:
            return result.to_dict('records')
        else:
            return []

    # === 계좌 관련 ===

    def get_balance(self) -> Dict:
        """계좌 잔고 조회"""
        # 계좌 요약 정보
        balance_obj = account_api.get_inquire_balance_obj()
        # 보유 종목 목록
        balance_lst = account_api.get_inquire_balance_lst()

        if balance_obj is not None and not balance_obj.empty:
            summary = balance_obj.iloc[0].to_dict()
        else:
            summary = {}

        if balance_lst is not None and not balance_lst.empty:
            holdings = balance_lst.to_dict('records')
        else:
            holdings = []

        return {
            "status": "success",
            "summary": summary,
            "holdings": holdings,
            "total_count": len(holdings)
        }

    def get_buy_possible(self, stock_code: str, price: int = 0) -> Dict:
        """매수 가능 조회"""
        result = account_api.get_inquire_psbl_order(stock_code, price)

        if result is not None and not result.empty:
            data = result.iloc[0].to_dict()
            return {
                "status": "success",
                "stock_code": stock_code,
                "max_buy_amount": int(data.get('max_buy_amt', 0)),
                "max_buy_qty": int(data.get('max_buy_qty', 0)),
                "available_cash": int(data.get('ord_psbl_cash', 0))
            }
        else:
            return {
                "status": "error",
                "message": "매수가능조회 실패"
            }

    # === 시세 관련 ===

    def get_current_price(self, stock_code: str) -> Dict:
        """현재가 조회"""
        result = market_api.get_inquire_price("J", stock_code)

        if result is not None and not result.empty:
            data = result.iloc[0].to_dict()
            return {
                "status": "success",
                "stock_code": stock_code,
                "current_price": int(data.get('stck_prpr', 0)),
                "change_rate": float(data.get('prdy_ctrt', 0)),
                "volume": int(data.get('acml_vol', 0)),
                "high_price": int(data.get('stck_hgpr', 0)),
                "low_price": int(data.get('stck_lwpr', 0)),
                "open_price": int(data.get('stck_oprc', 0))
            }
        else:
            return {
                "status": "error",
                "message": "현재가 조회 실패"
            }

    def get_orderbook(self, stock_code: str) -> Dict:
        """호가 조회"""
        result = market_api.get_inquire_asking_price_exp_ccn("1", "J", stock_code)

        if result is not None and not result.empty:
            data = result.iloc[0].to_dict()

            # 호가 데이터 파싱
            asks = []  # 매도호가
            bids = []  # 매수호가

            for i in range(1, 11):  # 1~10호가
                ask_price = int(data.get(f'askp{i}', 0))
                ask_volume = int(data.get(f'askp_rsqn{i}', 0))
                bid_price = int(data.get(f'bidp{i}', 0))
                bid_volume = int(data.get(f'bidp_rsqn{i}', 0))

                if ask_price > 0:
                    asks.append({"price": ask_price, "volume": ask_volume})
                if bid_price > 0:
                    bids.append({"price": bid_price, "volume": bid_volume})

            return {
                "status": "success",
                "stock_code": stock_code,
                "asks": asks,
                "bids": bids,
                "total_ask_volume": int(data.get('total_askp_rsqn', 0)),
                "total_bid_volume": int(data.get('total_bidp_rsqn', 0))
            }
        else:
            return {
                "status": "error",
                "message": "호가 조회 실패"
            }

    def get_daily_prices(self, stock_code: str, period_type: str = "D") -> List[Dict]:
        """일봉 데이터 조회"""
        result = market_api.get_inquire_daily_itemchartprice("2", "J", stock_code, period_code=period_type)

        if result is not None and not result.empty:
            return result.to_dict('records')
        else:
            return []

    # === 시장 스크리닝 관련 ===
    def get_market_screening_candidates(self, market_type: str = "all") -> Dict:
        """🎯 수익성 중심 시장 스크리닝 - 기술적 지표 통합 강화"""
        logger.info(f"시장 스크리닝 시작: {market_type}")
        
        candidates = {
            'gap': [],
            'volume': [],
            'momentum': [],
            'technical': []  # 🆕 기술적 지표 기반 후보
        }
        
        markets = ["0000", "0001", "1001"] if market_type == "all" else [market_type]
        
        # 🎯 시간대별 적응형 전략
        from datetime import datetime
        current_time = datetime.now()
        is_pre_market = current_time.hour < 9 or (current_time.hour == 9 and current_time.minute < 30)
        is_early_market = current_time.hour < 11
        
        for market in markets:
            try:
                # === 🆕 기술적 지표 우선 스크리닝 ===
                try:
                    technical_candidates = self._get_technical_indicator_candidates(market, is_pre_market)
                    if technical_candidates:
                        candidates['technical'].extend(technical_candidates)
                        logger.info(f"📈 기술적 지표 후보: {len(technical_candidates)}개 ({market})")
                except Exception as e:
                    logger.error(f"기술적 지표 후보 조회 오류 ({market}): {e}")

                # === 갭 트레이딩 후보 (기술적 지표 필터링 적용) ===
                try:
                    gap_candidates = market_api.get_gap_trading_candidates(market)
                    if gap_candidates is not None and not gap_candidates.empty:
                        # 🆕 기술적 지표로 필터링
                        filtered_gap_data = self._process_gap_candidates_with_technical_filter(gap_candidates)
                        candidates['gap'].extend(filtered_gap_data)
                        logger.info(f"📊 갭 후보 (기술적필터링): {len(filtered_gap_data)}개 ({market})")
                    else:
                        logger.warning(f"⚠️ 갭 후보 없음 - 백업 전략 시도 ({market})")
                        backup_gap_candidates = self._get_backup_gap_candidates(market, is_pre_market)
                        if backup_gap_candidates:
                            candidates['gap'].extend(backup_gap_candidates)
                            logger.info(f"🔄 갭 백업 후보: {len(backup_gap_candidates)}개 ({market})")
                        
                except Exception as e:
                    logger.error(f"갭 후보 조회 오류 ({market}): {e}")

                # === 거래량 돌파 후보 (기술적 지표 필터링 적용) ===
                try:
                    volume_candidates = market_api.get_volume_breakout_candidates(market)
                    if volume_candidates is not None and not volume_candidates.empty:
                        # 🆕 기술적 지표로 필터링
                        filtered_volume_data = self._process_volume_candidates_with_technical_filter(volume_candidates)
                        candidates['volume'].extend(filtered_volume_data)
                        logger.info(f"📊 거래량 후보 (기술적필터링): {len(filtered_volume_data)}개 ({market})")
                    else:
                        logger.warning(f"⚠️ 거래량 후보 없음 - 백업 전략 시도 ({market})")
                        backup_volume_candidates = self._get_backup_volume_candidates(market, is_pre_market)
                        if backup_volume_candidates:
                            candidates['volume'].extend(backup_volume_candidates)
                            logger.info(f"🔄 거래량 백업 후보: {len(backup_volume_candidates)}개 ({market})")
                        
                except Exception as e:
                    logger.error(f"거래량 후보 조회 오류 ({market}): {e}")

                # === 모멘텀 후보 (기술적 지표 필터링 적용) ===
                try:
                    momentum_candidates = market_api.get_momentum_candidates(market)
                    if momentum_candidates is not None and not momentum_candidates.empty:
                        # 🆕 기술적 지표로 필터링
                        filtered_momentum_data = self._process_momentum_candidates_with_technical_filter(momentum_candidates)
                        candidates['momentum'].extend(filtered_momentum_data)
                        logger.info(f"📊 모멘텀 후보 (기술적필터링): {len(filtered_momentum_data)}개 ({market})")
                    else:
                        logger.warning(f"⚠️ 모멘텀 후보 없음 - 백업 전략 시도 ({market})")
                        backup_momentum_candidates = self._get_backup_momentum_candidates(market, is_pre_market)
                        if backup_momentum_candidates:
                            candidates['momentum'].extend(backup_momentum_candidates)
                            logger.info(f"🔄 모멘텀 백업 후보: {len(backup_momentum_candidates)}개 ({market})")
                        
                except Exception as e:
                    logger.error(f"모멘텀 후보 조회 오류 ({market}): {e}")

                # 🆕 API 제한 극복을 위한 추가 스크리닝
                try:
                    additional_candidates = self._get_extended_screening_candidates(market, is_pre_market)
                    if additional_candidates:
                        # 중복 제거하여 각 카테고리에 분산 추가
                        for category, items in additional_candidates.items():
                            if category in candidates:
                                candidates[category].extend(items)
                        logger.info(f"🔍 확장 스크리닝 완료: {sum(len(v) for v in additional_candidates.values())}개 추가 ({market})")
                except Exception as e:
                    logger.error(f"확장 스크리닝 오류 ({market}): {e}")

                # 짧은 대기 (API 호출 간격)
                time.sleep(0.2)

            except Exception as e:
                logger.error(f"시장 {market} 스크리닝 오류: {e}")

        # 🆕 결과 종합 및 우선순위 정렬
        total_candidates = sum(len(v) for v in candidates.values())
        logger.info(f"🎯 시장 스크리닝 완료: 총 {total_candidates}개 후보 발견")
        logger.info(f"📊 카테고리별: 기술적{len(candidates['technical'])} 갭{len(candidates['gap'])} 거래량{len(candidates['volume'])} 모멘텀{len(candidates['momentum'])}")
        
        # 우선순위 정렬 (기술적 지표 점수 기준)
        self._sort_candidates_by_technical_score(candidates)
        
        return candidates

    def _get_technical_indicator_candidates(self, market: str, is_pre_market: bool) -> List[Dict]:
        """🆕 기술적 지표 기반 우선 스크리닝"""
        try:
            from ..analysis.technical_indicators import TechnicalIndicators
            
            candidates = []
            
            # 🎯 1단계: 다양한 API로 폭넓은 종목 수집
            screening_methods = [
                # 이격도 기반 (RSI 과매도 유사)
                lambda: market_api.get_disparity_rank(fid_input_iscd=market, fid_rank_sort_cls_code="1", fid_hour_cls_code="20"),
                # 거래량 급증 (모멘텀)
                lambda: market_api.get_volume_rank(fid_input_iscd=market, fid_blng_cls_code="1"),
                # 등락률 상위 (추세)
                lambda: market_api.get_fluctuation_rank(fid_input_iscd=market, fid_rank_sort_cls_code="0"),
                # 체결강도 상위 (매수 우위)
                lambda: market_api.get_volume_power_rank(fid_input_iscd=market)
            ]
            
            collected_stocks = set()
            
            for method in screening_methods:
                try:
                    data = method()
                    if data is not None and not data.empty:
                        for _, row in data.head(30).iterrows():  # 각 방법에서 30개씩
                            stock_code = row.get('stck_shrn_iscd', '')
                            if stock_code and stock_code not in collected_stocks:
                                collected_stocks.add(stock_code)
                except Exception as e:
                    logger.debug(f"스크리닝 방법 오류: {e}")
                    continue
                    
                time.sleep(0.1)  # API 제한 방지
            
            logger.info(f"📊 1단계 수집완료: {len(collected_stocks)}개 종목")
            
            # 🎯 2단계: 기술적 지표 분석 및 필터링
            for stock_code in list(collected_stocks)[:100]:  # 최대 100개까지 분석
                try:
                    # 가격 데이터 조회 (DataFrame 반환)
                    price_data = market_api.get_inquire_daily_price("J", stock_code)
                    if price_data is None or price_data.empty or len(price_data) < 20:
                        continue
                    
                    # 현재가 정보
                    current_data = market_api.get_inquire_price("J", stock_code)
                    if current_data is None or current_data.empty:
                        continue
                    
                    current_info = current_data.iloc[0]
                    current_price = int(current_info.get('stck_prpr', 0))
                    
                    # DataFrame에서 종가 컬럼 추출 (올바른 컬럼명 사용)
                    if 'stck_clpr' in price_data.columns:
                        closes = price_data['stck_clpr'].astype(int).tolist()
                    elif 'close' in price_data.columns:
                        closes = price_data['close'].astype(int).tolist()
                    else:
                        # 컬럼명을 찾지 못한 경우 스킵
                        logger.debug(f"종목 {stock_code}: 종가 컬럼을 찾을 수 없음. 컬럼: {price_data.columns.tolist()}")
                        continue
                    
                    if not closes or len(closes) < 20 or current_price <= 0:
                        continue
                    
                    # RSI 계산
                    rsi = TechnicalIndicators.calculate_rsi(closes)[-1]
                    
                    # MACD 계산
                    macd_data = TechnicalIndicators.calculate_macd(closes)
                    macd_line = macd_data['macd'][-1]
                    macd_signal = macd_data['signal'][-1]
                    macd_histogram = macd_data['histogram'][-1]
                    
                    # 이동평균 계산
                    ma_data = TechnicalIndicators.calculate_moving_averages(closes, [5, 20, 60])
                    ma_5 = ma_data['ma_5'][-1]
                    ma_20 = ma_data['ma_20'][-1]
                    ma_60 = ma_data['ma_60'][-1]
                    
                    # 🎯 기술적 신호 분석
                    technical_score = 0
                    signals = []
                    
                    # RSI 신호
                    if 30 <= rsi <= 50:  # 과매도에서 회복
                        technical_score += 25
                        signals.append(f"RSI회복({rsi:.1f})")
                    elif rsi < 30:  # 과매도 (반등 기대)
                        technical_score += 20
                        signals.append(f"RSI과매도({rsi:.1f})")
                    elif rsi > 70:  # 과매수 (주의)
                        technical_score -= 10
                        signals.append(f"RSI과매수({rsi:.1f})")
                    
                    # MACD 신호
                    if macd_line > macd_signal and macd_histogram > 0:  # 상승 신호
                        technical_score += 25
                        signals.append("MACD상승")
                    elif macd_histogram > 0 and len(macd_data['histogram']) > 1:
                        if macd_data['histogram'][-2] <= 0:  # 음수→양수 전환
                            technical_score += 30
                            signals.append("MACD전환")
                    
                    # 이동평균 정배열
                    if current_price > ma_5 > ma_20 > ma_60:  # 완벽한 상승배열
                        technical_score += 35
                        signals.append("완벽상승배열")
                    elif current_price > ma_5 > ma_20:  # 단기 상승배열
                        technical_score += 20
                        signals.append("단기상승배열")
                    elif ma_5 > ma_20:  # 골든크로스
                        technical_score += 15
                        signals.append("골든크로스")
                    
                    # 🎯 종합 평가 (50점 이상만 선별)
                    if technical_score >= 50:  # 기준점 낮춤 (더 많은 후보)
                        candidates.append({
                            'stock_code': stock_code,
                            'current_price': current_price,
                            'rsi': rsi,
                            'macd_line': macd_line,
                            'macd_signal': macd_signal,
                            'ma_5': ma_5,
                            'ma_20': ma_20,
                            'ma_60': ma_60,
                            'technical_score': technical_score,
                            'signals': signals,
                            'reason': f"기술적점수{technical_score}점",
                            'strategy': 'technical_priority'
                        })
                
                except Exception as e:
                    logger.debug(f"종목 {stock_code} 기술적 분석 오류: {e}")
                    continue
                
                # API 제한 방지
                time.sleep(0.05)
            
            # 점수 순으로 정렬
            candidates.sort(key=lambda x: x['technical_score'], reverse=True)
            
            logger.info(f"📈 기술적 지표 후보 선별완료: {len(candidates)}개 (50점 이상)")
            return candidates[:50]  # 상위 50개만 반환
            
        except Exception as e:
            logger.error(f"기술적 지표 스크리닝 오류: {e}")
            return []

    def _process_gap_candidates_with_technical_filter(self, gap_candidates: pd.DataFrame) -> List[Dict]:
        """🆕 갭 후보에 기술적 지표 필터링 적용"""
        try:
            from ..analysis.technical_indicators import TechnicalIndicators
            
            filtered_candidates = []
            
            for _, row in gap_candidates.head(20).iterrows():
                try:
                    stock_code = row.get('stck_shrn_iscd', '')
                    if not stock_code:
                        continue
                    
                    # 기본 갭 정보
                    gap_info = self._calculate_gap_info(row)
                    if not gap_info:
                        continue
                    
                    # 📈 기술적 지표 검증
                    technical_check = self._quick_technical_check(stock_code)
                    if not technical_check:
                        continue
                    
                    # 기술적 지표가 양호한 경우만 포함
                    if technical_check['score'] >= 30:  # 낮은 기준 (더 많은 후보)
                        candidate = {
                            **gap_info,
                            'technical_score': technical_check['score'],
                            'technical_signals': technical_check['signals'],
                            'strategy': 'gap_with_technical'
                        }
                        filtered_candidates.append(candidate)
                
                except Exception as e:
                    logger.debug(f"갭 후보 기술적 필터링 오류: {e}")
                    continue
                
                time.sleep(0.05)
            
            return filtered_candidates
            
        except Exception as e:
            logger.error(f"갭 후보 기술적 필터링 오류: {e}")
            return []

    def _process_volume_candidates_with_technical_filter(self, volume_candidates: pd.DataFrame) -> List[Dict]:
        """🆕 거래량 후보에 기술적 지표 필터링 적용"""
        try:
            filtered_candidates = []
            
            for _, row in volume_candidates.head(20).iterrows():
                try:
                    stock_code = row.get('stck_shrn_iscd', '')
                    if not stock_code:
                        continue
                    
                    # 기본 거래량 정보
                    volume_info = self._calculate_volume_info(row)
                    if not volume_info:
                        continue
                    
                    # 📈 기술적 지표 검증
                    technical_check = self._quick_technical_check(stock_code)
                    if not technical_check:
                        continue
                    
                    # 기술적 지표가 양호한 경우만 포함
                    if technical_check['score'] >= 30:
                        candidate = {
                            **volume_info,
                            'technical_score': technical_check['score'],
                            'technical_signals': technical_check['signals'],
                            'strategy': 'volume_with_technical'
                        }
                        filtered_candidates.append(candidate)
                
                except Exception as e:
                    logger.debug(f"거래량 후보 기술적 필터링 오류: {e}")
                    continue
                
                time.sleep(0.05)
            
            return filtered_candidates
            
        except Exception as e:
            logger.error(f"거래량 후보 기술적 필터링 오류: {e}")
            return []

    def _process_momentum_candidates_with_technical_filter(self, momentum_candidates: pd.DataFrame) -> List[Dict]:
        """🆕 모멘텀 후보에 기술적 지표 필터링 적용"""
        try:
            filtered_candidates = []
            
            for _, row in momentum_candidates.head(20).iterrows():
                try:
                    stock_code = row.get('stck_shrn_iscd', '')
                    if not stock_code:
                        continue
                    
                    # 기본 모멘텀 정보
                    momentum_info = self._calculate_momentum_info(row)
                    if not momentum_info:
                        continue
                    
                    # 📈 기술적 지표 검증
                    technical_check = self._quick_technical_check(stock_code)
                    if not technical_check:
                        continue
                    
                    # 기술적 지표가 양호한 경우만 포함
                    if technical_check['score'] >= 30:
                        candidate = {
                            **momentum_info,
                            'technical_score': technical_check['score'],
                            'technical_signals': technical_check['signals'],
                            'strategy': 'momentum_with_technical'
                        }
                        filtered_candidates.append(candidate)
                
                except Exception as e:
                    logger.debug(f"모멘텀 후보 기술적 필터링 오류: {e}")
                    continue
                
                time.sleep(0.05)
            
            return filtered_candidates
            
        except Exception as e:
            logger.error(f"모멘텀 후보 기술적 필터링 오류: {e}")
            return []

    def _quick_technical_check(self, stock_code: str) -> Dict:
        """🆕 빠른 기술적 지표 검증"""
        try:
            from ..analysis.technical_indicators import TechnicalIndicators
            
            # 최근 20일 데이터만 조회 (빠른 분석)
            price_data = market_api.get_inquire_daily_price("J", stock_code)
            if price_data is None or price_data.empty or len(price_data) < 10:
                return None
            
            # 현재가 정보
            current_data = market_api.get_inquire_price("J", stock_code)
            if current_data is None or current_data.empty:
                return None
            
            current_info = current_data.iloc[0]
            current_price = int(current_info.get('stck_prpr', 0))
            
            # DataFrame에서 종가 컬럼 추출 (20일 제한)
            price_data_limited = price_data.head(20)
            if 'stck_clpr' in price_data_limited.columns:
                closes = price_data_limited['stck_clpr'].astype(int).tolist()
            elif 'close' in price_data_limited.columns:
                closes = price_data_limited['close'].astype(int).tolist()
            else:
                # 컬럼명을 찾지 못한 경우 None 반환
                logger.debug(f"빠른 체크 {stock_code}: 종가 컬럼을 찾을 수 없음")
                return None
            
            # 현재가 추가
            closes.append(current_price)
            
            if not closes or current_price <= 0:
                return None
            
            # 빠른 기술적 지표 계산
            score = 0
            signals = []
            
            # RSI (10일 단축 버전)
            rsi = TechnicalIndicators.calculate_rsi(closes, period=min(10, len(closes)))[-1]
            if 30 <= rsi <= 65:  # 매수 적정 구간
                score += 20
                signals.append(f"RSI양호({rsi:.0f})")
            elif rsi < 30:
                score += 15
                signals.append(f"RSI과매도({rsi:.0f})")
            
            # 간단한 이동평균 체크
            if len(closes) >= 5:
                ma_5 = sum(closes[-5:]) / 5
                if current_price > ma_5:
                    score += 15
                    signals.append("5일선상향")
            
            # 단기 모멘텀 체크
            if len(closes) >= 3:
                recent_change = (closes[-1] - closes[-3]) / closes[-3] * 100
                if 0 < recent_change < 10:  # 적정 상승
                    score += 15
                    signals.append(f"단기상승({recent_change:.1f}%)")
            
            return {
                'score': score,
                'signals': signals,
                'rsi': rsi
            }
            
        except Exception as e:
            logger.debug(f"빠른 기술적 체크 오류 ({stock_code}): {e}")
            return None

    def _get_extended_screening_candidates(self, market: str, is_pre_market: bool) -> Dict:
        """🆕 API 제한 극복을 위한 확장 스크리닝"""
        try:
            extended_candidates = {
                'gap': [],
                'volume': [],
                'momentum': []
            }
            
            # 🎯 다양한 접근법으로 더 많은 종목 발굴
            screening_approaches = [
                # 하락률 순위에서 반등 후보 찾기
                ('gap', lambda: market_api.get_fluctuation_rank(fid_input_iscd=market, fid_rank_sort_cls_code="1")),
                # 대량 체결 건수 상위
                ('volume', lambda: market_api.get_bulk_trans_num_rank(fid_input_iscd=market)),
                # 호가잔량 불균형
                ('momentum', lambda: market_api.get_quote_balance_rank(fid_input_iscd=market)),
            ]
            
            for category, method in screening_approaches:
                try:
                    data = method()
                    if data is not None and not data.empty:
                        # 상위 15개씩 추가 분석
                        for _, row in data.head(15).iterrows():
                            try:
                                stock_code = row.get('stck_shrn_iscd', '')
                                if not stock_code:
                                    continue
                                
                                # 기술적 지표 빠른 검증
                                technical_check = self._quick_technical_check(stock_code)
                                if technical_check and technical_check['score'] >= 25:
                                    candidate = {
                                        'stock_code': stock_code,
                                        'technical_score': technical_check['score'],
                                        'signals': technical_check['signals'],
                                        'strategy': f'extended_{category}',
                                        'reason': f"확장스크리닝({technical_check['score']}점)"
                                    }
                                    extended_candidates[category].append(candidate)
                            
                            except Exception as e:
                                logger.debug(f"확장 스크리닝 개별 종목 오류: {e}")
                                continue
                        
                        time.sleep(0.1)
                    
                except Exception as e:
                    logger.debug(f"확장 스크리닝 방법 오류: {e}")
                    continue
            
            total_extended = sum(len(v) for v in extended_candidates.values())
            logger.info(f"🔍 확장 스크리닝 완료: {total_extended}개 추가 후보")
            
            return extended_candidates
            
        except Exception as e:
            logger.error(f"확장 스크리닝 오류: {e}")
            return {'gap': [], 'volume': [], 'momentum': []}

    def _sort_candidates_by_technical_score(self, candidates: Dict) -> None:
        """🆕 기술적 지표 점수 기준으로 후보 정렬"""
        try:
            for category in candidates:
                if candidates[category]:
                    candidates[category].sort(
                        key=lambda x: x.get('technical_score', 0), 
                        reverse=True
                    )
                    
                    # 상위 30개로 제한 (너무 많으면 분석 시간 증가)
                    candidates[category] = candidates[category][:30]
            
            logger.info("🎯 후보 정렬 완료: 기술적 점수 기준")
            
        except Exception as e:
            logger.error(f"후보 정렬 오류: {e}")

    def get_screening_summary(self) -> Dict:
        """스크리닝 요약 정보"""
        try:
            screening_results = self.get_market_screening_candidates("all")

            return {
                "status": "success",
                "total_candidates": screening_results.get('total_count', 0),
                "gap_count": len(screening_results.get('gap_candidates', [])),
                "volume_count": len(screening_results.get('volume_candidates', [])),
                "momentum_count": len(screening_results.get('momentum_candidates', [])),
                "last_screening": screening_results.get('timestamp', datetime.now().strftime('%H:%M:%S'))
            }
        except Exception as e:
            logger.error(f"스크리닝 요약 오류: {e}")
            return {
                "status": "error",
                "error_message": str(e)
            }

    # === 편의 메서드 ===

    def is_market_open(self) -> bool:
        """장 운영 시간 확인"""
        now = datetime.now()

        # 주말 체크
        if now.weekday() >= 5:  # 토요일(5), 일요일(6)
            return False

        # 장 운영 시간: 09:00 ~ 15:30
        current_time = now.time()
        market_open = datetime.strptime("09:00", "%H:%M").time()
        market_close = datetime.strptime("15:30", "%H:%M").time()

        return market_open <= current_time <= market_close

    def get_account_info(self) -> Dict:
        """계좌 정보 요약"""
        balance_info = self.get_balance()
        token_info = self.get_token_info()

        return {
            "account": token_info.get("account", ""),
            "product": token_info.get("product", ""),
            "is_market_open": self.is_market_open(),
            "total_holdings": balance_info.get("total_count", 0),
            "status": "active" if token_info.get("status") == "success" else "inactive"
        }

    # === 통계 메서드 ===

    @staticmethod
    def get_api_stats() -> Dict:
        """API 호출 통계 (간소화)"""
        return {
            "message": "API 통계는 kis_auth 모듈에서 확인하세요",
            "status": "info"
        }

    # === 기존 호환성 메서드들 ===

    def get_websocket_approval_key(self) -> str:
        """웹소켓 접속키 발급 (호환성)"""
        logger.warning("웹소켓 접속키는 kis_websocket_client에서 자동 관리됩니다")
        return ""

    def _make_request(self, method: str, url: str, **kwargs) -> Optional[Dict]:
        """내부 요청 메서드 (호환성용)"""
        logger.warning("_make_request는 사용하지 마세요. 직접 kis_auth._url_fetch 사용 권장")
        return None

    # === Rate Limiting ===

    def wait_for_rate_limit(self, seconds: float = 0.1) -> None:
        """API 호출 간격 조절"""
        time.sleep(seconds)

    # === 공식 API 직접 접근 ===

    def call_order_api(self, function_name: str, **kwargs):
        """주문 API 직접 호출"""
        if hasattr(order_api, function_name):
            return getattr(order_api, function_name)(**kwargs)
        else:
            raise ValueError(f"주문 API 함수 '{function_name}'를 찾을 수 없습니다")

    def call_account_api(self, function_name: str, **kwargs):
        """계좌 API 직접 호출"""
        if hasattr(account_api, function_name):
            return getattr(account_api, function_name)(**kwargs)
        else:
            raise ValueError(f"계좌 API 함수 '{function_name}'를 찾을 수 없습니다")

    def call_market_api(self, function_name: str, **kwargs):
        """시세 API 직접 호출"""
        if hasattr(market_api, function_name):
            return getattr(market_api, function_name)(**kwargs)
        else:
            raise ValueError(f"시세 API 함수 '{function_name}'를 찾을 수 없습니다")

    # === 기존 호환성 속성들 ===

    @property
    def base_url(self) -> Optional[str]:
        """기본 URL (호환성)"""
        env = kis.getTREnv()
        return env.my_url if env else None

    @property
    def account_no(self) -> Optional[str]:
        """계좌번호 (호환성)"""
        env = kis.getTREnv()
        return f"{env.my_acct}{env.my_prod}" if env else None

    def _calculate_gap_info(self, row) -> Dict:
        """갭 정보 계산"""
        try:
            stock_code = str(row.get('stck_shrn_iscd', ''))
            stock_name = str(row.get('hts_kor_isnm', ''))
            
            try:
                current_price = int(row.get('stck_prpr', 0)) if row.get('stck_prpr', 0) != '' else 0
            except (ValueError, TypeError):
                current_price = 0
            
            try:
                gap_rate = float(row.get('gap_rate', 0)) if row.get('gap_rate', 0) != '' else 0.0
            except (ValueError, TypeError):
                gap_rate = 0.0
            
            try:
                change_rate = float(row.get('prdy_ctrt', 0)) if row.get('prdy_ctrt', 0) != '' else 0.0
            except (ValueError, TypeError):
                change_rate = 0.0
            
            if stock_code and current_price > 0:
                return {
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'current_price': current_price,
                    'gap_rate': gap_rate,
                    'change_rate': change_rate,
                    'reason': f"갭{gap_rate:.1f}% 변동{change_rate:.1f}%"
                }
            return None
        except Exception as e:
            logger.debug(f"갭 정보 계산 오류: {e}")
            return None

    def _calculate_volume_info(self, row) -> Dict:
        """거래량 정보 계산"""
        try:
            stock_code = str(row.get('mksc_shrn_iscd', ''))
            stock_name = str(row.get('hts_kor_isnm', ''))
            
            try:
                current_price = int(row.get('stck_prpr', 0)) if row.get('stck_prpr', 0) != '' else 0
            except (ValueError, TypeError):
                current_price = 0
            
            try:
                volume = int(row.get('acml_vol', 0)) if row.get('acml_vol', 0) != '' else 0
            except (ValueError, TypeError):
                volume = 0
            
            try:
                volume_ratio = float(row.get('vol_inrt', 0)) if row.get('vol_inrt', 0) != '' else 0.0
            except (ValueError, TypeError):
                volume_ratio = 0.0
            
            if stock_code and current_price > 0:
                return {
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'current_price': current_price,
                    'volume': volume,
                    'volume_ratio': volume_ratio,
                    'reason': f"거래량{volume:,}주 비율{volume_ratio:.1f}배"
                }
            return None
        except Exception as e:
            logger.debug(f"거래량 정보 계산 오류: {e}")
            return None

    def _calculate_momentum_info(self, row) -> Dict:
        """모멘텀 정보 계산"""
        try:
            stock_code = str(row.get('mksc_shrn_iscd', ''))
            stock_name = str(row.get('hts_kor_isnm', ''))
            
            try:
                current_price = int(row.get('stck_prpr', 0)) if row.get('stck_prpr', 0) != '' else 0
            except (ValueError, TypeError):
                current_price = 0
            
            try:
                power = float(row.get('cttr', 0)) if row.get('cttr', 0) != '' else 0.0
            except (ValueError, TypeError):
                power = 0.0
            
            try:
                volume = int(row.get('acml_vol', 0)) if row.get('acml_vol', 0) != '' else 0
            except (ValueError, TypeError):
                volume = 0
            
            if stock_code and current_price > 0:
                return {
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'current_price': current_price,
                    'power': power,
                    'volume': volume,
                    'reason': f"체결강도{power:.1f} 거래량{volume:,}주"
                }
            return None
        except Exception as e:
            logger.debug(f"모멘텀 정보 계산 오류: {e}")
            return None

    def _get_backup_gap_candidates(self, market: str, is_pre_market: bool) -> List[Dict]:
        """🔄 갭 트레이딩 백업 후보 조회 (매우 관대한 기준)"""
        try:
            logger.info(f"🔄 갭 백업 후보 조회 시작 ({market})")
            
            # 🎯 매우 관대한 기준으로 등락률 상위 조회
            if is_pre_market:
                min_fluctuation = "0.1"  # 0.1% 이상
            else:
                min_fluctuation = "0.3"  # 0.3% 이상
            
            backup_data = market_api.get_fluctuation_rank(
                fid_input_iscd=market,
                fid_rank_sort_cls_code="0",  # 상승률순
                fid_rsfl_rate1=min_fluctuation
            )
            
            if backup_data is None or backup_data.empty:
                logger.warning(f"갭 백업 조회에서도 데이터 없음 ({market})")
                return []
            
            backup_candidates = []
            for idx, row in backup_data.head(10).iterrows():  # 상위 10개만
                try:
                    stock_code = row.get('stck_shrn_iscd', '')
                    stock_name = row.get('hts_kor_isnm', '')
                    current_price = int(row.get('stck_prpr', 0))
                    change_rate = float(row.get('prdy_ctrt', 0))
                    
                    if stock_code and current_price > 500 and change_rate > 0:  # 최소 조건
                        backup_candidates.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'current_price': current_price,
                            'change_rate': change_rate,
                            'strategy': 'gap_backup',
                            'reason': f"백업갭 변동{change_rate:.1f}%",
                            'technical_score': 0  # 기본값
                        })
                        
                except Exception as e:
                    logger.debug(f"갭 백업 후보 처리 오류: {e}")
                    continue
                    
            logger.info(f"🔄 갭 백업 후보: {len(backup_candidates)}개 발견")
            return backup_candidates
            
        except Exception as e:
            logger.error(f"갭 백업 후보 조회 오류: {e}")
            return []

    def _get_backup_volume_candidates(self, market: str, is_pre_market: bool) -> List[Dict]:
        """🔄 거래량 백업 후보 조회 (매우 관대한 기준)"""
        try:
            logger.info(f"🔄 거래량 백업 후보 조회 시작 ({market})")
            
            # 🎯 매우 관대한 거래량 기준으로 조회
            if is_pre_market:
                volume_threshold = "1000"  # 1천주
            else:
                volume_threshold = "5000"  # 5천주
            
            backup_data = market_api.get_volume_rank(
                fid_input_iscd=market,
                fid_blng_cls_code="1",  # 거래증가율
                fid_vol_cnt=volume_threshold
            )
            
            if backup_data is None or backup_data.empty:
                logger.warning(f"거래량 백업 조회에서도 데이터 없음 ({market})")
                return []
            
            backup_candidates = []
            for idx, row in backup_data.head(8).iterrows():  # 상위 8개만
                try:
                    stock_code = row.get('mksc_shrn_iscd', '')
                    stock_name = row.get('hts_kor_isnm', '')
                    current_price = int(row.get('stck_prpr', 0))
                    volume = int(row.get('acml_vol', 0))
                    
                    if stock_code and current_price > 500 and volume > 1000:  # 최소 조건
                        backup_candidates.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'current_price': current_price,
                            'volume': volume,
                            'strategy': 'volume_backup',
                            'reason': f"백업거래량 {volume:,}주",
                            'technical_score': 0  # 기본값
                        })
                except Exception as e:
                    logger.debug(f"거래량 백업 후보 처리 오류: {e}")
                    continue
                    
            logger.info(f"🔄 거래량 백업 후보: {len(backup_candidates)}개 발견")
            return backup_candidates
            
        except Exception as e:
            logger.error(f"거래량 백업 후보 조회 오류: {e}")
            return []

    def _get_backup_momentum_candidates(self, market: str, is_pre_market: bool) -> List[Dict]:
        """🔄 모멘텀 백업 후보 조회 (매우 관대한 기준)"""
        try:
            logger.info(f"🔄 모멘텀 백업 후보 조회 시작 ({market})")
            
            # 🎯 매우 관대한 체결강도 기준으로 조회
            if is_pre_market:
                power_threshold = "500"  # 500주
            else:
                power_threshold = "2000"  # 2천주
            
            backup_data = market_api.get_volume_power_rank(
                fid_input_iscd=market,
                fid_vol_cnt=power_threshold
            )
            
            if backup_data is None or backup_data.empty:
                logger.warning(f"모멘텀 백업 조회에서도 데이터 없음 ({market})")
                return []
            
            backup_candidates = []
            for idx, row in backup_data.head(6).iterrows():  # 상위 6개만
                try:
                    stock_code = row.get('mksc_shrn_iscd', '')
                    stock_name = row.get('hts_kor_isnm', '')
                    current_price = int(row.get('stck_prpr', 0))
                    power = float(row.get('cttr', 0))
                    
                    if stock_code and current_price > 500 and power > 50:  # 최소 조건
                        backup_candidates.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'current_price': current_price,
                            'power': power,
                            'strategy': 'momentum_backup',
                            'reason': f"백업체결강도 {power:.1f}",
                            'technical_score': 0  # 기본값
                        })
                        
                except Exception as e:
                    logger.debug(f"모멘텀 백업 후보 처리 오류: {e}")
                    continue
                    
            logger.info(f"🔄 모멘텀 백업 후보: {len(backup_candidates)}개 발견")
            return backup_candidates
            
        except Exception as e:
            logger.error(f"모멘텀 백업 후보 조회 오류: {e}")
            return []
