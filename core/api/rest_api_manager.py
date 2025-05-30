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
        """🎯 수익성 중심 시장 스크리닝 - 적응형 백업 로직 포함"""
        logger.info(f"시장 스크리닝 시작: {market_type}")
        
        candidates = {
            'gap': [],
            'volume': [],
            'momentum': []
        }
        
        markets = ["0000", "0001", "1001"] if market_type == "all" else [market_type]
        
        # 🎯 시간대별 적응형 전략
        from datetime import datetime
        current_time = datetime.now()
        is_pre_market = current_time.hour < 9 or (current_time.hour == 9 and current_time.minute < 30)
        is_early_market = current_time.hour < 11
        
        for market in markets:
            try:
                # === 갭 트레이딩 후보 ===
                try:
                    gap_candidates = market_api.get_gap_trading_candidates(market)
                    if gap_candidates is not None and not gap_candidates.empty:
                        gap_data = self._process_gap_candidates(gap_candidates)
                        candidates['gap'].extend(gap_data)
                        logger.info(f"📊 갭 후보: {len(gap_data)}개 ({market})")
                    else:
                        logger.warning(f"⚠️ 갭 후보 없음 - 백업 전략 시도 ({market})")
                        # 🆕 백업 전략: 더 관대한 기준으로 재시도
                        backup_gap_candidates = self._get_backup_gap_candidates(market, is_pre_market)
                        if backup_gap_candidates:
                            candidates['gap'].extend(backup_gap_candidates)
                            logger.info(f"🔄 갭 백업 후보: {len(backup_gap_candidates)}개 ({market})")
                        
                except Exception as e:
                    logger.error(f"갭 후보 조회 오류 ({market}): {e}")

                # === 거래량 돌파 후보 ===
                try:
                    volume_candidates = market_api.get_volume_breakout_candidates(market)
                    if volume_candidates is not None and not volume_candidates.empty:
                        volume_data = self._process_volume_candidates(volume_candidates)
                        candidates['volume'].extend(volume_data)
                        logger.info(f"📊 거래량 후보: {len(volume_data)}개 ({market})")
                    else:
                        logger.warning(f"⚠️ 거래량 후보 없음 - 백업 전략 시도 ({market})")
                        # 🆕 백업 전략: 더 관대한 기준으로 재시도
                        backup_volume_candidates = self._get_backup_volume_candidates(market, is_pre_market)
                        if backup_volume_candidates:
                            candidates['volume'].extend(backup_volume_candidates)
                            logger.info(f"🔄 거래량 백업 후보: {len(backup_volume_candidates)}개 ({market})")
                        
                except Exception as e:
                    logger.error(f"거래량 후보 조회 오류 ({market}): {e}")

                # === 모멘텀 후보 ===
                try:
                    momentum_candidates = market_api.get_momentum_candidates(market)
                    if momentum_candidates is not None and not momentum_candidates.empty:
                        momentum_data = self._process_momentum_candidates(momentum_candidates)
                        candidates['momentum'].extend(momentum_data)
                        logger.info(f"📊 모멘텀 후보: {len(momentum_data)}개 ({market})")
                    else:
                        logger.warning(f"⚠️ 모멘텀 후보 없음 - 백업 전략 시도 ({market})")
                        # 🆕 백업 전략: 더 관대한 기준으로 재시도
                        backup_momentum_candidates = self._get_backup_momentum_candidates(market, is_pre_market)
                        if backup_momentum_candidates:
                            candidates['momentum'].extend(backup_momentum_candidates)
                            logger.info(f"🔄 모멘텀 백업 후보: {len(backup_momentum_candidates)}개 ({market})")
                        
                except Exception as e:
                    logger.error(f"모멘텀 후보 조회 오류 ({market}): {e}")

                # 짧은 대기 (API 호출 간격)
                time.sleep(0.2)

            except Exception as e:
                logger.error(f"시장 {market} 스크리닝 오류: {e}")

        # 🎯 결과 통합 및 중복 제거
        total_count = len(candidates['gap']) + len(candidates['volume']) + len(candidates['momentum'])
        all_candidates = self._merge_and_deduplicate_candidates(candidates)
        
        logger.info(f"✅ 시장 스크리닝 완료: 총 {total_count}개 후보")
        logger.info(f"   갭({len(candidates['gap'])}) 볼륨({len(candidates['volume'])}) 모멘텀({len(candidates['momentum'])})")
        
        # 🔧 stock_discovery에서 기대하는 형태로 반환
        return {
            'status': 'success',
            'gap_candidates': candidates['gap'],
            'volume_candidates': candidates['volume'], 
            'momentum_candidates': candidates['momentum'],
            'background': all_candidates,  # 기존 호환성 유지
            'total_count': total_count,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
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
                            'strategy_type': 'gap_backup',
                            'score': change_rate,  # 간단한 점수
                            'source': f'backup_fluctuation_{market}'
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
                            'strategy_type': 'volume_backup',
                            'score': volume / 10000,  # 간단한 점수
                            'source': f'backup_volume_{market}'
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
                # 🔄 최후의 수단: 등락률 상위로 대체
                logger.warning(f"모멘텀 백업 조회 실패 - 등락률로 대체 ({market})")
                return self._get_fallback_momentum_candidates(market)
            
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
                            'strategy_type': 'momentum_backup',
                            'score': power,  # 간단한 점수
                            'source': f'backup_power_{market}'
                        })
                        
                except Exception as e:
                    logger.debug(f"모멘텀 백업 후보 처리 오류: {e}")
                    continue
            
            logger.info(f"🔄 모멘텀 백업 후보: {len(backup_candidates)}개 발견")
            return backup_candidates
            
        except Exception as e:
            logger.error(f"모멘텀 백업 후보 조회 오류: {e}")
            return self._get_fallback_momentum_candidates(market)
    
    def _get_fallback_momentum_candidates(self, market: str) -> List[Dict]:
        """🆘 최후의 수단: 등락률 상위로 모멘텀 후보 대체"""
        try:
            logger.info(f"🆘 최후 대안: 등락률 상위로 모멘텀 대체 ({market})")
            
            fallback_data = market_api.get_fluctuation_rank(
                fid_input_iscd=market,
                fid_rank_sort_cls_code="0",  # 상승률순
                fid_rsfl_rate1="0.5"  # 0.5% 이상
            )
            
            if fallback_data is None or fallback_data.empty:
                return []
            
            fallback_candidates = []
            for idx, row in fallback_data.head(5).iterrows():  # 상위 5개만
                try:
                    stock_code = row.get('stck_shrn_iscd', '')
                    stock_name = row.get('hts_kor_isnm', '')
                    current_price = int(row.get('stck_prpr', 0))
                    change_rate = float(row.get('prdy_ctrt', 0))
                    
                    if stock_code and current_price > 500 and change_rate > 0.5:
                        fallback_candidates.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'current_price': current_price,
                            'change_rate': change_rate,
                            'strategy_type': 'momentum_fallback',
                            'score': change_rate,
                            'source': f'fallback_fluctuation_{market}'
                        })
                        
                except Exception as e:
                    continue
            
            logger.info(f"🆘 최후 대안 후보: {len(fallback_candidates)}개 발견")
            return fallback_candidates
            
        except Exception as e:
            logger.error(f"최후 대안 후보 조회 오류: {e}")
            return []

    def _process_gap_candidates(self, gap_candidates: pd.DataFrame) -> List[Dict]:
        """갭 후보 데이터 처리"""
        try:
            processed = []
            for idx, row in gap_candidates.iterrows():
                try:
                    # 🔧 안전한 타입 변환
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
                    
                    try:
                        volume_ratio = float(row.get('volume_ratio', 0)) if row.get('volume_ratio', 0) != '' else 0.0
                    except (ValueError, TypeError):
                        volume_ratio = 0.0
                    
                    try:
                        score = float(row.get('profit_score', 0)) if row.get('profit_score', 0) != '' else max(gap_rate, change_rate)  # 백업 점수
                    except (ValueError, TypeError):
                        score = max(gap_rate, change_rate)  # 백업 점수
                    
                    if stock_code and current_price > 0:
                        processed.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'current_price': current_price,
                            'gap_rate': gap_rate,
                            'change_rate': change_rate,
                            'volume_ratio': volume_ratio,
                            'strategy_type': 'gap_trading',
                            'score': score,
                            'source': 'gap_screening'
                        })
                except Exception as e:
                    logger.debug(f"갭 후보 행 처리 오류: {e}")
                    continue
                    
            return processed
        except Exception as e:
            logger.error(f"갭 후보 처리 오류: {e}")
            return []

    def _process_volume_candidates(self, volume_candidates: pd.DataFrame) -> List[Dict]:
        """거래량 후보 데이터 처리"""
        try:
            processed = []
            for idx, row in volume_candidates.iterrows():
                try:
                    # 🔧 안전한 타입 변환
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
                    
                    try:
                        score = float(row.get('vol_inrt', 0)) if row.get('vol_inrt', 0) != '' else volume / 100000
                    except (ValueError, TypeError):
                        score = volume / 100000  # 백업 점수
                    
                    if stock_code and current_price > 0:
                        processed.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'current_price': current_price,
                            'volume': volume,
                            'volume_ratio': volume_ratio,
                            'strategy_type': 'volume_breakout',
                            'score': score,
                            'source': 'volume_screening'
                        })
                except Exception as e:
                    logger.debug(f"거래량 후보 행 처리 오류: {e}")
                    continue
                    
            return processed
        except Exception as e:
            logger.error(f"거래량 후보 처리 오류: {e}")
            return []

    def _process_momentum_candidates(self, momentum_candidates: pd.DataFrame) -> List[Dict]:
        """모멘텀 후보 데이터 처리"""
        try:
            processed = []
            for idx, row in momentum_candidates.iterrows():
                try:
                    # 🔧 안전한 타입 변환
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
                    
                    try:
                        score = float(row.get('cttr', 0)) if row.get('cttr', 0) != '' else power
                    except (ValueError, TypeError):
                        score = power  # 백업 점수
                    
                    if stock_code and current_price > 0:
                        processed.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'current_price': current_price,
                            'power': power,
                            'volume': volume,
                            'strategy_type': 'momentum',
                            'score': score,
                            'source': 'momentum_screening'
                        })
                except Exception as e:
                    logger.debug(f"모멘텀 후보 행 처리 오류: {e}")
                    continue
                    
            return processed
        except Exception as e:
            logger.error(f"모멘텀 후보 처리 오류: {e}")
            return []

    def _merge_and_deduplicate_candidates(self, candidates: Dict) -> List[Dict]:
        """후보 통합 및 중복 제거"""
        try:
            all_candidates = []
            
            # 모든 후보 통합
            for strategy_type, candidate_list in candidates.items():
                all_candidates.extend(candidate_list)
            
            # 종목 코드 기준 중복 제거 (높은 점수 우선)
            unique_candidates = {}
            for candidate in all_candidates:
                stock_code = candidate.get('stock_code', '')
                if stock_code:
                    existing = unique_candidates.get(stock_code)
                    if not existing or candidate.get('score', 0) > existing.get('score', 0):
                        unique_candidates[stock_code] = candidate
            
            # 점수 순으로 정렬
            sorted_candidates = sorted(
                unique_candidates.values(),
                key=lambda x: x.get('score', 0),
                reverse=True
            )
            
            logger.info(f"후보 통합 완료: {len(all_candidates)}개 → {len(sorted_candidates)}개 (중복제거)")
            return sorted_candidates[:20]  # 상위 20개만
            
        except Exception as e:
            logger.error(f"후보 통합 오류: {e}")
            return []

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
