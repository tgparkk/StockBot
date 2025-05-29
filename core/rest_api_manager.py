"""
KIS REST API 통합 관리자 (리팩토링 버전)
공식 문서 기반 + 모듈화
"""
import time
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Union
from utils.logger import setup_logger

# KIS 모듈 import
from . import kis_auth as kis
from . import kis_order_api as order_api
from . import kis_account_api as account_api
from . import kis_market_api as market_api

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

    def get_market_screening_candidates(self, screening_type: str = "all") -> Dict:
        """
        시장 스크리닝 후보 조회

        Args:
            screening_type: 스크리닝 타입 ("all", "gap", "volume", "momentum")

        Returns:
            Dict: 전략별 후보 종목들
        """
        candidates = {
            'gap_candidates': [],      # 갭 트레이딩 후보
            'volume_candidates': [],   # 거래량 돌파 후보
            'momentum_candidates': [], # 모멘텀 후보
            'screening_time': datetime.now(),
            'total_candidates': 0,
            'status': 'success'
        }

        try:
            logger.info(f"시장 스크리닝 시작: {screening_type}")

            if screening_type in ["all", "gap"]:
                # 갭 트레이딩: 등락률 상위 조회
                logger.debug("갭 트레이딩 후보 탐색 중...")
                gap_data = market_api.get_gap_trading_candidates()
                candidates['gap_candidates'] = self._parse_gap_candidates(gap_data)

            if screening_type in ["all", "volume"]:
                # 거래량 돌파: 거래량 증가율 상위 조회
                logger.debug("거래량 돌파 후보 탐색 중...")
                volume_data = market_api.get_volume_breakout_candidates()
                candidates['volume_candidates'] = self._parse_volume_candidates(volume_data)

            if screening_type in ["all", "momentum"]:
                # 모멘텀: 체결강도 상위 조회
                logger.debug("모멘텀 후보 탐색 중...")
                momentum_data = market_api.get_momentum_candidates()
                candidates['momentum_candidates'] = self._parse_momentum_candidates(momentum_data)

            # 총 후보 수 계산
            candidates['total_candidates'] = (
                len(candidates['gap_candidates']) +
                len(candidates['volume_candidates']) +
                len(candidates['momentum_candidates'])
            )

            logger.info(f"✅ 시장 스크리닝 완료: 총 {candidates['total_candidates']}개 후보")
            logger.info(f"   갭({len(candidates['gap_candidates'])}) 볼륨({len(candidates['volume_candidates'])}) 모멘텀({len(candidates['momentum_candidates'])})")

            return candidates

        except Exception as e:
            logger.error(f"❌ 시장 스크리닝 오류: {e}")
            candidates.update({
                'status': 'error',
                'error_message': str(e)
            })
            return candidates

    def _parse_gap_candidates(self, data: Optional[pd.DataFrame]) -> List[Dict]:
        """갭 트레이딩 후보 파싱 (실제 갭 데이터)"""
        candidates = []

        if data is not None and not data.empty:
            logger.debug(f"갭 후보 원본 데이터: {len(data)}건")

            for _, row in data.iterrows():  # 이미 필터링된 갭 데이터
                try:
                    gap_rate = float(row.get('gap_rate', 0))
                    change_rate = float(row.get('prdy_ctrt', 0))
                    volume_ratio = float(row.get('volume_ratio', 0))

                    # 갭 트레이딩 후보 조건 재확인
                    if gap_rate >= 2.0 and change_rate > 0 and volume_ratio >= 1.5:
                        candidates.append({
                            'stock_code': row.get('stck_shrn_iscd', ''),
                            'stock_name': row.get('hts_kor_isnm', ''),
                            'current_price': int(row.get('stck_prpr', 0)),
                            'open_price': int(row.get('stck_oprc', 0)),
                            'prev_close': int(row.get('stck_sdpr', 0)),
                            'gap_size': int(row.get('gap_size', 0)),
                            'gap_rate': gap_rate,
                            'change_rate': change_rate,
                            'volume': int(row.get('acml_vol', 0)),
                            'volume_ratio': volume_ratio,
                            'strategy': 'gap_trading',
                            'rank': int(row.get('data_rank', 0))
                        })
                except (ValueError, TypeError) as e:
                    logger.warning(f"갭 후보 파싱 오류: {e}")
                    continue

        logger.debug(f"갭 트레이딩 후보: {len(candidates)}개")
        return candidates

    def _parse_volume_candidates(self, data: Optional[pd.DataFrame]) -> List[Dict]:
        """거래량 돌파 후보 파싱"""
        candidates = []

        if data is not None and not data.empty:
            logger.debug(f"거래량 후보 원본 데이터: {len(data)}건")

            for _, row in data.head(30).iterrows():  # 상위 30개
                try:
                    volume = int(row.get('acml_vol', 0))
                    volume_increase_rate = float(row.get('vol_inrt', 0))

                    if volume >= 10000 and volume_increase_rate > 0:  # 1만주 이상 + 증가율 양수
                        candidates.append({
                            'stock_code': row.get('mksc_shrn_iscd', ''),
                            'stock_name': row.get('hts_kor_isnm', ''),
                            'current_price': int(row.get('stck_prpr', 0)),
                            'change_rate': float(row.get('prdy_ctrt', 0)),
                            'volume': volume,
                            'volume_increase_rate': volume_increase_rate,
                            'strategy': 'volume_breakout',
                            'rank': int(row.get('data_rank', 0))
                        })
                except (ValueError, TypeError) as e:
                    logger.warning(f"거래량 후보 파싱 오류: {e}")
                    continue

        logger.debug(f"거래량 돌파 후보: {len(candidates)}개")
        return candidates

    def _parse_momentum_candidates(self, data: Optional[pd.DataFrame]) -> List[Dict]:
        """모멘텀 후보 파싱 - 🎯 수익성 중심 엄격한 기준"""
        candidates = []

        if data is not None and not data.empty:
            logger.debug(f"🎯 모멘텀 후보 원본 데이터: {len(data)}건")

            for _, row in data.head(5).iterrows():  # 🎯 상위 5개로 제한 (기존 8개)
                try:
                    execution_strength = float(row.get('tday_rltv', 0))
                    change_rate = float(row.get('prdy_ctrt', 0))
                    current_price = int(row.get('stck_prpr', 0))
                    volume = int(row.get('acml_vol', 0))

                    # 🎯 엄격한 모멘텀 조건
                    if (execution_strength >= 100 and     # 🎯 체결강도 100 이상 (기존 70)
                        change_rate >= 2.0 and            # 🎯 상승률 2% 이상 (기존 >0)
                        current_price >= 1000 and         # 🎯 최소 1,000원 이상
                        current_price <= 200000 and       # 🎯 최대 20만원 이하
                        volume >= 100000):                # 🎯 거래량 10만주 이상
                        
                        stock_code = row.get('stck_shrn_iscd', '')

                        # 🎯 수익성 점수 계산
                        profit_score = execution_strength * change_rate * (volume / 100000)

                        basic_info = {
                            'stock_code': stock_code,
                            'stock_name': row.get('hts_kor_isnm', ''),
                            'current_price': current_price,
                            'change_rate': change_rate,
                            'volume': volume,
                            'execution_strength': execution_strength,
                            'profit_score': profit_score,  # 🎯 수익성 점수
                            'strategy': 'momentum',
                            'rank': int(row.get('data_rank', 0))
                        }

                        candidates.append(basic_info)
                        logger.info(f"🎯 모멘텀 후보: {stock_code} 체결강도{execution_strength:.0f} 상승률{change_rate:.1f}% 수익점수{profit_score:.1f}")

                except (ValueError, TypeError) as e:
                    logger.warning(f"🎯 모멘텀 후보 파싱 오류: {e}")
                    continue

        # 🎯 수익성 점수 기준 정렬
        candidates.sort(key=lambda x: x.get('profit_score', 0), reverse=True)
        result = candidates[:3]  # 🎯 최종 3개만 선정
        logger.debug(f"🎯 엄격 모멘텀 후보: {len(result)}개")
        return result

    def _parse_enhanced_gap_candidates(self, gap_data: Optional[pd.DataFrame], disparity_data: Optional[pd.DataFrame]) -> List[Dict]:
        """향상된 갭 트레이딩 후보 파싱 (등락률 + 이격도 조합)"""
        candidates = []

        # 기본 등락률 상위 종목
        gap_candidates = self._parse_gap_candidates(gap_data)
        candidates.extend(gap_candidates)

        # 이격도 기반 과매도 종목 추가
        if disparity_data is not None and not disparity_data.empty:
            logger.debug(f"이격도 후보 원본 데이터: {len(disparity_data)}건")

            for _, row in disparity_data.head(15).iterrows():  # 상위 15개
                try:
                    disparity_20 = float(row.get('d20_dsrt', 100))
                    change_rate = float(row.get('prdy_ctrt', 0))

                    # 이격도 85 이하(과매도) + 상승률 0.5% 이상
                    if disparity_20 <= 85 and change_rate >= 0.5:
                        candidates.append({
                            'stock_code': row.get('mksc_shrn_iscd', ''),
                            'stock_name': row.get('hts_kor_isnm', ''),
                            'current_price': int(row.get('stck_prpr', 0)),
                            'change_rate': change_rate,
                            'volume': int(row.get('acml_vol', 0)),
                            'disparity_20': disparity_20,
                            'strategy': 'gap_trading_enhanced',
                            'rank': int(row.get('data_rank', 0))
                        })
                except (ValueError, TypeError) as e:
                    logger.warning(f"이격도 후보 파싱 오류: {e}")
                    continue

        # 중복 제거 (종목코드 기준)
        unique_candidates = {}
        for candidate in candidates:
            stock_code = candidate.get('stock_code', '')
            if stock_code and stock_code not in unique_candidates:
                unique_candidates[stock_code] = candidate

        result = list(unique_candidates.values())
        logger.debug(f"향상된 갭 트레이딩 후보: {len(result)}개")
        return result

    def _parse_enhanced_volume_candidates(self, volume_data: Optional[pd.DataFrame], bulk_trans_data: Optional[pd.DataFrame]) -> List[Dict]:
        """향상된 거래량 돌파 후보 파싱 (거래량 순위 + 대량체결건수 조합)"""
        candidates = []

        # 기본 거래량 순위 종목
        volume_candidates = self._parse_volume_candidates(volume_data)
        candidates.extend(volume_candidates)

        # 대량체결건수 상위 종목 추가
        if bulk_trans_data is not None and not bulk_trans_data.empty:
            logger.debug(f"대량체결 후보 원본 데이터: {len(bulk_trans_data)}건")

            for _, row in bulk_trans_data.head(20).iterrows():  # 상위 20개
                try:
                    buy_count = int(row.get('shnu_cntg_csnu', 0))
                    sell_count = int(row.get('seln_cntg_csnu', 0))
                    change_rate = float(row.get('prdy_ctrt', 0))

                    # 매수체결건수가 매도보다 많고 상승률 양수
                    if buy_count > sell_count and change_rate > 0:
                        buy_sell_ratio = buy_count / max(sell_count, 1)
                        if buy_sell_ratio >= 1.2:  # 매수가 20% 이상 많음
                            candidates.append({
                                'stock_code': row.get('mksc_shrn_iscd', ''),
                                'stock_name': row.get('hts_kor_isnm', ''),
                                'current_price': int(row.get('stck_prpr', 0)),
                                'change_rate': change_rate,
                                'volume': int(row.get('acml_vol', 0)),
                                'buy_count': buy_count,
                                'sell_count': sell_count,
                                'buy_sell_ratio': buy_sell_ratio,
                                'strategy': 'volume_breakout_enhanced',
                                'rank': int(row.get('data_rank', 0))
                            })
                except (ValueError, TypeError) as e:
                    logger.warning(f"대량체결 후보 파싱 오류: {e}")
                    continue

        # 중복 제거 (종목코드 기준)
        unique_candidates = {}
        for candidate in candidates:
            stock_code = candidate.get('stock_code', '')
            if stock_code and stock_code not in unique_candidates:
                unique_candidates[stock_code] = candidate

        result = list(unique_candidates.values())
        logger.debug(f"향상된 거래량 돌파 후보: {len(result)}개")
        return result

    def _parse_enhanced_momentum_candidates(self, momentum_data: Optional[pd.DataFrame], bulk_trans_data: Optional[pd.DataFrame]) -> List[Dict]:
        """향상된 모멘텀 후보 파싱 (체결강도 + 대량체결건수 조합)"""
        candidates = []

        # 기본 체결강도 상위 종목
        momentum_candidates = self._parse_momentum_candidates(momentum_data)
        candidates.extend(momentum_candidates)

        # 대량체결건수와 교차 검증
        if bulk_trans_data is not None and not bulk_trans_data.empty:
            logger.debug(f"모멘텀 교차검증 데이터: {len(bulk_trans_data)}건")

            # 대량체결 종목 코드 세트 생성
            bulk_trans_codes = set()
            for _, row in bulk_trans_data.iterrows():
                stock_code = row.get('mksc_shrn_iscd', '')
                if stock_code:
                    bulk_trans_codes.add(stock_code)

            # 기존 모멘텀 후보 중 대량체결에도 포함된 종목들에 가점
            for candidate in candidates:
                if candidate.get('stock_code', '') in bulk_trans_codes:
                    candidate['enhanced_score'] = candidate.get('execution_strength', 0) * 1.2
                    candidate['strategy'] = 'momentum_enhanced'
                else:
                    candidate['enhanced_score'] = candidate.get('execution_strength', 0)

        # 향상된 점수 기준으로 정렬
        candidates.sort(key=lambda x: x.get('enhanced_score', 0), reverse=True)

        result = candidates[:25]  # 상위 25개
        logger.debug(f"향상된 모멘텀 후보: {len(result)}개")
        return result

    def _parse_unified_gap_candidates(self, gap_data: Dict[str, Optional[pd.DataFrame]]) -> List[Dict]:
        """통합된 갭 트레이딩 후보 파싱"""
        candidates = []

        # 기본 등락률 상위 종목
        if gap_data.get("basic") is not None:
            basic_candidates = self._parse_gap_candidates(gap_data["basic"])
            candidates.extend(basic_candidates)

        # 이격도 기반 과매도 종목 추가
        if gap_data.get("enhanced") is not None:
            enhanced_candidates = self._parse_enhanced_gap_candidates(None, gap_data["enhanced"])
            candidates.extend(enhanced_candidates)

        # 중복 제거 (종목코드 기준)
        unique_candidates = {}
        for candidate in candidates:
            stock_code = candidate.get('stock_code', '')
            if stock_code and stock_code not in unique_candidates:
                unique_candidates[stock_code] = candidate

        result = list(unique_candidates.values())
        logger.debug(f"통합 갭 트레이딩 후보: {len(result)}개")
        return result

    def _parse_unified_volume_candidates(self, volume_data: Dict[str, Optional[pd.DataFrame]],
                                       quote_balance_data: Optional[pd.DataFrame]) -> List[Dict]:
        """통합된 거래량 돌파 후보 파싱"""
        candidates = []

        # 기본 거래량 순위 종목
        if volume_data.get("basic") is not None:
            basic_candidates = self._parse_volume_candidates(volume_data["basic"])
            candidates.extend(basic_candidates)

        # 대량체결건수 종목
        if volume_data.get("enhanced") is not None:
            enhanced_candidates = self._parse_enhanced_volume_candidates(None, volume_data["enhanced"])
            candidates.extend(enhanced_candidates)

        # 호가잔량 순매수 우세 종목 추가
        if quote_balance_data is not None and not quote_balance_data.empty:
            logger.debug(f"호가잔량 후보 원본 데이터: {len(quote_balance_data)}건")

            for _, row in quote_balance_data.head(15).iterrows():  # 상위 15개
                try:
                    net_buy_volume = int(row.get('total_ntsl_bidp_rsqn', 0))
                    buy_ratio = float(row.get('shnu_rsqn_rate', 0))
                    change_rate = float(row.get('prdy_ctrt', 0))

                    # 순매수잔량 > 0 + 매수비율 60% 이상 + 상승률 > 0
                    if net_buy_volume > 0 and buy_ratio >= 60 and change_rate > 0:
                        candidates.append({
                            'stock_code': row.get('mksc_shrn_iscd', ''),
                            'stock_name': row.get('hts_kor_isnm', ''),
                            'current_price': int(row.get('stck_prpr', 0)),
                            'change_rate': change_rate,
                            'volume': int(row.get('acml_vol', 0)),
                            'net_buy_volume': net_buy_volume,
                            'buy_ratio': buy_ratio,
                            'strategy': 'volume_quote_balance',
                            'rank': int(row.get('data_rank', 0))
                        })
                except (ValueError, TypeError) as e:
                    logger.warning(f"호가잔량 후보 파싱 오류: {e}")
                    continue

        # 중복 제거 (종목코드 기준)
        unique_candidates = {}
        for candidate in candidates:
            stock_code = candidate.get('stock_code', '')
            if stock_code and stock_code not in unique_candidates:
                unique_candidates[stock_code] = candidate

        result = list(unique_candidates.values())
        logger.debug(f"통합 거래량 돌파 후보: {len(result)}개")
        return result

    def _parse_unified_momentum_candidates(self, momentum_data: Dict[str, Optional[pd.DataFrame]],
                                         quote_balance_data: Optional[pd.DataFrame]) -> List[Dict]:
        """통합된 모멘텀 후보 파싱"""
        candidates = []

        # 기본 체결강도 상위 종목
        if momentum_data.get("basic") is not None:
            basic_candidates = self._parse_momentum_candidates(momentum_data["basic"])
            candidates.extend(basic_candidates)

        # 대량체결건수 교차검증 종목
        if momentum_data.get("enhanced") is not None:
            enhanced_candidates = self._parse_enhanced_momentum_candidates(None, momentum_data["enhanced"])
            candidates.extend(enhanced_candidates)

        # 호가잔량 매수비율 우세 종목 추가
        if quote_balance_data is not None and not quote_balance_data.empty:
            logger.debug(f"호가잔량 매수비율 후보 원본 데이터: {len(quote_balance_data)}건")

            for _, row in quote_balance_data.head(20).iterrows():  # 상위 20개
                try:
                    buy_ratio = float(row.get('shnu_rsqn_rate', 0))
                    sell_ratio = float(row.get('seln_rsqn_rate', 0))
                    change_rate = float(row.get('prdy_ctrt', 0))

                    # 매수비율 70% 이상 + 매수우세 + 상승률 > 0.5%
                    if buy_ratio >= 70 and buy_ratio > sell_ratio and change_rate >= 0.5:
                        candidates.append({
                            'stock_code': row.get('mksc_shrn_iscd', ''),
                            'stock_name': row.get('hts_kor_isnm', ''),
                            'current_price': int(row.get('stck_prpr', 0)),
                            'change_rate': change_rate,
                            'volume': int(row.get('acml_vol', 0)),
                            'buy_ratio': buy_ratio,
                            'sell_ratio': sell_ratio,
                            'strategy': 'momentum_quote_balance',
                            'rank': int(row.get('data_rank', 0))
                        })
                except (ValueError, TypeError) as e:
                    logger.warning(f"호가잔량 매수비율 후보 파싱 오류: {e}")
                    continue

        # 향상된 점수 계산 및 정렬
        for candidate in candidates:
            execution_strength = candidate.get('execution_strength', 0)
            buy_ratio = candidate.get('buy_ratio', 0)
            change_rate = candidate.get('change_rate', 0)

            # 복합 점수 계산 (체결강도 + 매수비율 + 상승률)
            candidate['momentum_score'] = (execution_strength * 0.5) + (buy_ratio * 0.3) + (change_rate * 20)

        # 모멘텀 점수 기준 정렬
        candidates.sort(key=lambda x: x.get('momentum_score', 0), reverse=True)

        result = candidates[:25]  # 상위 25개
        logger.debug(f"통합 모멘텀 후보: {len(result)}개")
        return result

    def get_screening_summary(self) -> Dict:
        """스크리닝 요약 정보"""
        try:
            candidates = self.get_market_screening_candidates("all")

            return {
                "status": "success",
                "total_candidates": candidates.get('total_candidates', 0),
                "gap_count": len(candidates.get('gap_candidates', [])),
                "volume_count": len(candidates.get('volume_candidates', [])),
                "momentum_count": len(candidates.get('momentum_candidates', [])),
                "last_screening": candidates.get('screening_time', datetime.now()).strftime('%H:%M:%S')
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
