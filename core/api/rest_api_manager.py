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
    get_fluctuation_rank, get_volume_rank
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
        logger.info("🔑 KIS API 인증 시작...")

        if not kis.auth(svr):
            logger.error("❌ KIS API 인증 실패!")
            logger.error("📋 문제 해결 방법:")
            logger.error("  1. .env 파일이 프로젝트 루트에 있는지 확인")
            logger.error("  2. .env 파일에 실제 KIS API 키가 입력되어 있는지 확인")
            logger.error("  3. KIS_APP_KEY, KIS_APP_SECRET 값이 정확한지 확인")
            logger.error("  4. 계좌번호와 HTS ID가 올바른지 확인")
            raise ValueError("KIS API 인증 실패 - 설정을 확인해주세요")

        logger.info("✅ KIS API 인증 성공!")


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

    def cancel_order(self, order_no: str, ord_orgno: str = "", ord_dvsn: str = "01",
                     qty_all_ord_yn: str = "Y") -> Dict:
        """주문 취소

        Args:
            order_no: 원주문번호 (orgn_odno)
            ord_orgno: 주문조직번호 (보통 공백으로 처리)
            ord_dvsn: 주문구분 (01:지정가, 05:시장가 등, 기본값 01)
            qty_all_ord_yn: 잔량전부주문여부 (Y:전량취소, N:일부취소, 기본값 Y)
        """
        try:
            # 주문조직번호가 없으면 공백으로 처리 (KIS API 요구사항)
            if not ord_orgno:
                ord_orgno = ""

            # get_order_rvsecncl 함수 호출
            result = order_api.get_order_rvsecncl(
                ord_orgno=ord_orgno,               # 주문조직번호
                orgn_odno=order_no,                # 원주문번호
                ord_dvsn=ord_dvsn,                 # 주문구분
                rvse_cncl_dvsn_cd="02",           # 취소:02 (정정:01)
                ord_qty=0,                         # 전량취소시 0
                ord_unpr=0,                        # 취소시 0
                qty_all_ord_yn=qty_all_ord_yn      # 잔량전부주문여부
            )

            if result is not None and not result.empty:
                # 취소 성공
                cancel_data = result.iloc[0].to_dict()
                return {
                    "status": "success",
                    "order_no": order_no,
                    "cancel_order_no": cancel_data.get('odno', ''),
                    "message": "주문 취소 완료",
                    "details": cancel_data
                }
            else:
                return {
                    "status": "error",
                    "order_no": order_no,
                    "message": "주문 취소 실패 - API 응답 없음"
                }

        except Exception as e:
            logger.error(f"주문 취소 오류: {e}")
            return {
                "status": "error",
                "order_no": order_no,
                "message": f"주문 취소 오류: {str(e)}"
            }

    def get_today_orders(self, include_filled: bool = True) -> List[Dict]:
        """당일 주문 내역 조회
        
        Args:
            include_filled: True면 전체(체결+미체결), False면 미체결만
        """
        # 미체결만 조회할 경우 CCLD_DVSN='02' 사용
        ccld_dvsn = "00" if include_filled else "02"  # 00:전체, 02:미체결
        
        # 당일 날짜
        today = datetime.now().strftime("%Y%m%d")
        
        # 직접 파라미터를 지정하여 호출
        result = order_api.get_inquire_daily_ccld_lst(
            dv="01",                    # 3개월 이내
            inqr_strt_dt=today,        # 조회시작일자 (오늘)
            inqr_end_dt=today,         # 조회종료일자 (오늘)
            ccld_dvsn=ccld_dvsn,       # 체결구분
        )

        if result is not None and not result.empty:
            orders = result.to_dict('records')
            
            # 미체결만 필요한 경우 필터링
            if not include_filled:
                orders = [
                    order for order in orders 
                    if int(order.get('rmn_qty', 0)) > 0 and order.get('cncl_yn', 'N') != 'Y'
                ]
            
            logger.info(f"📋 당일 주문 조회: 전체 {len(result)}건, 반환 {len(orders)}건 ({'전체' if include_filled else '미체결만'})")
            return orders
        else:
            logger.debug("📋 당일 주문 내역 없음")
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
