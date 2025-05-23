"""
한국투자증권 API Wrapper
"""
import os
import json
import time
import hashlib
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
from utils.logger import setup_logger

# 환경변수 로드
load_dotenv('config/.env')

logger = setup_logger(__name__)

TOKEN_FILE_PATH = os.path.join(os.path.abspath(os.getcwd()), "token_info.json")

class KISBroker:
    """한국투자증권 API 래퍼 클래스"""
        
    # API Endpoints
    ENDPOINTS = {
        # 인증
        "token": "/oauth2/tokenP",
        
        # 주문
        "order": "/uapi/domestic-stock/v1/trading/order-cash",
        "order_modify": "/uapi/domestic-stock/v1/trading/order-rvsecncl",
        
        # 조회
        "balance": "/uapi/domestic-stock/v1/trading/inquire-balance",
        "account": "/uapi/domestic-stock/v1/trading/inquire-account-balance",
        "today_orders": "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
        "price": "/uapi/domestic-stock/v1/quotations/inquire-price",
        "orderbook": "/uapi/domestic-stock/v1/quotations/inquire-orderbook",
        "members": "/uapi/domestic-stock/v1/quotations/inquire-member",
        
        # 기간별 시세 데이터
        "daily_price": "/uapi/domestic-stock/v1/quotations/inquire-daily-price",  # 일봉, 주봉, 월봉
        "minute_price": "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",  # 분봉
    }
    
    def __init__(self):
        
        # API 인증 정보
        self.base_url = os.getenv('KIS_BASE_URL')
        self.api_key = os.getenv('KIS_APP_KEY')
        self.api_secret = os.getenv('KIS_APP_SECRET')
        self.account_no = os.getenv('KIS_ACCOUNT_NO')
        
        if not all([self.api_key, self.api_secret, self.account_no]):
            raise ValueError("API 인증 정보가 설정되지 않았습니다. .env 파일을 확인하세요.")
        
        # 계좌번호 분리 (앞 8자리-뒤 2자리)
        self.account_prefix = self.account_no[:8]
        self.account_suffix = self.account_no[8:10] if len(self.account_no) >= 10 else "01"
        
        # 토큰 정보
        self.access_token = None
        self.token_expires_at = None
        
        # 기존 토큰 로드 시도 후 토큰 발급
        self._load_token_from_file()
        if not self._is_token_valid():
            self._get_access_token()
        
    def _load_token_from_file(self) -> bool:
        """
        파일에서 토큰 정보 로드
        
        Returns:
            토큰 로드 성공 여부
        """
        try:
            if not os.path.exists(TOKEN_FILE_PATH):
                logger.info("토큰 파일이 존재하지 않습니다.")
                return False
                
            with open(TOKEN_FILE_PATH, 'r') as f:
                token_info = json.load(f)
                
            current_token = token_info.get('current', {})
            
            if not current_token or current_token.get('status') != 'SUCCESS':
                logger.info("유효한 토큰 정보가 파일에 없습니다.")
                return False
                
            # 토큰 정보 복원
            self.access_token = current_token.get('token')
            expire_time = current_token.get('expire_time')
            
            if expire_time:
                self.token_expires_at = datetime.fromtimestamp(expire_time)
            else:
                logger.warning("토큰 만료시간 정보가 없습니다.")
                return False
                
            # 토큰 유효성 검증
            if self._is_token_valid():
                logger.info(f"파일에서 토큰 로드 성공. 만료시간: {self.token_expires_at}")
                return True
            else:
                logger.info("파일의 토큰이 만료되었습니다.")
                return False
                
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(f"토큰 파일 로드 중 오류: {e}")
            return False
        except Exception as e:
            logger.error(f"토큰 파일 로드 중 예상치 못한 오류: {e}")
            return False
    
    def _is_token_valid(self) -> bool:
        """
        토큰 유효성 확인
        
        Returns:
            토큰 유효 여부
        """
        if not self.access_token:
            return False
            
        if not self.token_expires_at:
            return False
            
        # 현재 시간보다 5분 이상 여유가 있어야 유효
        return datetime.now() < (self.token_expires_at - timedelta(minutes=5))
        
    def _get_access_token(self) -> None:
        """액세스 토큰 발급"""
        url = f"{self.base_url}{self.ENDPOINTS['token']}"
        
        headers = {
            "content-type": "application/json"
        }
        
        body = {
            "grant_type": "client_credentials",
            "appkey": self.api_key,
            "appsecret": self.api_secret
        }
        
        try:
            logger.info("새로운 토큰 발급 요청...")
            response = requests.post(url, headers=headers, json=body, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            self.access_token = data.get('access_token')
            
            # 토큰 만료 시간 설정 (발급시간 + 유효기간 - 여유시간 5분)
            expires_in = int(data.get('expires_in', 86400))  # 기본 24시간
            issue_time = time.time()
            expire_time = issue_time + expires_in - 300  # 5분 여유
            
            self.token_expires_at = datetime.fromtimestamp(expire_time)
            
            logger.info(f"토큰 발급 성공. 만료시간: {self.token_expires_at}")
            
            # 토큰 정보를 파일에 저장
            self.save_token_to_file(
                token=self.access_token,
                issue_time=issue_time,
                expire_time=expire_time,
                status="SUCCESS"
            )
            
        except requests.exceptions.RequestException as e:
            error_msg = f"토큰 발급 실패: {e}"
            logger.error(error_msg)
            
            # 실패 정보도 파일에 저장
            self.save_token_to_file(
                status="FAILED",
                error_message=str(e)
            )
            raise

    def save_token_to_file(self, token: str = None, issue_time: float = None, 
                         expire_time: float = None, status: str = "SUCCESS", 
                         error_message: str = None):
        """토큰 정보를 파일에 저장"""
        try:
            # 파일이 존재하면 기존 내용 로드
            token_info = {}
            if os.path.exists(TOKEN_FILE_PATH):
                try:
                    with open(TOKEN_FILE_PATH, 'r') as f:
                        token_info = json.load(f)
                        # 기존 정보 보존을 위해 'history' 키가 없으면 생성
                        if 'history' not in token_info:
                            token_info['history'] = []
                except (json.JSONDecodeError, FileNotFoundError):
                    # 파일이 손상되었거나 없으면 새로 생성
                    token_info = {'current': {}, 'history': []}
            else:
                token_info = {'current': {}, 'history': []}    

            current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 현재 토큰 정보 업데이트
            if status == "SUCCESS" and token:
                token_info['current'] = {
                    'token': token,
                    'issue_time': issue_time,
                    'issue_time_str': datetime.fromtimestamp(issue_time).strftime("%Y-%m-%d %H:%M:%S") if issue_time else None,
                    'expire_time': expire_time,
                    'expire_time_str': datetime.fromtimestamp(expire_time).strftime("%Y-%m-%d %H:%M:%S") if expire_time else None,
                    'status': status,
                    'updated_at': current_time_str
                }
            
            # 히스토리에 추가
            history_entry = {
                'token': token[:10] + '...' if token else None,  # 보안상 전체 토큰은 저장하지 않음
                'issue_time_str': datetime.fromtimestamp(issue_time).strftime("%Y-%m-%d %H:%M:%S") if issue_time else None,
                'expire_time_str': datetime.fromtimestamp(expire_time).strftime("%Y-%m-%d %H:%M:%S") if expire_time else None,
                'status': status,
                'error_message': error_message,
                'recorded_at': current_time_str
            }
            token_info['history'].append(history_entry)

            # 히스토리 최대 5개로 제한
            if len(token_info['history']) > 5:
                token_info['history'] = token_info['history'][-5:]

            # 파일에 저장
            with open(TOKEN_FILE_PATH, 'w') as f:
                json.dump(token_info, f, indent=2)
            
            logger.log_system(f"토큰 정보를 파일에 저장했습니다: {TOKEN_FILE_PATH}")
            
        except Exception as e:
            logger.log_error(e, "토큰 정보를 파일에 저장하는 중 오류 발생")
            
    def _ensure_token_valid(self) -> None:
        """토큰 유효성 확인 및 갱신"""
        if not self._is_token_valid():
            logger.info("토큰 갱신 필요")
            self._get_access_token()
            
    def _make_hash(self, data: Dict) -> str:
        """해시값 생성 (주문용)"""
        data_str = json.dumps(data, ensure_ascii=False).encode('utf-8')
        return hashlib.sha256(data_str).hexdigest()
    
    def _get_base_headers(self, tr_id: Optional[str] = None, 
                         additional_headers: Optional[Dict] = None,
                         json_data: Optional[Dict] = None) -> Dict:
        """
        기본 API 헤더 생성
        
        Args:
            tr_id: 거래 ID
            additional_headers: 추가 헤더
            json_data: POST 요청시 JSON 데이터 (해시키 생성용)
            
        Returns:
            완성된 헤더 딕셔너리
        """
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.api_key,
            "appsecret": self.api_secret,
            "custtype": "P",  # 개인
        }
        
        # 거래 ID가 있으면 추가
        if tr_id:
            headers["tr_id"] = tr_id
            
        # POST 요청시 해시키 추가
        if json_data:
            headers["hashkey"] = self._make_hash(json_data)
            
        # 추가 헤더가 있으면 병합
        if additional_headers:
            headers.update(additional_headers)
            
        return headers
    
    def _check_api_response(self, response: requests.Response, endpoint: str) -> Dict:
        """
        API 응답 체크 및 에러 처리
        
        Args:
            response: requests 응답 객체
            endpoint: 호출한 엔드포인트
            
        Returns:
            파싱된 응답 데이터
            
        Raises:
            Exception: API 에러 발생 시
        """
        try:
            # HTTP 상태 코드 체크
            response.raise_for_status()
            
            # JSON 파싱
            data = response.json()
            
            # KIS API 응답 코드 체크
            rt_cd = data.get('rt_cd', '')
            msg1 = data.get('msg1', '')
            
            if rt_cd == '0':
                # 성공
                logger.debug(f"API 호출 성공 - {endpoint}: {msg1}")
                return data
            elif rt_cd == '1':
                # 경고 (일반적으로 처리 가능)
                logger.warning(f"API 경고 - {endpoint}: {msg1}")
                return data
            else:
                # 에러
                error_msg = f"API 에러 - {endpoint}: [{rt_cd}] {msg1}"
                logger.error(error_msg)
                raise Exception(error_msg)
                
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP 에러 - {endpoint}: {e}"
            logger.error(error_msg)
            raise Exception(error_msg)
        except requests.exceptions.RequestException as e:
            error_msg = f"네트워크 에러 - {endpoint}: {e}"
            logger.error(error_msg)
            raise Exception(error_msg)
        except json.JSONDecodeError as e:
            error_msg = f"JSON 파싱 에러 - {endpoint}: {e}"
            logger.error(error_msg)
            raise Exception(error_msg)
        except Exception as e:
            error_msg = f"알 수 없는 에러 - {endpoint}: {e}"
            logger.error(error_msg)
            raise Exception(error_msg)
        
    def _call_api(self, endpoint: str, method: str = "GET", 
                  headers: Optional[Dict] = None, params: Optional[Dict] = None,
                  json_data: Optional[Dict] = None, tr_id: Optional[str] = None) -> Dict:
        """
        API 호출 공통 메서드
        
        Args:
            endpoint: API 엔드포인트
            method: HTTP 메서드
            headers: 추가 헤더
            params: 쿼리 파라미터
            json_data: JSON 바디 데이터
            tr_id: 거래 ID
            
        Returns:
            API 응답 데이터
            
        Raises:
            Exception: API 호출 실패 시
        """
        self._ensure_token_valid()
        
        url = f"{self.base_url}{endpoint}"
        
        # 기본 헤더 생성
        request_headers = self._get_base_headers(tr_id=tr_id, additional_headers=headers, json_data=json_data)
        
        try:
            # logger.debug(f"API 호출 시작 - {method} {endpoint}")
            
            # API 호출
            if method.upper() == "GET":
                response = requests.get(
                    url, 
                    headers=request_headers, 
                    params=params, 
                    timeout=30
                )
            elif method.upper() == "POST":
                response = requests.post(
                    url, 
                    headers=request_headers, 
                    params=params, 
                    json=json_data, 
                    timeout=30
                )
            else:
                raise ValueError(f"지원하지 않는 HTTP 메서드: {method}")
            
            # 응답 체크 및 반환
            return self._check_api_response(response, endpoint)
            
        except Exception as e:
            logger.error(f"API 호출 실패 - {endpoint}: {e}")
            raise
            
    def get_current_price(self, stock_code: str) -> Dict:
        """
        현재가 조회
        
        Args:
            stock_code: 종목코드
            
        Returns:
            현재가 정보
        """
        tr_id = "FHKST01010100"
        
        params = {
            "fid_cond_mrkt_div_code": "J",  # 주식
            "fid_input_iscd": stock_code
        }
        
        result = self._call_api(
            endpoint=self.ENDPOINTS['price'],
            method="GET",
            params=params,
            tr_id=tr_id
        )
        
        output = result.get('output', {})
        
        return {
            "stock_code": stock_code,
            "current_price": int(output.get('stck_prpr', 0)),  # 현재가
            "change_rate": float(output.get('prdy_ctrt', 0)),  # 전일대비율
            "volume": int(output.get('acml_vol', 0)),  # 누적거래량
            "high": int(output.get('stck_hgpr', 0)),  # 고가
            "low": int(output.get('stck_lwpr', 0)),  # 저가
            "open": int(output.get('stck_oprc', 0)),  # 시가
            "timestamp": datetime.now()
        }
        
    def get_orderbook(self, stock_code: str) -> Dict:
        """
        호가 조회
        
        Args:
            stock_code: 종목코드
            
        Returns:
            호가 정보
        """
        tr_id = "FHKST01010200"
        
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": stock_code
        }
        
        result = self._call_api(
            endpoint=self.ENDPOINTS['orderbook'],
            method="GET",
            params=params,
            tr_id=tr_id
        )
        
        output = result.get('output1', [])
        
        # 호가 데이터 파싱
        asks = []  # 매도호가
        bids = []  # 매수호가
        
        for i in range(1, 11):  # 10호가
            asks.append({
                "price": int(output.get(f'askp{i}', 0)),
                "volume": int(output.get(f'askp_rsqn{i}', 0))
            })
            bids.append({
                "price": int(output.get(f'bidp{i}', 0)),
                "volume": int(output.get(f'bidp_rsqn{i}', 0))
            })
            
        return {
            "stock_code": stock_code,
            "asks": asks,
            "bids": bids,
            "timestamp": datetime.now()
        }
        
    def buy_order(self, stock_code: str, quantity: int, price: int = 0) -> Dict:
        """
        매수 주문
        
        Args:
            stock_code: 종목코드
            quantity: 수량
            price: 가격 (0이면 시장가)
            
        Returns:
            주문 결과
        """
        tr_id = "TTTC0012U"
        
        # 주문 구분: 00=지정가, 01=시장가
        order_type = "01" if price == 0 else "00"
        
        data = {
            "CANO": self.account_prefix,
            "ACNT_PRDT_CD": self.account_suffix,
            "PDNO": stock_code,
            "ORD_DVSN": order_type,
            "ORD_QTY": str(quantity),
            "ORD_UNPR": str(price) if price > 0 else "0"
        }
        
        result = self._call_api(
            endpoint=self.ENDPOINTS['order'],
            method="POST",
            json_data=data,
            tr_id=tr_id
        )
        
        output = result.get('output', {})
        
        return {
            "order_no": output.get('ODNO'),  # 주문번호
            "order_time": output.get('ORD_TMD'),  # 주문시각
            "stock_code": stock_code,
            "quantity": quantity,
            "price": price,
            "order_type": "BUY",
            "status": "ORDERED"
        }
        
    def sell_order(self, stock_code: str, quantity: int, price: int = 0) -> Dict:
        """
        매도 주문
        
        Args:
            stock_code: 종목코드
            quantity: 수량
            price: 가격 (0이면 시장가)
            
        Returns:
            주문 결과
        """
        tr_id = "TTTC0011U"
        
        # 주문 구분: 00=지정가, 01=시장가
        order_type = "01" if price == 0 else "00"
        
        data = {
            "CANO": self.account_prefix,
            "ACNT_PRDT_CD": self.account_suffix,
            "PDNO": stock_code,
            "ORD_DVSN": order_type,
            "ORD_QTY": str(quantity),
            "ORD_UNPR": str(price) if price > 0 else "0"
        }
        
        result = self._call_api(
            endpoint=self.ENDPOINTS['order'],
            method="POST",
            json_data=data,
            tr_id=tr_id
        )
        
        output = result.get('output', {})
        
        return {
            "order_no": output.get('ODNO'),
            "order_time": output.get('ORD_TMD'),
            "stock_code": stock_code,
            "quantity": quantity,
            "price": price,
            "order_type": "SELL",
            "status": "ORDERED"
        }
        
    def cancel_order(self, order_no: str, stock_code: str, quantity: int) -> Dict:
        """
        주문 취소
        
        Args:
            order_no: 주문번호
            stock_code: 종목코드
            quantity: 취소수량
            
        Returns:
            취소 결과
        """
        tr_id = "TTTC0013U"
        
        data = {
            "CANO": self.account_prefix,
            "ACNT_PRDT_CD": self.account_suffix,
            "KRX_FWDG_ORD_ORGNO": "",  # 원주문번호 (정정시 사용)
            "ORGN_ODNO": order_no,  # 주문번호
            "ORD_DVSN": "00",  # 주문구분
            "RVSE_CNCL_DVSN_CD": "02",  # 취소
            "ORD_QTY": "0",  # 정정시 수량
            "ORD_UNPR": "0",  # 정정시 가격
            "QTY_ALL_ORD_YN": "Y"  # 전량 지정
        }
        
        result = self._call_api(
            endpoint=self.ENDPOINTS['order_modify'],
            method="POST",
            json_data=data,
            tr_id=tr_id
        )
        
        return {
            "order_no": order_no,
            "status": "CANCELLED",
            "message": result.get('msg1', '')
        }
        
    def get_balance(self) -> List[Dict]:
        """
        계좌 잔고 조회
        
        Returns:
            보유 종목 리스트
        """
        tr_id = "TTTC8434R"
        
        params = {
            "CANO": self.account_prefix,
            "ACNT_PRDT_CD": self.account_suffix,
            "AFHR_FLPR_YN": "N",  # 시간외단일가여부
            "OFL_YN": "N",  # 오프라인여부
            "INQR_DVSN": "01",  # 조회구분 01:대출일별
            "UNPR_DVSN": "01",  # 단가구분
            "FUND_STTL_ICLD_YN": "N",  # 펀드결제분포함여부
            "FNCG_AMT_AUTO_RDPT_YN": "N",  # 융자금액자동상환여부
            "PRCS_DVSN": "00",  # 처리구분 00:전일매매포함
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": ""
        }
        
        result = self._call_api(
            endpoint=self.ENDPOINTS['balance'],
            method="GET",
            params=params,
            tr_id=tr_id
        )
        
        output = result.get('output1', [])
        
        positions = []
        for item in output:
            if int(item.get('hldg_qty', 0)) > 0:  # 보유수량이 있는 경우만
                positions.append({
                    "stock_code": item.get('pdno'),  # 종목코드
                    "stock_name": item.get('prdt_name'),  # 종목명
                    "quantity": int(item.get('hldg_qty', 0)),  # 보유수량
                    "avg_price": float(item.get('pchs_avg_pric', 0)),  # 매입평균가
                    "current_price": float(item.get('prpr', 0)),  # 현재가
                    "eval_amount": float(item.get('evlu_amt', 0)),  # 평가금액
                    "profit_loss": float(item.get('evlu_pfls_amt', 0)),  # 평가손익
                    "profit_rate": float(item.get('evlu_pfls_rt', 0))  # 수익률
                })
                
        return positions
        
    def get_account_info(self) -> Dict:
        """
        계좌 정보 조회
        
        Returns:
            계좌 정보
        """
        tr_id = "CTRP6548R"
        
        params = {
            "CANO": self.account_prefix,
            "ACNT_PRDT_CD": self.account_suffix,
            "INQR_DVSN_1": "",
            "BSPR_BF_DT_APLY_YN": ""
        }
        
        result = self._call_api(
            endpoint=self.ENDPOINTS['account'],
            method="GET",
            params=params,
            tr_id=tr_id
        )
        
        output2 = result.get('output2', {})
        
        return {
            #"total_eval_amount": float(output2.get('tot_evlu_amt', 0)),  # 총평가금액
            "total_purchase_amount": float(output2.get('pchs_amt_smtl', 0)),  # 총매입금액
            "total_profit_loss": float(output2.get('evlu_pfls_amt_smtl', 0)),  # 총평가손익
            #"available_cash": float(output2.get('prvs_rcdl_excc_amt', 0)),  # 가용현금
            "total_deposit": float(output2.get('tot_asst_amt', 0))  # 총자산금액
        }
        
    def get_today_orders(self) -> List[Dict]:
        """
        당일 체결 내역 조회
        
        Returns:
            체결 내역 리스트
        """
        tr_id = "TTTC0081R"
        
        params = {
            "CANO": self.account_prefix,
            "ACNT_PRDT_CD": self.account_suffix,
            "INQR_STRT_DT": datetime.now().strftime('%Y%m%d'),
            "INQR_END_DT": datetime.now().strftime('%Y%m%d'),
            "SLL_BUY_DVSN_CD": "00",  # 00:전체, 01:매도, 02:매수
            "INQR_DVSN": "01",  # 01:역순
            "PDNO": "",
            "CCLD_DVSN": "01",  # 01:체결, 02:미체결
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "INQR_DVSN_3": "",
            "INQR_DVSN_1": "",
            "CSNU": "P",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": ""
        }
        
        result = self._call_api(
            endpoint=self.ENDPOINTS['today_orders'],
            method="GET",
            params=params,
            tr_id=tr_id
        )
        
        output = result.get('output1', [])
        
        today_orders = []
        for item in output:
            today_orders.append({
                "order_no": item.get('odno'),  # 주문번호
                "stock_code": item.get('pdno'),  # 종목코드
                "stock_name": item.get('prdt_name'),  # 종목명
                "order_type": "BUY" if item.get('sll_buy_dvsn_cd') == "02" else "SELL",
                "order_time": item.get('ord_tmd'),  # 주문시각
                #"exec_time": item.get('ccld_tmd'),  # 체결시각
                "order_quantity": int(item.get('ord_qty', 0)),  # 주문수량
                "exec_quantity": int(item.get('ccld_qty', 0)),  # 체결수량
                "order_price": float(item.get('ord_unpr', 0))  # 주문가격
                #"exec_price": float(item.get('ccld_pric', 0)),  # 체결가격
                #"status": item.get('ord_gno_brno')  # 주문상태
            })
            
        return today_orders
        
    def get_daily_prices(self, stock_code: str, period_type: str = "D") -> List[Dict]:
        """
        기간별 시세 데이터 조회
        
        Args:
            stock_code: 종목코드
            period_type: 기간 구분 ("D": 일봉, "W": 주봉, "M": 월봉)
            
        Returns:
            시세 데이터 리스트
        """
        # 지원하는 기간 구분 확인
        if period_type not in ["D", "W", "M"]:
            raise ValueError(f"지원하지 않는 기간 구분: {period_type}. 'D', 'W', 'M' 중 선택하세요.")
        
        # TR ID는 고정
        tr_id = "FHKST01010400"
        
        params = {
            "fid_cond_mrkt_div_code": "J",  # 주식
            "fid_input_iscd": stock_code,
            "fid_period_div_code": period_type,  # 기간분류코드 D:일, W:주, M:월
            "fid_org_adj_prc": "1"  # 수정주가 원주가 구분 (1: 수정주가)
        }
        
        result = self._call_api(
            endpoint=self.ENDPOINTS['daily_price'],
            method="GET",
            params=params,
            tr_id=tr_id
        )
        
        output = result.get('output1', [])
        
        prices = []
        for item in output:
            prices.append({
                "date": item.get('stck_bsop_date'),  # 날짜
                "open": int(item.get('stck_oprc', 0)),  # 시가
                "high": int(item.get('stck_hgpr', 0)),  # 고가
                "low": int(item.get('stck_lwpr', 0)),  # 저가
                "close": int(item.get('stck_clpr', 0)),  # 종가
                "volume": int(item.get('acml_vol', 0)),  # 거래량
                "change_rate": float(item.get('prdy_ctrt', 0)),  # 전일대비율
                "period_type": period_type  # 기간 구분 추가
            })
            
        return prices
    
    def get_minute_prices(self, stock_code: str, time_unit: str = "1") -> List[Dict]:
        """
        주식 당일 분봉 조회
        
        Args:
            stock_code: 종목코드
            time_unit: 시간 단위 ("1": 1분, "3": 3분, "5": 5분, "10": 10분, "15": 15분, "30": 30분, "60": 60분)
                      또는 시간 형식 (HHMMSS, 예: "093000" = 오전 9시 30분)
            
        Returns:
            분봉 데이터 리스트 (최대 30건, 당일 데이터만)
            
        Note:
            - 당일 분봉 데이터만 제공됩니다 (전일자 분봉 미제공)
            - 한 번의 호출에 최대 30건까지 확인 가능
            - FID_INPUT_HOUR_1에 미래일시 입력 시 현재가로 조회됩니다
            - output2의 첫번째 배열 체결량은 첫체결 전까지 이전 분봉 체결량이 표시됩니다
        """
        tr_id = "FHKST03010200"
        
        params = {
            "fid_cond_mrkt_div_code": "J",  # 주식
            "fid_input_iscd": stock_code,
            "fid_input_hour_1": time_unit,  # 시간 단위 또는 시간(HHMMSS)
            "fid_pw_data_incu_yn": "Y",  # 과거 데이터 포함 여부
            "fid_etc_cls_code": ""
        }
        
        result = self._call_api(
            endpoint=self.ENDPOINTS['minute_price'],
            method="GET",
            params=params,
            tr_id=tr_id
        )
        
        # 분봉 데이터는 output2에 있음
        output = result.get('output2', [])
        
        prices = []
        for item in output:
            # 빈 데이터 체크
            if not item.get('stck_cntg_hour'):
                continue
                
            prices.append({
                "business_date": item.get('stck_bsop_date', ''),  # 주식 영업일자
                "time": item.get('stck_cntg_hour', ''),  # 주식 체결시간 (HHMMSS)
                "current_price": int(item.get('stck_prpr', 0)),  # 주식 현재가
                "open": int(item.get('stck_oprc', 0)),  # 주식 시가
                "high": int(item.get('stck_hgpr', 0)),  # 주식 최고가
                "low": int(item.get('stck_lwpr', 0)),  # 주식 최저가
                "volume": int(item.get('cntg_vol', 0)),  # 체결 거래량
                "amount": int(item.get('acml_tr_pbmn', 0)) if item.get('acml_tr_pbmn') else 0,  # 누적 거래대금
                "time_unit": time_unit  # 요청한 시간 단위
            })
            
        return prices

    def get_token_info(self) -> Dict:
        """
        현재 토큰 정보 반환
        
        Returns:
            토큰 정보 딕셔너리
        """
        return {
            "has_token": bool(self.access_token),
            "is_valid": self._is_token_valid(),
            "expires_at": self.token_expires_at.isoformat() if self.token_expires_at else None,
            "expires_in_minutes": (
                int((self.token_expires_at - datetime.now()).total_seconds() / 60)
                if self.token_expires_at and self.token_expires_at > datetime.now()
                else 0
            ),
            "token_preview": (
                self.access_token[:10] + "..." if self.access_token else None
            )
        }
    
    def force_token_refresh(self) -> None:
        """
        강제로 토큰 재발급
        """
        logger.info("토큰 강제 재발급 요청")
        self.access_token = None
        self.token_expires_at = None
        self._get_access_token()
