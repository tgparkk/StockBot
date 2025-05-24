"""
한국투자증권 API Wrapper
기존 broker.py 기능 + 전략용 데이터 조회 기능 통합 + 멀티스레드 안전성
"""
import os
import json
import time
import hashlib
import requests
import threading
from collections import deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from utils.logger import setup_logger

# KIS 데이터 모델 import
try:
    from core.data.kis_data_models import KISCurrentPrice, KISHistoricalData, KISOrderBook
except ImportError:
    # 순환 import 방지를 위한 fallback
    KISCurrentPrice = None
    KISHistoricalData = None
    KISOrderBook = None

# 환경변수 로드
load_dotenv('config/.env')

logger = setup_logger(__name__)

TOKEN_FILE_PATH = os.path.join(os.path.abspath(os.getcwd()), "token_info.json")

class RateLimiter:
    """API 호출 횟수 제한 클래스"""

    def __init__(self, max_calls_per_second: int = 20):
        """
        Rate Limiter 초기화

        Args:
            max_calls_per_second: 초당 최대 호출 횟수 (KIS API 기준: 20)
        """
        self.max_calls = max_calls_per_second
        self.calls = deque()
        self.lock = threading.Lock()

    def wait_if_needed(self) -> None:
        """필요시 대기 (Rate Limiting)"""
        with self.lock:
            now = time.time()

            # 1초 이전 호출 기록들 제거
            while self.calls and self.calls[0] <= now - 1.0:
                self.calls.popleft()

            # 현재 1초 내 호출 횟수가 제한을 초과하면 대기
            if len(self.calls) >= self.max_calls:
                sleep_time = self.calls[0] + 1.0 - now
                if sleep_time > 0:
                    logger.debug(f"Rate limit 도달. {sleep_time:.2f}초 대기...")
                    time.sleep(sleep_time)
                    # 대기 후 재귀 호출로 다시 체크
                    return self.wait_if_needed()

            # 현재 호출 시간 기록
            self.calls.append(now)

class KISRestAPIManager:
    """한국투자증권 REST API 관리자 (멀티스레드 안전)"""

    # 클래스 레벨에서 rate limiter 공유 (모든 인스턴스가 같은 제한 공유)
    _rate_limiter = RateLimiter(max_calls_per_second=20)
    _token_lock = threading.RLock()  # 재진입 가능한 락 (같은 스레드가 여러 번 획득 가능)

    # API 호출 통계 (클래스 레벨)
    _api_call_count = 0
    _api_error_count = 0
    _stats_lock = threading.Lock()

    # API Endpoints
    ENDPOINTS = {
        # 인증
        "api_token": "/oauth2/tokenP",
        "api_websocket_key": "/oauth2/Approval",  # 웹소켓 접속키 발급

        # 주문
        "api_order": "/uapi/domestic-stock/v1/trading/order-cash",
        "api_order_modify": "/uapi/domestic-stock/v1/trading/order-rvsecncl",

        # 조회
        "api_balance": "/uapi/domestic-stock/v1/trading/inquire-balance",
        "api_account": "/uapi/domestic-stock/v1/trading/inquire-account-balance",
        "api_today_orders": "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
        "api_price": "/uapi/domestic-stock/v1/quotations/inquire-price",
        "api_orderbook": "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",  # 호가 조회

        # 기간별 시세 데이터
        "api_daily_price": "/uapi/domestic-stock/v1/quotations/inquire-daily-price",  # 일봉, 주봉, 월봉
        "api_minute_price": "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",  # 분봉

        # 순위/스크리닝 관련
        "api_volume_rank": "/uapi/domestic-stock/v1/quotations/volume-rank",  # 거래량순위
        "api_change_rank": "/uapi/domestic-stock/v1/ranking/fluctuation",  # 등락률순위
        "api_bid_ask_rank": "/uapi/domestic-stock/v1/ranking/quote-balance",  # 호가잔량순위
    }

    def __init__(self):

        # API 인증 정보
        self.base_url = os.getenv('KIS_BASE_URL')
        self.api_key = os.getenv('KIS_APP_KEY')
        self.api_secret = os.getenv('KIS_APP_SECRET')
        self.account_no = os.getenv('KIS_ACCOUNT_NO')

        if not all([self.api_key, self.api_secret, self.account_no]):
            raise ValueError("API 인증 정보가 설정되지 않았습니다. .env 파일을 확인하세요.")

        # 계좌번호 None 체크 추가
        if not self.account_no:
            raise ValueError("계좌번호가 설정되지 않았습니다.")

        # 계좌번호 분리 (앞 8자리-뒤 2자리)
        self.account_prefix = self.account_no[:8]
        self.account_suffix = self.account_no[8:10] if len(self.account_no) >= 10 else "01"

        # 토큰 정보 (스레드 안전성을 위해 _token_lock 사용)
        self.access_token = None
        self.token_expires_at = None

        # Connection Pool 설정 (성능 및 안정성 향상)
        self.session = self._create_session()

        # 기존 토큰 로드 시도 후 토큰 발급
        self._load_token_from_file()
        if not self._is_token_valid():
            self._get_access_token()

    def _create_session(self) -> requests.Session:
        """Connection Pool과 Retry 정책이 적용된 세션 생성"""
        session = requests.Session()

        # Retry 정책 설정
        retry_strategy = Retry(
            total=3,  # 최대 3번 재시도
            status_forcelist=[429, 500, 502, 503, 504],  # 재시도할 HTTP 상태 코드
            backoff_factor=1,  # 재시도 간 대기 시간 (1, 2, 4초...)
            raise_on_status=False
        )

        # HTTP Adapter 설정 (Connection Pool)
        adapter = HTTPAdapter(
            pool_connections=100,  # Connection Pool 크기
            pool_maxsize=100,      # 최대 연결 수
            max_retries=retry_strategy,
            pool_block=False       # 연결 풀이 가득 찰 때 블로킹하지 않음
        )

        # HTTP와 HTTPS 모두에 적용
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _load_token_from_file(self) -> bool:
        """
        파일에서 토큰 정보 로드 (스레드 안전)

        Returns:
            토큰 로드 성공 여부
        """
        with self._token_lock:
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
        토큰 유효성 확인 (스레드 안전)

        Returns:
            토큰 유효 여부
        """
        with self._token_lock:
            if not self.access_token:
                return False

            if not self.token_expires_at:
                return False

            # 현재 시간보다 5분 이상 여유가 있어야 유효
            return datetime.now() < (self.token_expires_at - timedelta(minutes=5))

    def _get_access_token(self) -> None:
        """액세스 토큰 발급 (스레드 안전)"""
        with self._token_lock:
            # 다른 스레드가 이미 토큰을 발급했는지 재확인
            if self._is_token_valid():
                logger.debug("다른 스레드에서 이미 토큰 발급 완료")
                return

            url = f"{self.base_url}{self.ENDPOINTS['api_token']}"

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

                # Rate limiting 적용
                self._rate_limiter.wait_if_needed()

                response = self.session.post(url, headers=headers, json=body, timeout=10)
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

    def save_token_to_file(self, token: Optional[str] = None, issue_time: Optional[float] = None,
                         expire_time: Optional[float] = None, status: str = "SUCCESS",
                         error_message: Optional[str] = None):
        """토큰 정보를 파일에 저장 (스레드 안전)"""
        with self._token_lock:
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
        """토큰 유효성 확인 및 갱신 (스레드 안전)"""
        # 먼저 락 없이 빠른 체크 (성능 최적화)
        if self._is_token_valid():
            return

        # 토큰이 유효하지 않으면 락을 획득하고 재발급
        with self._token_lock:
            # 다시 한 번 체크 (다른 스레드가 이미 갱신했을 수 있음)
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
        API 응답 체크 및 에러 처리 (장외시간 대응 포함)

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
            output = data.get('output', [])

                        # 장외시간 판별: msg1이나 msg_cd의 내용으로 실제 장외시간인지 확인
            def is_after_hours_response(msg1_text: str, msg_cd_text: str = "") -> bool:
                """메시지 내용으로 장외시간 응답인지 판별"""
                after_hours_keywords = [
                    "장외시간", "시간외", "거래시간", "조회시간", "운영시간",
                    "시간이 아닙니다", "시간 외", "장 종료", "거래 중단"
                ]

                combined_msg = f"{msg1_text} {msg_cd_text}".lower()
                return any(keyword.lower() in combined_msg for keyword in after_hours_keywords)

            # rt_cd가 성공('0')이고 output이 빈 배열이며, 실제로 장외시간 메시지인 경우만 특별 처리
            if rt_cd == '0' and isinstance(output, list) and len(output) == 0:
                msg_cd = data.get('msg_cd', '')

                # 실제 장외시간 메시지인지 확인
                if is_after_hours_response(msg1, msg_cd):
                    logger.warning(f"API 장외시간 응답 - {endpoint}: {msg1}")
                    return {
                        'rt_cd': '0',
                        'msg1': msg1,  # 원본 메시지 유지
                        'msg_cd': msg_cd,  # 원본 메시지 코드 유지
                        'output': []
                    }
                # 장외시간이 아닌 정상적인 빈 배열 응답 (예: 보유종목 없음)은 원본 그대로 반환
                else:
                    logger.debug(f"API 정상 빈 응답 - {endpoint}: {msg1}")
                    return data

            # rt_cd가 없고 msg1도 없으며 output이 빈 배열인 경우 (일부 순위 API)
            elif not rt_cd and not msg1 and isinstance(output, list) and len(output) == 0:
                # 이 경우는 장외시간일 가능성이 높음
                logger.warning(f"API 빈 응답 (장외시간 추정) - {endpoint}: 데이터 없음")
                return {
                    'rt_cd': '0',
                    'msg1': '장외시간 - 데이터 없음',
                    'output': []
                }

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
        API 호출 공통 메서드 (Rate Limiting 적용)

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
        # 토큰 유효성 확인 및 갱신
        self._ensure_token_valid()

        # Rate limiting 적용 (초당 20건 제한)
        self._rate_limiter.wait_if_needed()

        url = f"{self.base_url}{endpoint}"

        # 기본 헤더 생성
        request_headers = self._get_base_headers(tr_id=tr_id, additional_headers=headers, json_data=json_data)

        try:
            # logger.debug(f"API 호출 시작 - {method} {endpoint}")

            # API 호출 (Connection Pool 사용)
            if method.upper() == "GET":
                response = self.session.get(
                    url,
                    headers=request_headers,
                    params=params,
                    timeout=30
                )
            elif method.upper() == "POST":
                response = self.session.post(
                    url,
                    headers=request_headers,
                    params=params,
                    json=json_data,
                    timeout=30
                )
            else:
                raise ValueError(f"지원하지 않는 HTTP 메서드: {method}")

            # 응답 체크 및 반환
            result = self._check_api_response(response, endpoint)

            # 성공 통계 업데이트
            self._update_api_stats(success=True)

            return result

        except Exception as e:
            # 실패 통계 업데이트
            self._update_api_stats(success=False)
            logger.error(f"API 호출 실패 - {endpoint}: {e}")
            raise

    @classmethod
    def _update_api_stats(cls, success: bool = True) -> None:
        """API 호출 통계 업데이트 (스레드 안전)"""
        with cls._stats_lock:
            cls._api_call_count += 1
            if not success:
                cls._api_error_count += 1

    @classmethod
    def get_api_stats(cls) -> Dict:
        """API 호출 통계 조회"""
        with cls._stats_lock:
            success_rate = 0.0
            if cls._api_call_count > 0:
                success_rate = (cls._api_call_count - cls._api_error_count) / cls._api_call_count * 100

            return {
                'total_calls': cls._api_call_count,
                'total_errors': cls._api_error_count,
                'success_rate': round(success_rate, 2),
                'current_rate_limit': cls._rate_limiter.max_calls,
                'calls_in_last_second': len(cls._rate_limiter.calls)
            }

    @classmethod
    def reset_api_stats(cls) -> None:
        """API 통계 리셋"""
        with cls._stats_lock:
            cls._api_call_count = 0
            cls._api_error_count = 0

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
            endpoint=self.ENDPOINTS['api_price'],
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
            endpoint=self.ENDPOINTS['api_orderbook'],
            method="GET",
            params=params,
            tr_id=tr_id
        )

        # 호가 데이터는 output1에 있음 (개별 종목) 또는 output (빈 배열)
        output = result.get('output1', {})
        if not output and result.get('output') == []:
            # 장외시간 응답: 빈 배열
            output = {}

        # 호가 데이터 파싱
        asks = []  # 매도호가
        bids = []  # 매수호가

        if isinstance(output, dict):
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
            endpoint=self.ENDPOINTS['api_order'],
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
            endpoint=self.ENDPOINTS['api_order'],
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
            endpoint=self.ENDPOINTS['api_order_modify'],
            method="POST",
            json_data=data,
            tr_id=tr_id
        )

        return {
            "order_no": order_no,
            "status": "CANCELLED",
            "message": result.get('msg1', '')
        }

    def get_balance(self) -> Dict:
        """
        계좌 잔고 조회 (보유 종목 + 계좌 요약 정보)

        Returns:
            {
                'positions': 보유 종목 리스트,
                'account_summary': 계좌 요약 정보 (예수금, 평가금액 등)
            }
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
            endpoint=self.ENDPOINTS['api_balance'],
            method="GET",
            params=params,
            tr_id=tr_id
        )

        # output1: 보유 종목 상세 (기존)
        output1 = result.get('output1', [])
        # output2: 계좌 요약 정보 (실제로는 list일 수 있음)
        output2_raw = result.get('output2', [])

        # output2 구조 파악 및 안전한 접근 (공식 문서: output2는 array)
        output2 = {}
        if isinstance(output2_raw, list):
            if len(output2_raw) > 0 and isinstance(output2_raw[0], dict):
                # output2가 array이고 첫 번째 요소가 dict인 경우
                output2 = output2_raw[0]
            elif len(output2_raw) > 1:
                # 여러 요소가 있는 경우 각각 확인해서 계좌 요약 정보 찾기
                for item in output2_raw:
                    if isinstance(item, dict) and ('dnca_tot_amt' in item or 'tot_evlu_amt' in item):
                        output2 = item
                        break
        elif isinstance(output2_raw, dict):
            # output2가 dict인 경우 (예외적인 경우)
            output2 = output2_raw

        # 보유 종목 리스트 파싱
        positions = []
        for item in output1:
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

        # 계좌 요약 정보 파싱 (안전한 접근)
        def safe_float(value, default=0.0):
            """안전한 float 변환"""
            try:
                return float(value) if value else default
            except (ValueError, TypeError):
                return default

                # 사용자가 요청한 핵심 필드들 우선 처리
        account_summary = {
            # 🎯 사용자 요청 핵심 필드 (공식 API 문서 기준)
            "deposit_balance": safe_float(output2.get('dnca_tot_amt', 0)),  # 예수금총액 ⭐
            "total_eval_amount": safe_float(output2.get('tot_evlu_amt', 0)),  # 총평가금액 ⭐
            "securities_eval_amount": safe_float(output2.get('scts_evlu_amt', 0)),  # 유가증권평가금액 ⭐
            "net_asset_amount": safe_float(output2.get('nass_amt', 0)),  # 순자산금액 ⭐

            # 추가 유용한 필드들
            "foreign_deposit": safe_float(output2.get('frcr_evlu_tota', 0)),  # 외화예수금평가액
            "total_profit_loss": safe_float(output2.get('evlu_pfls_smtl_amt', 0)),  # 평가손익합계금액
            "total_profit_rate": safe_float(output2.get('bfdy_tot_asst_evlu_amt', 0)),  # 전일대비평가손익율
            "total_asset_amount": safe_float(output2.get('tot_asst_amt', 0)),  # 총자산금액
            "purchase_amount_total": safe_float(output2.get('pchs_amt_smtl_amt', 0)),  # 매입금액합계금액

            # 디버깅 정보 (문제 해결용)
            "debug_info": {
                "output2_type": str(type(output2_raw)),
                "output2_length": len(output2_raw) if isinstance(output2_raw, list) else 1,
                "output2_keys": list(output2.keys()) if isinstance(output2, dict) else [],
                "output2_sample": output2 if output2 else "비어있음"
            }
        }

        return {
            'positions': positions,
            'account_summary': account_summary
        }

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
            endpoint=self.ENDPOINTS['api_account'],
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
            endpoint=self.ENDPOINTS['api_today_orders'],
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
            endpoint=self.ENDPOINTS['api_daily_price'],
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
            endpoint=self.ENDPOINTS['api_minute_price'],
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

    def get_websocket_approval_key(self) -> str:
        """
        웹소켓 접속키 발급

        Returns:
            웹소켓 접속키 (approval_key)

        Raises:
            Exception: 접속키 발급 실패 시
        """
        self._ensure_token_valid()

        url = f"{self.base_url}{self.ENDPOINTS['api_websocket_key']}"

        headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.api_key,
            "appsecret": self.api_secret,
            "custtype": "P"  # 개인
        }

        body = {
            "grant_type": "client_credentials",
            "appkey": self.api_key,
            "secretkey": self.api_secret
        }

        try:
            logger.info("웹소켓 접속키 발급 요청...")
            response = self.session.post(url, headers=headers, json=body, timeout=10)
            response.raise_for_status()

            data = response.json()
            logger.info(f"웹소켓 접속키 API 응답: {data}")

            # 웹소켓 접속키 API는 일반 API와 응답 구조가 다름
            # approval_key가 있으면 성공으로 간주
            approval_key = data.get('approval_key')
            if approval_key:
                logger.info("웹소켓 접속키 발급 성공")
                return approval_key
            else:
                # 에러 응답 처리
                rt_cd = data.get('rt_cd', 'N/A')
                msg1 = data.get('msg1', '알 수 없는 오류')
                msg_cd = data.get('msg_cd', 'N/A')
                logger.error(f"웹소켓 접속키 발급 실패 - rt_cd: {rt_cd}, msg_cd: {msg_cd}, msg1: {msg1}")
                raise Exception(f"웹소켓 접속키 발급 실패: [{rt_cd}] {msg1}")

        except requests.exceptions.RequestException as e:
            error_msg = f"웹소켓 접속키 발급 API 호출 실패: {e}"
            logger.error(error_msg)
            raise Exception(error_msg)

    # ========== 전략용 데이터 조회 기능 (kis_api_manager.py 통합) ==========

    def get_current_price_model(self, stock_code: str) -> Optional['KISCurrentPrice']:
        """
        현재가 정보 조회 (KISCurrentPrice 모델로 반환)

        Args:
            stock_code: 종목코드

        Returns:
            KISCurrentPrice 객체
        """
        try:
            # 기존 get_current_price 메서드 사용
            raw_data = self.get_current_price(stock_code)

            if not raw_data or 'current_price' not in raw_data:
                return None

            # 전일종가 추정 (시가에서 갭만큼 역산)
            current_price = raw_data.get('current_price', 0)
            open_price = raw_data.get('open', 0)
            prev_close = open_price if open_price > 0 else current_price

            # KISCurrentPrice가 import되지 않은 경우 Dict 반환 (하위 호환성)
            if KISCurrentPrice is None:
                return {
                    'stck_shrn_iscd': raw_data.get('stock_code', ''),
                    'stck_prpr': current_price,
                    'prdy_vrss': current_price - prev_close,
                    'prdy_vrss_sign': '2' if current_price > prev_close else '5',
                    'prdy_ctrt': raw_data.get('change_rate', 0.0),
                    'stck_oprc': open_price,
                    'stck_hgpr': raw_data.get('high', 0),
                    'stck_lwpr': raw_data.get('low', 0),
                    'stck_clpr': prev_close,
                    'acml_vol': raw_data.get('volume', 0),
                    'acml_tr_pbmn': 0,
                    'seln_cntg_qty': 0,
                    'shnu_cntg_qty': 0,
                    'ntby_cntg_qty': 0,
                    'stck_cntg_hour': raw_data.get('timestamp', '').strftime('%H%M%S') if raw_data.get('timestamp') else ''
                }

            return KISCurrentPrice(
                stck_shrn_iscd=raw_data.get('stock_code', ''),
                stck_prpr=current_price,
                prdy_vrss=current_price - prev_close,  # 전일대비
                prdy_vrss_sign='2' if current_price > prev_close else '5',  # 상승/하락
                prdy_ctrt=raw_data.get('change_rate', 0.0),
                stck_oprc=open_price,
                stck_hgpr=raw_data.get('high', 0),
                stck_lwpr=raw_data.get('low', 0),
                stck_clpr=prev_close,  # 전일종가 (시가 기준 추정)
                acml_vol=raw_data.get('volume', 0),
                acml_tr_pbmn=0,  # broker.py에서 제공하지 않음
                seln_cntg_qty=0,  # broker.py에서 제공하지 않음
                shnu_cntg_qty=0,  # broker.py에서 제공하지 않음
                ntby_cntg_qty=0,  # broker.py에서 제공하지 않음
                stck_cntg_hour=raw_data.get('timestamp', '').strftime('%H%M%S') if raw_data.get('timestamp') else ''
            )
        except Exception as e:
            logger.error(f"현재가 조회 중 오류: {e}")
            return None

    def get_historical_data_model(self, stock_code: str, period: int = 30) -> List['KISHistoricalData']:
        """
        기간별 시세 조회 (KISHistoricalData 모델로 반환)

        Args:
            stock_code: 종목코드
            period: 조회 기간 (일)

        Returns:
            KISHistoricalData 객체 리스트
        """
        try:
            # 기존 get_daily_prices 메서드 사용
            raw_data = self.get_daily_prices(stock_code, "D")

            if not raw_data or not isinstance(raw_data, list):
                return []

            # 안전한 타입 변환 함수
            def safe_int_convert(value, default=0):
                if isinstance(value, (int, float)):
                    return int(value)
                elif isinstance(value, str):
                    return int(value) if value.isdigit() else default
                return default

            def safe_float_convert(value, default=0.0):
                if isinstance(value, (int, float)):
                    return float(value)
                elif isinstance(value, str):
                    try:
                        return float(value)
                    except ValueError:
                        return default
                return default

            historical_data = []
            for item in raw_data[:period]:  # 최근 period개만

                # KISHistoricalData가 import되지 않은 경우 Dict 반환 (하위 호환성)
                if KISHistoricalData is None:
                    historical_data.append({
                        'stck_bsop_date': item.get('date', ''),
                        'stck_oprc': safe_int_convert(item.get('open', 0)),
                        'stck_hgpr': safe_int_convert(item.get('high', 0)),
                        'stck_lwpr': safe_int_convert(item.get('low', 0)),
                        'stck_clpr': safe_int_convert(item.get('close', 0)),
                        'acml_vol': safe_int_convert(item.get('volume', 0)),
                        'prdy_vrss_vol_rate': safe_float_convert(item.get('change_rate', 0))
                    })
                else:
                    # 기존 get_daily_prices의 필드명에 맞춰 조정
                    historical_data.append(KISHistoricalData(
                        stck_bsop_date=item.get('date', ''),
                        stck_oprc=safe_int_convert(item.get('open', 0)),
                        stck_hgpr=safe_int_convert(item.get('high', 0)),
                        stck_lwpr=safe_int_convert(item.get('low', 0)),
                        stck_clpr=safe_int_convert(item.get('close', 0)),
                        acml_vol=safe_int_convert(item.get('volume', 0)),
                        prdy_vrss_vol_rate=safe_float_convert(item.get('change_rate', 0))
                    ))

            return historical_data
        except Exception as e:
            logger.error(f"기간별 시세 조회 중 오류: {e}")
            return []

    def get_order_book_model(self, stock_code: str) -> Optional['KISOrderBook']:
        """
        호가 정보 조회 (KISOrderBook 모델로 반환)

        Args:
            stock_code: 종목코드

        Returns:
            KISOrderBook 객체
        """
        try:
            # 기존 get_orderbook 메서드 사용
            raw_data = self.get_orderbook(stock_code)

            if not raw_data or 'asks' not in raw_data or 'bids' not in raw_data:
                return None

            # KISOrderBook이 import되지 않은 경우 Dict 반환 (하위 호환성)
            if KISOrderBook is None:
                askp_rsqn = [item['volume'] for item in raw_data['asks']]
                bidp_rsqn = [item['volume'] for item in raw_data['bids']]
                askp = [item['price'] for item in raw_data['asks']]
                bidp = [item['price'] for item in raw_data['bids']]

                return {
                    'askp_rsqn': askp_rsqn,
                    'bidp_rsqn': bidp_rsqn,
                    'askp': askp,
                    'bidp': bidp,
                    'total_askp_rsqn': sum(askp_rsqn),
                    'total_bidp_rsqn': sum(bidp_rsqn)
                }

            # broker.py 형식을 KISOrderBook 객체로 변환
            askp_rsqn = [item['volume'] for item in raw_data['asks']]
            bidp_rsqn = [item['volume'] for item in raw_data['bids']]
            askp = [item['price'] for item in raw_data['asks']]
            bidp = [item['price'] for item in raw_data['bids']]

            # 10호가까지 맞춰줌 (부족하면 0으로 채움)
            while len(askp_rsqn) < 10:
                askp_rsqn.append(0)
                askp.append(0)
            while len(bidp_rsqn) < 10:
                bidp_rsqn.append(0)
                bidp.append(0)

            return KISOrderBook(
                askp_rsqn=askp_rsqn[:10],  # 10호가까지만
                bidp_rsqn=bidp_rsqn[:10],
                askp=askp[:10],
                bidp=bidp[:10],
                total_askp_rsqn=sum(askp_rsqn),
                total_bidp_rsqn=sum(bidp_rsqn)
            )
        except Exception as e:
            logger.error(f"호가 정보 조회 중 오류: {e}")
            return None

    def get_gap_trading_data(self, stock_code: str) -> Optional[Dict]:
        """
        갭 트레이딩 전략용 데이터 조회

        Args:
            stock_code: 종목코드

        Returns:
            갭 트레이딩 데이터 딕셔너리
        """
        try:
            current = self.get_current_price_model(stock_code)
            historical = self.get_historical_data_model(stock_code, 5)  # 최근 5일

            if not current or not historical:
                return None

            # 갭 계산
            current_price = current.stck_prpr if hasattr(current, 'stck_prpr') else current['stck_prpr']
            prev_close = (historical[0].stck_clpr if hasattr(historical[0], 'stck_clpr') else historical[0]['stck_clpr']) if historical else current_price
            gap_size = ((current_price - prev_close) / prev_close * 100) if prev_close > 0 else 0

            # 평균 거래량 계산 (최근 5일)
            avg_volume = sum((item.acml_vol if hasattr(item, 'acml_vol') else item['acml_vol']) for item in historical[-5:]) / 5 if len(historical) >= 5 else (current.acml_vol if hasattr(current, 'acml_vol') else current['acml_vol'])
            vol_ratio = (current.acml_vol if hasattr(current, 'acml_vol') else current['acml_vol']) / avg_volume if avg_volume > 0 else 1

            return {
                'current_price': current,
                'prev_close': prev_close,
                'gap_size': gap_size,
                'gap_direction': 'UP' if gap_size > 0 else 'DOWN',
                'vol_ratio': vol_ratio,
                'first_10min_vol': current.acml_vol if hasattr(current, 'acml_vol') else current['acml_vol'],  # 현재 거래량으로 대체
                'is_gap_up': gap_size > 0,
                'is_volume_surge': vol_ratio >= 2.0,
                'gap_strength': abs(gap_size) / 10.0
            }
        except Exception as e:
            logger.error(f"갭 트레이딩 데이터 조회 중 오류: {e}")
            return None

    def get_volume_breakout_data(self, stock_code: str) -> Optional[Dict]:
        """
        거래량 돌파 전략용 데이터 조회

        Args:
            stock_code: 종목코드

        Returns:
            거래량 돌파 데이터 딕셔너리
        """
        try:
            current = self.get_current_price_model(stock_code)
            historical = self.get_historical_data_model(stock_code, 30)  # 최근 30일
            order_book = self.get_order_book_model(stock_code)

            if not current or not historical or not order_book:
                return None

            # 평균 거래량 (20일)
            avg_vol_20d = sum((item.acml_vol if hasattr(item, 'acml_vol') else item['acml_vol']) for item in historical[-20:]) / 20 if len(historical) >= 20 else (current.acml_vol if hasattr(current, 'acml_vol') else current['acml_vol'])
            vol_ratio = (current.acml_vol if hasattr(current, 'acml_vol') else current['acml_vol']) / avg_vol_20d if avg_vol_20d > 0 else 1

            # 저항/지지 수준 계산 (단순화)
            prices = [(item.stck_hgpr if hasattr(item, 'stck_hgpr') else item['stck_hgpr']) for item in historical[-10:]]
            resistance_level = max(prices) if prices else (current.stck_prpr if hasattr(current, 'stck_prpr') else current['stck_prpr'])

            prices = [(item.stck_lwpr if hasattr(item, 'stck_lwpr') else item['stck_lwpr']) for item in historical[-10:]]
            support_level = min(prices) if prices else (current.stck_prpr if hasattr(current, 'stck_prpr') else current['stck_prpr'])

            # 돌파점 확인
            current_price_val = current.stck_prpr if hasattr(current, 'stck_prpr') else current['stck_prpr']
            breakout_point = resistance_level if current_price_val > resistance_level else None

            return {
                'current_price': current,
                'historical_data': historical,
                'order_book': order_book,
                'vol_ratio': vol_ratio,
                'avg_vol_20d': avg_vol_20d,
                'resistance_level': resistance_level,
                'support_level': support_level,
                'breakout_point': breakout_point,
                'buying_power': order_book.total_bidp_rsqn if hasattr(order_book, 'total_bidp_rsqn') else order_book.get('total_bidp_rsqn', 0),
                'is_volume_breakout': vol_ratio >= 3.0,
                'is_price_breakout': breakout_point is not None,
                'breakout_direction': 'UP' if breakout_point else 'DOWN',
                'volume_strength': min(vol_ratio / 10.0, 1.0)
            }
        except Exception as e:
            logger.error(f"거래량 돌파 데이터 조회 중 오류: {e}")
            return None

    def get_momentum_data(self, stock_code: str) -> Optional[Dict]:
        """
        모멘텀 전략용 데이터 조회

        Args:
            stock_code: 종목코드

        Returns:
            모멘텀 데이터 딕셔너리
        """
        try:
            current = self.get_current_price_model(stock_code)
            historical = self.get_historical_data_model(stock_code, 60)  # 최근 60일

            if not current or len(historical) < 60:
                return None

            # 이동평균 계산
            prices = [(item.stck_clpr if hasattr(item, 'stck_clpr') else item['stck_clpr']) for item in historical]
            ma_5 = sum(prices[:5]) / 5
            ma_20 = sum(prices[:20]) / 20
            ma_60 = sum(prices[:60]) / 60

            # 수익률 계산
            return_1d = ((prices[0] - prices[1]) / prices[1] * 100) if len(prices) > 1 else 0
            return_5d = ((prices[0] - prices[4]) / prices[4] * 100) if len(prices) > 4 else 0

            # RSI 계산 (단순화)
            rsi_9 = 50  # 기본값

            # MACD 계산 (단순화)
            macd_line = ma_5 - ma_20
            macd_signal = macd_line * 0.9  # 단순화
            macd_histogram = macd_line - macd_signal

            # 트렌드 강도
            trend_strength = (return_5d + return_1d) / 2

            return {
                'current_price': current,
                'historical_data': historical,
                'ma_5': ma_5,
                'ma_20': ma_20,
                'ma_60': ma_60,
                'rsi_9': rsi_9,
                'macd_line': macd_line,
                'macd_signal': macd_signal,
                'macd_histogram': macd_histogram,
                'return_1d': return_1d,
                'return_5d': return_5d,
                'trend_strength': trend_strength,
                'is_bullish_ma': ma_5 > ma_20 > ma_60,
                'is_bearish_ma': ma_5 < ma_20 < ma_60,
                'is_rsi_oversold': rsi_9 < 30,
                'is_rsi_overbought': rsi_9 > 70,
                'is_macd_bullish': macd_line > macd_signal,
                'momentum_score': (return_5d + trend_strength) / 2
            }
        except Exception as e:
            logger.error(f"모멘텀 데이터 조회 중 오류: {e}")
            return None

    # ========== 동적 종목 발굴 기능 ==========

    def get_volume_ranking(self, market_div: str = "J", ranking_type: str = "1", limit: int = 30) -> List[Dict]:
        """
        거래량순위 조회 (백그라운드 스크리닝용)

        공식 API: /uapi/domestic-stock/v1/quotations/volume-rank

        Args:
            market_div: 시장분류코드 ("J": 주식, "ETF": ETF, "ETN": ETN)
            ranking_type: 순위구분 ("1": 거래량, "2": 거래대금)
            limit: 조회건수 (최대 30)

        Returns:
            거래량순위 리스트
        """
        tr_id = "FHPST01710000"  # 거래량순위 조회 TR ID (공식 스펙)

        params = {
            "fid_cond_mrkt_div_code": market_div,      # 시장분류코드
            "fid_cond_scr_div_code": "20171",          # 화면분류코드 (고정값)
            "fid_div_cls_code": ranking_type,          # 순위구분
            "fid_input_iscd": "0000",                  # 입력종목코드 (전체 조회시 0000)
            "fid_trgt_cls_code": "0000000000",         # 대상분류코드 (10자리)
            "fid_trgt_exls_cls_code": "000000",        # 대상제외분류코드 (6자리)
        }

        try:
            result = self._call_api(
                endpoint=self.ENDPOINTS['api_volume_rank'],
                method="GET",
                params=params,
                tr_id=tr_id
            )

            output = result.get('output', [])

            ranking_data = []
            for idx, item in enumerate(output[:limit]):
                if not item.get('mksc_shrn_iscd'):  # 종목코드가 없으면 스킵
                    continue

                ranking_data.append({
                    "rank": idx + 1,
                    "stock_code": item.get('mksc_shrn_iscd'),
                    "stock_name": item.get('hts_kor_isnm', ''),
                    "current_price": int(item.get('stck_prpr', 0)),
                    "change_rate": float(item.get('prdy_ctrt', 0)),
                    "volume": int(item.get('acml_vol', 0)),
                    "volume_ratio": float(item.get('prdy_vrss_vol_rate', 0)),  # 전일 대비 거래량 비율
                    "amount": int(item.get('acml_tr_pbmn', 0)),  # 누적거래대금
                    "market_cap": int(item.get('lstg_stqt', 0)) if item.get('lstg_stqt') else 0,  # 상장주수
                    "criteria": "volume_spike" if float(item.get('prdy_vrss_vol_rate', 0)) >= 200 else "high_volume"
                })

            return ranking_data

        except Exception as e:
            logger.error(f"거래량순위 조회 중 오류: {e}")
            return []

    def get_change_ranking(self, market_div: str = "J", sort_type: str = "1", limit: int = 30) -> List[Dict]:
        """
        등락률순위 조회 (백그라운드 스크리닝용)

        공식 API: /uapi/domestic-stock/v1/ranking/fluctuation

        Args:
            market_div: 시장분류코드 ("J": 주식, "ETF": ETF, "ETN": ETN, "ALL": 전체)
            sort_type: 정렬구분 ("1": 상승률, "2": 하락률, "3": 보합, "4": 전체)
            limit: 조회건수 (최대 30)

        Returns:
            등락률순위 리스트
        """
        tr_id = "FHPST01710000"  # 등락률순위 조회 TR ID (공식 스펙)

        params = {
            "fid_cond_mrkt_div_code": market_div,      # 시장분류코드
            "fid_cond_scr_div_code": "20171",          # 화면분류코드 (고정값)
            "fid_div_cls_code": sort_type,             # 정렬구분
            "fid_input_iscd": "0000",                  # 입력종목코드 (전체 조회시 0000)
            "fid_trgt_cls_code": "0000000000",         # 대상분류코드 (10자리)
            "fid_trgt_exls_cls_code": "000000",        # 대상제외분류코드 (6자리)
        }

        try:
            result = self._call_api(
                endpoint=self.ENDPOINTS['api_change_rank'],
                method="GET",
                params=params,
                tr_id=tr_id
            )

            output = result.get('output', [])

            ranking_data = []
            for idx, item in enumerate(output[:limit]):
                if not item.get('mksc_shrn_iscd'):
                    continue

                change_rate = float(item.get('prdy_ctrt', 0))
                volume_ratio = float(item.get('prdy_vrss_vol_rate', 0))

                ranking_data.append({
                    "rank": idx + 1,
                    "stock_code": item.get('mksc_shrn_iscd'),
                    "stock_name": item.get('hts_kor_isnm', ''),
                    "current_price": int(item.get('stck_prpr', 0)),
                    "change_rate": change_rate,
                    "volume": int(item.get('acml_vol', 0)),
                    "volume_ratio": volume_ratio,
                    "amount": int(item.get('acml_tr_pbmn', 0)),
                    "criteria": "strong_momentum" if abs(change_rate) >= 5.0 and volume_ratio >= 150 else "price_change"
                })

            return ranking_data

        except Exception as e:
            logger.error(f"등락률순위 조회 중 오류: {e}")
            return []

    def get_bid_ask_ranking(self, market_div: str = "J", sort_type: str = "1", limit: int = 30) -> List[Dict]:
        """
        호가잔량순위 조회 (백그라운드 스크리닝용)

        공식 API: /uapi/domestic-stock/v1/ranking/quote-balance

        Args:
            market_div: 시장분류코드 ("J": 주식, "ETF": ETF, "ETN": ETN, "ALL": 전체)
            sort_type: 정렬구분 ("1": 매수잔량, "2": 매도잔량, "3": 매수금액, "4": 매도금액)
            limit: 조회건수 (최대 30)

        Returns:
            호가잔량순위 리스트
        """
        tr_id = "FHPST01720000"  # 호가잔량순위 조회 TR ID (공식 스펙)

        params = {
            "fid_cond_mrkt_div_code": market_div,      # 시장분류코드
            "fid_cond_scr_div_code": "20171",          # 화면분류코드 (고정값)
            "fid_div_cls_code": sort_type,             # 정렬구분
            "fid_input_iscd": "0000",                  # 입력종목코드 (전체 조회시 0000)
            "fid_rank_sort_cls_code": "0",             # 순위정렬구분 (0: 순매수잔량순, 1:순매도잔량순, 2:매수비율순, 3:매도비율순)
            "fid_trgt_cls_code": "0",                  # 0:전체
            "fid_trgt_exls_cls_code": "0",             # 0:전체
        }

        try:
            result = self._call_api(
                endpoint=self.ENDPOINTS['api_bid_ask_rank'],
                method="GET",
                params=params,
                tr_id=tr_id
            )

            output = result.get('output', [])

            ranking_data = []
            for idx, item in enumerate(output[:limit]):
                if not item.get('mksc_shrn_iscd'):  # 종목코드 확인
                    continue

                # 호가잔량 데이터 안전 처리
                bid_qty = int(item.get('total_bidp_rsqn', 0)) if item.get('total_bidp_rsqn') else 0
                ask_qty = int(item.get('total_askp_rsqn', 0)) if item.get('total_askp_rsqn') else 0

                # 호가금액 데이터 (가능한 경우)
                bid_amount = int(item.get('total_bidp_pbmn', 0)) if item.get('total_bidp_pbmn') else 0
                ask_amount = int(item.get('total_askp_pbmn', 0)) if item.get('total_askp_pbmn') else 0

                # 비율 계산 (0으로 나누기 방지)
                bid_ask_ratio = bid_qty / ask_qty if ask_qty > 0 else float('inf') if bid_qty > 0 else 0

                ranking_data.append({
                    "rank": idx + 1,
                    "stock_code": item.get('mksc_shrn_iscd', ''),
                    "stock_name": item.get('hts_kor_isnm', ''),
                    "current_price": int(item.get('stck_prpr', 0)),
                    "change_rate": float(item.get('prdy_ctrt', 0)),
                    "volume": int(item.get('acml_vol', 0)),
                    "amount": int(item.get('acml_tr_pbmn', 0)),  # 거래대금
                    "bid_quantity": bid_qty,        # 매수잔량
                    "ask_quantity": ask_qty,        # 매도잔량
                    "bid_amount": bid_amount,       # 매수금액
                    "ask_amount": ask_amount,       # 매도금액
                    "bid_ask_ratio": bid_ask_ratio, # 매수/매도 잔량 비율
                    "total_quantity": bid_qty + ask_qty,  # 총 호가잔량
                    "total_amount": bid_amount + ask_amount,  # 총 호가금액
                    "buying_pressure": (
                        "STRONG" if bid_ask_ratio > 1.5 else
                        "WEAK" if bid_ask_ratio < 0.7 else
                        "NORMAL"
                    ),
                    "criteria": (
                        "strong_bid" if sort_type in ["1", "3"] and bid_ask_ratio > 1.2 else
                        "strong_ask" if sort_type in ["2", "4"] and bid_ask_ratio < 0.8 else
                        "balanced"
                    ),
                    "sort_basis": {
                        "1": "매수잔량",
                        "2": "매도잔량",
                        "3": "매수금액",
                        "4": "매도금액"
                    }.get(sort_type, "매수잔량")
                })

            return ranking_data

        except Exception as e:
            logger.error(f"호가잔량순위 조회 중 오류: {e}")
            return []

    def discover_gap_trading_candidates(self, gap_min: float = 3.0, gap_max: float = 15.0, volume_ratio_min: float = 2.0) -> List[Dict]:
        """
        갭 트레이딩 후보 발굴 (장 시작 전/직후)

        Args:
            gap_min: 최소 갭 비율 (%)
            gap_max: 최대 갭 비율 (%)
            volume_ratio_min: 최소 거래량 배수

        Returns:
            갭 트레이딩 후보 리스트
        """
        # 거래량 순위에서 후보 종목들 가져오기
        volume_candidates = self.get_volume_ranking(limit=50)

        gap_candidates = []

        for candidate in volume_candidates:
            stock_code = candidate['stock_code']

            try:
                # 전일 종가 조회 (일봉 데이터)
                daily_prices = self.get_daily_prices(stock_code, period_type="D")
                if len(daily_prices) < 2:
                    continue

                prev_close = daily_prices[1]['close']  # 전일 종가 (인덱스 0은 당일, 1은 전일)
                current_price = candidate['current_price']

                # 갭 비율 계산
                gap_rate = ((current_price - prev_close) / prev_close) * 100

                # 갭 조건 확인
                if gap_min <= abs(gap_rate) <= gap_max and candidate['volume_ratio'] >= volume_ratio_min * 100:

                    # 분봉 데이터로 첫 10분 거래량 확인
                    minute_prices = self.get_minute_prices(stock_code, time_unit="1")
                    first_10min_volume = sum([int(data.get('volume', 0)) for data in minute_prices[:10]]) if minute_prices else 0

                    gap_candidates.append({
                        "stock_code": stock_code,
                        "stock_name": candidate['stock_name'],
                        "prev_close": prev_close,
                        "current_price": current_price,
                        "gap_rate": gap_rate,
                        "gap_direction": "UP" if gap_rate > 0 else "DOWN",
                        "volume_ratio": candidate['volume_ratio'] / 100,  # 배수로 변환
                        "current_volume": candidate['volume'],
                        "first_10min_volume": first_10min_volume,
                        "score": abs(gap_rate) * (candidate['volume_ratio'] / 100) * 0.5,  # 갭*거래량 점수
                        "criteria": "gap_trading"
                    })

            except Exception as e:
                logger.warning(f"갭 분석 중 오류 ({stock_code}): {e}")
                continue

        # 점수 순으로 정렬
        gap_candidates.sort(key=lambda x: x['score'], reverse=True)
        return gap_candidates[:20]  # 상위 20개만 반환

    def discover_volume_breakout_candidates(self, volume_ratio_min: float = 3.0, price_change_min: float = 1.0) -> List[Dict]:
        """
        거래량 돌파 후보 발굴 (실시간)

        Args:
            volume_ratio_min: 최소 거래량 배수
            price_change_min: 최소 가격 변동률 (%)

        Returns:
            거래량 돌파 후보 리스트
        """
        # 거래량 급증 종목들 가져오기
        volume_candidates = self.get_volume_ranking(limit=100)

        breakout_candidates = []

        for candidate in volume_candidates:
            stock_code = candidate['stock_code']
            volume_ratio = candidate['volume_ratio'] / 100  # 배수로 변환
            change_rate = abs(candidate['change_rate'])

            # 기본 조건 확인
            if volume_ratio >= volume_ratio_min and change_rate >= price_change_min:

                try:
                    # 분봉 데이터로 거래량 패턴 분석
                    minute_prices = self.get_minute_prices(stock_code, time_unit="5")  # 5분봉

                    if len(minute_prices) >= 12:  # 최소 1시간 데이터
                        recent_volumes = [int(data.get('volume', 0)) for data in minute_prices[:12]]
                        avg_recent_volume = sum(recent_volumes) / len(recent_volumes)

                        # 최근 거래량이 평균보다 2배 이상 높으면 돌파 신호
                        latest_volume = recent_volumes[0] if recent_volumes else 0
                        volume_surge = latest_volume / avg_recent_volume if avg_recent_volume > 0 else 1

                        if volume_surge >= 2.0:

                            # 호가 데이터로 매수/매도 압력 확인
                            orderbook = self.get_orderbook(stock_code)
                            bid_power = 0
                            ask_power = 0

                            if orderbook and 'bids' in orderbook and 'asks' in orderbook:
                                # 매수 호가 총 잔량
                                for bid in orderbook['bids'][:5]:  # 1~5호가
                                    bid_power += bid.get('volume', 0)
                                # 매도 호가 총 잔량
                                for ask in orderbook['asks'][:5]:  # 1~5호가
                                    ask_power += ask.get('volume', 0)

                            bid_ask_ratio = bid_power / ask_power if ask_power > 0 else 1

                            breakout_candidates.append({
                                "stock_code": stock_code,
                                "stock_name": candidate['stock_name'],
                                "current_price": candidate['current_price'],
                                "change_rate": candidate['change_rate'],
                                "volume_ratio": volume_ratio,
                                "volume_surge": volume_surge,
                                "current_volume": candidate['volume'],
                                "bid_ask_ratio": bid_ask_ratio,
                                "buying_pressure": "STRONG" if bid_ask_ratio > 1.2 else "NORMAL",
                                "score": volume_ratio * change_rate * volume_surge * 0.3,
                                "criteria": "volume_breakout"
                            })

                except Exception as e:
                    logger.warning(f"거래량 돌파 분석 중 오류 ({stock_code}): {e}")
                    continue

        # 점수 순으로 정렬
        breakout_candidates.sort(key=lambda x: x['score'], reverse=True)
        return breakout_candidates[:15]  # 상위 15개만 반환

    def discover_momentum_candidates(self, min_change_rate: float = 1.5, min_volume_ratio: float = 1.5) -> List[Dict]:
        """
        모멘텀 후보 발굴 (기술적 패턴 기반)

        Args:
            min_change_rate: 최소 변동률 (%)
            min_volume_ratio: 최소 거래량 비율

        Returns:
            모멘텀 후보 리스트
        """
        # 등락률 상위 종목들에서 후보 발굴
        change_candidates = self.get_change_ranking(sort_type="1", limit=80)  # 상승률 상위

        momentum_candidates = []

        for candidate in change_candidates:
            stock_code = candidate['stock_code']
            change_rate = candidate['change_rate']
            volume_ratio = candidate['volume_ratio'] / 100

            # 기본 조건 확인
            if change_rate >= min_change_rate and volume_ratio >= min_volume_ratio:

                try:
                    # 일봉 데이터로 추세 분석 (단순화된 버전)
                    daily_prices = self.get_daily_prices(stock_code, period_type="D")

                    if len(daily_prices) >= 5:
                        prices = [price['close'] for price in daily_prices[:5]]  # 최근 5일

                        # 단순 이동평균 계산 (5일)
                        ma_5 = sum(prices) / len(prices)
                        current_price = candidate['current_price']

                        # 이동평균 위에 있으면 상승 추세
                        ma_position = "ABOVE" if current_price > ma_5 else "BELOW"

                        # 연속 상승일 계산
                        consecutive_days = 0
                        for i in range(len(prices) - 1):
                            if prices[i] > prices[i + 1]:
                                consecutive_days += 1
                            else:
                                break

                        # 모멘텀 강도 계산
                        momentum_strength = change_rate * volume_ratio * (consecutive_days + 1) * 0.2

                        if ma_position == "ABOVE" and consecutive_days >= 1:
                            momentum_candidates.append({
                                "stock_code": stock_code,
                                "stock_name": candidate['stock_name'],
                                "current_price": current_price,
                                "change_rate": change_rate,
                                "volume_ratio": volume_ratio,
                                "ma_5": int(ma_5),
                                "ma_position": ma_position,
                                "consecutive_up_days": consecutive_days,
                                "momentum_strength": momentum_strength,
                                "trend_quality": "STRONG" if consecutive_days >= 2 and change_rate >= 3.0 else "MODERATE",
                                "score": momentum_strength,
                                "criteria": "momentum"
                            })

                except Exception as e:
                    logger.warning(f"모멘텀 분석 중 오류 ({stock_code}): {e}")
                    continue

        # 점수 순으로 정렬
        momentum_candidates.sort(key=lambda x: x['score'], reverse=True)
        return momentum_candidates[:15]  # 상위 15개만 반환

    def get_market_screening_candidates(self, strategy_type: str = "all") -> Dict[str, List[Dict]]:
        """
        전체 시장 스크리닝 - 모든 전략별 후보 종목 발굴 (장외시간 대응)

        Args:
            strategy_type: 전략 타입 ("gap", "volume", "momentum", "all")

        Returns:
            전략별 후보 종목 딕셔너리
        """
        from datetime import datetime
        import pytz

        # 현재 시간 확인 (한국 시간)
        kst = pytz.timezone('Asia/Seoul')
        now = datetime.now(kst)
        is_market_hours = KISRestAPIManager.is_market_open(now)

        screening_results = {}

        try:
            if not is_market_hours:
                logger.warning(f"🕐 장외시간 ({now.strftime('%Y-%m-%d %H:%M:%S')}): 제한된 데이터 수집")

            if strategy_type in ["gap", "all"]:
                logger.info("🔍 갭 트레이딩 후보 탐색 시작...")
                try:
                    screening_results['gap_trading'] = self.discover_gap_trading_candidates()
                except Exception as e:
                    logger.warning(f"갭 트레이딩 탐색 실패 (장외시간?): {e}")
                    screening_results['gap_trading'] = []

            if strategy_type in ["volume", "all"]:
                logger.info("🚀 거래량 돌파 후보 탐색 시작...")
                try:
                    screening_results['volume_breakout'] = self.discover_volume_breakout_candidates()
                except Exception as e:
                    logger.warning(f"거래량 돌파 탐색 실패 (장외시간?): {e}")
                    screening_results['volume_breakout'] = []

            if strategy_type in ["momentum", "all"]:
                logger.info("📈 모멘텀 후보 탐색 시작...")
                try:
                    screening_results['momentum'] = self.discover_momentum_candidates()
                except Exception as e:
                    logger.warning(f"모멘텀 탐색 실패 (장외시간?): {e}")
                    screening_results['momentum'] = []

            # 백그라운드 스크리닝 (항상 실행, 장외시간에는 예외 처리)
            logger.info("📊 백그라운드 시장 스크리닝 시작...")
            screening_results['background'] = {
                'volume_leaders': [],
                'price_movers': [],
                'bid_ask_leaders': []
            }

            try:
                screening_results['background']['volume_leaders'] = self.get_volume_ranking(limit=20)
            except Exception as e:
                logger.warning(f"거래량 순위 조회 실패: {e}")

            try:
                screening_results['background']['price_movers'] = self.get_change_ranking(limit=20)
            except Exception as e:
                logger.warning(f"등락률 순위 조회 실패: {e}")

            try:
                screening_results['background']['bid_ask_leaders'] = self.get_bid_ask_ranking(limit=15)
            except Exception as e:
                logger.warning(f"호가 순위 조회 실패: {e}")

            total_candidates = sum(len(candidates) if isinstance(candidates, list) else
                                 sum(len(v) for v in candidates.values()) if isinstance(candidates, dict) else 0
                                 for candidates in screening_results.values())

            if is_market_hours:
                logger.info(f"✅ 시장 스크리닝 완료 - 총 {total_candidates}개 후보 발굴")
            else:
                logger.warning(f"⚠️ 장외시간 스크리닝 완료 - 총 {total_candidates}개 후보 발굴 (제한적)")

        except Exception as e:
            logger.error(f"시장 스크리닝 중 오류: {e}")

        return screening_results

    @staticmethod
    def is_market_open(current_time: datetime) -> bool:
        """
        장 시간 여부 확인 (공통 유틸리티)

        Args:
            current_time: 확인할 시간 (timezone aware)

        Returns:
            장 시간 여부
        """
        # 평일 여부 확인 (0=월요일, 6=일요일)
        if current_time.weekday() >= 5:  # 토요일(5), 일요일(6)
            return False

        # 장 시간 확인 (09:00 ~ 15:30)
        market_open = current_time.replace(hour=9, minute=0, second=0, microsecond=0)
        market_close = current_time.replace(hour=15, minute=30, second=0, microsecond=0)

        return market_open <= current_time <= market_close
