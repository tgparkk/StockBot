"""
KIS API 인증/토큰 관리 모듈 (공식 문서 기반)
"""
import os
import json
import time
import yaml
import requests
from datetime import datetime
from typing import Dict, Optional, NamedTuple
from utils.logger import setup_logger

# 설정 import (settings.py에서 .env 파일을 읽어서 제공)
from config.settings import (
    KIS_BASE_URL, APP_KEY, SECRET_KEY,
    ACCOUNT_NUMBER, HTS_ID
)

logger = setup_logger(__name__)

# 토큰 파일 경로
TOKEN_FILE_PATH = os.path.join(os.path.abspath(os.getcwd()), "token_info.json")

# KIS 환경 설정 구조체
class KISEnv(NamedTuple):
    my_app: str      # 앱키
    my_sec: str      # 앱시크리트
    my_acct: str     # 계좌번호 (8자리)
    my_prod: str     # 계좌상품코드 (2자리)
    my_token: str    # 토큰
    my_url: str      # API URL

# 전역 변수
_TRENV: Optional[KISEnv] = None
_last_auth_time = datetime.now()
_autoReAuth = False
_DEBUG = False
_isPaper = False

# API 호출 속도 제어를 위한 전역 변수들 추가
_last_api_call_time = None
_min_api_interval = 0.06  # 최소 60ms 간격 (초당 16-17회로 안전하게 설정, KIS 제한: 1초당 20건)
_max_retries = 3  # 최대 재시도 횟수
_retry_delay_base = 1.0  # 기본 재시도 지연 시간(초) - 줄임

# 기본 헤더
_base_headers = {
    "Content-Type": "application/json",
    "Accept": "text/plain",
    "charset": "UTF-8",
    'User-Agent': 'StockBot/1.0'
}


def save_token(my_token: str, my_expired: str) -> None:
    """토큰 저장"""
    valid_date = datetime.strptime(my_expired, '%Y-%m-%d %H:%M:%S')
    logger.debug(f'토큰 저장: {valid_date}')

    with open(TOKEN_FILE_PATH, 'w', encoding='utf-8') as f:
        f.write(f'token: {my_token}\n')
        f.write(f'valid-date: {valid_date}\n')


def read_token() -> Optional[str]:
    """토큰 읽기"""
    try:
        with open(TOKEN_FILE_PATH, encoding='UTF-8') as f:
            tkg_tmp = yaml.load(f, Loader=yaml.FullLoader)

        # 토큰 만료일시
        exp_dt = datetime.strftime(tkg_tmp['valid-date'], '%Y-%m-%d %H:%M:%S')
        # 현재일시
        now_dt = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

        # 만료일시 > 현재일시 인 경우 기존 토큰 리턴
        if exp_dt > now_dt:
            return tkg_tmp['token']
        else:
            logger.debug(f'토큰 만료: {tkg_tmp["valid-date"]}')
            return None

    except Exception as e:
        logger.debug(f'토큰 읽기 오류: {e}')
        return None


def _getBaseHeader() -> Dict:
    """기본 헤더 반환"""
    if _autoReAuth:
        reAuth()
    return _base_headers.copy()


def _setTRENV(cfg: Dict) -> None:
    """KIS 환경 설정"""
    global _TRENV
    _TRENV = KISEnv(
        my_app=cfg['my_app'],
        my_sec=cfg['my_sec'],
        my_acct=cfg['my_acct'],
        my_prod=cfg['my_prod'],
        my_token=cfg['my_token'],
        my_url=cfg['my_url']
    )


def isPaperTrading() -> bool:
    """모의투자 여부"""
    return _isPaper


def changeTREnv(token_key: str, svr: str = 'prod', product: str = '01') -> None:
    """환경 변경"""
    global _isPaper

    cfg = {}

    # settings.py에서 설정 로드
    if svr == 'prod':  # 실전투자
        cfg['my_app'] = APP_KEY
        cfg['my_sec'] = SECRET_KEY
        cfg['my_url'] = KIS_BASE_URL
        _isPaper = False
    elif svr == 'vps':  # 모의투자
        cfg['my_app'] = APP_KEY  # 모의투자도 동일한 키 사용
        cfg['my_sec'] = SECRET_KEY
        cfg['my_url'] = 'https://openapivts.koreainvestment.com:29443'  # 모의투자 URL
        _isPaper = True

    # 계좌번호 설정
    if ACCOUNT_NUMBER and len(ACCOUNT_NUMBER) >= 10:
        cfg['my_acct'] = ACCOUNT_NUMBER[:8]  # 앞 8자리
        cfg['my_prod'] = ACCOUNT_NUMBER[8:10]  # 뒤 2자리
    else:
        cfg['my_acct'] = ACCOUNT_NUMBER or ''
        cfg['my_prod'] = product

    cfg['my_token'] = token_key

    _setTRENV(cfg)


def _getResultObject(json_data: Dict):
    """결과 객체 생성"""
    from collections import namedtuple
    _tc_ = namedtuple('res', json_data.keys())
    return _tc_(**json_data)


def auth(svr: str = 'prod', product: str = '01') -> bool:
    """토큰 발급"""
    global _last_auth_time

    # 기존 토큰 확인
    saved_token = read_token()

    if saved_token is None:
        # 새 토큰 발급
        p = {
            "grant_type": "client_credentials",
            "appkey": APP_KEY,  # 실전/모의 동일한 키 사용
            "appsecret": SECRET_KEY
        }

        url = KIS_BASE_URL
        #if svr == 'vps':
        #    url = 'https://openapivts.koreainvestment.com:29443'  # 모의투자 URL

        url += '/oauth2/tokenP'

        try:
            res = requests.post(url, data=json.dumps(p), headers=_getBaseHeader())

            if res.status_code == 200:
                result = _getResultObject(res.json())
                my_token = result.access_token
                my_expired = result.access_token_token_expired
                save_token(my_token, my_expired)
                logger.info('토큰 발급 완료')
            else:
                logger.error('토큰 발급 실패!')
                return False

        except Exception as e:
            logger.error(f'토큰 발급 오류: {e}')
            return False
    else:
        my_token = saved_token
        logger.debug('기존 토큰 사용')

    # 환경 설정
    changeTREnv(f"Bearer {my_token}", svr, product)

    # 헤더 업데이트
    if _TRENV:
        _base_headers["authorization"] = _TRENV.my_token
        _base_headers["appkey"] = _TRENV.my_app
        _base_headers["appsecret"] = _TRENV.my_sec
    else:
        logger.error("_TRENV가 설정되지 않았습니다")

    _last_auth_time = datetime.now()

    if _DEBUG:
        logger.debug(f'[{_last_auth_time}] 인증 완료!')

    return True


def reAuth(svr: str = 'prod', product: str = '01') -> None:
    """토큰 재발급"""
    n2 = datetime.now()
    if (n2 - _last_auth_time).seconds >= 86400:  # 24시간
        auth(svr, product)


def getTREnv() -> Optional[KISEnv]:
    """환경 정보 반환"""
    return _TRENV


def set_order_hash_key(headers: Dict, params: Dict) -> None:
    """주문 해시키 설정"""
    if not _TRENV:
        return

    url = f"{_TRENV.my_url}/uapi/hashkey"

    try:
        res = requests.post(url, data=json.dumps(params), headers=headers)
        if res.status_code == 200:
            headers['hashkey'] = _getResultObject(res.json()).HASH
    except Exception as e:
        logger.error(f"해시키 발급 오류: {e}")


class APIResp:
    """API 응답 처리 클래스"""

    def __init__(self, resp: requests.Response):
        self._rescode = resp.status_code
        self._resp = resp
        self._header = self._setHeader()
        self._body = self._setBody()
        self._err_code = self._body.msg_cd if hasattr(self._body, 'msg_cd') else ''
        self._err_message = self._body.msg1 if hasattr(self._body, 'msg1') else ''

    def getResCode(self) -> int:
        return self._rescode

    def _setHeader(self):
        from collections import namedtuple
        fld = {}
        for x in self._resp.headers.keys():
            if x.islower():
                fld[x] = self._resp.headers.get(x)
        _th_ = namedtuple('header', fld.keys())
        return _th_(**fld)

    def _setBody(self):
        from collections import namedtuple
        try:
            body_data = self._resp.json()
            _tb_ = namedtuple('body', body_data.keys())
            return _tb_(**body_data)
        except:
            # JSON 파싱 실패시 빈 객체 반환
            _tb_ = namedtuple('body', ['rt_cd', 'msg_cd', 'msg1'])
            return _tb_(rt_cd='1', msg_cd='ERROR', msg1='JSON 파싱 실패')

    def getHeader(self):
        return self._header

    def getBody(self):
        return self._body

    def getResponse(self):
        return self._resp

    def isOK(self) -> bool:
        try:
            return self.getBody().rt_cd == '0'
        except:
            return False

    def getErrorCode(self) -> str:
        return self._err_code

    def getErrorMessage(self) -> str:
        return self._err_message

    def printError(self, url: str) -> None:
        logger.error(f'API 오류: {self.getResCode()} - {url}')
        logger.error(f'rt_cd: {self.getBody().rt_cd}, msg_cd: {self.getErrorCode()}, msg1: {self.getErrorMessage()}')


def _url_fetch(api_url: str, ptr_id: str, tr_cont: str, params: Dict,
               appendHeaders: Optional[Dict] = None, postFlag: bool = False,
               hashFlag: bool = True) -> Optional[APIResp]:
    """API 호출 공통 함수 (속도 제한 및 재시도 로직 포함)"""
    if not _TRENV:
        logger.error("인증되지 않음. auth() 호출 필요")
        return None

    url = f"{_TRENV.my_url}{api_url}"
    
    # TR ID 설정 (모의투자용 변환)
    tr_id = ptr_id
    if ptr_id[0] in ('T', 'J', 'C'):
        if isPaperTrading():
            tr_id = 'V' + ptr_id[1:]

    # 재시도 로직
    for attempt in range(_max_retries + 1):
        try:
            # API 호출 속도 제한 적용
            _wait_for_api_limit()
            
            # 헤더 설정
            headers = _getBaseHeader()
            headers["tr_id"] = tr_id
            headers["custtype"] = "P"  # 개인
            headers["tr_cont"] = tr_cont

            # 추가 헤더
            if appendHeaders:
                headers.update(appendHeaders)

            if _DEBUG:
                logger.debug(f"API 호출 ({attempt + 1}/{_max_retries + 1}): {url}, TR: {tr_id}")

            # API 호출
            if postFlag:
                if hashFlag:
                    set_order_hash_key(headers, params)
                res = requests.post(url, headers=headers, data=json.dumps(params))
            else:
                res = requests.get(url, headers=headers, params=params)

            # 응답 처리
            if res.status_code == 200:
                ar = APIResp(res)
                if ar.isOK():
                    if _DEBUG:
                        logger.debug(f"API 응답 성공: {tr_id}")
                    return ar
                else:
                    # API 응답은 200이지만 비즈니스 오류
                    if ar.getErrorCode() == 'EGW00201':  # 속도 제한 오류
                        if attempt < _max_retries:
                            wait_time = _retry_delay_base * (2 ** attempt)  # 지수 백오프
                            logger.warning(f"속도 제한 오류 발생. {wait_time}초 후 재시도 ({attempt + 1}/{_max_retries + 1})")
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"API 오류: {res.status_code} - {ar.getErrorMessage()}")
                            return ar
                    else:
                        # 다른 비즈니스 오류는 즉시 반환
                        logger.error(f"API 비즈니스 오류: {ar.getErrorCode()} - {ar.getErrorMessage()}")
                        return ar
            else:
                # HTTP 오류
                if res.status_code == 500 and _is_rate_limit_error(res.text):
                    if attempt < _max_retries:
                        wait_time = _retry_delay_base * (2 ** attempt)  # 지수 백오프
                        logger.warning(f"HTTP 500 속도 제한 오류. {wait_time}초 후 재시도 ({attempt + 1}/{_max_retries + 1})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"API 오류: {res.status_code} - {res.text}")
                        return None
                else:
                    logger.error(f"API 오류: {res.status_code} - {res.text}")
                    return None

        except Exception as e:
            if attempt < _max_retries:
                wait_time = _retry_delay_base * (2 ** attempt)
                logger.warning(f"API 호출 예외 발생. {wait_time}초 후 재시도 ({attempt + 1}/{_max_retries + 1}): {e}")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"API 호출 오류: {e}")
                return None

    logger.error(f"API 호출 최대 재시도 횟수 초과: {tr_id}")
    return None


def _wait_for_api_limit():
    """API 호출 속도 제한을 위한 대기"""
    global _last_api_call_time
    
    current_time = time.time()
    
    if _last_api_call_time is not None:
        elapsed = current_time - _last_api_call_time
        if elapsed < _min_api_interval:
            wait_time = _min_api_interval - elapsed
            if _DEBUG:
                logger.debug(f"API 속도 제한: {wait_time:.3f}초 대기 (이전 호출로부터 {elapsed:.3f}초 경과)")
            time.sleep(wait_time)
    
    _last_api_call_time = time.time()


def _is_rate_limit_error(response_text: str) -> bool:
    """응답이 속도 제한 오류인지 확인"""
    try:
        response_data = json.loads(response_text)
        return (response_data.get('msg_cd') == 'EGW00201' or 
                '초당 거래건수를 초과' in response_data.get('msg1', ''))
    except:
        return False


def set_api_rate_limit(interval_seconds: float = 0.35, max_retries: int = 3, retry_delay: float = 2.0):
    """API 호출 속도 제한 설정을 동적으로 변경"""
    global _min_api_interval, _max_retries, _retry_delay_base
    
    _min_api_interval = interval_seconds
    _max_retries = max_retries
    _retry_delay_base = retry_delay
    
    logger.info(f"API 속도 제한 설정 변경: 간격={interval_seconds}초, 최대재시도={max_retries}회, 재시도지연={retry_delay}초")


def get_api_rate_limit_info():
    """현재 API 속도 제한 설정 정보 반환"""
    return {
        'min_interval': _min_api_interval,
        'max_retries': _max_retries,
        'retry_delay_base': _retry_delay_base
    }
