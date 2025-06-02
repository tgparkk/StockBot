"""
KIS API 시세 조회 관련 함수 (공식 문서 기반)
"""
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from utils.logger import setup_logger
from . import kis_auth as kis

logger = setup_logger(__name__)


def get_inquire_price(div_code: str = "J", itm_no: str = "", tr_cont: str = "",
                      FK100: str = "", NK100: str = "") -> Optional[pd.DataFrame]:
    """주식현재가 시세"""
    url = '/uapi/domestic-stock/v1/quotations/inquire-price'
    tr_id = "FHKST01010100"  # 주식현재가 시세

    params = {
        "FID_COND_MRKT_DIV_CODE": div_code,     # J:주식/ETF/ETN, W:ELW
        "FID_INPUT_ISCD": itm_no                # 종목번호(6자리)
    }

    res = kis._url_fetch(url, tr_id, tr_cont, params)

    if res and res.isOK():
        body = res.getBody()
        current_data = pd.DataFrame(getattr(body, 'output', []), index=[0])
        return current_data
    else:
        logger.error("주식현재가 조회 실패")
        return None


def get_inquire_ccnl(div_code: str = "J", itm_no: str = "", tr_cont: str = "",
                     FK100: str = "", NK100: str = "") -> Optional[pd.DataFrame]:
    """주식현재가 체결 (최근 30건)"""
    url = '/uapi/domestic-stock/v1/quotations/inquire-ccnl'
    tr_id = "FHKST01010300"  # 주식현재가 체결

    params = {
        "FID_COND_MRKT_DIV_CODE": div_code,     # J:주식/ETF/ETN, W:ELW
        "FID_INPUT_ISCD": itm_no                # 종목번호(6자리)
    }

    res = kis._url_fetch(url, tr_id, tr_cont, params)

    if res and res.isOK():
        body = res.getBody()
        current_data = pd.DataFrame(getattr(body, 'output', []))
        return current_data
    else:
        logger.error("주식현재가 체결 조회 실패")
        return None


def get_inquire_daily_price(div_code: str = "J", itm_no: str = "", period_code: str = "D",
                            adj_prc_code: str = "1", tr_cont: str = "",
                            FK100: str = "", NK100: str = "") -> Optional[pd.DataFrame]:
    """주식현재가 일자별 (최근 30일)"""
    url = '/uapi/domestic-stock/v1/quotations/inquire-daily-price'
    tr_id = "FHKST01010400"  # 주식현재가 일자별

    params = {
        "FID_COND_MRKT_DIV_CODE": div_code,     # J:주식/ETF/ETN, W:ELW
        "FID_INPUT_ISCD": itm_no,               # 종목번호(6자리)
        "FID_PERIOD_DIV_CODE": period_code,     # D:일, W:주, M:월
        "FID_ORG_ADJ_PRC": adj_prc_code         # 0:수정주가반영, 1:수정주가미반영
    }

    res = kis._url_fetch(url, tr_id, tr_cont, params)

    if res and res.isOK():
        body = res.getBody()
        current_data = pd.DataFrame(getattr(body, 'output', []))
        return current_data
    else:
        logger.error("주식현재가 일자별 조회 실패")
        return None


def get_inquire_asking_price_exp_ccn(output_dv: str = '1', div_code: str = "J", itm_no: str = "",
                                      tr_cont: str = "", FK100: str = "", NK100: str = "") -> Optional[pd.DataFrame]:
    """주식현재가 호가/예상체결"""
    url = '/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn'
    tr_id = "FHKST01010200"  # 주식현재가 호가 예상체결

    params = {
        "FID_COND_MRKT_DIV_CODE": div_code,     # J:주식/ETF/ETN, W:ELW
        "FID_INPUT_ISCD": itm_no                # 종목번호(6자리)
    }

    res = kis._url_fetch(url, tr_id, tr_cont, params)

    if res and res.isOK():
        body = res.getBody()
        if output_dv == "1":
            current_data = pd.DataFrame(getattr(body, 'output1', []), index=[0])  # 호가조회
        else:
            current_data = pd.DataFrame(getattr(body, 'output2', []), index=[0])  # 예상체결가조회
        return current_data
    else:
        logger.error("주식현재가 호가/예상체결 조회 실패")
        return None


def get_inquire_daily_itemchartprice(output_dv: str = "1", div_code: str = "J", itm_no: str = "",
                                     inqr_strt_dt: Optional[str] = None, inqr_end_dt: Optional[str] = None,
                                     period_code: str = "D", adj_prc: str = "1", tr_cont: str = "",
                                     FK100: str = "", NK100: str = "") -> Optional[pd.DataFrame]:
    """국내주식기간별시세(일/주/월/년)"""
    url = '/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice'
    tr_id = "FHKST03010100"  # 국내주식기간별시세

    if inqr_strt_dt is None:
        inqr_strt_dt = (datetime.now() - timedelta(days=100)).strftime("%Y%m%d")
    if inqr_end_dt is None:
        inqr_end_dt = datetime.today().strftime("%Y%m%d")

    params = {
        "FID_COND_MRKT_DIV_CODE": div_code,     # J:주식/ETF/ETN, W:ELW
        "FID_INPUT_ISCD": itm_no,               # 종목번호(6자리)
        "FID_INPUT_DATE_1": inqr_strt_dt,       # 조회시작일자
        "FID_INPUT_DATE_2": inqr_end_dt,        # 조회종료일자
        "FID_PERIOD_DIV_CODE": period_code,     # D:일봉, W:주봉, M:월봉, Y:년봉
        "FID_ORG_ADJ_PRC": adj_prc              # 0:수정주가, 1:원주가
    }

    res = kis._url_fetch(url, tr_id, tr_cont, params)

    if res and res.isOK():
        body = res.getBody()
        if output_dv == "1":
            current_data = pd.DataFrame(getattr(body, 'output1', []), index=[0])
        else:
            current_data = pd.DataFrame(getattr(body, 'output2', []))
        return current_data
    else:
        logger.error("국내주식기간별시세 조회 실패")
        return None


def get_inquire_time_itemconclusion(output_dv: str = "1", div_code: str = "J", itm_no: str = "",
                                     inqr_hour: Optional[str] = None, tr_cont: str = "",
                                     FK100: str = "", NK100: str = "") -> Optional[pd.DataFrame]:
    """주식현재가 당일시간대별체결"""
    url = '/uapi/domestic-stock/v1/quotations/inquire-time-itemconclusion'
    tr_id = "FHPST01060000"  # 주식현재가 당일시간대별체결

    if inqr_hour is None:
        now = datetime.now()
        inqr_hour = f"{now.hour:02d}{now.minute:02d}{now.second:02d}"

    params = {
        "FID_COND_MRKT_DIV_CODE": div_code,     # J:주식/ETF/ETN, W:ELW
        "FID_INPUT_ISCD": itm_no,               # 종목번호(6자리)
        "FID_INPUT_HOUR_1": inqr_hour           # 기준시간(HHMMSS)
    }

    res = kis._url_fetch(url, tr_id, tr_cont, params)

    if res and res.isOK():
        body = res.getBody()
        if output_dv == "1":
            current_data = pd.DataFrame(getattr(body, 'output1', []), index=[0])
        else:
            current_data = pd.DataFrame(getattr(body, 'output2', []))
        return current_data
    else:
        logger.error("주식현재가 당일시간대별체결 조회 실패")
        return None


def get_inquire_daily_price_2(div_code: str = "J", itm_no: str = "", tr_cont: str = "",
                               FK100: str = "", NK100: str = "") -> Optional[pd.DataFrame]:
    """주식현재가 시세2"""
    url = '/uapi/domestic-stock/v1/quotations/inquire-price-2'
    tr_id = "FHPST01010000"  # 주식현재가 시세2

    params = {
        "FID_COND_MRKT_DIV_CODE": div_code,     # J:주식/ETF/ETN, W:ELW
        "FID_INPUT_ISCD": itm_no                # 종목번호(6자리)
    }

    res = kis._url_fetch(url, tr_id, tr_cont, params)

    if res and res.isOK():
        body = res.getBody()
        current_data = pd.DataFrame(getattr(body, 'output', []), index=[0])
        return current_data
    else:
        logger.error("주식현재가 시세2 조회 실패")
        return None


# =============================================================================
# 시장 스크리닝용 순위 조회 API들
# =============================================================================

def get_volume_power_rank(fid_cond_mrkt_div_code: str = "J",
                         fid_cond_scr_div_code: str = "20168",
                         fid_input_iscd: str = "0000",
                         fid_div_cls_code: str = "1",
                         fid_input_price_1: str = "",
                         fid_input_price_2: str = "",
                         fid_vol_cnt: str = "",
                         fid_trgt_cls_code: str = "0",
                         fid_trgt_exls_cls_code: str = "0",
                         tr_cont: str = "") -> Optional[pd.DataFrame]:
    """
    체결강도 상위 조회 (TR: FHPST01680000)

    Args:
        fid_cond_mrkt_div_code: 조건 시장 분류 코드 (J: 주식)
        fid_cond_scr_div_code: 조건 화면 분류 코드 (20168)
        fid_input_iscd: 입력 종목코드 (0000:전체, 0001:거래소, 1001:코스닥, 2001:코스피200)
        fid_div_cls_code: 분류 구분 코드 (0:전체, 1:보통주, 2:우선주)
        fid_input_price_1: 입력 가격1 (가격 ~)
        fid_input_price_2: 입력 가격2 (~ 가격)
        fid_vol_cnt: 거래량 수 (거래량 ~)
        fid_trgt_cls_code: 대상 구분 코드 (0:전체)
        fid_trgt_exls_cls_code: 대상 제외 구분 코드 (0:전체)
        tr_cont: 연속 거래 여부

    Returns:
        체결강도 상위 종목 데이터 (최대 30건)
    """
    url = '/uapi/domestic-stock/v1/ranking/volume-power'
    tr_id = "FHPST01680000"  # 체결강도 상위

    params = {
        "fid_trgt_exls_cls_code": fid_trgt_exls_cls_code,
        "fid_cond_mrkt_div_code": fid_cond_mrkt_div_code,
        "fid_cond_scr_div_code": fid_cond_scr_div_code,
        "fid_input_iscd": fid_input_iscd,
        "fid_div_cls_code": fid_div_cls_code,
        "fid_input_price_1": fid_input_price_1,
        "fid_input_price_2": fid_input_price_2,
        "fid_vol_cnt": fid_vol_cnt,
        "fid_trgt_cls_code": fid_trgt_cls_code
    }

    try:
        res = kis._url_fetch(url, tr_id, tr_cont, params)

        if res and res.isOK():
            body = res.getBody()
            output_data = getattr(body, 'output', [])
            if output_data:
                current_data = pd.DataFrame(output_data)
                logger.info(f"체결강도 상위 조회 성공: {len(current_data)}건")
                return current_data
            else:
                logger.warning("체결강도 상위 조회: 데이터 없음")
                return pd.DataFrame()
        else:
            logger.error("체결강도 상위 조회 실패")
            return None
    except Exception as e:
        logger.error(f"체결강도 상위 조회 오류: {e}")
        return None


def get_volume_rank(fid_cond_mrkt_div_code: str = "J",
                   fid_cond_scr_div_code: str = "20171",
                   fid_input_iscd: str = "0000",
                   fid_div_cls_code: str = "1",
                   fid_blng_cls_code: str = "0",
                   fid_trgt_cls_code: str = "111111111",
                   fid_trgt_exls_cls_code: str = "0000000000",
                   fid_input_price_1: str = "",
                   fid_input_price_2: str = "",
                   fid_vol_cnt: str = "",
                   fid_input_date_1: str = "",
                   tr_cont: str = "") -> Optional[pd.DataFrame]:
    """
    거래량순위 조회 (TR: FHPST01710000)

    Args:
        fid_cond_mrkt_div_code: 조건 시장 분류 코드 (J: 주식)
        fid_cond_scr_div_code: 조건 화면 분류 코드 (20171)
        fid_input_iscd: 입력 종목코드 (0000:전체, 0001:거래소, 1001:코스닥)
        fid_div_cls_code: 분류 구분 코드 (0:전체, 1:보통주, 2:우선주)
        fid_blng_cls_code: 소속 구분 코드 (0:평균거래량, 1:거래증가율, 2:평균거래회전율, 3:거래금액순, 4:평균거래금액회전율)
        fid_trgt_cls_code: 대상 구분 코드 (9자리, 111111111:모든 증거금)
        fid_trgt_exls_cls_code: 대상 제외 구분 코드 (10자리, 0000000000:모든 종목 포함)
        fid_input_price_1: 입력 가격1 (가격 ~)
        fid_input_price_2: 입력 가격2 (~ 가격)
        fid_vol_cnt: 거래량 수 (거래량 ~)
        fid_input_date_1: 입력 날짜1 (공란 입력)
        tr_cont: 연속 거래 여부

    Returns:
        거래량순위 종목 데이터 (최대 30건)
    """
    url = '/uapi/domestic-stock/v1/quotations/volume-rank'
    tr_id = "FHPST01710000"  # 거래량순위

    params = {
        "FID_COND_MRKT_DIV_CODE": fid_cond_mrkt_div_code,
        "FID_COND_SCR_DIV_CODE": fid_cond_scr_div_code,
        "FID_INPUT_ISCD": fid_input_iscd,
        "FID_DIV_CLS_CODE": fid_div_cls_code,
        "FID_BLNG_CLS_CODE": fid_blng_cls_code,
        "FID_TRGT_CLS_CODE": fid_trgt_cls_code,
        "FID_TRGT_EXLS_CLS_CODE": fid_trgt_exls_cls_code,
        "FID_INPUT_PRICE_1": fid_input_price_1,
        "FID_INPUT_PRICE_2": fid_input_price_2,
        "FID_VOL_CNT": fid_vol_cnt,
        "FID_INPUT_DATE_1": fid_input_date_1
    }

    try:
        res = kis._url_fetch(url, tr_id, tr_cont, params)

        if res and res.isOK():
            body = res.getBody()
            output_data = getattr(body, 'output', None) or getattr(body, 'Output', [])
            if output_data:
                current_data = pd.DataFrame(output_data)
                logger.info(f"거래량순위 조회 성공: {len(current_data)}건")
                return current_data
            else:
                logger.warning("거래량순위 조회: 데이터 없음")
                return pd.DataFrame()
        else:
            logger.error("거래량순위 조회 실패")
            return None
    except Exception as e:
        logger.error(f"거래량순위 조회 오류: {e}")
        return None


def get_fluctuation_rank(fid_cond_mrkt_div_code: str = "J",
                        fid_cond_scr_div_code: str = "20170",
                        fid_input_iscd: str = "0000",
                        fid_rank_sort_cls_code: str = "0",
                        fid_input_cnt_1: str = "0",
                        fid_prc_cls_code: str = "0",
                        fid_input_price_1: str = "",
                        fid_input_price_2: str = "",
                        fid_vol_cnt: str = "",
                        fid_trgt_cls_code: str = "0",
                        fid_trgt_exls_cls_code: str = "0",
                        fid_div_cls_code: str = "0",
                        fid_rsfl_rate1: str = "",
                        fid_rsfl_rate2: str = "",
                        tr_cont: str = "") -> Optional[pd.DataFrame]:
    """
    등락률 순위 조회 (TR: FHPST01700000)

    Args:
        fid_cond_mrkt_div_code: 조건 시장 분류 코드 (J: 주식)
        fid_cond_scr_div_code: 조건 화면 분류 코드 (20170)
        fid_input_iscd: 입력 종목코드 (0000:전체, 0001:코스피, 1001:코스닥, 2001:코스피200)
        fid_rank_sort_cls_code: 순위 정렬 구분 코드 (0:상승율순, 1:하락율순, 2:시가대비상승율, 3:시가대비하락율, 4:변동율)
        fid_input_cnt_1: 입력 수1 (0:전체, 누적일수 입력)
        fid_prc_cls_code: 가격 구분 코드 (0:저가대비/고가대비, 1:종가대비)
        fid_input_price_1: 입력 가격1 (가격 ~)
        fid_input_price_2: 입력 가격2 (~ 가격)
        fid_vol_cnt: 거래량 수 (거래량 ~)
        fid_trgt_cls_code: 대상 구분 코드 (0:전체)
        fid_trgt_exls_cls_code: 대상 제외 구분 코드 (0:전체)
        fid_div_cls_code: 분류 구분 코드 (0:전체)
        fid_rsfl_rate1: 등락 비율1 (비율 ~)
        fid_rsfl_rate2: 등락 비율2 (~ 비율)
        tr_cont: 연속 거래 여부

    Returns:
        등락률 순위 종목 데이터 (최대 30건)
    """
    url = '/uapi/domestic-stock/v1/ranking/fluctuation'
    tr_id = "FHPST01700000"  # 등락률 순위

    # 🆕 등락률 범위 자동 설정 로직
    if fid_rsfl_rate1 and not fid_rsfl_rate2:
        # fid_rsfl_rate1만 있는 경우 상한을 자동 설정
        try:
            min_rate = float(fid_rsfl_rate1)
            if fid_rank_sort_cls_code == "0":  # 상승률순
                fid_rsfl_rate2 = "30.0"  # 최대 30% 상승까지
            else:  # 하락률순
                fid_rsfl_rate2 = "0.0"   # 최대 0%까지 (하락)
            logger.debug(f"📊 등락률 범위 자동 설정: {fid_rsfl_rate1}% ~ {fid_rsfl_rate2}%")
        except ValueError:
            # 변환 실패시 기본값 사용
            fid_rsfl_rate2 = "30.0" if fid_rank_sort_cls_code == "0" else "0.0"
    elif not fid_rsfl_rate1 and not fid_rsfl_rate2:
        # 둘 다 없는 경우 전체 범위
        fid_rsfl_rate1 = ""
        fid_rsfl_rate2 = ""

    params = {
        "fid_rsfl_rate2": fid_rsfl_rate2,
        "fid_cond_mrkt_div_code": fid_cond_mrkt_div_code,
        "fid_cond_scr_div_code": fid_cond_scr_div_code,
        "fid_input_iscd": fid_input_iscd,
        "fid_rank_sort_cls_code": fid_rank_sort_cls_code,
        "fid_input_cnt_1": fid_input_cnt_1,
        "fid_prc_cls_code": fid_prc_cls_code,
        "fid_input_price_1": fid_input_price_1,
        "fid_input_price_2": fid_input_price_2,
        "fid_vol_cnt": fid_vol_cnt,
        "fid_trgt_cls_code": fid_trgt_cls_code,
        "fid_trgt_exls_cls_code": fid_trgt_exls_cls_code,
        "fid_div_cls_code": fid_div_cls_code,
        "fid_rsfl_rate1": fid_rsfl_rate1
    }

    try:
        # 🔧 시간대별 컨텍스트 정보 추가
        from datetime import datetime
        current_time = datetime.now()
        time_context = f"현재시간:{current_time.strftime('%H:%M:%S')}"
        is_market_open = 9 <= current_time.hour < 16
        time_context += f" 장운영:{'Y' if is_market_open else 'N'}"

        logger.info(f"🔍 등락률순위 API 호출 - {time_context}")
        logger.debug(f"📋 요청파라미터: 시장={fid_input_iscd}, 등락률={fid_rsfl_rate1}~{fid_rsfl_rate2}%, 정렬={fid_rank_sort_cls_code}")

        res = kis._url_fetch(url, tr_id, tr_cont, params)

        if res and res.isOK():
            try:
                # 🔧 응답 구조 상세 분석
                body = res.getBody()
                logger.debug(f"📄 응답 body 타입: {type(body)}")

                # rt_cd, msg_cd, msg1 확인
                rt_cd = getattr(body, 'rt_cd', 'Unknown')
                msg_cd = getattr(body, 'msg_cd', 'Unknown')
                msg1 = getattr(body, 'msg1', 'Unknown')

                logger.info(f"📡 API 응답상태: rt_cd={rt_cd}, msg_cd={msg_cd}, msg1='{msg1}'")

                # output 확인
                if hasattr(body, 'output'):
                    output_data = getattr(body, 'output', [])
                    if output_data:
                        current_data = pd.DataFrame(output_data)
                        logger.info(f"✅ 등락률 순위 조회 성공: {len(current_data)}건")
                        return current_data
                    else:
                        logger.warning(f"⚠️ 등락률 순위: output이 빈 리스트 (조건 만족 종목 없음)")
                        logger.info(f"🔍 필터조건: 시장={fid_input_iscd}, 등락률={fid_rsfl_rate1}~{fid_rsfl_rate2}%, 정렬={fid_rank_sort_cls_code}")
                        return pd.DataFrame()
                else:
                    logger.error(f"❌ 응답에 output 필드 없음 - body 구조: {dir(body)}")
                    return pd.DataFrame()

            except AttributeError as e:
                logger.error(f"❌ 등락률 순위 응답 구조 오류: {e}")
                logger.debug(f"응답 구조: {type(res.getBody())}")
                return pd.DataFrame()
        else:
            if res:
                rt_cd = getattr(res, 'rt_cd', getattr(res.getBody(), 'rt_cd', 'Unknown') if res.getBody() else 'Unknown')
                msg1 = getattr(res, 'msg1', getattr(res.getBody(), 'msg1', 'Unknown') if res.getBody() else 'Unknown')
                logger.error(f"❌ 등락률 순위 조회 실패 - rt_cd:{rt_cd}, msg:'{msg1}'")

                # 🔧 일반적인 오류 원인 안내
                if rt_cd == '1':
                    if '시간' in str(msg1) or 'time' in str(msg1).lower():
                        logger.warning("💡 힌트: 장 운영 시간 외에는 일부 API가 제한될 수 있습니다")
                    elif '조회' in str(msg1) or 'inquiry' in str(msg1).lower():
                        logger.warning("💡 힌트: API 호출 한도 초과이거나 조회 조건이 너무 제한적일 수 있습니다")

            else:
                logger.error("❌ 등락률 순위 조회 실패 - 응답 없음 (네트워크 또는 인증 문제)")
            return None
    except Exception as e:
        logger.error(f"❌ 등락률 순위 조회 예외: {e}")
        return None


# =============================================================================
# 통합된 전략별 후보 조회 함수들
# =============================================================================

def _get_adaptive_criteria():
    """시간대별 적응형 기준 설정"""
    from datetime import datetime

    current_time = datetime.now()
    is_pre_market = current_time.hour < 9 or (current_time.hour == 9 and current_time.minute < 30)

    if is_pre_market:
        # 프리마켓: 매우 관대한 기준
        criteria = {
            'min_gap_rate': 0.5,
            'min_change_rate': 0.3,
            'min_volume_ratio': 1.2,
            'min_daily_volume': 40000,
            'min_price': 1000,
            'max_price': 1000000,
            'fluctuation_threshold': "0.3",
            'max_candidates': 30,
            'description': "프리마켓 갭트레이딩 기준: 갭0.5% 변동0.3% 거래량1.2배 (매우 관대)"
        }
        logger.info("🌅 " + criteria['description'])
    elif current_time.hour < 11:
        # 장 초반: 관대한 기준
        criteria = {
            'min_gap_rate': 1.0,
            'min_change_rate': 0.5,
            'min_volume_ratio': 1.5,
            'min_daily_volume': 60000,
            'min_price': 1000,
            'max_price': 1000000,
            'fluctuation_threshold': "0.5",
            'max_candidates': 20,
            'description': "장초반 갭트레이딩 기준: 갭1.0% 변동0.5% 거래량1.5배 (관대)"
        }
        logger.info("🌄 " + criteria['description'])
    else:
        # 정규 시간: 기본 기준
        criteria = {
            'min_gap_rate': 1.5,
            'min_change_rate': 0.8,
            'min_volume_ratio': 1.8,
            'min_daily_volume': 80000,
            'min_price': 1000,
            'max_price': 1000000,
            'fluctuation_threshold': "0.8",
            'max_candidates': 20,
            'description': "정규시간 갭트레이딩 기준: 갭1.5% 변동0.8% 거래량1.8배 (완화)"
        }
        logger.info("🕐 " + criteria['description'])

    return criteria, is_pre_market

def _get_initial_candidates(market: str, criteria: dict):
    """1차 필터링으로 초기 후보 획득 (백업 전략 포함)"""
    logger.info("🎯 갭 트레이딩 후보 적응형 필터링 중...")

    # 1차 시도: 적응형 상승률 기준
    candidate_data = get_fluctuation_rank(
        fid_input_iscd=market,
        fid_rank_sort_cls_code="0",
        fid_rsfl_rate1=criteria['fluctuation_threshold']
    )

    if candidate_data is not None and not candidate_data.empty:
        logger.info(f"🎯 1차 필터링 성공: {len(candidate_data)}개 종목")
        return candidate_data

    # 백업 전략 1: 더 관대한 기준
    fallback_threshold = str(float(criteria['fluctuation_threshold']) * 0.5)
    logger.warning(f"🎯 1차 필터링 데이터 없음 - {fallback_threshold}% 이상으로 재시도")
    candidate_data = get_fluctuation_rank(
        fid_input_iscd=market,
        fid_rank_sort_cls_code="0",
        fid_rsfl_rate1=fallback_threshold
    )

    if candidate_data is not None and not candidate_data.empty:
        logger.info(f"🎯 2차 시도 성공: {len(candidate_data)}개 종목")
        return candidate_data

    # 백업 전략 2: 조건 없이 전체 조회
    logger.warning("🎯 2차 필터링도 데이터 없음 - 조건 제거하고 전체 조회")
    candidate_data = get_fluctuation_rank(
        fid_input_iscd=market,
        fid_rank_sort_cls_code="0",
        fid_rsfl_rate1="",
        fid_vol_cnt=""
    )

    if candidate_data is not None and not candidate_data.empty:
        logger.info(f"🎯 3차 시도 성공: {len(candidate_data)}개 종목")
        return candidate_data

    # 백업 전략 3: 다른 시장으로 시도
    if market != "0000":
        logger.warning("🎯 3차 백업: 전체 시장(0000)으로 재시도")
        candidate_data = get_fluctuation_rank(
            fid_input_iscd="0000",
            fid_rank_sort_cls_code="0",
            fid_rsfl_rate1="",
            fid_vol_cnt=""
        )

        if candidate_data is not None and not candidate_data.empty:
            logger.info(f"🎯 4차 시도 성공: {len(candidate_data)}개 종목")
            return candidate_data

    # 최종 백업: 하락률순으로도 시도
    logger.warning("🎯 최종 백업: 하락률순 조회 (반대매매 후보)")
    candidate_data = get_fluctuation_rank(
        fid_input_iscd="0000",
        fid_rank_sort_cls_code="1",
        fid_rsfl_rate1="",
        fid_vol_cnt=""
    )

    if candidate_data is not None and not candidate_data.empty:
        logger.info(f"🎯 최종 시도 성공: {len(candidate_data)}개 종목")
        return candidate_data

    logger.error("🎯 갭 트레이딩: 모든 백업 전략에도 데이터 없음")
    logger.info("💡 가능한 원인: 1) 장 운영시간 외 2) API 제한 3) 시장 참여자 부족 4) 네트워크 문제")
    return pd.DataFrame()

def _calculate_gap_metrics(stock_code: str, row, is_pre_market: bool):
    """개별 종목의 갭 메트릭 계산"""
    try:
        # 현재가 정보 조회
        current_data = get_inquire_price("J", stock_code)
        if current_data is None or current_data.empty:
            return None

        current_info = current_data.iloc[0]

        # 기본 가격 정보 추출
        current_price = int(current_info.get('stck_prpr', 0))
        open_price = int(current_info.get('stck_oprc', 0))
        prev_close = int(current_info.get('stck_sdpr', 0))

        # 프리마켓 시가 처리
        if is_pre_market and open_price <= 0:
            logger.debug(f"🌅 프리마켓 종목 {stock_code}: 시가 없음 - 현재가로 추정")
            open_price = current_price
        elif not is_pre_market and open_price <= 0:
            logger.debug(f"🎯 종목 {stock_code}: 시가 없음 - 제외")
            return None

        if prev_close <= 0 or current_price <= 0:
            logger.debug(f"🎯 종목 {stock_code}: 가격 정보 불완전 - 제외")
            return None

        # 갭 계산
        gap_size = open_price - prev_close
        gap_rate = (gap_size / prev_close) * 100

        # 변동률 안전 변환
        change_rate_raw = current_info.get('prdy_ctrt', '0')
        try:
            change_rate = float(str(change_rate_raw))
        except (ValueError, TypeError):
            logger.debug(f"🎯 종목 {stock_code}: 변동률 변환 오류 - 제외")
            return None

        return {
            'stock_code': stock_code,
            'stock_name': row.get('hts_kor_isnm', ''),
            'current_price': current_price,
            'open_price': open_price,
            'prev_close': prev_close,
            'gap_size': gap_size,
            'gap_rate': round(gap_rate, 2),
            'change_rate': change_rate,
            'volume': int(current_info.get('acml_vol', 0)),
            'avg_volume_raw': current_info.get('avrg_vol', 0)
        }

    except Exception as e:
        logger.warning(f"🎯 종목 {stock_code} 갭 계산 오류: {e}")
        return None

def _calculate_volume_metrics(stock_code: str, volume: int, avg_volume_raw):
    """거래량 비율 계산 (API 조회 포함)"""
    try:
        avg_volume = int(avg_volume_raw) if avg_volume_raw else 0
    except (ValueError, TypeError):
        avg_volume = 0

    # 평균 거래량이 없거나 부족한 경우 API 조회
    if avg_volume <= 0:
        try:
            logger.debug(f"🔍 {stock_code}: 평균 거래량 정보 없음 - API 조회 시작")
            historical_data = get_inquire_daily_price("J", stock_code)
            if historical_data is not None and not historical_data.empty and len(historical_data) >= 5:
                # 최근 5일간 거래량 평균 계산
                volumes = []
                for _, row in historical_data.head(5).iterrows():
                    vol = int(row.get('acml_vol', 0)) if row.get('acml_vol') else 0
                    if vol > 0:
                        volumes.append(vol)

                if volumes:
                    calculated_avg_volume = sum(volumes) // len(volumes)
                    safe_avg_volume = max(calculated_avg_volume, 5000)
                    logger.debug(f"📊 {stock_code}: 5일 평균 거래량 계산 완료 - {safe_avg_volume:,}주")
                else:
                    safe_avg_volume = max(volume // 5, 10000)
                    logger.debug(f"📊 {stock_code}: 거래량 데이터 부족 - 추정치 사용: {safe_avg_volume:,}주")
            else:
                safe_avg_volume = max(volume // 5, 10000)
                logger.debug(f"📊 {stock_code}: API 조회 실패 - 추정치 사용: {safe_avg_volume:,}주")
        except Exception as e:
            logger.warning(f"📊 {stock_code}: 평균 거래량 계산 오류 - {e}")
            safe_avg_volume = max(volume // 5, 10000)
    elif avg_volume < 5000:
        safe_avg_volume = 5000
        logger.debug(f"📊 {stock_code}: 평균 거래량 보정 - {avg_volume:,}주 → {safe_avg_volume:,}주")
    else:
        safe_avg_volume = avg_volume

    # 거래량 비율 계산 및 상한 제한
    volume_ratio = volume / safe_avg_volume
    volume_ratio = min(volume_ratio, 100)  # 최대 100배로 제한

    logger.debug(f"🔧 {stock_code} 거래량 계산: 현재={volume:,}주, 평균={safe_avg_volume:,}주, 비율={volume_ratio:.1f}배")

    return volume_ratio

def _apply_adaptive_filters(metrics: dict, criteria: dict):
    """적응형 조건 필터 적용"""
    stock_code = metrics['stock_code']

    # 갭 크기 체크
    if metrics['gap_rate'] < criteria['min_gap_rate']:
        logger.debug(f"🎯 종목 {stock_code}: 갭 부족 - {metrics['gap_rate']:.2f}%")
        return False

    # 거래량 비율 계산
    volume_ratio = _calculate_volume_metrics(
        stock_code,
        metrics['volume'],
        metrics['avg_volume_raw']
    )

    # 조건 체크
    if (volume_ratio < criteria['min_volume_ratio'] or
        metrics['change_rate'] < criteria['min_change_rate'] or
        metrics['volume'] < criteria['min_daily_volume']):
        logger.debug(f"🎯 종목 {stock_code}: 조건 미달 - 거래량{volume_ratio:.1f}배 변동률{metrics['change_rate']:.1f}% 볼륨{metrics['volume']:,}주")
        return False

    # 가격대 필터
    if (metrics['current_price'] < criteria['min_price'] or
        metrics['current_price'] > criteria['max_price']):
        logger.debug(f"🎯 종목 {stock_code}: 가격대 제외 ({metrics['current_price']}원)")
        return False

    # 통과한 경우 volume_ratio 추가
    metrics['volume_ratio'] = round(volume_ratio, 2)
    return True

def _score_and_sort_candidates(gap_candidates: list):
    """수익성 점수 계산 및 정렬"""
    if not gap_candidates:
        logger.info("🎯 적응형 갭 트레이딩 조건을 만족하는 종목 없음")
        return pd.DataFrame()

    # 수익성 점수 계산
    for candidate in gap_candidates:
        candidate['profit_score'] = (
            candidate['gap_rate'] *
            candidate['volume_ratio'] *
            candidate['change_rate']
        )
        candidate['data_rank'] = len(gap_candidates)

    # DataFrame 생성 및 정렬
    gap_df = pd.DataFrame(gap_candidates)
    gap_df = gap_df.sort_values('profit_score', ascending=False)

    logger.info(f"🎯 적응형 갭 트레이딩 후보 {len(gap_df)}개 발견")
    return gap_df

def get_gap_trading_candidates(market: str = "0000",
                               min_gap_rate: float = 2.0,
                               min_change_rate: float = 1.0,
                               min_volume_ratio: float = 2.0) -> Optional[pd.DataFrame]:
    """갭 트레이딩 후보 조회 - 🎯 모듈화된 버전"""
    try:
        # 1단계: 시간대별 적응형 기준 설정
        criteria, is_pre_market = _get_adaptive_criteria()

        # 2단계: 초기 후보 획득 (백업 전략 포함)
        candidate_data = _get_initial_candidates(market, criteria)
        if candidate_data.empty:
            return pd.DataFrame()

        logger.info(f"🎯 적응형 필터링 완료: {len(candidate_data)}개 종목 확보")

        # 3단계: 각 종목 분석 및 필터링
        gap_candidates = []
        for idx, row in candidate_data.head(criteria['max_candidates']).iterrows():
            stock_code = row.get('stck_shrn_iscd', '')
            if not stock_code:
                continue

            # 갭 메트릭 계산
            metrics = _calculate_gap_metrics(stock_code, row, is_pre_market)
            if metrics is None:
                continue

            # 적응형 필터 적용
            if _apply_adaptive_filters(metrics, criteria):
                # 최종 후보 데이터 구성
                candidate = {
                    'stck_shrn_iscd': stock_code,
                    'hts_kor_isnm': metrics['stock_name'],
                    'stck_prpr': metrics['current_price'],
                    'stck_oprc': metrics['open_price'],
                    'stck_sdpr': metrics['prev_close'],
                    'gap_size': metrics['gap_size'],
                    'gap_rate': metrics['gap_rate'],
                    'change_rate': metrics['change_rate'],
                    'acml_vol': metrics['volume'],
                    'volume_ratio': metrics['volume_ratio']
                }
                gap_candidates.append(candidate)

                logger.info(f"🎯 갭 후보: {stock_code}({metrics['stock_name']}) 갭{metrics['gap_rate']:.1f}% 거래량{metrics['volume_ratio']:.1f}배 변동률{metrics['change_rate']:.1f}%")

        # 4단계: 점수 계산 및 정렬
        return _score_and_sort_candidates(gap_candidates)

    except Exception as e:
        logger.error(f"🎯 갭 트레이딩 후보 조회 오류: {e}")
        return None


def get_volume_breakout_candidates(market: str = "0000") -> Optional[pd.DataFrame]:
    """거래량 돌파 후보 조회 - 🎯 적응형 기준 (시간대별 조정)"""
    from datetime import datetime

    current_time = datetime.now()
    is_pre_market = current_time.hour < 9 or (current_time.hour == 9 and current_time.minute < 30)

    if is_pre_market:
        # 프리마켓: 매우 관대한 기준
        volume_threshold = "5000"  # 5천주
        logger.info("🌅 프리마켓 거래량 기준: 5천주 (관대)")
    elif current_time.hour < 11:
        # 장 초반: 관대한 기준
        volume_threshold = "20000"  # 2만주
        logger.info("🌄 장초반 거래량 기준: 2만주")
    else:
        # 정규 시간: 기본 기준
        volume_threshold = "50000"  # 5만주
        logger.info("🕐 정규시간 거래량 기준: 5만주")

    return get_volume_rank(
        fid_input_iscd=market,
        fid_blng_cls_code="1",  # 거래증가율
        fid_vol_cnt=volume_threshold
    )


def get_momentum_candidates(market: str = "0000") -> Optional[pd.DataFrame]:
    """모멘텀 후보 조회 - 🎯 적응형 기준 (시간대별 조정)"""
    from datetime import datetime

    current_time = datetime.now()
    is_pre_market = current_time.hour < 9 or (current_time.hour == 9 and current_time.minute < 30)

    if is_pre_market:
        # 프리마켓: 매우 관대한 기준
        volume_threshold = "3000"  # 3천주
        logger.info("🌅 프리마켓 체결강도 기준: 3천주 (관대)")
    elif current_time.hour < 11:
        # 장 초반: 관대한 기준
        volume_threshold = "10000"  # 1만주
        logger.info("🌄 장초반 체결강도 기준: 1만주")
    else:
        # 정규 시간: 기본 기준
        volume_threshold = "30000"  # 3만주 (5만주에서 완화)
        logger.info("🕐 정규시간 체결강도 기준: 3만주")

    return get_volume_power_rank(
        fid_input_iscd=market,
        fid_vol_cnt=volume_threshold
    )


def get_bulk_trans_num_rank(fid_cond_mrkt_div_code: str = "J",
                           fid_cond_scr_div_code: str = "11909",
                           fid_input_iscd: str = "0000",
                           fid_rank_sort_cls_code: str = "0",
                           fid_div_cls_code: str = "0",
                           fid_input_price_1: str = "",
                           fid_aply_rang_prc_1: str = "",
                           fid_aply_rang_prc_2: str = "",
                           fid_input_iscd_2: str = "",
                           fid_trgt_exls_cls_code: str = "0",
                           fid_trgt_cls_code: str = "0",
                           fid_vol_cnt: str = "",
                           tr_cont: str = "") -> Optional[pd.DataFrame]:
    """
    대량체결건수 상위 조회 (TR: FHKST190900C0)

    Args:
        fid_cond_mrkt_div_code: 조건 시장 분류 코드 (J: 주식)
        fid_cond_scr_div_code: 조건 화면 분류 코드 (11909)
        fid_input_iscd: 입력 종목코드 (0000:전체, 0001:거래소, 1001:코스닥, 2001:코스피200, 4001:KRX100)
        fid_rank_sort_cls_code: 순위 정렬 구분 코드 (0:매수상위, 1:매도상위)
        fid_div_cls_code: 분류 구분 코드 (0:전체)
        fid_input_price_1: 입력 가격1 (건별금액 ~)
        fid_aply_rang_prc_1: 적용 범위 가격1 (가격 ~)
        fid_aply_rang_prc_2: 적용 범위 가격2 (~ 가격)
        fid_input_iscd_2: 입력 종목코드2 (공백:전체종목, 개별종목 조회시 종목코드)
        fid_trgt_exls_cls_code: 대상 제외 구분 코드 (0:전체)
        fid_trgt_cls_code: 대상 구분 코드 (0:전체)
        fid_vol_cnt: 거래량 수 (거래량 ~)
        tr_cont: 연속 거래 여부

    Returns:
        대량체결건수 상위 종목 데이터 (최대 30건)
    """
    url = '/uapi/domestic-stock/v1/ranking/bulk-trans-num'
    tr_id = "FHKST190900C0"  # 대량체결건수 상위

    params = {
        "fid_aply_rang_prc_2": fid_aply_rang_prc_2,
        "fid_cond_mrkt_div_code": fid_cond_mrkt_div_code,
        "fid_cond_scr_div_code": fid_cond_scr_div_code,
        "fid_input_iscd": fid_input_iscd,
        "fid_rank_sort_cls_code": fid_rank_sort_cls_code,
        "fid_div_cls_code": fid_div_cls_code,
        "fid_input_price_1": fid_input_price_1,
        "fid_aply_rang_prc_1": fid_aply_rang_prc_1,
        "fid_input_iscd_2": fid_input_iscd_2,
        "fid_trgt_exls_cls_code": fid_trgt_exls_cls_code,
        "fid_trgt_cls_code": fid_trgt_cls_code,
        "fid_vol_cnt": fid_vol_cnt
    }

    try:
        res = kis._url_fetch(url, tr_id, tr_cont, params)

        if res and res.isOK():
            body = res.getBody()
            output_data = getattr(body, 'output', [])
            if output_data:
                current_data = pd.DataFrame(output_data)
                logger.info(f"대량체결건수 상위 조회 성공: {len(current_data)}건")
                return current_data
            else:
                logger.warning("대량체결건수 상위 조회: 데이터 없음")
                return pd.DataFrame()
        else:
            logger.error("대량체결건수 상위 조회 실패")
            return None
    except Exception as e:
        logger.error(f"대량체결건수 상위 조회 오류: {e}")
        return None


def get_disparity_rank(fid_cond_mrkt_div_code: str = "J",
                      fid_cond_scr_div_code: str = "20178",
                      fid_input_iscd: str = "0000",
                      fid_rank_sort_cls_code: str = "0",
                      fid_hour_cls_code: str = "20",
                      fid_div_cls_code: str = "0",
                      fid_input_price_1: str = "",
                      fid_input_price_2: str = "",
                      fid_trgt_cls_code: str = "0",
                      fid_trgt_exls_cls_code: str = "0",
                      fid_vol_cnt: str = "",
                      tr_cont: str = "") -> Optional[pd.DataFrame]:
    """
    이격도 순위 조회 (TR: FHPST01780000)

    Args:
        fid_cond_mrkt_div_code: 조건 시장 분류 코드 (J: 주식)
        fid_cond_scr_div_code: 조건 화면 분류 코드 (20178)
        fid_input_iscd: 입력 종목코드 (0000:전체, 0001:거래소, 1001:코스닥, 2001:코스피200)
        fid_rank_sort_cls_code: 순위 정렬 구분 코드 (0:이격도상위순, 1:이격도하위순)
        fid_hour_cls_code: 시간 구분 코드 (5:이격도5, 10:이격도10, 20:이격도20, 60:이격도60, 120:이격도120)
        fid_div_cls_code: 분류 구분 코드 (0:전체, 1:관리종목, 2:투자주의, 3:투자경고, 4:투자위험예고, 5:투자위험, 6:보통주, 7:우선주)
        fid_input_price_1: 입력 가격1 (가격 ~)
        fid_input_price_2: 입력 가격2 (~ 가격)
        fid_trgt_cls_code: 대상 구분 코드 (0:전체)
        fid_trgt_exls_cls_code: 대상 제외 구분 코드 (0:전체)
        fid_vol_cnt: 거래량 수 (거래량 ~)
        tr_cont: 연속 거래 여부

    Returns:
        이격도 순위 종목 데이터 (최대 30건)
    """
    url = '/uapi/domestic-stock/v1/ranking/disparity'
    tr_id = "FHPST01780000"  # 이격도 순위

    params = {
        "FID_INPUT_PRICE_2": fid_input_price_2,          # 입력 가격2
        "FID_COND_MRKT_DIV_CODE": fid_cond_mrkt_div_code, # 조건 시장 분류 코드
        "FID_COND_SCR_DIV_CODE": fid_cond_scr_div_code,   # 조건 화면 분류 코드
        "FID_DIV_CLS_CODE": fid_div_cls_code,             # 분류 구분 코드
        "FID_RANK_SORT_CLS_CODE": fid_rank_sort_cls_code, # 순위 정렬 구분 코드
        "FID_HOUR_CLS_CODE": fid_hour_cls_code,           # 시간 구분 코드
        "FID_INPUT_ISCD": fid_input_iscd,                 # 입력 종목코드
        "FID_TRGT_CLS_CODE": fid_trgt_cls_code,           # 대상 구분 코드
        "FID_TRGT_EXLS_CLS_CODE": fid_trgt_exls_cls_code, # 대상 제외 구분 코드
        "FID_INPUT_PRICE_1": fid_input_price_1,           # 입력 가격1
        "FID_VOL_CNT": fid_vol_cnt                        # 거래량 수
    }

    try:
        logger.debug(f"🔍 이격도순위 API 호출 - 시장:{fid_input_iscd}, 이격도:{fid_hour_cls_code}일")
        logger.debug(f"📋 파라미터: {params}")

        res = kis._url_fetch(url, tr_id, tr_cont, params)

        if res and res.isOK():
            body = res.getBody()
            output_data = getattr(body, 'output', [])
            if output_data:
                current_data = pd.DataFrame(output_data)
                #logger.info(f"✅ 이격도 순위 조회 성공: {len(current_data)}건 (이격도{fid_hour_cls_code}일)")
                return current_data
            else:
                logger.warning("이격도 순위 조회: 데이터 없음")
                return pd.DataFrame()
        else:
            logger.error("이격도 순위 조회 실패")
            return None
    except Exception as e:
        logger.error(f"이격도 순위 조회 오류: {e}")
        return None


def get_quote_balance_rank(fid_cond_mrkt_div_code: str = "J",
                          fid_cond_scr_div_code: str = "20172",
                          fid_input_iscd: str = "0000",
                          fid_rank_sort_cls_code: str = "0",
                          fid_div_cls_code: str = "0",
                          fid_trgt_cls_code: str = "0",
                          fid_trgt_exls_cls_code: str = "0",
                          fid_input_price_1: str = "",
                          fid_input_price_2: str = "",
                          fid_vol_cnt: str = "",
                          tr_cont: str = "") -> Optional[pd.DataFrame]:
    """
    호가잔량 순위 조회 (TR: FHPST01720000)

    Args:
        fid_cond_mrkt_div_code: 조건 시장 분류 코드 (J: 주식)
        fid_cond_scr_div_code: 조건 화면 분류 코드 (20172)
        fid_input_iscd: 입력 종목코드 (0000:전체, 0001:코스피, 1001:코스닥, 2001:코스피200)
        fid_rank_sort_cls_code: 순위 정렬 구분 코드 (0:순매수잔량순, 1:순매도잔량순, 2:매수비율순, 3:매도비율순)
        fid_div_cls_code: 분류 구분 코드 (0:전체)
        fid_trgt_cls_code: 대상 구분 코드 (0:전체)
        fid_trgt_exls_cls_code: 대상 제외 구분 코드 (0:전체)
        fid_input_price_1: 입력 가격1 (가격 ~)
        fid_input_price_2: 입력 가격2 (~ 가격)
        fid_vol_cnt: 거래량 수 (거래량 ~)
        tr_cont: 연속 거래 여부

    Returns:
        호가잔량 순위 종목 데이터 (최대 30건)
    """
    url = '/uapi/domestic-stock/v1/ranking/quote-balance'
    tr_id = "FHPST01720000"  # 호가잔량 순위

    params = {
        "fid_vol_cnt": fid_vol_cnt,
        "fid_cond_mrkt_div_code": fid_cond_mrkt_div_code,
        "fid_cond_scr_div_code": fid_cond_scr_div_code,
        "fid_input_iscd": fid_input_iscd,
        "fid_rank_sort_cls_code": fid_rank_sort_cls_code,
        "fid_div_cls_code": fid_div_cls_code,
        "fid_trgt_cls_code": fid_trgt_cls_code,
        "fid_trgt_exls_cls_code": fid_trgt_exls_cls_code,
        "fid_input_price_1": fid_input_price_1,
        "fid_input_price_2": fid_input_price_2
    }

    try:
        res = kis._url_fetch(url, tr_id, tr_cont, params)

        if res and res.isOK():
            body = res.getBody()
            output_data = getattr(body, 'output', [])
            if output_data:
                current_data = pd.DataFrame(output_data)
                logger.info(f"호가잔량 순위 조회 성공: {len(current_data)}건")
                return current_data
            else:
                logger.warning("호가잔량 순위 조회: 데이터 없음")
                return pd.DataFrame()
        else:
            logger.error("호가잔량 순위 조회 실패")
            return None
    except Exception as e:
        logger.error(f"호가잔량 순위 조회 오류: {e}")
        return None


def get_multi_period_disparity(stock_code: str = "0000") -> Optional[Dict]:
    """
    🆕 다중 기간 이격도 종합 분석

    Args:
        stock_code: 종목코드 (특정 종목 분석시 사용)

    Returns:
        {
            'short_term': DataFrame,   # 5일 이격도
            'medium_term': DataFrame,  # 20일 이격도
            'long_term': DataFrame,    # 60일 이격도
            'analysis': Dict          # 종합 분석 결과
        }
    """
    try:
        result = {
            'short_term': None,
            'medium_term': None,
            'long_term': None,
            'analysis': {}
        }

        # 5일 이격도 (단기 과열/침체)
        d5_data = get_disparity_rank(
            fid_input_iscd="0000",
            fid_hour_cls_code="5",
            fid_vol_cnt="30000"  # 3만주 이상
        )

        # 20일 이격도 (중기 트렌드)
        d20_data = get_disparity_rank(
            fid_input_iscd="0000",
            fid_hour_cls_code="20",
            fid_vol_cnt="30000"
        )

        # 60일 이격도 (장기 흐름)
        d60_data = get_disparity_rank(
            fid_input_iscd="0000",
            fid_hour_cls_code="60",
            fid_vol_cnt="30000"
        )

        result['short_term'] = d5_data
        result['medium_term'] = d20_data
        result['long_term'] = d60_data

        # 🎯 종합 분석: 이격도 divergence 포착
        if all(data is not None and not data.empty for data in [d5_data, d20_data, d60_data]):
            # 타입 확인을 통과한 후 함수 호출
            if d5_data is not None and d20_data is not None and d60_data is not None:
                analysis = _analyze_disparity_divergence(d5_data, d20_data, d60_data)
                result['analysis'] = analysis

        logger.info(f"다중 기간 이격도 분석 완료")
        return result

    except Exception as e:
        logger.error(f"다중 기간 이격도 분석 오류: {e}")
        return None


def _analyze_disparity_divergence(d5_data: pd.DataFrame,
                                 d20_data: pd.DataFrame,
                                 d60_data: pd.DataFrame) -> Dict:
    """🎯 이격도 divergence 분석 (반전 시점 포착)"""
    try:
        analysis = {
            'strong_buy_candidates': [],    # 강매수 후보
            'buy_candidates': [],           # 매수 후보
            'sell_candidates': [],          # 매도 후보
            'strong_sell_candidates': [],   # 강매도 후보
            'divergence_signals': []        # divergence 신호
        }

        # 공통 종목 찾기 (모든 기간 데이터에 포함된 종목)
        common_stocks = set(d5_data['mksc_shrn_iscd']) & \
                       set(d20_data['mksc_shrn_iscd']) & \
                       set(d60_data['mksc_shrn_iscd'])

        for stock_code in list(common_stocks)[:50]:  # 상위 50개 종목만 분석
            try:
                # 각 기간별 이격도 추출
                d5_row = d5_data[d5_data['mksc_shrn_iscd'] == stock_code].iloc[0]
                d20_row = d20_data[d20_data['mksc_shrn_iscd'] == stock_code].iloc[0]
                d60_row = d60_data[d60_data['mksc_shrn_iscd'] == stock_code].iloc[0]

                d5_val = float(d5_row.get('d5_dsrt', 100))
                d20_val = float(d20_row.get('d20_dsrt', 100))
                d60_val = float(d60_row.get('d60_dsrt', 100))

                stock_name = d20_row.get('hts_kor_isnm', '')
                current_price = int(d20_row.get('stck_prpr', 0))
                change_rate = float(d20_row.get('prdy_ctrt', 0))

                stock_info = {
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'current_price': current_price,
                    'change_rate': change_rate,
                    'd5_disparity': d5_val,
                    'd20_disparity': d20_val,
                    'd60_disparity': d60_val
                }

                # 🎯 이격도 패턴 분석

                # 1. 강매수 신호: 모든 기간 과매도 + 단기 반등
                if (d60_val <= 85 and d20_val <= 90 and d5_val <= 95 and
                    change_rate >= 0.5):  # 장기/중기 과매도 + 단기 회복 + 상승
                    stock_info['signal_strength'] = 'STRONG_BUY'
                    stock_info['reason'] = f'전기간 과매도 반등 (60일:{d60_val:.1f}, 20일:{d20_val:.1f}, 5일:{d5_val:.1f})'
                    analysis['strong_buy_candidates'].append(stock_info)

                # 2. 매수 신호: 중장기 과매도 + 단기 정상
                elif (d20_val <= 90 and d60_val <= 92 and d5_val >= 95 and
                      change_rate >= 0):
                    stock_info['signal_strength'] = 'BUY'
                    stock_info['reason'] = f'중장기 과매도 (20일:{d20_val:.1f}, 60일:{d60_val:.1f})'
                    analysis['buy_candidates'].append(stock_info)

                # 3. 매도 신호: 단기 과열 + 중기 고점
                elif (d5_val >= 115 and d20_val >= 110 and change_rate >= 2.0):
                    stock_info['signal_strength'] = 'SELL'
                    stock_info['reason'] = f'단중기 과열 (5일:{d5_val:.1f}, 20일:{d20_val:.1f})'
                    analysis['sell_candidates'].append(stock_info)

                # 4. 강매도 신호: 모든 기간 과열
                elif (d5_val >= 120 and d20_val >= 115 and d60_val >= 110):
                    stock_info['signal_strength'] = 'STRONG_SELL'
                    stock_info['reason'] = f'전기간 과열 (60일:{d60_val:.1f}, 20일:{d20_val:.1f}, 5일:{d5_val:.1f})'
                    analysis['strong_sell_candidates'].append(stock_info)

                # 5. 🎯 Divergence 신호 (추세 반전 신호)
                # 장기상승 + 단기하락 = 조정 시작
                if (d60_val >= 105 and d20_val >= 102 and d5_val <= 98):
                    stock_info['signal_strength'] = 'DIVERGENCE_SELL'
                    stock_info['reason'] = f'하향 Divergence (장기 과열, 단기 조정)'
                    analysis['divergence_signals'].append(stock_info)

                # 장기하락 + 단기상승 = 반등 시작
                elif (d60_val <= 95 and d20_val <= 98 and d5_val >= 102):
                    stock_info['signal_strength'] = 'DIVERGENCE_BUY'
                    stock_info['reason'] = f'상향 Divergence (장기 침체, 단기 회복)'
                    analysis['divergence_signals'].append(stock_info)

            except Exception as e:
                logger.warning(f"이격도 divergence 분석 오류 ({stock_code}): {e}")
                continue

        # 신호 강도별 정렬
        for category in ['strong_buy_candidates', 'buy_candidates', 'sell_candidates', 'strong_sell_candidates']:
            analysis[category].sort(key=lambda x: abs(x['change_rate']), reverse=True)
            analysis[category] = analysis[category][:10]  # 상위 10개

        logger.info(f"🎯 이격도 divergence 분석 완료: "
                   f"강매수{len(analysis['strong_buy_candidates'])} "
                   f"매수{len(analysis['buy_candidates'])} "
                   f"매도{len(analysis['sell_candidates'])} "
                   f"강매도{len(analysis['strong_sell_candidates'])} "
                   f"divergence{len(analysis['divergence_signals'])}")

        return analysis

    except Exception as e:
        logger.error(f"이격도 divergence 분석 오류: {e}")
        return {}


def get_disparity_trading_signals() -> Optional[Dict]:
    """이격도 전체 시장 분석 및 트레이딩 신호"""
    try:
        logger.info("🎯 이격도 전체 시장 분석 시작")

        signals = {
            'kospi': {},
            'kosdaq': {},
            'combined': {}
        }

        # 코스피와 코스닥 이격도 분석
        for market, code in [('kospi', '0001'), ('kosdaq', '1001')]:
            try:
                # 이격도 20일 기준 상위/하위 조회
                upper_data = get_disparity_rank(
                    fid_input_iscd=code,
                    fid_rank_sort_cls_code="0",  # 상위
                    fid_hour_cls_code="20"
                )

                lower_data = get_disparity_rank(
                    fid_input_iscd=code,
                    fid_rank_sort_cls_code="1",  # 하위
                    fid_hour_cls_code="20"
                )

                if upper_data is not None and not upper_data.empty and lower_data is not None and not lower_data.empty:
                    market_signals = _analyze_market_disparity(upper_data, lower_data, market)
                    signals[market] = market_signals
                else:
                    logger.warning(f"{market} 이격도 데이터 없음")
                    signals[market] = {'status': 'no_data'}

            except Exception as e:
                logger.error(f"{market} 이격도 분석 오류: {e}")
                signals[market] = {'status': 'error', 'message': str(e)}

        # 통합 시장 신호
        signals['combined'] = _combine_market_signals(signals['kospi'], signals['kosdaq'])

        return signals

    except Exception as e:
        logger.error(f"이격도 트레이딩 신호 분석 오류: {e}")
        return None

def _analyze_market_disparity(upper_data: pd.DataFrame, lower_data: pd.DataFrame, market: str) -> Dict:
    """시장 이격도 분석"""
    try:
        analysis = {
            'market': market,
            'overbought_stocks': len(upper_data),
            'oversold_stocks': len(lower_data),
            'sentiment': 'neutral'
        }

        # 시장 심리 판단
        if analysis['oversold_stocks'] > analysis['overbought_stocks'] * 2:
            analysis['sentiment'] = 'oversold_dominant'
        elif analysis['overbought_stocks'] > analysis['oversold_stocks'] * 2:
            analysis['sentiment'] = 'overbought_dominant'

        return analysis

    except Exception as e:
        logger.error(f"시장 이격도 분석 오류: {e}")
        return {'market': market, 'sentiment': 'error'}


def _combine_market_signals(kospi_signals: Dict, kosdaq_signals: Dict) -> Dict:
    """시장 신호 통합"""
    try:
        combined = {
            'overall_sentiment': 'neutral',
            'recommendation': 'hold',
            'confidence': 'medium'
        }

        # 간단한 통합 로직
        kospi_sentiment = kospi_signals.get('sentiment', 'neutral')
        kosdaq_sentiment = kosdaq_signals.get('sentiment', 'neutral')

        if kospi_sentiment == 'oversold_dominant' and kosdaq_sentiment == 'oversold_dominant':
            combined['overall_sentiment'] = 'market_oversold'
            combined['recommendation'] = 'buy_opportunity'
            combined['confidence'] = 'high'
        elif kospi_sentiment == 'overbought_dominant' and kosdaq_sentiment == 'overbought_dominant':
            combined['overall_sentiment'] = 'market_overbought'
            combined['recommendation'] = 'sell_signal'
            combined['confidence'] = 'high'

        return combined

    except Exception as e:
        logger.error(f"시장 신호 통합 오류: {e}")
        return {'overall_sentiment': 'error', 'recommendation': 'hold', 'confidence': 'low'}


# =============================================================================
# 🎯 상한가 기반 매수 판단 시스템 (기존 get_inquire_price 활용)
# =============================================================================

def get_tick_unit(price: int) -> int:
    """
    🎯 가격대별 호가단위 계산 (한국거래소 기준)

    Args:
        price: 주식 가격

    Returns:
        호가단위 (원)
    """
    if price < 1000:
        return 1
    elif price < 5000:
        return 5
    elif price < 10000:
        return 10
    elif price < 50000:
        return 50
    elif price < 100000:
        return 100
    elif price < 500000:
        return 500
    else:
        return 1000


def adjust_price_to_tick_unit(price: int, tick_unit: Optional[int] = None, round_up: bool = True) -> int:
    """
    🎯 호가단위에 맞는 가격으로 조정

    Args:
        price: 조정할 가격
        tick_unit: 호가단위 (None이면 자동 계산)
        round_up: True=올림, False=내림

    Returns:
        호가단위에 맞게 조정된 가격
    """
    if tick_unit is None:
        tick_unit = get_tick_unit(price)

    if round_up:
        # 올림 처리
        adjusted_price = ((price + tick_unit - 1) // tick_unit) * tick_unit
    else:
        # 내림 처리
        adjusted_price = (price // tick_unit) * tick_unit

    return adjusted_price


def get_stock_tick_info(stock_code: str) -> Optional[Dict]:
    """
    🎯 종목의 호가단위 정보 조회 (API 활용)

    Args:
        stock_code: 종목코드

    Returns:
        {
            'stock_code': str,
            'current_price': int,
            'tick_unit': int,           # 실제 호가단위 (API)
            'calculated_tick': int,     # 계산된 호가단위
            'tick_match': bool          # 호가단위 일치 여부
        }
    """
    try:
        # 현재가 정보 조회 (호가단위 포함)
        current_data = get_inquire_price("J", stock_code)
        if current_data is None or current_data.empty:
            logger.error(f"종목 {stock_code} 정보 조회 실패")
            return None

        stock_info = current_data.iloc[0]

        current_price = int(stock_info.get('stck_prpr', 0))
        api_tick_unit = int(stock_info.get('aspr_unit', 0))  # API 호가단위
        calculated_tick = get_tick_unit(current_price)       # 계산된 호가단위

        if current_price <= 0:
            logger.error(f"종목 {stock_code} 유효하지 않은 가격: {current_price}")
            return None

        result = {
            'stock_code': stock_code,
            'current_price': current_price,
            'tick_unit': api_tick_unit if api_tick_unit > 0 else calculated_tick,
            'calculated_tick': calculated_tick,
            'tick_match': api_tick_unit == calculated_tick,
            'price_range': f"{current_price:,}원 (호가단위: {api_tick_unit if api_tick_unit > 0 else calculated_tick}원)"
        }

        if not result['tick_match'] and api_tick_unit > 0:
            logger.warning(f"⚠️ {stock_code} 호가단위 불일치: API={api_tick_unit}원, 계산={calculated_tick}원")

        return result

    except Exception as e:
        logger.error(f"호가단위 정보 조회 오류 ({stock_code}): {e}")
        return None


def calculate_safe_order_prices(stock_code: str, target_price: Optional[int] = None,
                               is_buy: bool = True) -> Optional[Dict]:
    """
    🎯 안전한 주문가격 계산 (호가단위 준수)

    Args:
        stock_code: 종목코드
        target_price: 목표가격 (None이면 현재가 기준)
        is_buy: True=매수, False=매도

    Returns:
        {
            'original_price': int,      # 원래 가격
            'adjusted_price': int,      # 조정된 가격
            'tick_unit': int,          # 호가단위
            'price_difference': int,    # 가격 차이
            'is_safe': bool,           # 안전한 가격 여부
            'order_type_suggestion': str # 주문 방식 제안
        }
    """
    try:
        # 호가단위 정보 조회
        tick_info = get_stock_tick_info(stock_code)
        if not tick_info:
            return None

        current_price = tick_info['current_price']
        tick_unit = tick_info['tick_unit']

        # 목표가격 설정
        actual_target_price = target_price if target_price is not None else current_price

        # 호가단위에 맞게 가격 조정
        if is_buy:
            # 매수: 올림 처리 (불리하게 조정하여 안전성 확보)
            adjusted_price = adjust_price_to_tick_unit(actual_target_price, tick_unit, round_up=True)
        else:
            # 매도: 내림 처리 (불리하게 조정하여 안전성 확보)
            adjusted_price = adjust_price_to_tick_unit(actual_target_price, tick_unit, round_up=False)

        price_difference = adjusted_price - actual_target_price

        # 주문 방식 제안
        if adjusted_price == current_price:
            order_suggestion = "시장가 주문 권장"
        elif is_buy and adjusted_price > current_price:
            order_suggestion = "지정가 주문 (현재가보다 높음 - 즉시 체결 가능)"
        elif not is_buy and adjusted_price < current_price:
            order_suggestion = "지정가 주문 (현재가보다 낮음 - 즉시 체결 가능)"
        else:
            order_suggestion = "지정가 주문 (대기 주문)"

        result = {
            'stock_code': stock_code,
            'original_price': actual_target_price,
            'adjusted_price': adjusted_price,
            'current_price': current_price,
            'tick_unit': tick_unit,
            'price_difference': price_difference,
            'is_safe': True,  # 호가단위 조정되었으므로 안전
            'order_type_suggestion': order_suggestion,
            'adjustment_direction': "상향" if price_difference > 0 else "하향" if price_difference < 0 else "조정없음"
        }

        logger.info(f"🎯 {stock_code} 안전가격 계산: {actual_target_price:,}원 → {adjusted_price:,}원 "
                   f"(호가단위:{tick_unit}원, {result['adjustment_direction']})")

        return result

    except Exception as e:
        logger.error(f"안전 주문가격 계산 오류 ({stock_code}): {e}")
        return None


def analyze_price_limit_risk(stock_code: str) -> Optional[Dict]:
    """
    🎯 상한가/하한가 위험도 분석 (기존 get_inquire_price 활용)

    Args:
        stock_code: 종목코드 (6자리)

    Returns:
        {
            'stock_code': str,             # 종목코드
            'current_price': int,          # 현재가
            'upper_limit': int,            # 상한가
            'lower_limit': int,            # 하한가
            'base_price': int,             # 기준가(전일종가)
            'price_change_rate': float,    # 전일대비율
            'upper_limit_approach': float, # 상한가 근접률 (0~100%)
            'lower_limit_approach': float, # 하한가 근접률 (0~100%)
            'risk_level': str,            # 위험도 (LOW/MEDIUM/HIGH/CRITICAL)
            'buy_signal': str,            # 매수신호 (STRONG_BUY/BUY/HOLD/SELL/AVOID)
            'recommendation_reason': str   # 추천 사유
        }
    """
    try:
        # 현재가 정보 조회 (상한가/하한가 포함)
        current_data = get_inquire_price("J", stock_code)
        if current_data is None or current_data.empty:
            logger.error(f"종목 {stock_code} 현재가 조회 실패")
            return None

        stock_info = current_data.iloc[0]

        # 주요 가격 정보 추출
        current_price = int(stock_info.get('stck_prpr', 0))      # 현재가
        upper_limit = int(stock_info.get('stck_mxpr', 0))        # 상한가
        lower_limit = int(stock_info.get('stck_llam', 0))        # 하한가
        base_price = int(stock_info.get('stck_sdpr', 0))         # 기준가(전일종가)
        change_rate = float(stock_info.get('prdy_ctrt', 0))      # 전일대비율

        if current_price <= 0 or upper_limit <= 0 or lower_limit <= 0 or base_price <= 0:
            logger.error(f"종목 {stock_code} 가격 정보 불완전")
            return None

        # 🎯 상한가/하한가 근접률 계산
        price_range = upper_limit - lower_limit  # 전체 가격 범위

        # 상한가 근접률: 기준가 대비 현재가가 상한가에 얼마나 가까운지 (0~100%)
        if current_price >= upper_limit:
            upper_limit_approach = 100.0  # 상한가 도달
        else:
            # (현재가 - 기준가) / (상한가 - 기준가) * 100
            upper_range = upper_limit - base_price
            if upper_range > 0:
                upper_limit_approach = ((current_price - base_price) / upper_range) * 100
                upper_limit_approach = max(0, min(100, upper_limit_approach))
            else:
                upper_limit_approach = 0.0

        # 하한가 근접률: 기준가 대비 현재가가 하한가에 얼마나 가까운지 (0~100%)
        if current_price <= lower_limit:
            lower_limit_approach = 100.0  # 하한가 도달
        else:
            # (기준가 - 현재가) / (기준가 - 하한가) * 100
            lower_range = base_price - lower_limit
            if lower_range > 0 and current_price < base_price:
                lower_limit_approach = ((base_price - current_price) / lower_range) * 100
                lower_limit_approach = max(0, min(100, lower_limit_approach))
            else:
                lower_limit_approach = 0.0

        # 🎯 위험도 및 매수 신호 판정
        risk_level, buy_signal, reason = _determine_buy_signal(
            upper_limit_approach, lower_limit_approach, change_rate, current_price
        )

        result = {
            'stock_code': stock_code,
            'current_price': current_price,
            'upper_limit': upper_limit,
            'lower_limit': lower_limit,
            'base_price': base_price,
            'price_change_rate': round(change_rate, 2),
            'upper_limit_approach': round(upper_limit_approach, 1),
            'lower_limit_approach': round(lower_limit_approach, 1),
            'risk_level': risk_level,
            'buy_signal': buy_signal,
            'recommendation_reason': reason,
            'price_range': price_range,
            'analysis_time': datetime.now().strftime('%H:%M:%S')
        }

        logger.info(f"🎯 {stock_code} 가격분석: {current_price:,}원 ({change_rate:+.1f}%) "
                   f"상한가근접{upper_limit_approach:.1f}% → {buy_signal}")

        return result

    except Exception as e:
        logger.error(f"상한가 위험도 분석 오류 ({stock_code}): {e}")
        return None


def _determine_buy_signal(upper_approach: float, lower_approach: float,
                         change_rate: float, current_price: int) -> Tuple[str, str, str]:
    """
    위험도 및 매수 신호 판정

    Returns:
        (risk_level, buy_signal, reason)
    """

    # 🎯 상한가 근접 위험도 체크 (최우선)
    if upper_approach >= 95:
        return "CRITICAL", "AVOID", f"상한가 임박 ({upper_approach:.1f}%) - 매수 위험"
    elif upper_approach >= 85:
        return "HIGH", "AVOID", f"상한가 근접 ({upper_approach:.1f}%) - 고위험 구간"
    elif upper_approach >= 70:
        return "HIGH", "HOLD", f"급등 구간 ({upper_approach:.1f}%) - 신중 관망"
    elif upper_approach >= 60:
        return "MEDIUM", "HOLD", f"상승 과열 ({upper_approach:.1f}%) - 조정 대기"

    # 🎯 하한가 근접 체크
    if lower_approach >= 95:
        return "CRITICAL", "AVOID", f"하한가 임박 ({lower_approach:.1f}%) - 추가 하락 위험"
    elif lower_approach >= 80:
        return "HIGH", "HOLD", f"급락 구간 ({lower_approach:.1f}%) - 바닥 확인 필요"
    elif lower_approach >= 60:
        return "MEDIUM", "BUY", f"과매도 구간 ({lower_approach:.1f}%) - 반등 기회"

    # 🎯 적정 매수 구간 판정
    if 20 <= upper_approach <= 50:
        if change_rate > 0:
            return "LOW", "BUY", f"상승 추세 ({upper_approach:.1f}%) - 매수 적기"
        else:
            return "LOW", "BUY", f"조정 매수 ({upper_approach:.1f}%) - 좋은 진입점"
    elif 0 <= upper_approach < 20:
        if change_rate >= 0:
            return "LOW", "STRONG_BUY", f"저점 돌파 ({upper_approach:.1f}%) - 강력 매수"
        else:
            return "LOW", "BUY", f"저점 근처 ({upper_approach:.1f}%) - 매수 기회"
    elif 50 < upper_approach < 70:
        return "MEDIUM", "HOLD", f"상승 중반 ({upper_approach:.1f}%) - 신중 접근"
    else:
        return "LOW", "BUY", f"정상 범위 ({upper_approach:.1f}%) - 매수 가능"


def smart_buy_decision(stock_code: str, target_amount: int = 1000000) -> Optional[Dict]:
    """
    🎯 스마트 매수 의사결정 (상한가 고려 + 포지션 관리 + 호가단위 준수)

    Args:
        stock_code: 종목코드
        target_amount: 목표 투자금액 (기본 100만원)

    Returns:
        {
            'buy_decision': bool,           # 매수 결정 (True/False)
            'buy_amount': int,              # 매수 금액
            'buy_quantity': int,            # 매수 수량
            'entry_strategy': str,          # 진입 전략
            'stop_loss_price': int,         # 손절가 (호가단위 적용)
            'target_price': int,            # 목표가 (호가단위 적용)
            'position_size': str,           # 포지션 크기
            'risk_management': List[str],   # 위험관리 사항
            'analysis_summary': Dict,       # 분석 요약
            'tick_info': Dict               # 호가단위 정보
        }
    """
    try:
        logger.info(f"🎯 스마트 매수 의사결정 시작: {stock_code} (목표: {target_amount:,}원)")

        # 1단계: 상한가 위험도 분석
        risk_analysis = analyze_price_limit_risk(stock_code)
        if not risk_analysis:
            return {'buy_decision': False, 'reason': '가격 분석 실패'}

        current_price = risk_analysis['current_price']
        buy_signal = risk_analysis['buy_signal']
        risk_level = risk_analysis['risk_level']
        upper_approach = risk_analysis['upper_limit_approach']

        # 2단계: 기본 매수 결정
        buy_decision = buy_signal in ['STRONG_BUY', 'BUY']

        if not buy_decision:
            return {
                'buy_decision': False,
                'reason': risk_analysis['recommendation_reason'],
                'analysis_summary': risk_analysis
            }

        # 3단계: 호가단위 정보 조회
        tick_info = get_stock_tick_info(stock_code)
        if not tick_info:
            logger.warning(f"⚠️ {stock_code} 호가단위 정보 조회 실패 - 기본값 사용")
            tick_unit = get_tick_unit(current_price)
            tick_info = {
                'stock_code': stock_code,
                'current_price': current_price,
                'tick_unit': tick_unit,
                'calculated_tick': tick_unit,
                'tick_match': True
            }

        # 4단계: 포지션 크기 결정 (위험도 기반)
        if buy_signal == 'STRONG_BUY' and risk_level == 'LOW':
            position_ratio = 0.8  # 80% 포지션
            entry_strategy = "적극적 매수 - 2회 분할"
        elif buy_signal == 'BUY' and risk_level == 'LOW':
            position_ratio = 0.6  # 60% 포지션
            entry_strategy = "일반 매수 - 3회 분할"
        elif buy_signal == 'BUY' and risk_level == 'MEDIUM':
            position_ratio = 0.3  # 30% 포지션
            entry_strategy = "신중 매수 - 5회 분할"
        else:
            position_ratio = 0.2  # 20% 포지션
            entry_strategy = "시험 매수 - 소량"

        # 5단계: 실제 매수 금액 및 수량 계산
        buy_amount = int(target_amount * position_ratio)
        buy_quantity = buy_amount // current_price
        actual_buy_amount = buy_quantity * current_price

        # 6단계: 손절가/목표가 설정 (호가단위 고려)
        if risk_level == 'LOW':
            stop_loss_rate = 0.05  # 5% 손절
            target_profit_rate = 0.15  # 15% 익절
        elif risk_level == 'MEDIUM':
            stop_loss_rate = 0.03  # 3% 손절 (타이트)
            target_profit_rate = 0.10  # 10% 익절
        else:
            stop_loss_rate = 0.02  # 2% 손절 (매우 타이트)
            target_profit_rate = 0.07  # 7% 익절

        # 🎯 호가단위에 맞는 안전한 가격 계산
        raw_stop_loss = int(current_price * (1 - stop_loss_rate))
        raw_target_price = int(current_price * (1 + target_profit_rate))

        # 손절가 조정 (매도이므로 내림)
        stop_loss_safe = calculate_safe_order_prices(stock_code, raw_stop_loss, is_buy=False)
        if stop_loss_safe:
            stop_loss_price = stop_loss_safe['adjusted_price']
        else:
            stop_loss_price = adjust_price_to_tick_unit(raw_stop_loss, tick_info['tick_unit'], round_up=False)

        # 목표가 조정 (매도이므로 내림)
        target_price_safe = calculate_safe_order_prices(stock_code, raw_target_price, is_buy=False)
        if target_price_safe:
            target_price = target_price_safe['adjusted_price']
        else:
            target_price = adjust_price_to_tick_unit(raw_target_price, tick_info['tick_unit'], round_up=False)

        # 7단계: 위험관리 사항
        risk_management = []

        if upper_approach > 50:
            risk_management.append("상한가 50% 이상 - 포지션 축소")
        if risk_level in ['MEDIUM', 'HIGH']:
            risk_management.append("분할 매수 필수")
        if current_price < 1000:
            risk_management.append("저가주 - 변동성 주의")
        if current_price > 100000:
            risk_management.append("고가주 - 유동성 확인")
        if not tick_info.get('tick_match', True):
            risk_management.append("호가단위 불일치 감지 - 주문시 재확인 필요")

        # 🎯 매수가격도 호가단위에 맞게 조정 (매수이므로 올림)
        buy_price_safe = calculate_safe_order_prices(stock_code, current_price, is_buy=True)
        if buy_price_safe:
            safe_buy_price = buy_price_safe['adjusted_price']
            # 수량 재계산 (안전한 가격 기준)
            buy_quantity = buy_amount // safe_buy_price
            actual_buy_amount = buy_quantity * safe_buy_price
        else:
            safe_buy_price = current_price

        # 최종 결과
        result = {
            'buy_decision': True,
            'buy_amount': actual_buy_amount,
            'buy_quantity': buy_quantity,
            'current_price': current_price,
            'safe_buy_price': safe_buy_price,  # 🆕 호가단위 적용된 안전한 매수가
            'entry_strategy': entry_strategy,
            'stop_loss_price': stop_loss_price,
            'target_price': target_price,
            'position_size': f"{position_ratio*100:.0f}%",
            'expected_return': f"{target_profit_rate*100:.0f}%",
            'max_loss': f"{stop_loss_rate*100:.0f}%",
            'risk_management': risk_management,
            'analysis_summary': risk_analysis,
            'tick_info': tick_info,  # 🆕 호가단위 정보
            'price_adjustments': {   # 🆕 가격 조정 내역
                'raw_stop_loss': raw_stop_loss,
                'adjusted_stop_loss': stop_loss_price,
                'raw_target': raw_target_price,
                'adjusted_target': target_price,
                'stop_loss_diff': stop_loss_price - raw_stop_loss,
                'target_diff': target_price - raw_target_price
            },
            'decision_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        logger.info(f"🎯 매수 결정: {stock_code} {buy_quantity:,}주 ({actual_buy_amount:,}원) "
                   f"진입{safe_buy_price:,} 목표{target_price:,} 손절{stop_loss_price:,} "
                   f"호가단위:{tick_info['tick_unit']}원")

        return result

    except Exception as e:
        logger.error(f"스마트 매수 의사결정 오류 ({stock_code}): {e}")
        return {'buy_decision': False, 'reason': f'분석 오류: {e}'}


def batch_buy_analysis(stock_codes: List[str], budget: int = 5000000) -> Optional[pd.DataFrame]:
    """
    🎯 다중 종목 매수 분석 및 포트폴리오 구성

    Args:
        stock_codes: 분석할 종목 리스트
        budget: 총 투자 예산 (기본 500만원)

    Returns:
        매수 추천 종목 DataFrame (우선순위별 정렬)
    """
    try:
        logger.info(f"🎯 다중 종목 매수 분석 시작: {len(stock_codes)}개 종목, 예산 {budget:,}원")

        results = []
        individual_budget = budget // max(len(stock_codes), 5)  # 종목당 최대 예산

        for i, stock_code in enumerate(stock_codes, 1):
            try:
                logger.info(f"📊 {i}/{len(stock_codes)} 분석: {stock_code}")

                # 스마트 매수 분석
                buy_analysis = smart_buy_decision(stock_code, individual_budget)

                if buy_analysis and buy_analysis.get('buy_decision'):
                    summary = buy_analysis['analysis_summary']

                    result_row = {
                        'stock_code': stock_code,
                        'buy_signal': summary['buy_signal'],
                        'risk_level': summary['risk_level'],
                        'current_price': summary['current_price'],
                        'upper_limit_approach': summary['upper_limit_approach'],
                        'price_change_rate': summary['price_change_rate'],
                        'buy_amount': buy_analysis['buy_amount'],
                        'buy_quantity': buy_analysis['buy_quantity'],
                        'position_size': buy_analysis['position_size'],
                        'entry_strategy': buy_analysis['entry_strategy'],
                        'expected_return': buy_analysis['expected_return'],
                        'stop_loss_price': buy_analysis['stop_loss_price'],
                        'target_price': buy_analysis['target_price'],
                        'recommendation_reason': summary['recommendation_reason']
                    }
                    results.append(result_row)

                time.sleep(0.2)  # API 제한 방지

            except Exception as e:
                logger.error(f"종목 {stock_code} 분석 오류: {e}")
                continue

        if results:
            df = pd.DataFrame(results)

            # 우선순위 정렬 (STRONG_BUY > BUY, LOW risk > MEDIUM risk)
            signal_priority = {'STRONG_BUY': 2, 'BUY': 1}
            risk_priority = {'LOW': 3, 'MEDIUM': 2, 'HIGH': 1}

            df['signal_score'] = df['buy_signal'].map(signal_priority)
            df['risk_score'] = df['risk_level'].map(risk_priority)
            df['total_score'] = df['signal_score'] + df['risk_score']

            # 우선순위 정렬
            df = df.sort_values(['total_score', 'upper_limit_approach'],
                               ascending=[False, True])

            # 임시 점수 컬럼 제거
            df = df.drop(['signal_score', 'risk_score', 'total_score'], axis=1)

            # 예산 배분 확인
            total_investment = df['buy_amount'].sum()
            df['budget_ratio'] = (df['buy_amount'] / budget * 100).round(1)

            logger.info(f"🎯 매수 추천 완료: {len(df)}개 종목, 총 투자액 {total_investment:,}원 "
                       f"({total_investment/budget*100:.1f}%)")

            return df
        else:
            logger.warning("매수 추천 종목 없음")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"다중 종목 매수 분석 오류: {e}")
        return None


def print_buy_decision_summary(analysis_result: Dict):
    """매수 의사결정 결과 요약 출력"""
    if not analysis_result or not analysis_result.get('buy_decision'):
        print(f"❌ 매수 비추천: {analysis_result.get('reason', '알 수 없음')}")
        return

    print("=" * 60)
    print(f"🎯 매수 의사결정 결과")
    print("=" * 60)
    print(f"종목코드: {analysis_result['analysis_summary']['stock_code']}")
    print(f"현재가: {analysis_result['current_price']:,}원")
    print(f"매수신호: {analysis_result['analysis_summary']['buy_signal']}")
    print(f"위험도: {analysis_result['analysis_summary']['risk_level']}")
    print(f"상한가 근접률: {analysis_result['analysis_summary']['upper_limit_approach']:.1f}%")
    print()
    print(f"💰 매수 계획:")
    print(f"  - 매수 금액: {analysis_result['buy_amount']:,}원")
    print(f"  - 매수 수량: {analysis_result['buy_quantity']:,}주")
    print(f"  - 포지션 크기: {analysis_result['position_size']}")
    print(f"  - 진입 전략: {analysis_result['entry_strategy']}")
    print()
    print(f"🎯 목표 설정:")
    print(f"  - 목표가: {analysis_result['target_price']:,}원 (+{analysis_result['expected_return']})")
    print(f"  - 손절가: {analysis_result['stop_loss_price']:,}원 ({analysis_result['max_loss']})")
    print()
    if analysis_result['risk_management']:
        print(f"⚠️ 위험관리:")
        for risk in analysis_result['risk_management']:
            print(f"  - {risk}")
    print()
    print(f"📋 추천 사유: {analysis_result['analysis_summary']['recommendation_reason']}")
    print("=" * 60)


# =============================================================================
# 🎯 데모 및 테스트 함수들
# =============================================================================

def demo_price_limit_analysis():
    """🎯 상한가 기반 매수 판단 시스템 데모"""
    try:
        print("=" * 70)
        print("🎯 상한가 기반 매수 판단 시스템 데모")
        print("=" * 70)

        # 샘플 종목들 (대형주)
        sample_stocks = [
            ("005930", "삼성전자"),
            ("000660", "SK하이닉스"),
            ("035420", "NAVER"),
            ("005490", "POSCO홀딩스")
        ]

        print("\n📊 1. 개별 종목 상한가 위험도 분석")
        print("-" * 50)

        for stock_code, stock_name in sample_stocks[:2]:  # 처음 2개만 상세 분석
            try:
                print(f"\n🔍 {stock_code} ({stock_name}) 분석:")

                # 상한가 위험도 분석
                risk_analysis = analyze_price_limit_risk(stock_code)
                if risk_analysis:
                    print(f"   현재가: {risk_analysis['current_price']:,}원")
                    print(f"   상한가: {risk_analysis['upper_limit']:,}원")
                    print(f"   하한가: {risk_analysis['lower_limit']:,}원")
                    print(f"   등락률: {risk_analysis['price_change_rate']:+.1f}%")
                    print(f"   상한가 근접률: {risk_analysis['upper_limit_approach']:.1f}%")
                    print(f"   하한가 근접률: {risk_analysis['lower_limit_approach']:.1f}%")
                    print(f"   위험도: {risk_analysis['risk_level']}")
                    print(f"   매수신호: {risk_analysis['buy_signal']}")
                    print(f"   추천사유: {risk_analysis['recommendation_reason']}")
                else:
                    print("   ❌ 분석 실패")

                time.sleep(1)  # API 제한 방지

            except Exception as e:
                print(f"   ❌ 분석 오류: {e}")

        print(f"\n📊 2. 스마트 매수 의사결정 (목표: 1,000,000원)")
        print("-" * 50)

        for stock_code, stock_name in sample_stocks[2:3]:  # 1개 종목 매수 분석
            try:
                print(f"\n💰 {stock_code} ({stock_name}) 매수 의사결정:")

                # 스마트 매수 분석
                buy_decision = smart_buy_decision(stock_code, 1000000)
                if buy_decision:
                    if buy_decision.get('buy_decision'):
                        print(f"   ✅ 매수 추천!")
                        print(f"   매수 금액: {buy_decision['buy_amount']:,}원")
                        print(f"   매수 수량: {buy_decision['buy_quantity']:,}주")
                        print(f"   포지션 크기: {buy_decision['position_size']}")
                        print(f"   진입 전략: {buy_decision['entry_strategy']}")
                        print(f"   목표가: {buy_decision['target_price']:,}원 (+{buy_decision['expected_return']})")
                        print(f"   손절가: {buy_decision['stop_loss_price']:,}원 ({buy_decision['max_loss']})")

                        if buy_decision['risk_management']:
                            print(f"   ⚠️ 위험관리: {', '.join(buy_decision['risk_management'])}")
                    else:
                        error_message = buy_decision.get('reason', '알 수 없음') if buy_decision else '분석 실패'
                        print(f"   ❌ 매수 비추천: {error_message}")
                else:
                    print("   ❌ 분석 실패")

                time.sleep(1)

            except Exception as e:
                print(f"   ❌ 분석 오류: {e}")

        print(f"\n📊 3. 다중 종목 포트폴리오 분석 (예산: 5,000,000원)")
        print("-" * 50)

        stock_codes = [code for code, _ in sample_stocks]
        portfolio_analysis = batch_buy_analysis(stock_codes, 5000000)

        if portfolio_analysis is not None and not portfolio_analysis.empty:
            print(f"\n🎯 매수 추천 종목 ({len(portfolio_analysis)}개):")
            for idx, (_, row) in enumerate(portfolio_analysis.iterrows(), 1):
                print(f"{idx}. {row['stock_code']} - {row['buy_signal']} (위험도: {row['risk_level']})")
                print(f"   현재가: {row['current_price']:,}원 ({row['price_change_rate']:+.1f}%)")
                print(f"   상한가 근접: {row['upper_limit_approach']:.1f}%")
                print(f"   매수금액: {row['buy_amount']:,}원 ({row['buy_quantity']:,}주)")
                print(f"   목표수익: {row['expected_return']}, 예산비중: {row['budget_ratio']:.1f}%")
                print(f"   전략: {row['entry_strategy']}")
                print()
        else:
            print("   ❌ 매수 추천 종목 없음")

        print("🎯 데모 완료!")
        print("=" * 70)

    except Exception as e:
        logger.error(f"데모 실행 오류: {e}")
        print(f"❌ 데모 실행 중 오류 발생: {e}")


def test_specific_stock_analysis(stock_code: str, investment_amount: int = 1000000):
    """
    🎯 특정 종목 상세 분석 테스트

    Args:
        stock_code: 종목코드
        investment_amount: 투자 금액
    """
    try:
        print("=" * 70)
        print(f"🎯 {stock_code} 종목 상세 분석")
        print("=" * 70)

        # 1. 상한가 위험도 분석
        print("\n📊 1. 상한가/하한가 위험도 분석")
        print("-" * 40)

        risk_analysis = analyze_price_limit_risk(stock_code)
        if risk_analysis:
            print(f"종목코드: {risk_analysis['stock_code']}")
            print(f"현재가: {risk_analysis['current_price']:,}원")
            print(f"기준가(전일종가): {risk_analysis['base_price']:,}원")
            print(f"상한가: {risk_analysis['upper_limit']:,}원")
            print(f"하한가: {risk_analysis['lower_limit']:,}원")
            print(f"전일대비: {risk_analysis['price_change_rate']:+.2f}%")
            print(f"가격범위: {risk_analysis['price_range']:,}원")
            print()
            print(f"상한가 근접률: {risk_analysis['upper_limit_approach']:.1f}%")
            print(f"하한가 근접률: {risk_analysis['lower_limit_approach']:.1f}%")
            print(f"위험도: {risk_analysis['risk_level']}")
            print(f"매수신호: {risk_analysis['buy_signal']}")
            print(f"추천사유: {risk_analysis['recommendation_reason']}")
        else:
            print("❌ 위험도 분석 실패")
            return

        # 2. 스마트 매수 의사결정
        print(f"\n💰 2. 스마트 매수 의사결정 (목표: {investment_amount:,}원)")
        print("-" * 40)

        buy_decision = smart_buy_decision(stock_code, investment_amount)
        if buy_decision:
            print_buy_decision_summary(buy_decision)
        else:
            print("❌ 매수 의사결정 실패")

        print("\n🎯 분석 완료!")
        print("=" * 70)

    except Exception as e:
        logger.error(f"종목 분석 테스트 오류: {e}")
        print(f"❌ 분석 중 오류 발생: {e}")


# 테스트 실행을 위한 예시 함수
if __name__ == "__main__":
    # 예시 1: 전체 데모 실행
    # demo_price_limit_analysis()

    # 예시 2: 특정 종목 분석
    # test_specific_stock_analysis("005930", 2000000)  # 삼성전자 200만원 투자

    pass


# =============================================================================
# 🎯 호가단위 오류 해결 전용 함수들
# =============================================================================

def test_tick_unit_functions(stock_code: str = "000990"):
    """
    🎯 호가단위 관련 함수들 테스트

    Args:
        stock_code: 테스트할 종목코드 (기본값: 000990)
    """
    try:
        print("=" * 70)
        print(f"🎯 호가단위 오류 해결 테스트: {stock_code}")
        print("=" * 70)

        # 1. 호가단위 정보 조회
        print("\n📊 1. 호가단위 정보 조회")
        print("-" * 40)

        tick_info = get_stock_tick_info(stock_code)
        if tick_info:
            print(f"종목코드: {tick_info['stock_code']}")
            print(f"현재가: {tick_info['current_price']:,}원")
            print(f"API 호가단위: {tick_info['tick_unit']}원")
            print(f"계산된 호가단위: {tick_info['calculated_tick']}원")
            print(f"호가단위 일치: {'✅' if tick_info['tick_match'] else '❌'}")
            print(f"가격 범위: {tick_info['price_range']}")
        else:
            print("❌ 호가단위 정보 조회 실패")
            return

        # 2. 다양한 가격대 호가단위 테스트
        print(f"\n📊 2. 가격대별 호가단위 테스트")
        print("-" * 40)

        test_prices = [500, 1500, 7500, 25000, 75000, 250000, 750000]
        for price in test_prices:
            tick = get_tick_unit(price)
            print(f"{price:,}원 → 호가단위: {tick}원")

        # 3. 안전한 주문가격 계산 테스트
        print(f"\n💰 3. 안전한 주문가격 계산 테스트")
        print("-" * 40)

        current_price = tick_info['current_price']

        # 매수 가격 테스트
        test_buy_prices = [
            current_price,
            current_price + 10,  # 약간 높은 가격
            int(current_price * 1.05),  # 5% 높은 가격
        ]

        for test_price in test_buy_prices:
            buy_safe = calculate_safe_order_prices(stock_code, test_price, is_buy=True)
            if buy_safe:
                print(f"매수 {test_price:,}원 → {buy_safe['adjusted_price']:,}원 "
                      f"({buy_safe['adjustment_direction']}, 차이:{buy_safe['price_difference']:+,}원)")

        # 매도 가격 테스트
        test_sell_prices = [
            current_price,
            current_price - 10,  # 약간 낮은 가격
            int(current_price * 0.95),  # 5% 낮은 가격
        ]

        for test_price in test_sell_prices:
            sell_safe = calculate_safe_order_prices(stock_code, test_price, is_buy=False)
            if sell_safe:
                print(f"매도 {test_price:,}원 → {sell_safe['adjusted_price']:,}원 "
                      f"({sell_safe['adjustment_direction']}, 차이:{sell_safe['price_difference']:+,}원)")

        # 4. 호가단위 적용된 스마트 매수 테스트
        print(f"\n🎯 4. 호가단위 적용된 스마트 매수 테스트")
        print("-" * 40)

        buy_decision = smart_buy_decision(stock_code, 1000000)
        if buy_decision and buy_decision.get('buy_decision'):
            print(f"✅ 매수 추천!")
            print(f"현재가: {buy_decision['current_price']:,}원")
            print(f"안전 매수가: {buy_decision['safe_buy_price']:,}원")
            print(f"목표가: {buy_decision['target_price']:,}원")
            print(f"손절가: {buy_decision['stop_loss_price']:,}원")
            print(f"호가단위: {buy_decision['tick_info']['tick_unit']}원")

            # 가격 조정 내역
            adjustments = buy_decision['price_adjustments']
            print(f"\n📋 가격 조정 내역:")
            print(f"  손절가: {adjustments['raw_stop_loss']:,}원 → {adjustments['adjusted_stop_loss']:,}원 "
                  f"({adjustments['stop_loss_diff']:+,}원)")
            print(f"  목표가: {adjustments['raw_target']:,}원 → {adjustments['adjusted_target']:,}원 "
                  f"({adjustments['target_diff']:+,}원)")
        else:
            reason = "알 수 없음"
            if buy_decision is not None:
                reason = buy_decision.get('reason', '알 수 없음')
            print(f"❌ 매수 비추천: {reason}")

        print(f"\n🎯 테스트 완료!")
        print("=" * 70)

    except Exception as e:
        logger.error(f"호가단위 테스트 오류: {e}")
        print(f"❌ 테스트 중 오류 발생: {e}")


def fix_order_price_for_existing_position(stock_code: str, order_price: int, is_buy: bool = False) -> Dict:
    """
    🎯 기존 포지션의 주문가격 호가단위 오류 수정

    Args:
        stock_code: 종목코드
        order_price: 원래 주문가격
        is_buy: True=매수, False=매도

    Returns:
        수정된 주문가격 정보
    """
    try:
        logger.info(f"🔧 호가단위 오류 수정: {stock_code} {order_price:,}원 ({'매수' if is_buy else '매도'})")

        # 안전한 주문가격 계산
        safe_price_info = calculate_safe_order_prices(stock_code, order_price, is_buy)

        if safe_price_info:
            result = {
                'success': True,
                'original_price': order_price,
                'fixed_price': safe_price_info['adjusted_price'],
                'price_difference': safe_price_info['price_difference'],
                'tick_unit': safe_price_info['tick_unit'],
                'adjustment_direction': safe_price_info['adjustment_direction'],
                'suggestion': safe_price_info['order_type_suggestion']
            }

            logger.info(f"✅ 가격 수정 완료: {order_price:,}원 → {result['fixed_price']:,}원 "
                       f"({result['adjustment_direction']}, 차이:{result['price_difference']:+,}원)")

        else:
            # 백업: 기본 호가단위 계산
            tick_unit = get_tick_unit(order_price)
            fixed_price = adjust_price_to_tick_unit(order_price, tick_unit, round_up=is_buy)

            result = {
                'success': True,
                'original_price': order_price,
                'fixed_price': fixed_price,
                'price_difference': fixed_price - order_price,
                'tick_unit': tick_unit,
                'adjustment_direction': "상향" if fixed_price > order_price else "하향" if fixed_price < order_price else "조정없음",
                'suggestion': "기본 호가단위 적용"
            }

            logger.warning(f"⚠️ 백업 방식으로 가격 수정: {order_price:,}원 → {fixed_price:,}원")

        return result

    except Exception as e:
        logger.error(f"주문가격 수정 오류 ({stock_code}): {e}")
        return {
            'success': False,
            'error': str(e),
            'original_price': order_price
        }


def get_safe_prices_for_trading_system(stock_code: str) -> Optional[Dict]:
    """
    🎯 트레이딩 시스템용 안전한 가격 세트 제공

    Args:
        stock_code: 종목코드

    Returns:
        트레이딩 시스템에서 사용할 안전한 가격들
    """
    try:
        # 현재가 및 호가단위 정보
        tick_info = get_stock_tick_info(stock_code)
        if not tick_info:
            logger.error(f"종목 {stock_code} 정보 조회 실패")
            return None

        current_price = tick_info['current_price']
        tick_unit = tick_info['tick_unit']

        # 다양한 상황의 안전한 가격 계산
        prices = {
            'current_price': current_price,
            'tick_unit': tick_unit,

            # 매수 관련 가격 (올림)
            'safe_buy_current': adjust_price_to_tick_unit(current_price, tick_unit, round_up=True),
            'safe_buy_plus_1tick': current_price + tick_unit,
            'safe_buy_plus_2tick': current_price + (tick_unit * 2),

            # 매도 관련 가격 (내림)
            'safe_sell_current': adjust_price_to_tick_unit(current_price, tick_unit, round_up=False),
            'safe_sell_minus_1tick': current_price - tick_unit,
            'safe_sell_minus_2tick': current_price - (tick_unit * 2),

            # 일반적인 손절/익절 가격 (호가단위 적용)
            'stop_loss_3pct': adjust_price_to_tick_unit(int(current_price * 0.97), tick_unit, round_up=False),
            'stop_loss_5pct': adjust_price_to_tick_unit(int(current_price * 0.95), tick_unit, round_up=False),
            'take_profit_5pct': adjust_price_to_tick_unit(int(current_price * 1.05), tick_unit, round_up=False),
            'take_profit_10pct': adjust_price_to_tick_unit(int(current_price * 1.10), tick_unit, round_up=False),
        }

        # 검증: 모든 가격이 호가단위에 맞는지 확인
        for price_name, price_value in prices.items():
            if price_name not in ['current_price', 'tick_unit']:
                if price_value % tick_unit != 0:
                    logger.warning(f"⚠️ {stock_code} {price_name}: {price_value:,}원이 호가단위에 맞지 않음")

        result = {
            'stock_code': stock_code,
            'prices': prices,
            'tick_info': tick_info,
            'generated_time': datetime.now().strftime('%H:%M:%S')
        }

        logger.info(f"🎯 {stock_code} 안전가격 세트 생성 완료 (호가단위: {tick_unit}원)")

        return result

    except Exception as e:
        logger.error(f"안전 가격 세트 생성 오류 ({stock_code}): {e}")
        return None


# =============================================================================
# 🎯 잔고 및 포지션 조회 API
# =============================================================================

def get_stock_balance(output_dv: str = "01", tr_cont: str = "",
                     FK100: str = "", NK100: str = "") -> Optional[pd.DataFrame]:
    """
    주식잔고조회 (TR: TTTC8434R)

    Args:
        output_dv: 출력구분 ("01": 일반조회)
        tr_cont: 연속거래키
        FK100: 연속조회검색조건100
        NK100: 연속조회키100

    Returns:
        주식잔고 데이터 (보유종목별 정보)
    """
    url = '/uapi/domestic-stock/v1/trading/inquire-balance'
    tr_id = "TTTC8434R"  # 주식잔고조회

    # KIS 환경 정보 안전 조회
    tr_env = kis.getTREnv()
    if tr_env is None:
        logger.error("❌ KIS 환경 정보 없음 - 인증이 필요합니다")
        return None

    params = {
        "CANO": tr_env.my_acct,           # 계좌번호
        "ACNT_PRDT_CD": tr_env.my_prod,  # 계좌상품코드
        "AFHR_FLPR_YN": "N",              # 시간외단일가여부
        "OFL_YN": "",                     # 오프라인여부
        "INQR_DVSN": "02",                # 조회구분(01:대출일별, 02:종목별)
        "UNPR_DVSN": "01",                # 단가구분(01:기준가, 02:현재가)
        "FUND_STTL_ICLD_YN": "N",         # 펀드결제분포함여부
        "FNCG_AMT_AUTO_RDPT_YN": "N",     # 융자금액자동상환여부
        "PRCS_DVSN": "00",                # 처리구분(00:전일매매포함, 01:전일매매미포함)
        "CTX_AREA_FK100": "",          # 연속조회검색조건100
        "CTX_AREA_NK100": ""           # 연속조회키100
    }

    try:
        logger.info("💰 주식잔고조회 API 호출")
        res = kis._url_fetch(url, tr_id, tr_cont, params)

        if res and res.isOK():
            body = res.getBody()

            # output1: 개별 종목 잔고
            output1_data = getattr(body, 'output1', [])
            # output2: 잔고요약
            output2_data = getattr(body, 'output2', [])

            if output1_data:
                balance_df = pd.DataFrame(output1_data)
                logger.info(f"✅ 주식잔고조회 성공: {len(balance_df)}개 종목")

                # 요약 정보도 추가
                if output2_data:
                    summary = output2_data[0] if isinstance(output2_data, list) else output2_data
                    logger.info(f"📊 잔고요약: 총평가액={summary.get('tot_evlu_amt', '0'):>12}원, "
                               f"평가손익={summary.get('evlu_pfls_smtl_amt', '0'):>10}원")

                return balance_df
            else:
                logger.info("📊 보유 종목 없음")
                return pd.DataFrame()
        else:
            logger.error("❌ 주식잔고조회 실패")
            return None

    except Exception as e:
        logger.error(f"❌ 주식잔고조회 오류: {e}")
        return None


def get_account_balance() -> Optional[Dict]:
    """
    계좌잔고조회 - 요약 정보

    Returns:
        계좌 요약 정보
    """
    try:
        balance_data = get_stock_balance()
        if balance_data is None:
            return None

        if balance_data.empty:
            return {
                'total_stocks': 0,
                'total_value': 0,
                'total_profit_loss': 0,
                'stocks': []
            }

        # 보유 종목 요약 생성
        stocks = []
        total_value = 0
        total_profit_loss = 0

        for _, row in balance_data.iterrows():
            stock_code = row.get('pdno', '')  # 종목코드
            stock_name = row.get('prdt_name', '')  # 종목명
            quantity = int(row.get('hldg_qty', 0))  # 보유수량
            avg_price = float(row.get('pchs_avg_pric', 0))  # 매입평균가
            current_price = float(row.get('prpr', 0))  # 현재가
            eval_amt = int(row.get('evlu_amt', 0))  # 평가금액
            profit_loss = int(row.get('evlu_pfls_amt', 0))  # 평가손익
            profit_loss_rate = float(row.get('evlu_pfls_rt', 0))  # 평가손익률

            if quantity > 0:  # 실제 보유 종목만
                stock_info = {
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'quantity': quantity,
                    'avg_price': avg_price,
                    'current_price': current_price,
                    'eval_amount': eval_amt,
                    'profit_loss': profit_loss,
                    'profit_loss_rate': profit_loss_rate
                }
                stocks.append(stock_info)
                total_value += eval_amt
                total_profit_loss += profit_loss

        result = {
            'total_stocks': len(stocks),
            'total_value': total_value,
            'total_profit_loss': total_profit_loss,
            'total_profit_loss_rate': (total_profit_loss / total_value * 100) if total_value > 0 else 0.0,
            'stocks': stocks,
            'inquiry_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        logger.info(f"💰 계좌요약: {len(stocks)}개 종목, 총 {total_value:,}원, "
                   f"손익 {total_profit_loss:+,}원 ({result['total_profit_loss_rate']:+.2f}%)")

        return result

    except Exception as e:
        logger.error(f"계좌잔고 요약 오류: {e}")
        return None

