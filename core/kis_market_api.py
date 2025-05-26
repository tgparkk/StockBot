"""
KIS API 시세 조회 관련 함수 (공식 문서 기반)
"""
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, List
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
        current_data = pd.DataFrame(res.getBody().output, index=[0])
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
        current_data = pd.DataFrame(res.getBody().output)
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
        current_data = pd.DataFrame(res.getBody().output)
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
        if output_dv == "1":
            current_data = pd.DataFrame(res.getBody().output1, index=[0])  # 호가조회
        else:
            current_data = pd.DataFrame(res.getBody().output2, index=[0])  # 예상체결가조회
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
        if output_dv == "1":
            current_data = pd.DataFrame(res.getBody().output1, index=[0])
        else:
            current_data = pd.DataFrame(res.getBody().output2)
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
        if output_dv == "1":
            current_data = pd.DataFrame(res.getBody().output1, index=[0])
        else:
            current_data = pd.DataFrame(res.getBody().output2)
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
        current_data = pd.DataFrame(res.getBody().output, index=[0])
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
                         fid_div_cls_code: str = "0",
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
            output_data = res.getBody().output
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
                   fid_div_cls_code: str = "0",
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
            output_data = res.getBody().output if hasattr(res.getBody(), 'output') else res.getBody().Output
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
        res = kis._url_fetch(url, tr_id, tr_cont, params)

        if res and res.isOK():
            output_data = res.getBody().output
            if output_data:
                current_data = pd.DataFrame(output_data)
                logger.info(f"등락률 순위 조회 성공: {len(current_data)}건")
                return current_data
            else:
                logger.warning("등락률 순위 조회: 데이터 없음")
                return pd.DataFrame()
        else:
            logger.error("등락률 순위 조회 실패")
            return None
    except Exception as e:
        logger.error(f"등락률 순위 조회 오류: {e}")
        return None


# =============================================================================
# 통합된 전략별 후보 조회 함수들
# =============================================================================

def get_gap_trading_candidates(market: str = "0000", enhanced: bool = True) -> Dict[str, Optional[pd.DataFrame]]:
    """갭 트레이딩 후보 조회 (기본 + 이격도 조합)"""
    result: Dict[str, Optional[pd.DataFrame]] = {"basic": None, "enhanced": None}

    # 기본 등락률 상위
    result["basic"] = get_fluctuation_rank(
        fid_input_iscd=market,
        fid_rank_sort_cls_code="0",  # 상승률순
        fid_rsfl_rate1="1.5"  # 1.5% 이상
    )

    # 향상된 버전: 이격도 과매도 종목
    if enhanced:
        result["enhanced"] = get_disparity_rank(
            fid_input_iscd=market,
            fid_rank_sort_cls_code="1",  # 이격도 하위순 (과매도)
            fid_hour_cls_code="20"  # 20일 이격도
        )

    return result


def get_volume_breakout_candidates(market: str = "0000", enhanced: bool = True) -> Dict[str, Optional[pd.DataFrame]]:
    """거래량 돌파 후보 조회 (기본 + 대량체결건수 조합)"""
    result: Dict[str, Optional[pd.DataFrame]] = {"basic": None, "enhanced": None}

    # 기본 거래량 순위
    result["basic"] = get_volume_rank(
        fid_input_iscd=market,
        fid_blng_cls_code="1",  # 거래증가율
        fid_vol_cnt="10000"  # 1만주 이상
    )

    # 향상된 버전: 대량체결건수 상위
    if enhanced:
        result["enhanced"] = get_bulk_trans_num_rank(
            fid_input_iscd=market,
            fid_rank_sort_cls_code="0",  # 매수상위
            fid_vol_cnt="10000"  # 1만주 이상
        )

    return result


def get_momentum_candidates(market: str = "0000", enhanced: bool = True) -> Dict[str, Optional[pd.DataFrame]]:
    """모멘텀 후보 조회 (기본 + 대량체결건수 조합)"""
    result: Dict[str, Optional[pd.DataFrame]] = {"basic": None, "enhanced": None}

    # 기본 체결강도 상위
    result["basic"] = get_volume_power_rank(
        fid_input_iscd=market,
        fid_vol_cnt="5000"  # 5천주 이상
    )

    # 향상된 버전: 대량체결건수 교차검증
    if enhanced:
        result["enhanced"] = get_bulk_trans_num_rank(
            fid_input_iscd=market,
            fid_rank_sort_cls_code="0",  # 매수상위
            fid_vol_cnt="5000"  # 5천주 이상
        )

    return result


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
            output_data = res.getBody().output
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
        "fid_input_price_2": fid_input_price_2,
        "fid_cond_mrkt_div_code": fid_cond_mrkt_div_code,
        "fid_cond_scr_div_code": fid_cond_scr_div_code,
        "fid_div_cls_code": fid_div_cls_code,
        "fid_rank_sort_cls_code": fid_rank_sort_cls_code,
        "fid_hour_cls_code": fid_hour_cls_code,
        "fid_input_iscd": fid_input_iscd,
        "fid_trgt_cls_code": fid_trgt_cls_code,
        "fid_trgt_exls_cls_code": fid_trgt_exls_cls_code,
        "fid_input_price_1": fid_input_price_1,
        "fid_vol_cnt": fid_vol_cnt
    }

    try:
        res = kis._url_fetch(url, tr_id, tr_cont, params)

        if res and res.isOK():
            output_data = res.getBody().output
            if output_data:
                current_data = pd.DataFrame(output_data)
                logger.info(f"이격도 순위 조회 성공: {len(current_data)}건")
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
            output_data = res.getBody().output
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


# =============================================================================
# 호가잔량 기반 헬퍼 함수들
# =============================================================================

def get_net_buy_candidates(market: str = "0000", enhanced: bool = True) -> Dict[str, Optional[pd.DataFrame]]:
    """순매수잔량 우세 후보 조회 (호가잔량 + 매수비율 조합)"""
    result: Dict[str, Optional[pd.DataFrame]] = {"basic": None, "enhanced": None}

    # 기본 순매수잔량 순위
    result["basic"] = get_quote_balance_rank(
        fid_input_iscd=market,
        fid_rank_sort_cls_code="0",  # 순매수잔량순
        fid_vol_cnt="10000"  # 1만주 이상
    )

    # 향상된 버전: 매수비율 우세 종목
    if enhanced:
        result["enhanced"] = get_quote_balance_rank(
            fid_input_iscd=market,
            fid_rank_sort_cls_code="2",  # 매수비율순
            fid_vol_cnt="5000"  # 5천주 이상
        )

    return result


def get_quote_imbalance_candidates(market: str = "0000") -> Optional[pd.DataFrame]:
    """호가 불균형 종목 조회 (순매도잔량 상위 - 반전 기회)"""
    return get_quote_balance_rank(
        fid_input_iscd=market,
        fid_rank_sort_cls_code="1",  # 순매도잔량순 (매도 우세)
        fid_vol_cnt="10000"  # 1만주 이상
    )


def get_balanced_quote_candidates(market: str = "0000") -> Optional[pd.DataFrame]:
    """균형잡힌 호가잔량 종목 조회 (매도비율순 - 안정성)"""
    return get_quote_balance_rank(
        fid_input_iscd=market,
        fid_rank_sort_cls_code="3",  # 매도비율순
        fid_vol_cnt="5000"  # 5천주 이상
    )
