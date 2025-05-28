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
        logger.debug(f"등락률 순위 API 호출 - 파라미터: {params}")
        res = kis._url_fetch(url, tr_id, tr_cont, params)

        if res and res.isOK():
            try:
                output_data = res.getBody().output
                if output_data:
                    current_data = pd.DataFrame(output_data)
                    logger.info(f"등락률 순위 조회 성공: {len(current_data)}건")
                    return current_data
                else:
                    logger.warning("등락률 순위 조회: output 데이터 없음")
                    return pd.DataFrame()
            except AttributeError as e:
                logger.error(f"등락률 순위 응답 구조 오류: {e}")
                logger.debug(f"응답 구조: {type(res.getBody())}")
                return pd.DataFrame()
        else:
            if res:
                logger.error(f"등락률 순위 조회 실패 - 응답코드: {getattr(res, 'rt_cd', 'Unknown')}")
                logger.error(f"오류 메시지: {getattr(res, 'msg1', 'Unknown')}")
            else:
                logger.error("등락률 순위 조회 실패 - 응답 없음")
            return None
    except Exception as e:
        logger.error(f"등락률 순위 조회 오류: {e}")
        return None


# =============================================================================
# 통합된 전략별 후보 조회 함수들
# =============================================================================

def get_gap_trading_candidates(market: str = "0000", 
                               min_gap_rate: float = 1.1,    # 🎯 1.1%로 상향 (기존 0.1%)
                               min_change_rate: float = 1.0,  # 🎯 1.0%로 상향 (기존 -5.0%) 
                               min_volume_ratio: float = 2.0) -> Optional[pd.DataFrame]: # 🎯 2.0배로 상향 (기존 1.0배)
    """갭 트레이딩 후보 조회 - 🎯 수익성 중심 엄격한 기준"""
    try:
        # 1단계: 상승률 상위 종목을 1차 필터링 (🎯 더 엄격한 조건)
        logger.info("🎯 갭 트레이딩 후보 엄격 필터링 중...")

        # 상승률 2% 이상 종목만 1차 필터링
        candidate_data = get_fluctuation_rank(
            fid_input_iscd=market,
            fid_rank_sort_cls_code="0",  # 상승률순
            fid_rsfl_rate1="2.0"  # 🎯 2% 이상 상승 종목만
        )

        if candidate_data is None or candidate_data.empty:
            logger.warning("🎯 엄격 필터링에서 데이터 없음 - 1% 이상으로 완화하여 재시도")
            candidate_data = get_fluctuation_rank(
                fid_input_iscd=market,
                fid_rank_sort_cls_code="0",  
                fid_rsfl_rate1="1.0"  # 1% 이상으로 완화
            )

            if candidate_data is None or candidate_data.empty:
                logger.error("🎯 갭 트레이딩: 1% 이상 상승 종목도 없음")
                return pd.DataFrame()

        logger.info(f"🎯 엄격 1차 필터링 완료: {len(candidate_data)}개 종목")

        # 2단계: 각 종목의 실제 갭 계산 (🎯 더 엄격한 기준 적용)
        gap_candidates = []

        for idx, row in candidate_data.head(20).iterrows():  # 🎯 상위 20개만 (기존 30개)
            try:
                stock_code = row.get('stck_shrn_iscd', '')
                if not stock_code:
                    continue

                # 현재가 정보 조회
                current_data = get_inquire_price("J", stock_code)
                if current_data is None or current_data.empty:
                    continue

                current_info = current_data.iloc[0]

                # 갭 계산에 필요한 데이터 추출
                current_price = int(current_info.get('stck_prpr', 0))
                open_price = int(current_info.get('stck_oprc', 0))
                prev_close = int(current_info.get('stck_sdpr', 0))

                # 🎯 장 시작 전 종목 제외 (더 확실한 데이터만 사용)
                if open_price <= 0:
                    logger.debug(f"🎯 종목 {stock_code}: 시가 없음(장전) - 제외")
                    continue

                if prev_close <= 0 or current_price <= 0:
                    logger.debug(f"🎯 종목 {stock_code}: 가격 정보 불완전 - 제외")
                    continue

                # 갭 크기 계산
                gap_size = open_price - prev_close
                gap_rate = (gap_size / prev_close) * 100

                # 🎯 엄격한 갭 트레이딩 조건
                if gap_rate >= min_gap_rate:  # 상향갭만 (2% 이상)
                    volume = int(current_info.get('acml_vol', 0))
                    
                    # 안전한 변동률 변환
                    change_rate_raw = current_info.get('prdy_ctrt', '0')
                    try:
                        change_rate = float(str(change_rate_raw))
                    except (ValueError, TypeError):
                        logger.debug(f"🎯 종목 {stock_code}: 변동률 변환 오류 - 제외")
                        continue

                    # 거래량 비율 계산
                    avg_volume = int(current_info.get('avrg_vol', 1))
                    volume_ratio = volume / max(avg_volume, 1)

                    # 🎯 수익성 중심 엄격 조건
                    # 1. 상향갭 2% 이상 (강한 갭)
                    # 2. 현재 변동률 1% 이상 (지속적 상승)
                    # 3. 거래량 2배 이상 (강한 관심)
                    # 4. 최소 거래량 5만주 이상 (유동성 확보)
                    
                    min_daily_volume = 50000  # 🎯 최소 5만주
                    
                    if (volume_ratio >= min_volume_ratio and 
                        change_rate >= min_change_rate and 
                        volume >= min_daily_volume):
                        
                        # 🎯 추가 수익성 필터
                        # 가격대별 최소 기준 적용
                        if current_price < 1000:  # 저가주 제외
                            logger.debug(f"🎯 종목 {stock_code}: 저가주 제외 ({current_price}원)")
                            continue
                        
                        if current_price > 500000:  # 고가주 제외 (유동성 문제)
                            logger.debug(f"🎯 종목 {stock_code}: 고가주 제외 ({current_price}원)")
                            continue
                        
                        gap_candidates.append({
                            'stck_shrn_iscd': stock_code,
                            'hts_kor_isnm': row.get('hts_kor_isnm', ''),
                            'stck_prpr': current_price,
                            'stck_oprc': open_price,
                            'stck_sdpr': prev_close,
                            'gap_size': gap_size,
                            'gap_rate': round(gap_rate, 2),
                            'prdy_ctrt': change_rate,
                            'acml_vol': volume,
                            'volume_ratio': round(volume_ratio, 2),
                            'profit_score': gap_rate * volume_ratio * change_rate,  # 🎯 수익성 점수
                            'data_rank': len(gap_candidates) + 1
                        })

                        logger.info(f"🎯 갭 후보: {stock_code}({row.get('hts_kor_isnm', '')}) 갭{gap_rate:.1f}% 거래량{volume_ratio:.1f}배 변동률{change_rate:.1f}%")
                    else:
                        logger.debug(f"🎯 종목 {stock_code}: 엄격 조건 미달 - 거래량{volume_ratio:.1f}배 변동률{change_rate:.1f}% 볼륨{volume:,}주")
                else:
                    logger.debug(f"🎯 종목 {stock_code}: 갭 부족 - {gap_rate:.2f}%")

            except Exception as e:
                logger.warning(f"🎯 종목 {stock_code} 갭 계산 오류: {e}")
                continue

        # 3단계: 🎯 수익성 점수 기준 정렬
        if gap_candidates:
            gap_df = pd.DataFrame(gap_candidates)
            gap_df = gap_df.sort_values('profit_score', ascending=False)  # 🎯 수익성 점수 내림차순
            logger.info(f"🎯 엄격 갭 트레이딩 후보 {len(gap_df)}개 발견 (수익성 중심)")
            return gap_df
        else:
            logger.info("🎯 엄격한 갭 트레이딩 조건을 만족하는 종목 없음")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"🎯 갭 트레이딩 후보 조회 오류: {e}")
        return None


def get_volume_breakout_candidates(market: str = "0000") -> Optional[pd.DataFrame]:
    """거래량 돌파 후보 조회 - 🎯 수익성 중심 엄격한 기준"""
    return get_volume_rank(
        fid_input_iscd=market,
        fid_blng_cls_code="1",  # 거래증가율
        fid_vol_cnt="100000"    # 🎯 10만주 이상 (기존 1만주)
    )


def get_momentum_candidates(market: str = "0000") -> Optional[pd.DataFrame]:
    """모멘텀 후보 조회 - 🎯 수익성 중심 엄격한 기준"""
    return get_volume_power_rank(
        fid_input_iscd=market,
        fid_vol_cnt="50000"     # 🎯 5만주 이상 (기존 5천주)
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
    """
    🆕 이격도 기반 실시간 매매 신호 생성
    
    Returns:
        {
            'timestamp': str,
            'buy_signals': List[Dict],
            'sell_signals': List[Dict], 
            'market_status': Dict
        }
    """
    try:
        from datetime import datetime
        
        # 다중 기간 이격도 분석 실행
        multi_disparity = get_multi_period_disparity()
        if not multi_disparity or not multi_disparity['analysis']:
            return None
        
        analysis = multi_disparity['analysis']
        
        # 매매 신호 정리
        buy_signals = []
        sell_signals = []
        
        # 강매수 신호 (최우선)
        for candidate in analysis.get('strong_buy_candidates', []):
            buy_signals.append({
                'stock_code': candidate['stock_code'],
                'stock_name': candidate['stock_name'],
                'signal_type': 'STRONG_BUY',
                'strategy_type': 'disparity_reversal',
                'score': 100 - candidate['d20_disparity'],  # 과매도 정도가 점수
                'reason': candidate['reason'],
                'current_price': candidate['current_price'],
                'change_rate': candidate['change_rate'],
                'priority': 1
            })
        
        # 일반 매수 신호
        for candidate in analysis.get('buy_candidates', []):
            buy_signals.append({
                'stock_code': candidate['stock_code'],
                'stock_name': candidate['stock_name'],
                'signal_type': 'BUY',
                'strategy_type': 'disparity_reversal',
                'score': 100 - candidate['d20_disparity'],
                'reason': candidate['reason'],
                'current_price': candidate['current_price'],
                'change_rate': candidate['change_rate'],
                'priority': 2
            })
        
        # Divergence 매수 신호
        for candidate in analysis.get('divergence_signals', []):
            if candidate['signal_strength'] == 'DIVERGENCE_BUY':
                buy_signals.append({
                    'stock_code': candidate['stock_code'],
                    'stock_name': candidate['stock_name'],
                    'signal_type': 'DIVERGENCE_BUY',
                    'strategy_type': 'disparity_reversal',
                    'score': 100 - candidate['d60_disparity'],  # 장기 이격도 기준
                    'reason': candidate['reason'],
                    'current_price': candidate['current_price'],
                    'change_rate': candidate['change_rate'],
                    'priority': 3
                })
        
        # 매도 신호들
        for candidate in analysis.get('sell_candidates', []):
            sell_signals.append({
                'stock_code': candidate['stock_code'],
                'stock_name': candidate['stock_name'],
                'signal_type': 'SELL',
                'reason': candidate['reason'],
                'current_price': candidate['current_price'],
                'disparity_level': candidate['d5_disparity']
            })
        
        # 점수별 정렬 (높은 점수 = 더 과매도)
        buy_signals.sort(key=lambda x: (x['priority'], -x['score']))
        
        # 시장 상태 요약
        market_status = {
            'total_analyzed_stocks': len(analysis.get('strong_buy_candidates', [])) + \
                                   len(analysis.get('buy_candidates', [])) + \
                                   len(analysis.get('sell_candidates', [])) + \
                                   len(analysis.get('strong_sell_candidates', [])),
            'oversold_count': len(analysis.get('strong_buy_candidates', [])) + len(analysis.get('buy_candidates', [])),
            'overbought_count': len(analysis.get('sell_candidates', [])) + len(analysis.get('strong_sell_candidates', [])),
            'divergence_count': len(analysis.get('divergence_signals', [])),
            'market_sentiment': 'OVERSOLD' if len(analysis.get('strong_buy_candidates', [])) > 5 else 
                              'OVERBOUGHT' if len(analysis.get('strong_sell_candidates', [])) > 5 else 'NEUTRAL'
        }
        
        result = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'buy_signals': buy_signals[:15],  # 상위 15개 매수 신호
            'sell_signals': sell_signals[:10], # 상위 10개 매도 신호
            'market_status': market_status
        }
        
        logger.info(f"🎯 이격도 매매 신호 생성: 매수{len(buy_signals)} 매도{len(sell_signals)} "
                   f"시장상태{market_status['market_sentiment']}")
        
        return result
        
    except Exception as e:
        logger.error(f"이격도 매매 신호 생성 오류: {e}")
        return None

