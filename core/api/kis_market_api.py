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


def get_inquire_time_itemchartprice(output_dv: str = "1", div_code: str = "J", itm_no: str = "",
                                   input_hour: Optional[str] = None, past_data_yn: str = "N",
                                   etc_cls_code: str = "", tr_cont: str = "",
                                   FK100: str = "", NK100: str = "") -> Optional[pd.DataFrame]:
    """
    주식당일분봉조회 API

    당일 분봉 데이터를 조회합니다. (전일자 분봉 미제공)
    실전계좌/모의계좌의 경우, 한 번의 호출에 최대 30건까지 확인 가능합니다.

    Args:
        output_dv: 출력 구분 (1: output1, 2: output2 - 분봉 데이터 배열)
        div_code: 조건 시장 분류 코드 (J:KRX, NX:NXT, UN:통합)
        itm_no: 입력 종목코드 (6자리, ex: 005930)
        input_hour: 입력 시간1 (HHMMSS 형식, 기본값: 현재시간)
        past_data_yn: 과거 데이터 포함 여부 (Y/N, 기본값: N)
        etc_cls_code: 기타 구분 코드 (기본값: "")
        tr_cont: 연속 거래 여부 (공백: 초기 조회, N: 다음 데이터 조회)
        FK100: 예약 파라미터
        NK100: 예약 파라미터

    Returns:
        output1: 종목 기본 정보 (전일대비, 현재가 등)
        output2: 분봉 데이터 배열 (시간별 OHLC + 거래량, 최대 30건)

    Note:
        - 당일 분봉 데이터만 제공됩니다
        - 미래일시 입력 시에는 현재가로 조회됩니다
        - output2의 첫번째 배열의 체결량은 첫체결 전까지 이전 분봉의 체결량이 표시됩니다
    """
    url = '/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice'
    tr_id = "FHKST03010200"  # 주식당일분봉조회

    # 입력 시간이 없으면 현재 시간 사용
    if input_hour is None:
        now = datetime.now()
        input_hour = f"{now.hour:02d}{now.minute:02d}{now.second:02d}"
        logger.debug(f"📊 입력 시간 자동 설정: {input_hour}")

    params = {
        "FID_COND_MRKT_DIV_CODE": div_code,          # 조건 시장 분류 코드
        "FID_INPUT_ISCD": itm_no,                    # 종목코드
        "FID_INPUT_HOUR_1": input_hour,              # 입력시간 (HHMMSS)
        "FID_PW_DATA_INCU_YN": past_data_yn,         # 과거 데이터 포함 여부
        "FID_ETC_CLS_CODE": etc_cls_code             # 기타 구분 코드
    }

    try:
        res = kis._url_fetch(url, tr_id, tr_cont, params)

        if res and res.isOK():
            body = res.getBody()

            if output_dv == "1":
                # 종목 기본 정보 (output1)
                output1_data = getattr(body, 'output1', {})
                if output1_data:
                    current_data = pd.DataFrame([output1_data])
                    logger.info(f"📊 {itm_no} 분봉 기본정보 조회 성공")
                    return current_data
                else:
                    logger.warning(f"📊 {itm_no} 분봉 기본정보 없음")
                    return pd.DataFrame()
            else:
                # 분봉 데이터 배열 (output2)
                output2_data = getattr(body, 'output2', [])
                if output2_data:
                    current_data = pd.DataFrame(output2_data)
                    logger.info(f"📊 {itm_no} 분봉 데이터 조회 성공: {len(current_data)}건 (시간: {input_hour})")

                    # 분봉 데이터 정보 로깅
                    if len(current_data) > 0:
                        first_time = current_data.iloc[0].get('stck_cntg_hour', 'N/A')
                        last_time = current_data.iloc[-1].get('stck_cntg_hour', 'N/A')
                        logger.debug(f"📊 분봉 시간 범위: {first_time} ~ {last_time}")

                    return current_data
                else:
                    logger.warning(f"📊 {itm_no} 분봉 데이터 없음 (시간: {input_hour})")
                    return pd.DataFrame()
        else:
            logger.error(f"📊 {itm_no} 주식당일분봉조회 실패")
            return None

    except Exception as e:
        logger.error(f"📊 {itm_no} 주식당일분봉조회 오류: {e}")
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


# 테스트 실행을 위한 예시 함수
if __name__ == "__main__":

    pass


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

