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
                    output_data = body.output
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

def get_gap_trading_candidates(market: str = "0000",
                               min_gap_rate: float = 2.0,  # 🎯 2% 기본 갭
                               min_change_rate: float = 1.0,  # 🎯 1.0% 기본 변동률
                               min_volume_ratio: float = 2.0) -> Optional[pd.DataFrame]:
    """갭 트레이딩 후보 조회 - 🎯 적응형 기준 (시간대별 조정)"""
    from datetime import datetime

    try:
        current_time = datetime.now()
        is_pre_market = current_time.hour < 9 or (current_time.hour == 9 and current_time.minute < 30)

        # 🎯 시간대별 기준 완화
        if is_pre_market:
            # 프리마켓: 매우 관대한 기준
            min_gap_rate = 0.5  # 0.5% 갭
            min_change_rate = 0.3  # 0.3% 변동률
            min_volume_ratio = 1.2  # 1.2배 거래량
            min_daily_volume = 40000  # 4만주
            min_price = 1000  # 1000원 이상
            max_price = 1000000  # 100만원 이하
            fluctuation_threshold = "0.3"  # 0.3% 이상
            logger.info("🌅 프리마켓 갭트레이딩 기준: 갭0.5% 변동0.3% 거래량1.2배 (매우 관대)")
        elif current_time.hour < 11:
            # 장 초반: 관대한 기준
            min_gap_rate = 1.0  # 1.0% 갭
            min_change_rate = 0.5  # 0.5% 변동률
            min_volume_ratio = 1.5  # 1.5배 거래량
            min_daily_volume = 60000  # 6만주
            min_price = 1000  # 1000원 이상
            max_price = 1000000  # 100만원 이하
            fluctuation_threshold = "0.5"  # 0.5% 이상
            logger.info("🌄 장초반 갭트레이딩 기준: 갭1.0% 변동0.5% 거래량1.5배 (관대)")
        else:
            # 정규 시간: 기본 기준 (기존보다 약간 완화)
            min_gap_rate = 1.5  # 1.5% 갭
            min_change_rate = 0.8  # 0.8% 변동률
            min_volume_ratio = 1.8  # 1.8배 거래량
            min_daily_volume = 80000  # 8만주
            min_price = 1000  # 1000원 이상
            max_price = 1000000  # 100만원 이하
            fluctuation_threshold = "0.8"  # 0.8% 이상
            logger.info("🕐 정규시간 갭트레이딩 기준: 갭1.5% 변동0.8% 거래량1.8배 (완화)")

        # 1단계: 상승률 상위 종목을 1차 필터링 (🎯 적응형 조건)
        logger.info("🎯 갭 트레이딩 후보 적응형 필터링 중...")

        # 적응형 상승률 기준 적용
        candidate_data = get_fluctuation_rank(
            fid_input_iscd=market,
            fid_rank_sort_cls_code="0",  # 상승률순
            fid_rsfl_rate1=fluctuation_threshold
        )

        if candidate_data is None or candidate_data.empty:
            # 🔧 백업 전략 1: 더 관대한 기준으로 재시도
            fallback_threshold = str(float(fluctuation_threshold) * 0.5)
            logger.warning(f"🎯 1차 필터링 데이터 없음 - {fallback_threshold}% 이상으로 재시도")
            candidate_data = get_fluctuation_rank(
                fid_input_iscd=market,
                fid_rank_sort_cls_code="0",
                fid_rsfl_rate1=fallback_threshold
            )

            if candidate_data is None or candidate_data.empty:
                # 🔧 백업 전략 2: 조건 없이 전체 조회
                logger.warning("🎯 2차 필터링도 데이터 없음 - 조건 제거하고 전체 조회")
                candidate_data = get_fluctuation_rank(
                    fid_input_iscd=market,
                    fid_rank_sort_cls_code="0",  # 상승률순만 유지
                    fid_rsfl_rate1="",  # 등락률 조건 제거
                    fid_vol_cnt=""      # 거래량 조건 제거
                )

                if candidate_data is None or candidate_data.empty:
                    # 🔧 백업 전략 3: 다른 시장으로 시도
                    if market != "0000":
                        logger.warning("🎯 3차 백업: 전체 시장(0000)으로 재시도")
                        candidate_data = get_fluctuation_rank(
                            fid_input_iscd="0000",  # 전체 시장
                            fid_rank_sort_cls_code="0",
                            fid_rsfl_rate1="",
                            fid_vol_cnt=""
                        )

                    if candidate_data is None or candidate_data.empty:
                        # 🔧 최종 백업: 하락률순으로도 시도 (반대 신호)
                        logger.warning("🎯 최종 백업: 하락률순 조회 (반대매매 후보)")
                        candidate_data = get_fluctuation_rank(
                            fid_input_iscd="0000",
                            fid_rank_sort_cls_code="1",  # 하락률순
                            fid_rsfl_rate1="",
                            fid_vol_cnt=""
                        )

                        if candidate_data is None or candidate_data.empty:
                            logger.error("🎯 갭 트레이딩: 모든 백업 전략에도 데이터 없음")
                            logger.info("💡 가능한 원인: 1) 장 운영시간 외 2) API 제한 3) 시장 참여자 부족 4) 네트워크 문제")
                            return pd.DataFrame()

        logger.info(f"🎯 적응형 필터링 완료: {len(candidate_data)}개 종목 확보")

        # 2단계: 각 종목의 실제 갭 계산 (🎯 적응형 기준 적용)
        gap_candidates = []
        max_candidates = 30 if is_pre_market else 20  # 프리마켓엔 더 많은 후보

        for idx, row in candidate_data.head(max_candidates).iterrows():
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

                # 🎯 프리마켓에는 시가 없을 수 있으므로 더 관대하게
                if is_pre_market and open_price <= 0:
                    logger.debug(f"🌅 프리마켓 종목 {stock_code}: 시가 없음 - 현재가로 추정")
                    open_price = current_price  # 현재가로 추정
                elif not is_pre_market and open_price <= 0:
                    logger.debug(f"🎯 종목 {stock_code}: 시가 없음 - 제외")
                    continue

                if prev_close <= 0 or current_price <= 0:
                    logger.debug(f"🎯 종목 {stock_code}: 가격 정보 불완전 - 제외")
                    continue

                # 갭 크기 계산
                gap_size = open_price - prev_close
                gap_rate = (gap_size / prev_close) * 100

                # 🎯 적응형 갭 트레이딩 조건
                if gap_rate >= min_gap_rate:  # 상향갭만
                    volume = int(current_info.get('acml_vol', 0))

                    # 평균 거래량 및 변동률 추출
                    avg_volume_raw = current_info.get('avrg_vol', 0)
                    try:
                        avg_volume = int(avg_volume_raw) if avg_volume_raw else 0
                    except (ValueError, TypeError):
                        avg_volume = 0

                    # 안전한 변동률 변환
                    change_rate_raw = current_info.get('prdy_ctrt', '0')
                    try:
                        change_rate = float(str(change_rate_raw))
                    except (ValueError, TypeError):
                        logger.debug(f"🎯 종목 {stock_code}: 변동률 변환 오류 - 제외")
                        continue

                    # 🔧 거래량 비율 계산 (API 조회 포함)
                    if avg_volume <= 0:
                        # 🆕 API를 통해 실제 평균 거래량 계산
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
                                    safe_avg_volume = max(calculated_avg_volume, 5000)  # 최소 5천주
                                    logger.debug(f"📊 {stock_code}: 5일 평균 거래량 계산 완료 - {safe_avg_volume:,}주")
                                else:
                                    safe_avg_volume = max(volume // 5, 10000)  # fallback
                                    logger.debug(f"📊 {stock_code}: 거래량 데이터 부족 - 추정치 사용: {safe_avg_volume:,}주")
                            else:
                                # API 조회 실패시 추정치 사용
                                safe_avg_volume = max(volume // 5, 10000)  # 현재의 1/5 또는 최소 1만주
                                logger.debug(f"📊 {stock_code}: API 조회 실패 - 추정치 사용: {safe_avg_volume:,}주")
                        except Exception as e:
                            logger.warning(f"📊 {stock_code}: 평균 거래량 계산 오류 - {e}")
                            safe_avg_volume = max(volume // 5, 10000)  # fallback
                    elif avg_volume < 5000:
                        # 너무 작은 평균 거래량 보정
                        safe_avg_volume = 5000
                        logger.debug(f"📊 {stock_code}: 평균 거래량 보정 - {avg_volume:,}주 → {safe_avg_volume:,}주")
                    else:
                        # 정상적인 평균 거래량 사용
                        safe_avg_volume = avg_volume

                    # 🔧 거래량 비율 계산 및 상한 제한
                    volume_ratio = volume / safe_avg_volume
                    volume_ratio = min(volume_ratio, 100)  # 최대 100배로 제한 (더 현실적)

                    logger.debug(f"🔧 {stock_code} 거래량 계산: 현재={volume:,}주, 평균={safe_avg_volume:,}주, 비율={volume_ratio:.1f}배")

                    # 🎯 적응형 조건 체크
                    if (volume_ratio >= min_volume_ratio and
                        change_rate >= min_change_rate and
                        volume >= min_daily_volume):

                        # 🎯 가격대별 필터
                        if current_price < min_price:
                            logger.debug(f"🎯 종목 {stock_code}: 저가주 제외 ({current_price}원)")
                            continue

                        if current_price > max_price:
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
                        logger.debug(f"🎯 종목 {stock_code}: 조건 미달 - 거래량{volume_ratio:.1f}배 변동률{change_rate:.1f}% 볼륨{volume:,}주")
                else:
                    logger.debug(f"🎯 종목 {stock_code}: 갭 부족 - {gap_rate:.2f}%")

            except Exception as e:
                logger.warning(f"🎯 종목 {stock_code} 갭 계산 오류: {e}")
                continue

        # 3단계: 🎯 수익성 점수 기준 정렬
        if gap_candidates:
            gap_df = pd.DataFrame(gap_candidates)
            gap_df = gap_df.sort_values('profit_score', ascending=False)  # 🎯 수익성 점수 내림차순
            logger.info(f"🎯 적응형 갭 트레이딩 후보 {len(gap_df)}개 발견")
            return gap_df
        else:
            logger.info("🎯 적응형 갭 트레이딩 조건을 만족하는 종목 없음")
            return pd.DataFrame()

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


def get_technical_indicator_screening(market: str = "0000", min_score: int = 60) -> Optional[pd.DataFrame]:
    """🆕 기술적 지표 기반 종목 스크리닝 - RSI, MACD, 이동평균선 활용"""
    from ..analysis.technical_indicators import TechnicalIndicators
    
    try:
        logger.info(f"📈 기술적 지표 스크리닝 시작: {market}, 최소점수 {min_score}")
        
        # 🎯 1단계: 기본 후보군 수집 (다양한 방법 조합)
        candidate_sources = []
        
        # 등락률 상위 (상승 추세)
        try:
            fluctuation_data = get_fluctuation_rank(
                fid_input_iscd=market,
                fid_rank_sort_cls_code="0",  # 상승률순
                fid_rsfl_rate1="0.5"  # 0.5% 이상
            )
            if fluctuation_data is not None and not fluctuation_data.empty:
                candidate_sources.append(('fluctuation_up', fluctuation_data.head(30)))
        except Exception as e:
            logger.debug(f"등락률 상위 조회 오류: {e}")
        
        # 등락률 하위 (반등 기대)
        try:
            fluctuation_down_data = get_fluctuation_rank(
                fid_input_iscd=market,
                fid_rank_sort_cls_code="1",  # 하락률순
                fid_rsfl_rate2="-1.0"  # -1.0% 이하
            )
            if fluctuation_down_data is not None and not fluctuation_down_data.empty:
                candidate_sources.append(('fluctuation_down', fluctuation_down_data.head(20)))
        except Exception as e:
            logger.debug(f"등락률 하위 조회 오류: {e}")
        
        # 거래량 급증
        try:
            volume_data = get_volume_rank(
                fid_input_iscd=market,
                fid_blng_cls_code="1",  # 거래증가율
                fid_vol_cnt="10000"
            )
            if volume_data is not None and not volume_data.empty:
                candidate_sources.append(('volume', volume_data.head(25)))
        except Exception as e:
            logger.debug(f"거래량 급증 조회 오류: {e}")
        
        # 체결강도 상위
        try:
            power_data = get_volume_power_rank(
                fid_input_iscd=market,
                fid_vol_cnt="5000"
            )
            if power_data is not None and not power_data.empty:
                candidate_sources.append(('power', power_data.head(25)))
        except Exception as e:
            logger.debug(f"체결강도 조회 오류: {e}")
        
        # 이격도 기반 (과매도/과매수)
        try:
            disparity_data = get_disparity_rank(
                fid_input_iscd=market,
                fid_rank_sort_cls_code="1",  # 하위 (과매도)
                fid_hour_cls_code="20"  # 20일
            )
            if disparity_data is not None and not disparity_data.empty:
                candidate_sources.append(('disparity', disparity_data.head(20)))
        except Exception as e:
            logger.debug(f"이격도 조회 오류: {e}")
        
        # 🎯 2단계: 종목 코드 수집 및 중복 제거
        collected_stocks = set()
        for source_name, data in candidate_sources:
            for _, row in data.iterrows():
                stock_code = row.get('stck_shrn_iscd') or row.get('mksc_shrn_iscd', '')
                if stock_code and len(stock_code) == 6:  # 유효한 종목코드
                    collected_stocks.add(stock_code)
        
        logger.info(f"📊 수집된 후보 종목: {len(collected_stocks)}개")
        
        if not collected_stocks:
            logger.warning("📊 기술적 분석할 후보 종목 없음")
            return pd.DataFrame()
        
        # 🎯 3단계: 기술적 지표 분석
        technical_results = []
        processed_count = 0
        
        for stock_code in list(collected_stocks)[:150]:  # 최대 150개 분석
            try:
                processed_count += 1
                if processed_count % 20 == 0:
                    logger.info(f"📈 기술적 분석 진행: {processed_count}/{min(150, len(collected_stocks))}")
                
                # 기본 정보 조회
                current_data = get_inquire_price("J", stock_code)
                if not current_data or current_data.empty:
                    continue
                
                current_info = current_data.iloc[0]
                current_price = int(current_info.get('stck_prpr', 0))
                stock_name = current_info.get('prdy_vrss_sign', '')  # 종목명 (임시)
                
                if current_price <= 0:
                    continue
                
                # 가격 데이터 조회 (최근 60일)
                price_data = get_inquire_daily_price("J", stock_code)
                if not price_data or len(price_data) < 20:
                    continue
                
                # 가격 데이터 준비
                closes = []
                highs = []
                lows = []
                volumes = []
                
                for _, row in price_data.head(60).iterrows():
                    close = int(row.get('stck_clpr', 0))
                    high = int(row.get('stck_hgpr', 0))
                    low = int(row.get('stck_lwpr', 0))
                    volume = int(row.get('acml_vol', 0))
                    
                    if close > 0:
                        closes.append(close)
                        highs.append(high if high > 0 else close)
                        lows.append(low if low > 0 else close)
                        volumes.append(volume)
                
                # 현재가 추가
                closes.append(current_price)
                highs.append(current_price)
                lows.append(current_price)
                
                if len(closes) < 15:  # 최소 데이터 요구
                    continue
                
                # 🎯 기술적 지표 계산
                technical_analysis = _analyze_technical_indicators(
                    closes, highs, lows, volumes, stock_code, stock_name, current_price
                )
                
                if technical_analysis and technical_analysis['total_score'] >= min_score:
                    technical_results.append(technical_analysis)
                
                # API 제한 방지
                time.sleep(0.03)
                
            except Exception as e:
                logger.debug(f"종목 {stock_code} 기술적 분석 오류: {e}")
                continue
        
        logger.info(f"📈 기술적 지표 분석 완료: {len(technical_results)}개 종목 선별 ({min_score}점 이상)")
        
        # 결과를 DataFrame으로 변환
        if technical_results:
            df = pd.DataFrame(technical_results)
            # 점수 순으로 정렬
            df = df.sort_values('total_score', ascending=False)
            return df
        else:
            return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"기술적 지표 스크리닝 오류: {e}")
        return None


def _analyze_technical_indicators(closes: List[int], highs: List[int], lows: List[int], 
                                volumes: List[int], stock_code: str, stock_name: str, 
                                current_price: int) -> Optional[Dict]:
    """기술적 지표 종합 분석"""
    try:
        from ..analysis.technical_indicators import TechnicalIndicators
        
        # 기본 정보
        result = {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'current_price': current_price,
            'total_score': 0,
            'signals': [],
            'indicators': {}
        }
        
        score = 0
        signals = []
        
        # 📊 1. RSI 분석
        try:
            rsi_values = TechnicalIndicators.calculate_rsi(closes, period=14)
            current_rsi = rsi_values[-1] if rsi_values else 50.0
            result['indicators']['rsi'] = current_rsi
            
            if current_rsi < 30:  # 과매도 → 반등 기대
                score += 25
                signals.append(f"RSI과매도({current_rsi:.1f})")
            elif 30 <= current_rsi <= 50:  # 과매도에서 회복
                score += 30
                signals.append(f"RSI회복({current_rsi:.1f})")
            elif 50 < current_rsi <= 65:  # 적정 상승
                score += 20
                signals.append(f"RSI적정({current_rsi:.1f})")
            elif current_rsi > 70:  # 과매수 → 주의
                score -= 10
                signals.append(f"RSI과매수({current_rsi:.1f})")
            
        except Exception as e:
            logger.debug(f"RSI 계산 오류 ({stock_code}): {e}")
            result['indicators']['rsi'] = 50.0
        
        # 📊 2. MACD 분석
        try:
            macd_data = TechnicalIndicators.calculate_macd(closes, fast=12, slow=26, signal=9)
            current_macd = macd_data['macd'][-1] if macd_data['macd'] else 0.0
            current_signal = macd_data['signal'][-1] if macd_data['signal'] else 0.0
            current_histogram = macd_data['histogram'][-1] if macd_data['histogram'] else 0.0
            
            result['indicators']['macd'] = current_macd
            result['indicators']['macd_signal'] = current_signal
            result['indicators']['macd_histogram'] = current_histogram
            
            # MACD Line > Signal Line (상승 신호)
            if current_macd > current_signal:
                score += 25
                signals.append("MACD상승신호")
                
                # 추가로 히스토그램이 양수면 더 강한 신호
                if current_histogram > 0:
                    score += 10
                    signals.append("MACD강세확인")
            
            # 히스토그램 음수→양수 전환 (매우 강한 신호)
            if (len(macd_data['histogram']) > 1 and 
                macd_data['histogram'][-2] <= 0 < current_histogram):
                score += 35
                signals.append("MACD전환신호")
            
        except Exception as e:
            logger.debug(f"MACD 계산 오류 ({stock_code}): {e}")
            result['indicators']['macd'] = 0.0
            result['indicators']['macd_signal'] = 0.0
            result['indicators']['macd_histogram'] = 0.0
        
        # 📊 3. 이동평균선 분석
        try:
            ma_data = TechnicalIndicators.calculate_moving_averages(closes, [5, 20, 60])
            ma_5 = ma_data.get('ma_5', [current_price])[-1]
            ma_20 = ma_data.get('ma_20', [current_price])[-1]
            ma_60 = ma_data.get('ma_60', [current_price])[-1]
            
            result['indicators']['ma_5'] = ma_5
            result['indicators']['ma_20'] = ma_20
            result['indicators']['ma_60'] = ma_60
            
            # 완벽한 상승배열: 현재가 > 5일선 > 20일선 > 60일선
            if current_price > ma_5 > ma_20 > ma_60:
                score += 40
                signals.append("완벽상승배열")
            # 단기 상승배열: 현재가 > 5일선 > 20일선
            elif current_price > ma_5 > ma_20:
                score += 25
                signals.append("단기상승배열")
            # 골든크로스: 5일선 > 20일선 (단순 비교)
            elif ma_5 > ma_20:
                score += 15
                signals.append("골든크로스")
            # 현재가가 5일선 위에 있음
            elif current_price > ma_5:
                score += 10
                signals.append("5일선돌파")
            
            # 5일선이 20일선을 상향돌파하는 신호 (최근 데이터로 확인)
            if len(ma_data.get('ma_5', [])) > 5 and len(ma_data.get('ma_20', [])) > 5:
                prev_ma5 = ma_data['ma_5'][-2] if len(ma_data['ma_5']) > 1 else ma_5
                prev_ma20 = ma_data['ma_20'][-2] if len(ma_data['ma_20']) > 1 else ma_20
                
                if prev_ma5 <= prev_ma20 < ma_5:  # 골든크로스 확인
                    score += 30
                    signals.append("골든크로스발생")
            
        except Exception as e:
            logger.debug(f"이동평균 계산 오류 ({stock_code}): {e}")
            result['indicators']['ma_5'] = current_price
            result['indicators']['ma_20'] = current_price
            result['indicators']['ma_60'] = current_price
        
        # 📊 4. 거래량 분석 (보조 지표)
        try:
            if volumes and len(volumes) >= 5:
                recent_volume = volumes[-1] if volumes else 0
                avg_volume = sum(volumes[-5:]) / min(5, len(volumes))
                
                if recent_volume > avg_volume * 1.5:  # 거래량 1.5배 이상
                    score += 15
                    signals.append(f"거래량급증({recent_volume/avg_volume:.1f}배)")
                elif recent_volume > avg_volume * 1.2:  # 거래량 1.2배 이상
                    score += 10
                    signals.append(f"거래량증가({recent_volume/avg_volume:.1f}배)")
                
                result['indicators']['volume_ratio'] = recent_volume / avg_volume if avg_volume > 0 else 1.0
            else:
                result['indicators']['volume_ratio'] = 1.0
                
        except Exception as e:
            logger.debug(f"거래량 분석 오류 ({stock_code}): {e}")
            result['indicators']['volume_ratio'] = 1.0
        
        # 📊 5. 가격 모멘텀 (단기 추세)
        try:
            if len(closes) >= 5:
                price_5d_ago = closes[-5]
                momentum_5d = (current_price - price_5d_ago) / price_5d_ago * 100
                
                if 0 < momentum_5d <= 15:  # 적정 상승 (과열 방지)
                    score += 20
                    signals.append(f"5일상승({momentum_5d:.1f}%)")
                elif momentum_5d > 15:  # 과열 주의
                    score += 5
                    signals.append(f"급상승주의({momentum_5d:.1f}%)")
                elif -5 <= momentum_5d < 0:  # 소폭 조정 (매수 기회)
                    score += 15
                    signals.append(f"소폭조정({momentum_5d:.1f}%)")
                
                result['indicators']['momentum_5d'] = momentum_5d
            else:
                result['indicators']['momentum_5d'] = 0.0
                
        except Exception as e:
            logger.debug(f"모멘텀 계산 오류 ({stock_code}): {e}")
            result['indicators']['momentum_5d'] = 0.0
        
        # 최종 결과
        result['total_score'] = score
        result['signals'] = signals
        result['analysis_summary'] = f"{len(signals)}개 신호 (총 {score}점)"
        
        return result
        
    except Exception as e:
        logger.error(f"기술적 지표 분석 오류 ({stock_code}): {e}")
        return None


def get_comprehensive_market_screening(markets: List[str] = ["0001", "1001"]) -> Optional[Dict]:
    """🎯 종합 시장 스크리닝 - 기술적 지표 + 전통적 스크리닝 결합"""
    try:
        logger.info("🎯 종합 시장 스크리닝 시작")
        
        all_results = {
            'technical_screening': {},
            'traditional_screening': {},
            'combined_recommendations': [],
            'market_summary': {},
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        for market in markets:
            market_name = "코스피" if market == "0001" else "코스닥" if market == "1001" else f"시장{market}"
            logger.info(f"📊 {market_name} 분석 시작")
            
            # 기술적 지표 스크리닝
            try:
                technical_results = get_technical_indicator_screening(market, min_score=50)
                if technical_results is not None and not technical_results.empty:
                    all_results['technical_screening'][market] = {
                        'count': len(technical_results),
                        'data': technical_results.head(20).to_dict('records'),  # 상위 20개
                        'status': 'success'
                    }
                    logger.info(f"📈 {market_name} 기술적 후보: {len(technical_results)}개")
                else:
                    all_results['technical_screening'][market] = {
                        'count': 0,
                        'data': [],
                        'status': 'no_data'
                    }
            except Exception as e:
                logger.error(f"{market_name} 기술적 스크리닝 오류: {e}")
                all_results['technical_screening'][market] = {
                    'count': 0,
                    'data': [],
                    'status': 'error',
                    'error': str(e)
                }
            
            # 전통적 스크리닝 (갭, 거래량, 모멘텀)
            try:
                traditional_candidates = {
                    'gap': [],
                    'volume': [],
                    'momentum': []
                }
                
                # 갭 트레이딩
                gap_data = get_gap_trading_candidates(market)
                if gap_data is not None and not gap_data.empty:
                    traditional_candidates['gap'] = gap_data.head(10).to_dict('records')
                
                # 거래량 돌파
                volume_data = get_volume_breakout_candidates(market)
                if volume_data is not None and not volume_data.empty:
                    traditional_candidates['volume'] = volume_data.head(10).to_dict('records')
                
                # 모멘텀
                momentum_data = get_momentum_candidates(market)
                if momentum_data is not None and not momentum_data.empty:
                    traditional_candidates['momentum'] = momentum_data.head(10).to_dict('records')
                
                all_results['traditional_screening'][market] = traditional_candidates
                traditional_count = sum(len(v) for v in traditional_candidates.values())
                logger.info(f"📊 {market_name} 전통적 후보: {traditional_count}개")
                
            except Exception as e:
                logger.error(f"{market_name} 전통적 스크리닝 오류: {e}")
                all_results['traditional_screening'][market] = {
                    'gap': [], 'volume': [], 'momentum': []
                }
            
            time.sleep(0.5)  # 시장간 대기
        
        # 종합 추천 생성
        all_results['combined_recommendations'] = _generate_combined_recommendations(all_results)
        all_results['market_summary'] = _generate_market_summary(all_results)
        
        logger.info("🎯 종합 시장 스크리닝 완료")
        return all_results
        
    except Exception as e:
        logger.error(f"종합 시장 스크리닝 오류: {e}")
        return None


def _generate_combined_recommendations(results: Dict) -> List[Dict]:
    """종합 추천 생성"""
    try:
        recommendations = []
        
        # 기술적 지표 기반 추천 (우선순위 높음)
        for market, data in results.get('technical_screening', {}).items():
            if data.get('status') == 'success' and data.get('data'):
                for item in data['data'][:5]:  # 상위 5개
                    recommendations.append({
                        'stock_code': item['stock_code'],
                        'stock_name': item.get('stock_name', ''),
                        'current_price': item['current_price'],
                        'recommendation_type': 'technical_priority',
                        'score': item['total_score'],
                        'signals': item.get('signals', []),
                        'market': "코스피" if market == "0001" else "코스닥",
                        'priority': 'high'
                    })
        
        # 전통적 스크리닝 추가 (보조)
        for market, categories in results.get('traditional_screening', {}).items():
            for category, items in categories.items():
                for item in items[:3]:  # 각 카테고리에서 3개씩
                    stock_code = item.get('stck_shrn_iscd') or item.get('mksc_shrn_iscd', '')
                    if stock_code:
                        recommendations.append({
                            'stock_code': stock_code,
                            'stock_name': item.get('hts_kor_isnm', ''),
                            'current_price': item.get('stck_prpr', 0),
                            'recommendation_type': f'traditional_{category}',
                            'score': 30,  # 기본 점수
                            'signals': [category],
                            'market': "코스피" if market == "0001" else "코스닥",
                            'priority': 'medium'
                        })
        
        # 중복 제거 및 점수순 정렬
        unique_recommendations = {}
        for rec in recommendations:
            stock_code = rec['stock_code']
            if stock_code not in unique_recommendations or rec['score'] > unique_recommendations[stock_code]['score']:
                unique_recommendations[stock_code] = rec
        
        final_recommendations = sorted(
            unique_recommendations.values(),
            key=lambda x: (x['priority'] == 'high', x['score']),
            reverse=True
        )
        
        return final_recommendations[:20]  # 상위 20개
        
    except Exception as e:
        logger.error(f"종합 추천 생성 오류: {e}")
        return []


def _generate_market_summary(results: Dict) -> Dict:
    """시장 요약 생성"""
    try:
        summary = {
            'total_technical_candidates': 0,
            'total_traditional_candidates': 0,
            'market_analysis': {},
            'best_opportunities': []
        }
        
        # 기술적 지표 후보 집계
        for market, data in results.get('technical_screening', {}).items():
            if data.get('status') == 'success':
                summary['total_technical_candidates'] += data.get('count', 0)
        
        # 전통적 후보 집계
        for market, categories in results.get('traditional_screening', {}).items():
            market_count = sum(len(items) for items in categories.values())
            summary['total_traditional_candidates'] += market_count
            
            market_name = "코스피" if market == "0001" else "코스닥"
            summary['market_analysis'][market_name] = {
                'gap_count': len(categories.get('gap', [])),
                'volume_count': len(categories.get('volume', [])),
                'momentum_count': len(categories.get('momentum', [])),
                'total_count': market_count
            }
        
        # 최고 기회 종목 (기술적 점수 80점 이상)
        for market, data in results.get('technical_screening', {}).items():
            if data.get('status') == 'success' and data.get('data'):
                for item in data['data']:
                    if item.get('total_score', 0) >= 80:
                        summary['best_opportunities'].append({
                            'stock_code': item['stock_code'],
                            'score': item['total_score'],
                            'signals': len(item.get('signals', [])),
                            'market': "코스피" if market == "0001" else "코스닥"
                        })
        
        return summary
        
    except Exception as e:
        logger.error(f"시장 요약 생성 오류: {e}")
        return {}


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
