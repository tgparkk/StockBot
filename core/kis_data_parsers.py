"""
KIS API 실시간 데이터 파싱 모듈
"""
from typing import Dict, Optional, List
from .kis_crypto import aes_cbc_base64_dec


def parse_stock_price_data(data: str) -> Dict:
    """
    주식체결가 데이터 파싱 (H0STCNT0)

    Args:
        data: '^'로 구분된 실시간 데이터

    Returns:
        파싱된 딕셔너리
    """
    values = data.split('^')

    return {
        'stock_code': values[0],          # 유가증권단축종목코드
        'trade_time': values[1],          # 주식체결시간
        'current_price': int(values[2]),  # 주식현재가
        'change_sign': values[3],         # 전일대비부호
        'change_amount': int(values[4]),  # 전일대비
        'change_rate': float(values[5]),  # 전일대비율
        'volume': int(values[12]),        # 체결거래량
        'acc_volume': int(values[13]),    # 누적거래량
        'acc_amount': int(values[14]),    # 누적거래대금
        'open_price': int(values[7]),     # 주식시가
        'high_price': int(values[8]),     # 주식최고가
        'low_price': int(values[9]),      # 주식최저가
        'timestamp': values[1]
    }


def parse_stock_orderbook_data(data: str) -> Dict:
    """
    주식호가 데이터 파싱 (H0STASP0)

    Args:
        data: '^'로 구분된 호가 데이터

    Returns:
        파싱된 호가 딕셔너리
    """
    values = data.split('^')

    # 매도호가 (3-12) / 매수호가 (13-22)
    # 매도잔량 (23-32) / 매수잔량 (33-42)

    asks = []  # 매도호가
    bids = []  # 매수호가

    for i in range(10):
        ask_price = int(values[3 + i]) if values[3 + i] else 0
        ask_volume = int(values[23 + i]) if values[23 + i] else 0
        bid_price = int(values[13 + i]) if values[13 + i] else 0
        bid_volume = int(values[33 + i]) if values[33 + i] else 0

        asks.append({'price': ask_price, 'volume': ask_volume})
        bids.append({'price': bid_price, 'volume': bid_volume})

    return {
        'stock_code': values[0],
        'trading_time': values[1],
        'time_code': values[2],
        'asks': asks,  # 매도호가 (높은가격순)
        'bids': bids,  # 매수호가 (높은가격순)
        'total_ask_volume': int(values[43]) if values[43] else 0,
        'total_bid_volume': int(values[44]) if values[44] else 0,
        'expected_price': int(values[47]) if values[47] else 0,
        'expected_volume': int(values[48]) if values[48] else 0,
        'timestamp': values[1]
    }


def parse_stock_execution_data(data: str, aes_key: str, aes_iv: str) -> Optional[Dict]:
    """
    주식체결통보 데이터 파싱 (H0STCNI0/H0STCNI9)

    Args:
        data: 암호화된 체결통보 데이터
        aes_key: AES 복호화 키
        aes_iv: AES 초기화 벡터

    Returns:
        파싱된 체결통보 딕셔너리
    """
    try:
        # AES 복호화
        decrypted_data = aes_cbc_base64_dec(aes_key, aes_iv, data)
        values = decrypted_data.split('^')

        execution_type = values[13] if len(values) > 13 else '1'

        if execution_type == '2':  # 체결통보
            return {
                'customer_id': values[0],
                'account_no': values[1],
                'order_no': values[2],
                'original_order_no': values[3],
                'buy_sell_code': values[4],
                'correction_code': values[5],
                'order_type': values[6],
                'order_condition': values[7],
                'stock_code': values[8],
                'execution_qty': int(values[9]) if values[9] else 0,
                'execution_price': int(values[10]) if values[10] else 0,
                'execution_time': values[11],
                'reject_yn': values[12],
                'execution_yn': values[13],
                'accept_yn': values[14],
                'branch_no': values[15],
                'order_qty': int(values[16]) if values[16] else 0,
                'account_name': values[17],
                'stock_name': values[18],
                'timestamp': values[11]
            }
        else:  # 주문접수통보
            return {
                'customer_id': values[0],
                'account_no': values[1],
                'order_no': values[2],
                'original_order_no': values[3],
                'buy_sell_code': values[4],
                'correction_code': values[5],
                'order_type': values[6],
                'order_condition': values[7],
                'stock_code': values[8],
                'order_qty': int(values[9]) if values[9] else 0,
                'order_price': int(values[10]) if values[10] else 0,
                'order_time': values[11],
                'reject_yn': values[12],
                'execution_yn': values[13],
                'accept_yn': values[14],
                'timestamp': values[11]
            }

    except Exception as e:
        print(f"체결통보 파싱 오류: {e}")
        return None


def parse_market_index_data(data: str) -> Dict:
    """
    시장지수 데이터 파싱 (H0UPCNT0)

    Args:
        data: '^'로 구분된 지수 데이터

    Returns:
        파싱된 지수 딕셔너리
    """
    values = data.split('^')

    return {
        'index_code': values[0],          # 지수코드
        'index_time': values[1],          # 지수시간
        'current_index': float(values[2]),# 현재지수
        'change_sign': values[3],         # 전일대비부호
        'change_amount': float(values[4]),# 전일대비
        'change_rate': float(values[5]),  # 전일대비율
        'open_index': float(values[6]),   # 시가지수
        'high_index': float(values[7]),   # 최고지수
        'low_index': float(values[8]),    # 최저지수
        'timestamp': values[1]
    }
