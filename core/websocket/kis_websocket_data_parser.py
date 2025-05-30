#!/usr/bin/env python3
"""
KIS 웹소켓 데이터 파싱 전담 클래스
"""
from typing import Dict, Optional
from datetime import datetime
from utils.logger import setup_logger

# AES 복호화 (체결통보용)
try:
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad
    from base64 import b64decode
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

logger = setup_logger(__name__)


class KISWebSocketDataParser:
    """KIS 웹소켓 데이터 파싱 전담 클래스"""

    def __init__(self):
        # 체결통보용 복호화 키
        self.aes_key: Optional[str] = None
        self.aes_iv: Optional[str] = None

        # 통계
        self.stats = {
            'data_processed': 0,
            'errors': 0
        }

    def set_encryption_keys(self, aes_key: str, aes_iv: str):
        """체결통보 암호화 키 설정"""
        self.aes_key = aes_key
        self.aes_iv = aes_iv
        logger.info("체결통보 암호화 키 설정 완료")

    def parse_contract_data(self, data: str) -> Dict:
        """실시간 체결 데이터 파싱"""
        try:
            parts = data.split('^')
            if len(parts) < 20:
                return {}

            parsed_data = {
                'stock_code': parts[0],
                'time': parts[1],
                'current_price': int(parts[2]) if parts[2] else 0,
                'change_sign': parts[3],
                'change': int(parts[4]) if parts[4] else 0,
                'change_rate': float(parts[5]) if parts[5] else 0.0,
                'volume': int(parts[13]) if parts[13] else 0,
                'acc_volume': int(parts[14]) if parts[14] else 0,
                'strength': float(parts[18]) if parts[18] else 0.0,
                'timestamp': datetime.now(),
                'source': 'websocket',
                'type': 'contract'
            }

            # 통계 업데이트
            self.stats['data_processed'] += 1
            return parsed_data

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"체결 데이터 파싱 오류: {e}")
            return {}

    def parse_bid_ask_data(self, data: str) -> Dict:
        """실시간 호가 데이터 파싱"""
        try:
            parts = data.split('^')
            if len(parts) < 45:
                return {}

            parsed_data = {
                'stock_code': parts[0],
                'time': parts[1],
                'ask_price1': int(parts[3]) if parts[3] else 0,
                'bid_price1': int(parts[13]) if parts[13] else 0,
                'ask_qty1': int(parts[23]) if parts[23] else 0,
                'bid_qty1': int(parts[33]) if parts[33] else 0,
                'total_ask_qty': int(parts[43]) if parts[43] else 0,
                'total_bid_qty': int(parts[44]) if parts[44] else 0,
                'timestamp': datetime.now(),
                'source': 'websocket',
                'type': 'bid_ask'
            }

            # 통계 업데이트
            self.stats['data_processed'] += 1
            return parsed_data

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"호가 데이터 파싱 오류: {e}")
            return {}

    def decrypt_notice_data(self, encrypted_data: str) -> str:
        """체결통보 데이터 복호화"""
        if not CRYPTO_AVAILABLE or not self.aes_key or not self.aes_iv:
            return ""

        try:
            cipher = AES.new(self.aes_key.encode('utf-8'), AES.MODE_CBC, self.aes_iv.encode('utf-8'))
            decrypted = unpad(cipher.decrypt(b64decode(encrypted_data)), AES.block_size)
            return decrypted.decode('utf-8')

        except Exception as e:
            logger.error(f"체결통보 복호화 오류: {e}")
            return ""

    def get_stats(self) -> Dict:
        """파싱 통계 반환"""
        return self.stats.copy()
