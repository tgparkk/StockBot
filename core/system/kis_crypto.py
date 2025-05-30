"""
KIS API AES 암호화/복호화 모듈
"""
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from base64 import b64decode


def aes_cbc_base64_dec(key: str, iv: str, cipher_text: str) -> str:
    """
    AES256 CBC 모드 Base64 디코딩

    Args:
        key: AES256 비밀키
        iv: 초기화 벡터
        cipher_text: Base64로 인코딩된 암호화 텍스트

    Returns:
        복호화된 문자열
    """
    cipher = AES.new(key.encode('utf-8'), AES.MODE_CBC, iv.encode('utf-8'))
    return bytes.decode(unpad(cipher.decrypt(b64decode(cipher_text)), AES.block_size))
