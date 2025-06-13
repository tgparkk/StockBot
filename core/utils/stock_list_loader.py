#!/usr/bin/env python3
"""
주식 종목 리스트 로더 유틸리티
"""
import pandas as pd
import warnings
from typing import List, Optional
from utils.logger import setup_logger

# openpyxl 스타일 경고 숨기기
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

logger = setup_logger(__name__)

def load_kospi_stocks(excel_path: str = "data_0737_20250613.xlsx") -> List[str]:
    """
    엑셀 파일에서 KOSPI 종목 리스트를 로드
    
    Args:
        excel_path: 엑셀 파일 경로
        
    Returns:
        KOSPI 종목 단축코드 리스트
    """
    try:
        # 엑셀 파일 읽기
        df = pd.read_excel(excel_path)
        
        # KOSPI 종목만 필터링
        kospi_df = df[df['시장구분'] == 'KOSPI'].copy()
        
        # 단축코드 추출 (문자열로 변환하여 일관성 확보)
        stock_codes = kospi_df['단축코드'].astype(str).tolist()
        
        # 6자리가 아닌 종목코드 필터링 (안전성 확보)
        valid_codes = [code for code in stock_codes if len(code) == 6 and code.isdigit()]
        
        logger.info(f"✅ KOSPI 종목 로드 완료: {len(valid_codes)}개 종목")
        logger.debug(f"📋 샘플 종목코드: {valid_codes[:10]}")
        
        return valid_codes
        
    except FileNotFoundError:
        logger.error(f"❌ 엑셀 파일을 찾을 수 없습니다: {excel_path}")
        return []
    except Exception as e:
        logger.error(f"❌ KOSPI 종목 로드 실패: {e}")
        return []

def get_stock_info_from_excel(stock_code: str, excel_path: str = "data_0737_20250613.xlsx") -> Optional[dict]:
    """
    특정 종목의 기본 정보를 엑셀에서 조회
    
    Args:
        stock_code: 종목 단축코드
        excel_path: 엑셀 파일 경로
        
    Returns:
        종목 정보 딕셔너리 (종목명, 상장주식수 등)
    """
    try:
        df = pd.read_excel(excel_path)
        
        # 해당 종목코드 조회
        stock_info = df[df['단축코드'].astype(str) == str(stock_code)]
        
        if stock_info.empty:
            return None
            
        info = stock_info.iloc[0]
        
        return {
            'stock_code': str(info['단축코드']),
            'stock_name': info['한글 종목명'],
            'stock_name_short': info['한글 종목약명'],
            'market_type': info['시장구분'],
            'listing_date': info['상장일'],
            'listed_shares': info['상장주식수'] if pd.notna(info['상장주식수']) else 0,
            'face_value': info['액면가'] if pd.notna(info['액면가']) else 0
        }
        
    except Exception as e:
        logger.error(f"❌ 종목 정보 조회 실패 {stock_code}: {e}")
        return None

def calculate_market_cap_filter(min_market_cap: int = 100_000_000_000, max_market_cap: int = 10_000_000_000_000) -> callable:
    """
    시가총액 필터 함수 생성기
    
    Args:
        min_market_cap: 최소 시가총액 (기본: 1,000억)
        max_market_cap: 최대 시가총액 (기본: 10조)
        
    Returns:
        시가총액 체크 함수
    """
    def check_market_cap(current_price: float, listed_shares: int) -> bool:
        """시가총액 기준 체크"""
        if listed_shares <= 0:
            return False
            
        market_cap = current_price * listed_shares
        return min_market_cap <= market_cap <= max_market_cap
    
    return check_market_cap

# 테스트 코드
if __name__ == "__main__":
    # KOSPI 종목 로드 테스트
    kospi_stocks = load_kospi_stocks()
    print(f"KOSPI 종목 수: {len(kospi_stocks)}")
    
    # 샘플 종목 정보 조회
    if kospi_stocks:
        sample_code = kospi_stocks[0]
        info = get_stock_info_from_excel(sample_code)
        print(f"샘플 종목 정보: {info}") 