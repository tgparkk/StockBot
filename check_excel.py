#!/usr/bin/env python3
"""
엑셀 파일 구조 확인 스크립트
"""
import pandas as pd

def check_excel_structure():
    """엑셀 파일 구조 확인"""
    try:
        # 엑셀 파일 읽기
        df = pd.read_excel('data_0737_20250613.xlsx')
        
        print("=" * 60)
        print("📊 엑셀 파일 구조 분석")
        print("=" * 60)
        
        print(f"전체 행 수: {len(df)}")
        print(f"전체 열 수: {len(df.columns)}")
        print()
        
        print("컬럼명:")
        for i, col in enumerate(df.columns):
            print(f"{i+1:2}. {col}")
        print()
        
        # 시장구분 컬럼이 있는지 확인
        if '시장구분' in df.columns:
            print("시장구분 종류:")
            print(df['시장구분'].value_counts())
            print()
            
            # KOSPI 종목 수 확인
            kospi_df = df[df['시장구분'] == 'KOSPI']
            print(f"KOSPI 종목 수: {len(kospi_df)}")
            print()
            
            # 단축코드 컬럼 확인
            if '단축코드' in df.columns:
                print("KOSPI 종목 단축코드 샘플 (첫 10개):")
                print(kospi_df['단축코드'].head(10).tolist())
                print()
                
                # 단축코드 길이 확인
                code_lengths = kospi_df['단축코드'].astype(str).str.len().value_counts()
                print("단축코드 길이별 분포:")
                print(code_lengths)
                print()
            else:
                print("⚠️ '단축코드' 컬럼을 찾을 수 없습니다.")
                
        else:
            print("⚠️ '시장구분' 컬럼을 찾을 수 없습니다.")
            
        # 첫 5행 출력
        print("첫 5행 데이터:")
        print(df.head())
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    check_excel_structure() 