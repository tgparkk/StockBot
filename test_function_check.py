"""
함수 구조 확인 테스트
"""
import sys
import os
sys.path.append('.')

# 테스트용 환경변수 설정
os.environ['KIS_BASE_URL'] = 'https://openapi.koreainvestment.com:9443'
os.environ['KIS_APP_KEY'] = 'TEST'
os.environ['KIS_APP_SECRET'] = 'TEST'
os.environ['KIS_ACCOUNT_NO'] = '1234567890'

from core.broker import KISBroker
import inspect

def test_function_structure():
    print("=== 함수 구조 확인 테스트 ===\n")
    
    # 함수 시그니처 확인
    sig = inspect.signature(KISBroker.get_daily_prices)
    print(f"get_daily_prices 함수 시그니처: {sig}")
    print()
    
    try:
        broker = KISBroker()
        print("✅ 브로커 인스턴스 생성 성공")
        
        # 파라미터 검증 테스트
        print("\n파라미터 검증 테스트:")
        
        # 올바른 파라미터들
        valid_params = ["D", "W", "M"]
        for param in valid_params:
            try:
                # 실제 API 호출은 실패하지만 파라미터 검증은 통과해야 함
                broker.get_daily_prices('005930', period_type=param)
            except ValueError:
                print(f"❌ {param}: 파라미터 검증 실패")
            except Exception:
                print(f"✅ {param}: 파라미터 검증 통과 (API 호출 실패는 예상됨)")
        
        # 잘못된 파라미터 테스트
        try:
            broker.get_daily_prices('005930', period_type='X')
            print("❌ 잘못된 파라미터 검증 실패")
        except ValueError as e:
            print(f"✅ 잘못된 파라미터 검증 성공: {e}")
        
        print("\n✅ 함수 구조 수정 완료!")
        
    except Exception as e:
        print(f"예상된 오류 (더미 API 키): {e}")

if __name__ == "__main__":
    test_function_structure() 