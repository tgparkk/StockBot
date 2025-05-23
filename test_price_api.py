"""
기간별 시세 조회 API 테스트 스크립트
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 테스트용 환경변수 설정 (실제 API 키가 없을 때)
if not os.getenv('KIS_APP_KEY'):
    os.environ['KIS_BASE_URL'] = 'https://openapi.koreainvestment.com:9443'
    os.environ['KIS_APP_KEY'] = 'TEST_APP_KEY'
    os.environ['KIS_APP_SECRET'] = 'TEST_APP_SECRET'
    os.environ['KIS_ACCOUNT_NO'] = '1234567890'

def test_price_apis():
    """시세 조회 API 테스트"""
    print("=== 기간별 시세 조회 API 테스트 ===\n")
    
    # API 키 확인
    if not all([os.getenv('KIS_APP_KEY'), os.getenv('KIS_APP_SECRET'), os.getenv('KIS_ACCOUNT_NO')]):
        print("❌ 실제 API 키가 설정되지 않았습니다.")
        print("config/.env 파일에 실제 API 키를 설정한 후 다시 시도하세요.")
        print("\n테스트용 더미 데이터로 함수 구조만 확인합니다...\n")
        test_function_structure()
        return
    
    try:
        from core.broker import KISBroker
        
        broker = KISBroker()
        test_stock = "005930"  # 삼성전자
        
        print(f"테스트 종목: {test_stock} (삼성전자)")
        print()
        
        # 1. 일봉 데이터 테스트
        print("1. 일봉 데이터 조회 테스트...")
        try:
            daily_data = broker.get_daily_prices(test_stock, period_type="D")
            print(f"   ✅ 일봉 데이터 {len(daily_data)}개 조회 성공")
            if daily_data:
                latest = daily_data[0]
                print(f"   최신 데이터: {latest['date']} 종가 {latest['close']:,}원")
        except Exception as e:
            print(f"   ❌ 일봉 데이터 조회 실패: {e}")
        print()
        
        # 2. 주봉 데이터 테스트
        print("2. 주봉 데이터 조회 테스트...")
        try:
            weekly_data = broker.get_daily_prices(test_stock, period_type="W")
            print(f"   ✅ 주봉 데이터 {len(weekly_data)}개 조회 성공")
            if weekly_data:
                latest = weekly_data[0]
                print(f"   최신 데이터: {latest['date']} 종가 {latest['close']:,}원")
        except Exception as e:
            print(f"   ❌ 주봉 데이터 조회 실패: {e}")
        print()
        
        # 3. 월봉 데이터 테스트
        print("3. 월봉 데이터 조회 테스트...")
        try:
            monthly_data = broker.get_daily_prices(test_stock, period_type="M")
            print(f"   ✅ 월봉 데이터 {len(monthly_data)}개 조회 성공")
            if monthly_data:
                latest = monthly_data[0]
                print(f"   최신 데이터: {latest['date']} 종가 {latest['close']:,}원")
        except Exception as e:
            print(f"   ❌ 월봉 데이터 조회 실패: {e}")
        print()
        
        # 4. 분봉 데이터 테스트
        print("4. 분봉 데이터 조회 테스트...")
        try:
            minute_data = broker.get_minute_prices(test_stock, period=5, time_unit="1")
            print(f"   ✅ 1분봉 데이터 {len(minute_data)}개 조회 성공")
            if minute_data:
                latest = minute_data[0]
                print(f"   최신 데이터: {latest['datetime']} 종가 {latest['close']:,}원")
        except Exception as e:
            print(f"   ❌ 분봉 데이터 조회 실패: {e}")
        print()
        
        # 5. 현재가 조회 테스트
        print("5. 현재가 조회 테스트...")
        try:
            current_price = broker.get_current_price(test_stock)
            print(f"   ✅ 현재가 조회 성공")
            print(f"   현재가: {current_price['current_price']:,}원")
            print(f"   전일대비: {current_price['change_rate']:+.2f}%")
            print(f"   거래량: {current_price['volume']:,}주")
        except Exception as e:
            print(f"   ❌ 현재가 조회 실패: {e}")
        print()
        
        print("=== 시세 조회 API 테스트 완료 ===")
        
    except Exception as e:
        print(f"❌ 테스트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()

def test_function_structure():
    """함수 구조 테스트 (실제 API 호출 없이)"""
    print("=== 함수 구조 테스트 ===\n")
    
    try:
        from core.broker import KISBroker
        
        # 브로커 인스턴스 생성 시도
        print("1. 브로커 클래스 구조 확인...")
        
        # 클래스 메서드 확인
        methods = [method for method in dir(KISBroker) if not method.startswith('_')]
        price_methods = [method for method in methods if 'price' in method.lower()]
        
        print(f"   전체 메서드 수: {len(methods)}")
        print(f"   시세 관련 메서드: {price_methods}")
        print()
        
        # 함수 시그니처 확인
        print("2. 시세 조회 함수 시그니처 확인...")
        
        import inspect
        
        # get_daily_prices 함수 시그니처
        if hasattr(KISBroker, 'get_daily_prices'):
            sig = inspect.signature(KISBroker.get_daily_prices)
            print(f"   get_daily_prices{sig}")
        
        # get_minute_prices 함수 시그니처
        if hasattr(KISBroker, 'get_minute_prices'):
            sig = inspect.signature(KISBroker.get_minute_prices)
            print(f"   get_minute_prices{sig}")
        
        # get_current_price 함수 시그니처
        if hasattr(KISBroker, 'get_current_price'):
            sig = inspect.signature(KISBroker.get_current_price)
            print(f"   get_current_price{sig}")
        
        print()
        print("✅ 함수 구조 확인 완료")
        
    except Exception as e:
        print(f"❌ 함수 구조 테스트 실패: {e}")

if __name__ == "__main__":
    test_price_apis() 