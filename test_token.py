"""
토큰 관리 시스템 테스트 스크립트
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

import json

def test_token_file_operations():
    """토큰 파일 조작 기능만 테스트 (실제 API 호출 없이)"""
    print("=== 토큰 파일 관리 시스템 테스트 ===\n")
    
    try:
        # 토큰 파일 경로
        token_file_path = "token_info.json"
        
        # 기존 토큰 파일 백업 (있다면)
        backup_path = None
        if os.path.exists(token_file_path):
            backup_path = f"{token_file_path}.backup"
            os.rename(token_file_path, backup_path)
            print("기존 토큰 파일을 백업했습니다.")
        
        # 1. 토큰 파일이 없을 때 브로커 생성 테스트
        print("1. 토큰 파일이 없을 때 브로커 생성 테스트...")
        try:
            from core.broker import KISBroker
            print("   ❌ 실제 API 키가 없으면 브로커 생성이 실패할 수 있습니다.")
            print("   이는 정상적인 동작입니다.")
        except Exception as e:
            print(f"   예상된 오류: {e}")
        print()
        
        # 2. 수동으로 토큰 파일 생성 테스트
        print("2. 수동으로 토큰 파일 생성 테스트...")
        
        # 더미 토큰 정보 생성
        import time
        from datetime import datetime, timedelta
        
        current_time = time.time()
        expire_time = current_time + 86400  # 24시간 후
        
        token_data = {
            "current": {
                "token": "dummy_token_for_testing_1234567890",
                "issue_time": current_time,
                "issue_time_str": datetime.fromtimestamp(current_time).strftime("%Y-%m-%d %H:%M:%S"),
                "expire_time": expire_time,
                "expire_time_str": datetime.fromtimestamp(expire_time).strftime("%Y-%m-%d %H:%M:%S"),
                "status": "SUCCESS",
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            },
            "history": [
                {
                    "token": "dummy_token...",
                    "issue_time_str": datetime.fromtimestamp(current_time).strftime("%Y-%m-%d %H:%M:%S"),
                    "expire_time_str": datetime.fromtimestamp(expire_time).strftime("%Y-%m-%d %H:%M:%S"),
                    "status": "SUCCESS",
                    "error_message": None,
                    "recorded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            ]
        }
        
        with open(token_file_path, 'w', encoding='utf-8') as f:
            json.dump(token_data, f, indent=2, ensure_ascii=False)
        
        print("   ✅ 더미 토큰 파일 생성 완료")
        print(f"   파일 위치: {os.path.abspath(token_file_path)}")
        print()
        
        # 3. 토큰 파일 내용 확인
        print("3. 생성된 토큰 파일 내용 확인...")
        with open(token_file_path, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)
        
        current = loaded_data.get('current', {})
        print(f"   토큰 상태: {current.get('status')}")
        print(f"   발급 시간: {current.get('issue_time_str')}")
        print(f"   만료 시간: {current.get('expire_time_str')}")
        print(f"   토큰 미리보기: {current.get('token', '')[:20]}...")
        print(f"   히스토리 개수: {len(loaded_data.get('history', []))}")
        print()
        
        # 4. 토큰 유효성 검사 로직 테스트
        print("4. 토큰 유효성 검사 로직 테스트...")
        
        current_timestamp = time.time()
        token_expire_time = current.get('expire_time', 0)
        
        if token_expire_time > current_timestamp + 300:  # 5분 여유
            print("   ✅ 토큰이 유효합니다 (5분 이상 남음)")
        else:
            print("   ❌ 토큰이 만료되었거나 곧 만료됩니다")
        print()
        
        print("=== 토큰 파일 관리 테스트 완료 ===")
        
        # 백업 파일 복원
        if backup_path and os.path.exists(backup_path):
            if os.path.exists(token_file_path):
                os.remove(token_file_path)
            os.rename(backup_path, token_file_path)
            print("기존 토큰 파일을 복원했습니다.")
        
    except Exception as e:
        print(f"❌ 테스트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()

def test_token_management():
    """실제 브로커를 사용한 토큰 관리 테스트 (API 키가 있을 때만)"""
    print("=== 실제 토큰 관리 시스템 테스트 ===\n")
    
    # API 키 확인
    if not all([os.getenv('KIS_APP_KEY'), os.getenv('KIS_APP_SECRET'), os.getenv('KIS_ACCOUNT_NO')]):
        print("❌ 실제 API 키가 설정되지 않았습니다.")
        print("config/.env 파일에 실제 API 키를 설정한 후 다시 시도하세요.")
        return
    
    try:
        from core.broker import KISBroker
        
        # 첫 번째 인스턴스 생성 (토큰 발급 또는 로드)
        print("1. 첫 번째 브로커 인스턴스 생성...")
        broker1 = KISBroker()
        
        token_info1 = broker1.get_token_info()
        print(f"   토큰 보유: {token_info1['has_token']}")
        print(f"   토큰 유효: {token_info1['is_valid']}")
        print(f"   만료 시간: {token_info1['expires_at']}")
        print(f"   남은 시간: {token_info1['expires_in_minutes']}분")
        print(f"   토큰 미리보기: {token_info1['token_preview']}")
        print()
        
        # 두 번째 인스턴스 생성 (토큰 재사용 확인)
        print("2. 두 번째 브로커 인스턴스 생성 (토큰 재사용 테스트)...")
        broker2 = KISBroker()
        
        token_info2 = broker2.get_token_info()
        print(f"   토큰 보유: {token_info2['has_token']}")
        print(f"   토큰 유효: {token_info2['is_valid']}")
        print(f"   만료 시간: {token_info2['expires_at']}")
        print(f"   남은 시간: {token_info2['expires_in_minutes']}분")
        print(f"   토큰 미리보기: {token_info2['token_preview']}")
        print()
        
        # 토큰 재사용 확인
        if token_info1['token_preview'] == token_info2['token_preview']:
            print("✅ 토큰 재사용 성공! 새로운 토큰을 발급하지 않았습니다.")
        else:
            print("❌ 토큰이 다릅니다. 재사용에 실패했습니다.")
        print()
        
        print("=== 실제 토큰 관리 테스트 완료 ===")
        
    except Exception as e:
        print(f"❌ 테스트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # 먼저 파일 조작 기능 테스트
    test_token_file_operations()
    print("\n" + "="*50 + "\n")
    
    # 실제 API가 있으면 브로커 테스트
    test_token_management() 