#!/usr/bin/env python3
"""
StockBot 설정 검증 스크립트
실행 전에 .env 파일과 KIS API 연결을 확인합니다.
"""
import os
import sys
from pathlib import Path

def main():
    print("=" * 60)
    print("🔧 StockBot 설정 검증 도구")
    print("=" * 60)

    project_root = Path(__file__).parent
    env_path = project_root / '.env'

    # 1. .env 파일 존재 확인
    print("\n📁 1. .env 파일 확인")
    print("-" * 30)

    if not env_path.exists():
        print("❌ .env 파일이 없습니다!")
        print(f"📍 위치: {env_path}")
        print("📝 .env 파일을 생성해주세요.")
        return False
    else:
        print(f"✅ .env 파일 존재: {env_path}")

    # 2. 설정값 로드 및 검증
    print("\n🔑 2. 설정값 검증")
    print("-" * 30)

    try:
        from config.settings import (
            APP_KEY, SECRET_KEY, ACCOUNT_NUMBER, HTS_ID,
            KIS_BASE_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
        )

        # 필수 설정 확인
        required_configs = {
            'KIS_APP_KEY': APP_KEY,
            'KIS_APP_SECRET': SECRET_KEY,
            'KIS_ACCOUNT_NO': ACCOUNT_NUMBER,
            'KIS_HTS_ID': HTS_ID
        }

        all_good = True

        for key, value in required_configs.items():
            if not value or value == f'your_{key.lower().replace("kis_", "")}_here':
                print(f"❌ {key}: 미설정")
                all_good = False
            else:
                print(f"✅ {key}: 설정됨 ({'*' * 10})")

        # 선택 설정 확인
        print(f"{'🔔' if TELEGRAM_BOT_TOKEN and TELEGRAM_BOT_TOKEN != 'your_telegram_bot_token_here' else '⚪'} TELEGRAM_BOT_TOKEN: {'설정됨' if TELEGRAM_BOT_TOKEN and TELEGRAM_BOT_TOKEN != 'your_telegram_bot_token_here' else '미설정 (선택사항)'}")
        print(f"{'📱' if TELEGRAM_CHAT_ID and TELEGRAM_CHAT_ID != 'your_telegram_chat_id_here' else '⚪'} TELEGRAM_CHAT_ID: {'설정됨' if TELEGRAM_CHAT_ID and TELEGRAM_CHAT_ID != 'your_telegram_chat_id_here' else '미설정 (선택사항)'}")

        if not all_good:
            print("\n❌ 필수 설정값이 부족합니다!")
            return False

    except Exception as e:
        print(f"❌ 설정 로드 오류: {e}")
        return False

    # 3. KIS API 연결 테스트
    print("\n🌐 3. KIS API 연결 테스트")
    print("-" * 30)

    try:
        from core.api import kis_auth

        # 인증 테스트
        if kis_auth.auth():
            print("✅ KIS API 인증 성공!")

            # 환경 정보 확인
            env_info = kis_auth.getTREnv()
            if env_info:
                print(f"📊 계좌: {env_info.my_acct}")
                print(f"🏢 상품코드: {env_info.my_prod}")
                print(f"🌐 서버: {env_info.my_url}")

        else:
            print("❌ KIS API 인증 실패!")
            return False

    except Exception as e:
        print(f"❌ KIS API 연결 오류: {e}")
        return False

    # 4. 최종 결과
    print("\n🎉 4. 검증 결과")
    print("-" * 30)
    print("✅ 모든 설정이 정상입니다!")
    print("🚀 StockBot을 실행할 준비가 되었습니다.")

    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⏹️ 사용자에 의해 중단되었습니다.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        sys.exit(1)
