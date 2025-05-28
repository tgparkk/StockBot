"""
StockBot 설정 파일
.env 파일에서 환경 변수를 읽어와서 설정을 관리합니다
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# .env 파일 로드
project_root = Path(__file__).parent.parent
env_path = project_root / '.env'
load_dotenv(env_path)

# KIS 한국투자증권 API 설정
KIS_BASE_URL = os.getenv('KIS_BASE_URL', 'https://openapi.koreainvestment.com:9443')
APP_KEY = os.getenv('KIS_APP_KEY', '')
SECRET_KEY = os.getenv('KIS_APP_SECRET', '')
ACCOUNT_NUMBER = os.getenv('KIS_ACCOUNT_NO', '')
ACCOUNT_NUMBER_PREFIX = ACCOUNT_NUMBER[:8] if ACCOUNT_NUMBER else ''
HTS_ID = os.getenv('KIS_HTS_ID', '')

# 텔레그램 봇 설정
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')

# 기타 설정
IS_DEMO = os.getenv('IS_DEMO', 'false').lower() == 'true'
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# === 거래 방식 설정 ===
TRADING_MODE = "swing"  # "day" = 당일매매, "swing" = 스윙트레이딩
DAY_TRADING_EXIT_TIME = "15:00"  # 당일매매시 강제 매도 시간 (장마감 30분 전)

# 설정 검증
def validate_settings():
    """필수 설정값 검증"""
    required_settings = {
        'APP_KEY': APP_KEY,
        'SECRET_KEY': SECRET_KEY,
        'ACCOUNT_NUMBER': ACCOUNT_NUMBER,
        'HTS_ID': HTS_ID,
    }

    missing = [key for key, value in required_settings.items() if not value]

    if missing:
        print(f"❌ 필수 설정값이 누락되었습니다: {', '.join(missing)}")
        print("📝 .env 파일에 다음 설정값들을 추가해주세요:")
        print("KIS_APP_KEY=your_app_key_here")
        print("KIS_APP_SECRET=your_app_secret_here")
        print("KIS_ACCOUNT_NO=your_account_number_here")
        print("KIS_HTS_ID=your_hts_id_here")
        print("TELEGRAM_BOT_TOKEN=your_telegram_bot_token_here")
        print("TELEGRAM_CHAT_ID=your_telegram_chat_id_here")
        return False

    print("✅ 모든 필수 설정값이 정상적으로 로드되었습니다")
    return True

def check_critical_settings():
    """중요 설정값 확인 (KIS API 키)"""
    if not APP_KEY or not SECRET_KEY:
        print(f"❌ KIS API 키가 설정되지 않았습니다!")
        print(f"APP_KEY: {'설정됨' if APP_KEY else '미설정'}")
        print(f"SECRET_KEY: {'설정됨' if SECRET_KEY else '미설정'}")
        print("📝 .env 파일을 확인해주세요.")
        return False
    return True

# import 시 중요 설정값 자동 확인
if not check_critical_settings():
    print("⚠️ 설정 오류로 인해 KIS API 인증이 실패할 수 있습니다.")

# 직접 실행 시 전체 검증
if __name__ == "__main__":
    validate_settings()
