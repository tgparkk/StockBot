#!/usr/bin/env python
"""
StockBot 디버깅 전용 실행 스크립트
개발 환경에서 편리한 디버깅을 위한 설정
"""
import os
import sys
import asyncio
import argparse
from dotenv import load_dotenv

# 프로젝트 루트를 Python 경로에 추가
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# 환경변수 로드
load_dotenv(os.path.join(project_root, 'config', '.env'))

from utils.logger import setup_logger

logger = setup_logger(__name__)

def setup_debug_environment():
    """디버깅 환경 설정"""
    # 환경변수 설정
    debug_env_vars = {
        'PYTHONPATH': project_root,
        'PYTHONUNBUFFERED': '1',
        'LOG_LEVEL': 'DEBUG',
    }

    for key, value in debug_env_vars.items():
        os.environ.setdefault(key, value)

    logger.info("🔧 디버깅 환경 설정 완료")

def check_requirements():
    """필요한 모듈들이 설치되어 있는지 확인"""
    required_modules = [
        'asyncio',
        'pandas',
        'numpy',
        'requests',
        'websocket-client',
        'python-telegram-bot',
        'peewee',
        'python-dotenv'
    ]

    missing_modules = []
    for module in required_modules:
        try:
            __import__(module.replace('-', '_'))
        except ImportError:
            missing_modules.append(module)

    if missing_modules:
        logger.error(f"❌ 누락된 모듈: {', '.join(missing_modules)}")
        logger.error("pip install -r requirements.txt를 실행하세요.")
        return False

    logger.info("✅ 모든 필수 모듈이 설치되어 있습니다.")
    return True

def check_config():
    """설정 파일들 확인"""
    config_files = [
        'config/.env',
        'token_info.json'
    ]

    missing_files = []
    for file_path in config_files:
        full_path = os.path.join(project_root, file_path)
        if not os.path.exists(full_path):
            missing_files.append(file_path)

    if missing_files:
        logger.warning(f"⚠️ 누락된 설정 파일: {', '.join(missing_files)}")
    else:
        logger.info("✅ 모든 설정 파일이 존재합니다.")

    # 환경변수 확인
    required_env_vars = ['KIS_APP_KEY', 'KIS_APP_SECRET', 'KIS_ACCOUNT_NO']
    missing_env_vars = []

    for var in required_env_vars:
        if not os.getenv(var):
            missing_env_vars.append(var)

    if missing_env_vars:
        logger.warning(f"⚠️ 누락된 환경변수: {', '.join(missing_env_vars)}")
        logger.warning("config/.env 파일에 설정하세요.")
    else:
        logger.info("✅ 필수 환경변수가 설정되어 있습니다.")

async def debug_run(args):
    """디버깅 모드로 실행"""
    logger.info("🐛 StockBot 디버깅 모드 시작")

    # 디버깅 환경 설정
    setup_debug_environment()

    # 텔레그램 봇 비활성화 (옵션)
    if args.no_telegram:
        os.environ['DISABLE_TELEGRAM'] = 'true'
        logger.info("🤖 텔레그램 봇 비활성화")

    # StockBot 임포트 (지연 임포트)
    from main import StockBotMain

    # StockBot 인스턴스 생성
    bot = StockBotMain()

    try:
        # 실행
        await bot.run()
    except KeyboardInterrupt:
        logger.info("👋 디버깅 중단됨")
    except Exception as e:
        logger.error(f"❌ 디버깅 중 오류: {e}")
        raise
    finally:
        await bot.cleanup()

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="StockBot 디버깅 실행기")
    parser.add_argument(
        '--no-telegram',
        action='store_true',
        help='텔레그램 봇 비활성화'
    )
    parser.add_argument(
        '--check-only',
        action='store_true',
        help='설정 확인만 수행 (실행하지 않음)'
    )

    args = parser.parse_args()

    print("🚀 StockBot 디버깅 환경 준비 중...")

    # 설정 확인
    if not check_requirements():
        sys.exit(1)

    check_config()

    if args.check_only:
        print("✅ 설정 확인 완료")
        return

    # 실행
    try:
        asyncio.run(debug_run(args))
    except KeyboardInterrupt:
        print("\n👋 디버깅 세션 종료")
    except Exception as e:
        print(f"\n❌ 실행 오류: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

