@echo off
title StockBot 설치 및 초기 구성
chcp 65001 > nul

echo.
echo ========================================
echo      🔧 StockBot 설치 및 초기 구성
echo ========================================
echo.

:: 현재 디렉토리를 스크립트 위치로 변경
cd /d "%~dp0"

:: Python 설치 확인
echo 🐍 Python 설치 확인 중...
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python이 설치되지 않았습니다.
    echo    Python 3.8 이상을 설치하고 PATH에 추가하세요.
    echo    다운로드: https://www.python.org/downloads/
    echo.
    pause
    exit /b 1
)

python --version
echo ✅ Python이 설치되어 있습니다.
echo.

:: 가상환경 생성
if not exist ".venv\" (
    echo 📦 가상환경 생성 중...
    python -m venv .venv
    if errorlevel 1 (
        echo ❌ 가상환경 생성 실패
        pause
        exit /b 1
    )
    echo ✅ 가상환경 생성 완료
) else (
    echo ✅ 가상환경이 이미 존재합니다.
)

:: 가상환경 활성화
echo 📦 가상환경 활성화 중...
call .venv\Scripts\activate.bat
if errorlevel 1 (
    echo ❌ 가상환경 활성화 실패
    pause
    exit /b 1
)

:: pip 업그레이드
echo 🔄 pip 업그레이드 중...
python -m pip install --upgrade pip

:: requirements.txt 설치
echo 📋 패키지 설치 중...
if exist "requirements.txt" (
    pip install -r requirements.txt
    if errorlevel 1 (
        echo ❌ 패키지 설치 실패
        pause
        exit /b 1
    )
    echo ✅ 모든 패키지 설치 완료
) else (
    echo ❌ requirements.txt 파일이 없습니다.
    pause
    exit /b 1
)

:: config 디렉토리 생성
if not exist "config\" mkdir config

:: .env 파일 템플릿 생성
if not exist "config\.env" (
    echo 📝 환경설정 파일 템플릿 생성 중...
    (
        echo # 한국투자증권 API 설정
        echo KIS_APP_KEY=your_app_key_here
        echo KIS_APP_SECRET=your_app_secret_here
        echo KIS_ACCOUNT_NO=your_account_number_here
        echo.
        echo # 텔레그램 봇 설정 ^(선택사항^)
        echo TELEGRAM_BOT_TOKEN=your_bot_token_here
        echo TELEGRAM_CHAT_ID=your_chat_id_here
        echo.
        echo # 기타 설정
        echo LOG_LEVEL=INFO
    ) > "config\.env"
    echo ✅ config\.env 파일 템플릿 생성 완료
    echo.
    echo ⚠️  중요: config\.env 파일에 실제 API 키를 입력하세요!
    echo.
)

:: 로그 디렉토리 생성
if not exist "logs\" mkdir logs
echo ✅ 로그 디렉토리 생성 완료

:: 데이터베이스 초기화
echo 📊 데이터베이스 초기화 중...
python database\init_db.py
if errorlevel 1 (
    echo ❌ 데이터베이스 초기화 실패
    pause
    exit /b 1
)
echo ✅ 데이터베이스 초기화 완료

echo.
echo ========================================
echo        ✅ 설치 및 초기 구성 완료!
echo ========================================
echo.
echo 📋 다음 단계:
echo.
echo 1. config\.env 파일을 편집하여 실제 API 키 입력
echo    - KIS_APP_KEY: 한국투자증권 앱 키
echo    - KIS_APP_SECRET: 한국투자증권 앱 시크릿
echo    - KIS_ACCOUNT_NO: 계좌번호
echo.
echo 2. 텔레그램 봇 사용시 (선택사항):
echo    - TELEGRAM_BOT_TOKEN: 봇 토큰
echo    - TELEGRAM_CHAT_ID: 채팅 ID
echo.
echo 3. 실행 방법:
echo    - run_stockbot.bat: 일반 실행
echo    - run_stockbot_no_telegram.bat: 텔레그램 없이 실행
echo.
echo ⚠️  주의: 실전 환경에서는 신중하게 사용하세요!
echo.

pause
