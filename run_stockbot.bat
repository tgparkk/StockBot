@echo off
title StockBot - 자동매매 시스템
chcp 65001 > nul

echo.
echo ========================================
echo    🚀 StockBot 자동매매 시스템 v1.0
echo ========================================
echo.

:: 현재 디렉토리를 스크립트 위치로 변경
cd /d "%~dp0"

:: 가상환경 경로 확인
if not exist ".venv\" (
    echo ❌ 가상환경이 없습니다. 먼저 가상환경을 생성하세요.
    echo    python -m venv .venv
    echo.
    pause
    exit /b 1
)

:: 가상환경 활성화
echo 📦 가상환경 활성화 중...
call .venv\Scripts\activate.bat
if errorlevel 1 (
    echo ❌ 가상환경 활성화 실패
    pause
    exit /b 1
)

:: Python 버전 확인
echo 🐍 Python 버전 확인...
python --version

:: 환경설정 파일 확인
if not exist "config\.env" (
    echo.
    echo ⚠️  환경설정 파일이 없습니다!
    echo    config\.env 파일을 생성하고 API 키를 설정하세요.
    echo.
    echo    필수 설정:
    echo    - KIS_APP_KEY=your_app_key
    echo    - KIS_APP_SECRET=your_app_secret
    echo    - KIS_ACCOUNT_NO=your_account_number
    echo.
    pause
    exit /b 1
)

:: 의존성 설치 여부 확인
echo.
echo 📋 의존성 확인 중...
python -c "import pandas, numpy, peewee, telegram" 2>nul
if errorlevel 1 (
    echo 📦 필수 패키지가 없습니다. 설치를 진행합니다...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo ❌ 패키지 설치 실패
        pause
        exit /b 1
    )
    echo ✅ 패키지 설치 완료
) else (
    echo ✅ 모든 의존성이 설치되어 있습니다.
)

:: 데이터베이스 초기화 확인
if not exist "data\stockbot.db" (
    echo.
    echo 📊 데이터베이스 초기화 중...
    python database\init_db.py
    if errorlevel 1 (
        echo ❌ 데이터베이스 초기화 실패
        pause
        exit /b 1
    )
    echo ✅ 데이터베이스 초기화 완료
)

:: 로그 디렉토리 생성
if not exist "logs" mkdir logs

echo.
echo 🚀 StockBot 시작 중...
echo.
echo ⚠️  주의사항:
echo    - 실전 투자 환경입니다. 신중하게 사용하세요.
echo    - Ctrl+C로 안전하게 종료할 수 있습니다.
echo    - 로그는 logs 폴더에 저장됩니다.
echo.
echo ========================================
echo.

:: main.py 실행
python main.py

:: 실행 결과 확인
if errorlevel 1 (
    echo.
    echo ❌ StockBot 실행 중 오류가 발생했습니다.
    echo    로그 파일을 확인하세요: logs\
    echo.
) else (
    echo.
    echo ✅ StockBot이 정상적으로 종료되었습니다.
    echo.
)

pause
