@echo off
title StockBot (테스트 모드 - 장외시간 대응)
chcp 65001 > nul

echo.
echo ========================================
echo 🧪 StockBot (테스트 모드 - 장외시간 대응)
echo ========================================
echo.

:: 현재 디렉토리를 스크립트 위치로 변경
cd /d "%~dp0"

:: 테스트 모드 환경변수 설정
set TESTING_MODE=true
set DISABLE_TELEGRAM=false

:: 가상환경 경로 확인
if not exist ".venv\" (
    echo ❌ 가상환경이 없습니다. install.bat을 먼저 실행하세요.
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

echo.
echo 🧪 테스트 모드 설정:
echo    - 장외시간 대응 활성화
echo    - API 에러 무시 (경고로 처리)
echo    - 웹소켓 연결 실패 시 계속 진행
echo    - 제한적 데이터 수집 모드
echo.
echo ⚠️  주의: 테스트 모드는 교육/개발 목적입니다.
echo    실제 거래는 장시간에만 수행하세요.
echo.
echo 🚀 StockBot 테스트 모드 시작 중...
echo.

:: 데이터베이스 초기화 확인
if not exist "data\trades.db" (
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

:: main.py 실행
python main.py

:: 실행 결과 확인
if errorlevel 1 (
    echo.
    echo ❌ StockBot 테스트 모드 실행 중 오류가 발생했습니다.
    echo    로그 파일을 확인하세요: logs\
    echo.
) else (
    echo.
    echo ✅ StockBot 테스트 모드가 정상적으로 종료되었습니다.
    echo.
)

pause
