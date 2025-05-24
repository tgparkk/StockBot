@echo off
title StockBot 업데이트
chcp 65001 > nul

echo.
echo ========================================
echo        🔄 StockBot 업데이트
echo ========================================
echo.

:: 현재 디렉토리를 스크립트 위치로 변경
cd /d "%~dp0"

:: 가상환경 활성화
if not exist ".venv\" (
    echo ❌ 가상환경이 없습니다. install.bat를 먼저 실행하세요.
    pause
    exit /b 1
)

echo 📦 가상환경 활성화 중...
call .venv\Scripts\activate.bat

:: pip 업그레이드
echo 🔄 pip 업그레이드 중...
python -m pip install --upgrade pip

:: requirements.txt 업데이트
echo 📋 패키지 업데이트 중...
pip install -r requirements.txt --upgrade
if errorlevel 1 (
    echo ❌ 패키지 업데이트 실패
    pause
    exit /b 1
)

echo ✅ 패키지 업데이트 완료

:: 데이터베이스 스키마 업데이트 (필요시)
echo 📊 데이터베이스 확인 중...
if exist "database\migrate.py" (
    echo 🔄 데이터베이스 마이그레이션 실행 중...
    python database\migrate.py
    if errorlevel 1 (
        echo ⚠️  데이터베이스 마이그레이션 경고 (수동 확인 필요)
    ) else (
        echo ✅ 데이터베이스 마이그레이션 완료
    )
) else (
    echo ✅ 데이터베이스 업데이트 불필요
)

:: 백업된 로그 정리
echo 🧹 오래된 로그 파일 정리 중...
if exist "logs\" (
    forfiles /p logs /m *.log /d -30 /c "cmd /c del @path" 2>nul
    echo ✅ 30일 이상된 로그 파일 정리 완료
)

echo.
echo ========================================
echo         ✅ 업데이트 완료!
echo ========================================
echo.
echo 변경사항:
echo - Python 패키지 최신 버전으로 업데이트
echo - 데이터베이스 스키마 확인 및 업데이트
echo - 오래된 로그 파일 정리
echo.
echo 이제 run_stockbot.bat로 StockBot을 실행할 수 있습니다.
echo.

pause
