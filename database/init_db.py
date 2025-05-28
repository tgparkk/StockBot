#!/usr/bin/env python3
"""
데이터베이스 초기화 스크립트 - 기존 trade_database.py와 완전 호환
StockBot에서 사용하는 데이터베이스 테이블 생성
"""
import os
import sys
import sqlite3
from pathlib import Path

# 프로젝트 루트 경로 설정
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.logger import setup_logger

logger = setup_logger(__name__)

# 데이터베이스 파일 경로 - 기존 trade_database.py와 동일
DB_PATH = project_root / "data" / "trades.db"

def create_database_directory():
    """데이터베이스 디렉토리 생성"""
    try:
        data_dir = project_root / "data"
        data_dir.mkdir(exist_ok=True)
        logger.info(f"✅ 데이터 디렉토리 확인/생성: {data_dir}")
        return True
    except Exception as e:
        logger.error(f"❌ 데이터 디렉토리 생성 실패: {e}")
        return False

def initialize_database():
    """데이터베이스 초기화 - 기존 TradeDatabase 클래스가 자동으로 처리"""
    try:
        logger.info("📊 데이터베이스 초기화 시작... (기존 시스템 활용)")
        
        # 1. 디렉토리 생성
        if not create_database_directory():
            return False
        
        # 2. TradeDatabase 클래스를 통한 자동 초기화
        sys.path.insert(0, str(project_root / "core"))
        from trade_database import TradeDatabase
        
        # TradeDatabase 인스턴스 생성으로 자동 테이블 생성
        db = TradeDatabase(str(DB_PATH))
        
        logger.info("✅ 데이터베이스 초기화 완료! (기존 TradeDatabase 활용)")
        logger.info(f"📍 데이터베이스 위치: {DB_PATH}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 데이터베이스 초기화 실패: {e}")
        return False

def verify_database():
    """데이터베이스 상태 검증"""
    try:
        if not DB_PATH.exists():
            logger.error(f"❌ 데이터베이스 파일이 존재하지 않음: {DB_PATH}")
            return False
        
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        
        # 테이블 목록 조회
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        # 기존 TradeDatabase에서 생성하는 테이블들
        expected_tables = ['trades', 'daily_summary', 'selected_stocks', 'time_slot_summary']
        missing_tables = [table for table in expected_tables if table not in tables]
        
        if missing_tables:
            logger.error(f"❌ 누락된 테이블: {missing_tables}")
            return False
        
        # 각 테이블의 레코드 수 확인
        for table in expected_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logger.info(f"📊 {table}: {count}개 레코드")
        
        conn.close()
        logger.info("✅ 데이터베이스 검증 완료")
        return True
        
    except Exception as e:
        logger.error(f"❌ 데이터베이스 검증 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    try:
        print("=" * 50)
        print("📊 StockBot 데이터베이스 초기화 (기존 시스템 호환)")
        print("=" * 50)
        
        # 데이터베이스 초기화
        if initialize_database():
            print("✅ 데이터베이스 초기화 성공!")
            
            # 검증
            if verify_database():
                print("✅ 데이터베이스 검증 성공!")
                print(f"📍 데이터베이스 위치: {DB_PATH}")
                return True
            else:
                print("❌ 데이터베이스 검증 실패!")
                return False
        else:
            print("❌ 데이터베이스 초기화 실패!")
            return False
            
    except Exception as e:
        print(f"❌ 초기화 과정 중 오류: {e}")
        return False
    finally:
        print("=" * 50)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 