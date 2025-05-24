"""
데이터베이스 초기화 스크립트
테이블 생성 및 초기 설정
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_manager import db_manager
from utils.logger import setup_logger

logger = setup_logger(__name__)

def main():
    """데이터베이스 초기화 실행"""
    try:
        logger.info("🚀 데이터베이스 초기화 시작")
        
        # 데이터베이스 초기화
        db_manager.initialize_database()
        
        logger.info("✅ 데이터베이스 초기화 완료!")
        
    except Exception as e:
        logger.error(f"❌ 데이터베이스 초기화 실패: {e}")
        return False
    
    finally:
        # 연결 정리
        db_manager.close_database()
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 