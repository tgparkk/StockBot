"""
데이터베이스 락 문제 해결 유틸리티
SQLite 데이터베이스 락 해제 및 정리
"""
import os
import sys
import time
import sqlite3
import psutil
from pathlib import Path

def check_stockbot_processes():
    """StockBot 관련 프로세스 확인"""
    stockbot_processes = []
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            # Python 프로세스 중 StockBot 관련 찾기
            if 'python' in proc.info['name'].lower():
                cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                if 'stockbot' in cmdline.lower() or 'main.py' in cmdline.lower():
                    stockbot_processes.append({
                        'pid': proc.info['pid'],
                        'name': proc.info['name'],
                        'cmdline': cmdline
                    })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    
    return stockbot_processes

def kill_stockbot_processes(processes, force=False):
    """StockBot 프로세스 종료"""
    killed = []
    
    for proc_info in processes:
        try:
            proc = psutil.Process(proc_info['pid'])
            
            if force:
                proc.kill()  # 강제 종료
                print(f"✅ 프로세스 강제 종료: PID {proc_info['pid']}")
            else:
                proc.terminate()  # 정상 종료
                print(f"✅ 프로세스 종료 요청: PID {proc_info['pid']}")
            
            killed.append(proc_info['pid'])
            
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            print(f"❌ 프로세스 종료 실패: PID {proc_info['pid']} - {e}")
    
    if killed and not force:
        print("⏳ 프로세스 종료 대기 중...")
        time.sleep(3)
    
    return killed

def clean_wal_files(db_path):
    """WAL 파일들 정리"""
    db_path = Path(db_path)
    
    wal_files = [
        db_path.with_suffix('.db-wal'),
        db_path.with_suffix('.db-shm'),
        Path(str(db_path) + '-wal'),
        Path(str(db_path) + '-shm')
    ]
    
    cleaned = []
    for wal_file in wal_files:
        if wal_file.exists():
            try:
                wal_file.unlink()
                cleaned.append(str(wal_file))
                print(f"✅ WAL 파일 정리: {wal_file.name}")
            except Exception as e:
                print(f"❌ WAL 파일 정리 실패: {wal_file.name} - {e}")
    
    if not cleaned:
        print("ℹ️  정리할 WAL 파일이 없습니다.")
    
    return cleaned

def test_database_connection(db_path, timeout=5):
    """데이터베이스 연결 테스트"""
    try:
        conn = sqlite3.connect(str(db_path), timeout=timeout)
        conn.execute("SELECT 1")
        conn.close()
        return True
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e).lower():
            return False
        else:
            raise
    except Exception as e:
        print(f"❌ 연결 테스트 오류: {e}")
        return False

def force_unlock_database(db_path):
    """데이터베이스 강제 락 해제"""
    try:
        print("🔧 데이터베이스 강제 락 해제 시도...")
        
        # 방법 1: WAL 체크포인트
        try:
            conn = sqlite3.connect(str(db_path), timeout=10.0)
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.close()
            print("✅ WAL 체크포인트 실행 완료")
            return True
        except Exception as e:
            print(f"❌ WAL 체크포인트 실패: {e}")
        
        # 방법 2: 백업 후 복원
        backup_path = Path(str(db_path) + '.backup')
        try:
            import shutil
            shutil.copy2(db_path, backup_path)
            print(f"✅ 백업 생성: {backup_path}")
            return True
        except Exception as e:
            print(f"❌ 백업 생성 실패: {e}")
        
        return False
        
    except Exception as e:
        print(f"❌ 강제 락 해제 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    print("🔧 StockBot 데이터베이스 락 해제 도구")
    print("=" * 50)
    
    # 데이터베이스 경로
    db_path = Path("data/trades.db")
    
    if not db_path.exists():
        print(f"❌ 데이터베이스 파일이 없습니다: {db_path}")
        return
    
    print(f"📁 데이터베이스 경로: {db_path.absolute()}")
    
    # 1단계: 실행 중인 StockBot 프로세스 확인
    print("\n1️⃣ StockBot 프로세스 확인...")
    processes = check_stockbot_processes()
    
    if processes:
        print(f"⚠️  실행 중인 StockBot 프로세스 발견: {len(processes)}개")
        for proc in processes:
            print(f"   - PID {proc['pid']}: {proc['cmdline'][:100]}...")
        
        # 사용자 선택
        while True:
            choice = input("\n프로세스를 종료하시겠습니까? (y/n/force): ").lower()
            if choice in ['y', 'yes']:
                kill_stockbot_processes(processes, force=False)
                break
            elif choice == 'force':
                kill_stockbot_processes(processes, force=True)
                break
            elif choice in ['n', 'no']:
                print("⚠️  프로세스가 실행 중인 상태로 진행합니다.")
                break
            else:
                print("❌ 올바른 선택지를 입력하세요 (y/n/force)")
    else:
        print("✅ 실행 중인 StockBot 프로세스가 없습니다.")
    
    # 2단계: WAL 파일 정리
    print("\n2️⃣ WAL 파일 정리...")
    clean_wal_files(db_path)
    
    # 3단계: 데이터베이스 연결 테스트
    print("\n3️⃣ 데이터베이스 연결 테스트...")
    if test_database_connection(db_path):
        print("✅ 데이터베이스 연결 성공! 락 문제가 해결되었습니다.")
    else:
        print("❌ 데이터베이스가 여전히 잠겨있습니다.")
        
        # 4단계: 강제 락 해제
        print("\n4️⃣ 강제 락 해제 시도...")
        if force_unlock_database(db_path):
            print("✅ 강제 락 해제 시도 완료")
            
            # 재테스트
            time.sleep(2)
            if test_database_connection(db_path):
                print("✅ 데이터베이스 연결 성공!")
            else:
                print("❌ 락 해제에 실패했습니다.")
                print("\n💡 수동 해결 방법:")
                print("   1. 컴퓨터를 재시작하세요")
                print("   2. 또는 SQLite 브라우저 등 다른 도구가 DB를 열고 있는지 확인하세요")
                print("   3. 최후의 수단으로 data/trades.db 파일을 삭제하고 새로 시작할 수 있습니다")
        else:
            print("❌ 강제 락 해제에 실패했습니다.")
    
    print("\n🎉 데이터베이스 락 해제 도구 실행 완료!")

if __name__ == "__main__":
    main() 