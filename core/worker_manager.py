#!/usr/bin/env python3
"""
워커 스레드 관리자 - 백그라운드 작업 전담
StockBot의 스레드 관리 로직을 분리하여 단일 책임 원칙을 적용
"""
import logging
import threading
import asyncio
import time
from typing import Dict, List, Callable, Optional, Any
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class WorkerStatus(Enum):
    """워커 상태"""
    IDLE = "idle"           # 대기 중
    STARTING = "starting"   # 시작 중
    RUNNING = "running"     # 실행 중
    STOPPING = "stopping"   # 중지 중
    STOPPED = "stopped"     # 중지됨
    ERROR = "error"         # 오류 발생


@dataclass
class WorkerConfig:
    """워커 설정"""
    name: str
    target_function: Callable
    daemon: bool = True
    auto_restart: bool = False
    restart_delay: float = 5.0
    max_restart_attempts: int = 3
    heartbeat_interval: float = 30.0  # 30초마다 상태 체크
    timeout: float = 10.0  # 종료 시 대기 시간
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)


@dataclass
class WorkerInfo:
    """워커 정보"""
    config: WorkerConfig
    thread: Optional[threading.Thread] = None
    status: WorkerStatus = WorkerStatus.IDLE
    start_time: Optional[float] = None
    last_heartbeat: Optional[float] = None
    restart_count: int = 0
    error_message: Optional[str] = None
    
    @property
    def is_alive(self) -> bool:
        """워커가 살아있는지 확인"""
        return self.thread and self.thread.is_alive()
    
    @property
    def runtime_seconds(self) -> float:
        """실행 시간 (초)"""
        if self.start_time:
            return time.time() - self.start_time
        return 0.0
    
    @property
    def is_healthy(self) -> bool:
        """워커가 건강한지 확인 (하트비트 기준)"""
        if not self.last_heartbeat:
            return False
        
        # 하트비트 간격의 2배를 초과하면 비정상으로 판단
        threshold = self.config.heartbeat_interval * 2
        return (time.time() - self.last_heartbeat) < threshold


class WorkerManager:
    """워커 스레드 관리자"""
    
    def __init__(self, shutdown_event: threading.Event):
        """초기화"""
        self.shutdown_event = shutdown_event
        self.workers: Dict[str, WorkerInfo] = {}
        self.is_running = False
        self.monitor_thread: Optional[threading.Thread] = None
        
        # 통계
        self.stats = {
            'total_workers': 0,
            'running_workers': 0,
            'failed_workers': 0,
            'total_restarts': 0,
            'start_time': time.time()
        }
        
        logger.info("✅ WorkerManager 초기화 완료")
    
    def register_worker(self, config: WorkerConfig) -> bool:
        """워커 등록"""
        try:
            if config.name in self.workers:
                logger.warning(f"워커 '{config.name}'이 이미 등록되어 있습니다.")
                return False
            
            worker_info = WorkerInfo(config=config)
            self.workers[config.name] = worker_info
            self.stats['total_workers'] += 1
            
            logger.info(f"📝 워커 등록: {config.name}")
            return True
            
        except Exception as e:
            logger.error(f"워커 등록 실패 ({config.name}): {e}")
            return False
    
    def start_worker(self, worker_name: str) -> bool:
        """개별 워커 시작"""
        try:
            if worker_name not in self.workers:
                logger.error(f"등록되지 않은 워커: {worker_name}")
                return False
            
            worker_info = self.workers[worker_name]
            
            if worker_info.is_alive:
                logger.warning(f"워커 '{worker_name}'이 이미 실행 중입니다.")
                return True
            
            # 워커 스레드 생성 및 시작
            worker_info.status = WorkerStatus.STARTING
            worker_info.thread = threading.Thread(
                target=self._worker_wrapper,
                args=(worker_name,),
                name=worker_name,
                daemon=worker_info.config.daemon
            )
            
            worker_info.thread.start()
            worker_info.start_time = time.time()
            worker_info.status = WorkerStatus.RUNNING
            
            self.stats['running_workers'] += 1
            
            logger.info(f"🚀 워커 시작: {worker_name}")
            return True
            
        except Exception as e:
            logger.error(f"워커 시작 실패 ({worker_name}): {e}")
            if worker_name in self.workers:
                self.workers[worker_name].status = WorkerStatus.ERROR
                self.workers[worker_name].error_message = str(e)
            return False
    
    def stop_worker(self, worker_name: str, timeout: Optional[float] = None) -> bool:
        """개별 워커 중지"""
        try:
            if worker_name not in self.workers:
                logger.error(f"등록되지 않은 워커: {worker_name}")
                return False
            
            worker_info = self.workers[worker_name]
            
            if not worker_info.is_alive:
                logger.info(f"워커 '{worker_name}'이 이미 중지되어 있습니다.")
                worker_info.status = WorkerStatus.STOPPED
                return True
            
            worker_info.status = WorkerStatus.STOPPING
            timeout = timeout or worker_info.config.timeout
            
            # 스레드 종료 대기
            worker_info.thread.join(timeout=timeout)
            
            if worker_info.thread.is_alive():
                logger.warning(f"워커 '{worker_name}' 강제 종료 대기 중...")
                worker_info.status = WorkerStatus.ERROR
                worker_info.error_message = "Timeout during shutdown"
                return False
            else:
                worker_info.status = WorkerStatus.STOPPED
                self.stats['running_workers'] = max(0, self.stats['running_workers'] - 1)
                logger.info(f"🛑 워커 중지: {worker_name}")
                return True
                
        except Exception as e:
            logger.error(f"워커 중지 실패 ({worker_name}): {e}")
            return False
    
    def start_all_workers(self) -> bool:
        """모든 워커 시작"""
        try:
            logger.info("🚀 모든 워커 시작 중...")
            
            success_count = 0
            for worker_name in self.workers:
                if self.start_worker(worker_name):
                    success_count += 1
            
            # 모니터링 스레드 시작
            self._start_monitoring()
            
            self.is_running = True
            
            logger.info(f"✅ 워커 시작 완료: {success_count}/{len(self.workers)}개")
            return success_count == len(self.workers)
            
        except Exception as e:
            logger.error(f"워커 일괄 시작 실패: {e}")
            return False
    
    def stop_all_workers(self, timeout: float = 30.0) -> bool:
        """모든 워커 중지"""
        try:
            logger.info("🛑 모든 워커 중지 중...")
            
            self.is_running = False
            
            # 개별 워커 중지
            success_count = 0
            for worker_name in self.workers:
                if self.stop_worker(worker_name, timeout=5.0):
                    success_count += 1
            
            # 모니터링 스레드 중지
            self._stop_monitoring()
            
            logger.info(f"✅ 워커 중지 완료: {success_count}/{len(self.workers)}개")
            return success_count == len(self.workers)
            
        except Exception as e:
            logger.error(f"워커 일괄 중지 실패: {e}")
            return False
    
    def restart_worker(self, worker_name: str) -> bool:
        """워커 재시작"""
        try:
            logger.info(f"🔄 워커 재시작: {worker_name}")
            
            if worker_name not in self.workers:
                logger.error(f"등록되지 않은 워커: {worker_name}")
                return False
            
            worker_info = self.workers[worker_name]
            
            # 재시작 횟수 체크
            if worker_info.restart_count >= worker_info.config.max_restart_attempts:
                logger.error(f"워커 '{worker_name}' 최대 재시작 횟수 초과 ({worker_info.restart_count})")
                worker_info.status = WorkerStatus.ERROR
                worker_info.error_message = "Max restart attempts exceeded"
                return False
            
            # 중지 후 재시작
            self.stop_worker(worker_name)
            time.sleep(worker_info.config.restart_delay)
            
            success = self.start_worker(worker_name)
            if success:
                worker_info.restart_count += 1
                self.stats['total_restarts'] += 1
                logger.info(f"✅ 워커 재시작 완료: {worker_name} (횟수: {worker_info.restart_count})")
            
            return success
            
        except Exception as e:
            logger.error(f"워커 재시작 실패 ({worker_name}): {e}")
            return False
    
    def get_worker_status(self, worker_name: str) -> Optional[Dict[str, Any]]:
        """워커 상태 조회"""
        if worker_name not in self.workers:
            return None
        
        worker_info = self.workers[worker_name]
        return {
            'name': worker_name,
            'status': worker_info.status.value,
            'is_alive': worker_info.is_alive,
            'is_healthy': worker_info.is_healthy,
            'runtime_seconds': worker_info.runtime_seconds,
            'restart_count': worker_info.restart_count,
            'error_message': worker_info.error_message,
            'last_heartbeat': worker_info.last_heartbeat
        }
    
    def get_all_status(self) -> Dict[str, Any]:
        """모든 워커 상태 조회"""
        workers_status = {}
        for worker_name in self.workers:
            workers_status[worker_name] = self.get_worker_status(worker_name)
        
        return {
            'workers': workers_status,
            'stats': self.stats.copy(),
            'manager_running': self.is_running,
            'total_runtime': time.time() - self.stats['start_time']
        }
    
    def update_heartbeat(self, worker_name: str):
        """워커 하트비트 업데이트"""
        if worker_name in self.workers:
            self.workers[worker_name].last_heartbeat = time.time()
    
    def _worker_wrapper(self, worker_name: str):
        """워커 실행 래퍼 - 에러 처리 및 모니터링"""
        worker_info = self.workers[worker_name]
        
        try:
            logger.info(f"🏃 워커 실행 시작: {worker_name}")
            
            # 초기 하트비트
            self.update_heartbeat(worker_name)
            
            # 실제 워커 함수 실행
            target_func = worker_info.config.target_function
            args = worker_info.config.args
            kwargs = worker_info.config.kwargs
            
            # 셧다운 이벤트를 kwargs에 자동 추가 (함수가 받을 수 있으면)
            import inspect
            sig = inspect.signature(target_func)
            if 'shutdown_event' in sig.parameters and 'shutdown_event' not in kwargs:
                kwargs['shutdown_event'] = self.shutdown_event
            
            # 워커 매니저 참조도 자동 추가
            if 'worker_manager' in sig.parameters and 'worker_manager' not in kwargs:
                kwargs['worker_manager'] = self
            
            target_func(*args, **kwargs)
            
        except Exception as e:
            logger.error(f"❌ 워커 실행 오류 ({worker_name}): {e}")
            worker_info.status = WorkerStatus.ERROR
            worker_info.error_message = str(e)
            self.stats['failed_workers'] += 1
            
            # 자동 재시작 시도
            if worker_info.config.auto_restart and self.is_running:
                logger.info(f"🔄 워커 자동 재시작 시도: {worker_name}")
                threading.Thread(
                    target=self._delayed_restart,
                    args=(worker_name,),
                    daemon=True
                ).start()
        
        finally:
            # 워커 종료 시 상태 업데이트
            if worker_info.status not in [WorkerStatus.ERROR, WorkerStatus.STOPPING]:
                worker_info.status = WorkerStatus.STOPPED
            
            self.stats['running_workers'] = max(0, self.stats['running_workers'] - 1)
            logger.info(f"🏁 워커 실행 종료: {worker_name}")
    
    def _delayed_restart(self, worker_name: str):
        """지연된 재시작 (별도 스레드에서 실행)"""
        try:
            worker_info = self.workers[worker_name]
            time.sleep(worker_info.config.restart_delay)
            
            if self.is_running:  # 여전히 실행 중이면 재시작
                self.restart_worker(worker_name)
                
        except Exception as e:
            logger.error(f"지연된 재시작 실패 ({worker_name}): {e}")
    
    def _start_monitoring(self):
        """모니터링 스레드 시작"""
        try:
            self.monitor_thread = threading.Thread(
                target=self._monitoring_loop,
                name="WorkerMonitor",
                daemon=True
            )
            self.monitor_thread.start()
            logger.info("👁️ 워커 모니터링 시작")
            
        except Exception as e:
            logger.error(f"모니터링 시작 실패: {e}")
    
    def _stop_monitoring(self):
        """모니터링 스레드 중지"""
        try:
            if self.monitor_thread and self.monitor_thread.is_alive():
                self.monitor_thread.join(timeout=5.0)
            logger.info("👁️ 워커 모니터링 중지")
            
        except Exception as e:
            logger.error(f"모니터링 중지 실패: {e}")
    
    def _monitoring_loop(self):
        """모니터링 루프"""
        logger.info("👁️ 워커 모니터링 루프 시작")
        
        monitor_interval = 30.0  # 30초마다 체크
        last_report_time = time.time()
        report_interval = 300.0  # 5분마다 상태 리포트
        
        while self.is_running and not self.shutdown_event.is_set():
            try:
                current_time = time.time()
                
                # 1. 워커 상태 체크
                for worker_name, worker_info in self.workers.items():
                    # 스레드가 죽었지만 상태가 RUNNING인 경우
                    if worker_info.status == WorkerStatus.RUNNING and not worker_info.is_alive:
                        logger.warning(f"⚠️ 워커 '{worker_name}' 예상치 못한 종료 감지")
                        worker_info.status = WorkerStatus.ERROR
                        worker_info.error_message = "Unexpected thread termination"
                        
                        # 자동 재시작
                        if worker_info.config.auto_restart:
                            logger.info(f"🔄 워커 '{worker_name}' 자동 재시작 예약")
                            threading.Thread(
                                target=self._delayed_restart,
                                args=(worker_name,),
                                daemon=True
                            ).start()
                
                # 2. 주기적 상태 리포트
                if current_time - last_report_time >= report_interval:
                    self._generate_monitoring_report()
                    last_report_time = current_time
                
                # 3. 다음 체크까지 대기
                self.shutdown_event.wait(timeout=monitor_interval)
                
            except Exception as e:
                logger.error(f"모니터링 루프 오류: {e}")
                self.shutdown_event.wait(timeout=60)  # 오류 시 1분 대기
        
        logger.info("👁️ 워커 모니터링 루프 종료")
    
    def _generate_monitoring_report(self):
        """모니터링 리포트 생성"""
        try:
            status = self.get_all_status()
            
            total_workers = len(self.workers)
            running_count = sum(1 for w in status['workers'].values() if w['is_alive'])
            healthy_count = sum(1 for w in status['workers'].values() if w['is_healthy'])
            error_count = sum(1 for w in status['workers'].values() if w['status'] == 'error')
            
            logger.info(
                f"👁️ 워커 모니터링 리포트 - "
                f"실행중: {running_count}/{total_workers}, "
                f"정상: {healthy_count}/{total_workers}, "
                f"오류: {error_count}, "
                f"총재시작: {status['stats']['total_restarts']}"
            )
            
            # 오류 상태인 워커들 로그
            for worker_name, worker_status in status['workers'].items():
                if worker_status['status'] == 'error':
                    logger.warning(
                        f"⚠️ 워커 오류: {worker_name} - {worker_status['error_message']} "
                        f"(재시작: {worker_status['restart_count']}회)"
                    )
                    
        except Exception as e:
            logger.error(f"모니터링 리포트 생성 오류: {e}")


# === 워커 함수들을 위한 헬퍼 함수들 ===

def heartbeat_wrapper(worker_manager: 'WorkerManager', worker_name: str, interval: float = 30.0):
    """워커 함수에 하트비트 기능을 추가하는 데코레이터"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_heartbeat = time.time()
            
            # 원본 함수 실행 (백그라운드에서)
            import threading
            
            def heartbeat_sender():
                nonlocal last_heartbeat
                while True:
                    current_time = time.time()
                    if current_time - last_heartbeat >= interval:
                        worker_manager.update_heartbeat(worker_name)
                        last_heartbeat = current_time
                    
                    time.sleep(min(interval / 2, 10))  # 최대 10초마다 체크
            
            # 하트비트 스레드 시작
            heartbeat_thread = threading.Thread(target=heartbeat_sender, daemon=True)
            heartbeat_thread.start()
            
            try:
                return func(*args, **kwargs)
            finally:
                # 함수 종료 시 하트비트 스레드도 종료되도록 처리
                pass
        
        return wrapper
    return decorator 