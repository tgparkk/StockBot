"""
시간대별 설정 관리자 (리팩토링 버전)
전략 스케줄러의 시간 관리 전담
"""
import pytz
from datetime import datetime, time, timedelta
from typing import Dict, Optional
from dataclasses import dataclass
from utils.logger import setup_logger
from utils.config_loader import ConfigLoader

logger = setup_logger(__name__)

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')

@dataclass
class TimeSlotConfig:
    """시간대별 설정"""
    name: str
    start_time: time
    end_time: time
    description: str
    primary_strategies: Dict[str, float]
    secondary_strategies: Dict[str, float]
    preparation_time: int = 15  # 사전 준비 시간(분)

class TimeSlotManager:
    """시간대별 설정 관리자"""

    def __init__(self, config_path: str = 'config/settings.ini'):
        """초기화"""
        self.config_loader = ConfigLoader(config_path)
        self.time_slots = self._load_time_slot_configs()

        logger.info(f"시간대 관리자 초기화 완료 ({len(self.time_slots)}개 시간대)")

    def _load_time_slot_configs(self) -> Dict[str, TimeSlotConfig]:
        """시간대별 설정 로드"""
        slots = {}

        try:
            # ConfigLoader에서 시간대별 전략 로드
            time_configs = self.config_loader.load_time_based_strategies()

            # TimeSlotConfig 객체로 변환
            for slot_name, config_data in time_configs.items():
                slots[slot_name] = TimeSlotConfig(
                    name=slot_name,
                    start_time=config_data['start_time'],
                    end_time=config_data['end_time'],
                    description=config_data['description'],
                    primary_strategies=config_data['primary_strategies'],
                    secondary_strategies=config_data['secondary_strategies']
                )

            logger.info("✅ 시간대별 설정 로드 완료")
            for slot_name, slot_config in slots.items():
                logger.info(f"   📅 {slot_name}: {slot_config.start_time} - {slot_config.end_time}")

        except Exception as e:
            logger.error(f"시간대별 설정 로드 실패: {e}")
            # 기본값으로 fallback
            slots = self._get_default_time_slots()

        return slots

    def _get_default_time_slots(self) -> Dict[str, TimeSlotConfig]:
        """기본 시간대 설정 (README.md 기반)"""
        logger.warning("⚠️ 기본값으로 시간대 설정 생성 (README.md 기반)")

        return {
            'golden_time': TimeSlotConfig(
                name='golden_time',
                start_time=time(9, 0),
                end_time=time(9, 30),
                description='장 시작 골든타임 - Gap Trading (100%)',
                primary_strategies={'gap_trading': 1.0},
                secondary_strategies={}
            ),
            'morning_leaders': TimeSlotConfig(
                name='morning_leaders',
                start_time=time(9, 30),
                end_time=time(11, 30),
                description='오전 주도주 시간 - Volume Breakout (70%) + Momentum (30%)',
                primary_strategies={'volume_breakout': 0.7},
                secondary_strategies={'momentum': 0.3}
            ),
            'lunch_time': TimeSlotConfig(
                name='lunch_time',
                start_time=time(11, 30),
                end_time=time(14, 0),
                description='점심 시간대 - Volume Breakout (80%) + Momentum (20%)',
                primary_strategies={'volume_breakout': 0.8},
                secondary_strategies={'momentum': 0.2}
            ),
            'closing_trend': TimeSlotConfig(
                name='closing_trend',
                start_time=time(14, 0),
                end_time=time(15, 20),
                description='마감 추세 시간 - Momentum (60%) + Volume Breakout (40%)',
                primary_strategies={'momentum': 0.6},
                secondary_strategies={'volume_breakout': 0.4}
            )
        }

    def get_current_time_slot(self) -> Optional[TimeSlotConfig]:
        """현재 시간대 조회"""
        now = datetime.now(KST).time()
        return self.get_time_slot_for_time(now)

    def get_time_slot_for_time(self, current_time: time) -> Optional[TimeSlotConfig]:
        """특정 시간의 시간대 조회"""
        for slot in self.time_slots.values():
            if slot.start_time <= current_time <= slot.end_time:
                return slot
        return None

    def get_next_preparation_time(self) -> Optional[datetime]:
        """다음 전략 준비 시간 계산"""
        now = datetime.now(KST)
        today = now.date()

        # 시간대별 준비 시간 (시작 15분 전)
        preparation_times = []

        try:
            for slot_config in self.time_slots.values():
                start_time = slot_config.start_time
                start_datetime = datetime.combine(today, start_time)
                prep_datetime = start_datetime - timedelta(minutes=15)
                preparation_times.append(prep_datetime)

            # 시간순 정렬
            preparation_times.sort()

            # 한국 시간대 적용
            preparation_times = [KST.localize(dt) for dt in preparation_times]

            # 현재 시간 이후의 가장 가까운 준비 시간 찾기
            for prep_time in preparation_times:
                if prep_time > now:
                    return prep_time

            # 오늘의 모든 준비 시간이 지났으면 다음 날 첫 번째 시간
            if preparation_times:
                tomorrow = today + timedelta(days=1)
                next_day_first_time = preparation_times[0].time()
                next_day_first = datetime.combine(tomorrow, next_day_first_time)
                return KST.localize(next_day_first)

        except Exception as e:
            logger.error(f"준비 시간 계산 실패: {e}")
            return self._get_default_next_preparation_time(now, today)

        return None

    def _get_default_next_preparation_time(self, now: datetime, today) -> Optional[datetime]:
        """기본 준비 시간 계산 (fallback용)"""
        logger.warning("⚠️ 기본 준비 시간 사용")

        # 기본 준비 시간들
        default_preparation_times = [
            datetime.combine(today, time(8, 45)),   # 골든타임 준비
            datetime.combine(today, time(9, 15)),   # 주도주 시간 준비
            datetime.combine(today, time(11, 15)),  # 점심시간 준비
            datetime.combine(today, time(13, 45)),  # 마감 추세 준비
        ]

        # 한국 시간대 적용
        preparation_times = [KST.localize(dt) for dt in default_preparation_times]

        # 현재 시간 이후의 가장 가까운 준비 시간 찾기
        for prep_time in preparation_times:
            if prep_time > now:
                return prep_time

        # 오늘의 모든 준비 시간이 지났으면 다음 날 첫 번째 시간
        tomorrow = today + timedelta(days=1)
        next_day_first = datetime.combine(tomorrow, time(8, 45))
        return KST.localize(next_day_first)

    def calculate_sleep_time(self, target_time: datetime) -> int:
        """대상 시간까지의 대기 시간 계산 (초)"""
        now = datetime.now(KST)
        time_diff = target_time - now
        return max(0, int(time_diff.total_seconds()))

    def is_market_hours(self) -> bool:
        """현재 시장 시간 여부 확인"""
        now = datetime.now(KST).time()
        return time(9, 0) <= now <= time(15, 30)

    def get_time_slots(self) -> Dict[str, TimeSlotConfig]:
        """모든 시간대 설정 반환"""
        return self.time_slots.copy()

    def reload_config(self):
        """설정 다시 로드"""
        logger.info("시간대 설정 다시 로드 중...")
        self.time_slots = self._load_time_slot_configs()
        logger.info("✅ 시간대 설정 다시 로드 완료")
