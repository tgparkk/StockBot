"""
설정 파일 로더
settings.ini에서 시간대별 전략 설정을 읽어오는 유틸리티
"""
import configparser
import os
from datetime import time
from typing import Dict, List, Optional
from utils.logger import setup_logger

logger = setup_logger(__name__)

class ConfigLoader:
    """설정 파일 로더"""
    
    def __init__(self, config_path: str = "config/settings.ini"):
        """
        설정 파일 로더 초기화
        
        Args:
            config_path: 설정 파일 경로
        """
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self._load_config()
    
    def _load_config(self):
        """설정 파일 로드"""
        try:
            if os.path.exists(self.config_path):
                self.config.read(self.config_path, encoding='utf-8')
                logger.info(f"설정 파일 로드 완료: {self.config_path}")
            else:
                logger.warning(f"설정 파일을 찾을 수 없습니다: {self.config_path}")
        except Exception as e:
            logger.error(f"설정 파일 로드 실패: {e}")
    
    def reload_config(self):
        """설정 파일 재로드"""
        self.config.clear()
        self._load_config()
        logger.info("설정 파일 재로드 완료")
    
    def load_time_based_strategies(self) -> Dict:
        """
        시간대별 전략 설정 로드
        
        Returns:
            시간대별 전략 구성 딕셔너리
        """
        time_configs = {}
        
        # strategy_로 시작하는 섹션들을 찾아서 처리
        for section_name in self.config.sections():
            if section_name.startswith('strategy_'):
                try:
                    config_name = section_name.replace('strategy_', '')
                    section = self.config[section_name]
                    
                    # 기본 정보 추출
                    start_time_str = section.get('start_time')
                    end_time_str = section.get('end_time')
                    description = section.get('description', '')
                    
                    if not start_time_str or not end_time_str:
                        logger.warning(f"시간 설정이 없는 섹션: {section_name}")
                        continue
                    
                    # 시간 변환
                    start_time = self._parse_time(start_time_str)
                    end_time = self._parse_time(end_time_str)
                    
                    if not start_time or not end_time:
                        logger.warning(f"시간 형식 오류: {section_name}")
                        continue
                    
                    # 전략 가중치 추출
                    primary_strategies = {}
                    secondary_strategies = {}
                    
                    for key, value in section.items():
                        if key.startswith('primary_'):
                            strategy_name = key.replace('primary_', '')
                            try:
                                primary_strategies[strategy_name] = float(value)
                            except ValueError:
                                logger.warning(f"잘못된 가중치 값: {key} = {value}")
                        
                        elif key.startswith('secondary_'):
                            strategy_name = key.replace('secondary_', '')
                            try:
                                secondary_strategies[strategy_name] = float(value)
                            except ValueError:
                                logger.warning(f"잘못된 가중치 값: {key} = {value}")
                    
                    # 시간대별 구성 생성
                    time_configs[config_name] = {
                        'start_time': start_time,
                        'end_time': end_time,
                        'primary_strategies': primary_strategies,
                        'secondary_strategies': secondary_strategies,
                        'description': description
                    }
                    
                    logger.debug(f"시간대별 전략 로드: {config_name}")
                
                except Exception as e:
                    logger.error(f"시간대별 전략 로드 실패 ({section_name}): {e}")
        
        logger.info(f"총 {len(time_configs)}개 시간대별 전략 로드 완료")
        return time_configs
    
    def load_strategy_config(self, strategy_name: str) -> Dict:
        """
        개별 전략 설정 로드
        
        Args:
            strategy_name: 전략 이름 (gap_trading, volume_breakout, momentum)
            
        Returns:
            전략 설정 딕셔너리
        """
        section_name = f"{strategy_name}_config"
        
        if section_name not in self.config:
            logger.warning(f"전략 설정 섹션을 찾을 수 없습니다: {section_name}")
            return {}
        
        try:
            section = self.config[section_name]
            config = {}
            
            for key, value in section.items():
                if key.startswith('#'):  # 주석은 건너뛰기
                    continue
                
                # 값 타입 변환 시도
                config[key] = self._convert_value(value)
            
            logger.debug(f"전략 설정 로드: {strategy_name}")
            return config
        
        except Exception as e:
            logger.error(f"전략 설정 로드 실패 ({strategy_name}): {e}")
            return {}
    
    def _parse_time(self, time_str: str) -> Optional[time]:
        """
        시간 문자열을 time 객체로 변환
        
        Args:
            time_str: 시간 문자열 (예: "09:30")
            
        Returns:
            time 객체 또는 None
        """
        try:
            hour, minute = map(int, time_str.split(':'))
            return time(hour, minute)
        except Exception as e:
            logger.error(f"시간 변환 실패: {time_str} - {e}")
            return None
    
    def _convert_value(self, value: str):
        """
        문자열 값을 적절한 타입으로 변환
        
        Args:
            value: 문자열 값
            
        Returns:
            변환된 값
        """
        value = value.strip()
        
        # Boolean 변환
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'
        
        # Float 변환 시도
        try:
            if '.' in value:
                return float(value)
        except ValueError:
            pass
        
        # Int 변환 시도
        try:
            return int(value)
        except ValueError:
            pass
        
        # 문자열 그대로 반환
        return value
    
    def get_config_value(self, section: str, key: str, default=None):
        """
        특정 설정 값 가져오기
        
        Args:
            section: 섹션 이름
            key: 키 이름
            default: 기본값
            
        Returns:
            설정 값
        """
        try:
            if section in self.config and key in self.config[section]:
                return self._convert_value(self.config[section][key])
            return default
        except Exception as e:
            logger.error(f"설정 값 가져오기 실패 ({section}.{key}): {e}")
            return default
    
    def get_all_strategy_configs(self) -> Dict:
        """
        모든 전략 설정 로드
        
        Returns:
            전략별 설정 딕셔너리
        """
        strategy_configs = {}
        
        strategy_names = ['gap_trading', 'volume_breakout', 'momentum']
        
        for strategy_name in strategy_names:
            config = self.load_strategy_config(strategy_name)
            if config:
                strategy_configs[strategy_name] = config
        
        return strategy_configs 