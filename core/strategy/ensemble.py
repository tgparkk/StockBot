"""
시간대별 전략 앙상블 매니저
시간대에 따라 다른 전략 조합을 사용하는 매니저
설정 파일(settings.ini)에서 시간대별 전략 구성을 로드
"""
from typing import Dict, List, Optional, Tuple
from datetime import datetime, time
import pandas as pd
from .base import BaseStrategy, Signal, MarketData
from .gap_trading import GapTradingStrategy
from .volume_breakout import VolumeBreakoutStrategy
from .momentum import MomentumStrategy
from utils.config_loader import ConfigLoader

class TimeBasedEnsembleManager:
    """시간대별 전략 앙상블 매니저"""
    
    def __init__(self, config_path: str = "config/settings.ini"):
        """
        시간대별 전략 매니저 초기화
        
        Args:
            config_path: 설정 파일 경로
        """
        self.config_loader = ConfigLoader(config_path)
        
        # 설정 파일에서 전략별 설정 로드
        strategy_configs = self.config_loader.get_all_strategy_configs()
        
        # 전략 인스턴스 생성
        self.strategies = {
            'gap_trading': GapTradingStrategy(strategy_configs.get('gap_trading', {})),
            'volume_breakout': VolumeBreakoutStrategy(strategy_configs.get('volume_breakout', {})),
            'momentum': MomentumStrategy(strategy_configs.get('momentum', {}))
        }
        
        # 설정 파일에서 시간대별 전략 구성 로드
        self.time_configs = self.config_loader.load_time_based_strategies()
        
        self.current_time_config = None
        self.last_signals = {}
        
        # 설정 로드 확인
        from utils.logger import setup_logger
        logger = setup_logger(__name__)
        logger.info(f"시간대별 전략 매니저 초기화 완료 (설정: {len(self.time_configs)}개 시간대)")
        
    def get_current_time_config(self, current_time: datetime) -> Optional[Dict]:
        """
        현재 시간에 맞는 전략 구성 반환
        
        Args:
            current_time: 현재 시간
            
        Returns:
            시간대별 전략 구성
        """
        current_time_only = current_time.time()
        
        for config_name, config in self.time_configs.items():
            if config['start_time'] <= current_time_only <= config['end_time']:
                return config
        
        # 장시간 외에는 None 반환
        return None
    
    def generate_ensemble_signal(self, market_data: MarketData, 
                                historical_data: pd.DataFrame = None) -> Optional[Signal]:
        """
        앙상블 신호 생성
        
        Args:
            market_data: 현재 시장 데이터
            historical_data: 과거 데이터
            
        Returns:
            최종 앙상블 신호
        """
        # 현재 시간대 구성 확인
        time_config = self.get_current_time_config(market_data.timestamp)
        if not time_config:
            return None  # 장시간 외
        
        self.current_time_config = time_config
        
        # 주요 전략 신호 수집
        primary_signals = self._collect_strategy_signals(
            time_config['primary_strategies'], 
            market_data, 
            historical_data
        )
        
        # 보조 전략 신호 수집
        secondary_signals = self._collect_strategy_signals(
            time_config['secondary_strategies'], 
            market_data, 
            historical_data
        )
        
        # 신호가 없으면 None 반환
        if not primary_signals:
            return None
        
        # 앙상블 신호 계산
        ensemble_signal = self._calculate_ensemble_signal(
            primary_signals, 
            secondary_signals, 
            time_config,
            market_data
        )
        
        if ensemble_signal and self._validate_ensemble_signal(ensemble_signal, market_data):
            self.last_signals[market_data.stock_code] = ensemble_signal
            return ensemble_signal
        
        return None
    
    def _collect_strategy_signals(self, strategy_weights: Dict[str, float], 
                                market_data: MarketData, 
                                historical_data: pd.DataFrame = None) -> List[Tuple[Signal, float]]:
        """
        전략별 신호 수집
        
        Args:
            strategy_weights: 전략별 가중치
            market_data: 시장 데이터
            historical_data: 과거 데이터
            
        Returns:
            (신호, 가중치) 튜플 리스트
        """
        signals = []
        
        for strategy_name, weight in strategy_weights.items():
            if strategy_name in self.strategies and weight > 0:
                strategy = self.strategies[strategy_name]
                
                # 전략별 신호 생성
                signal = strategy.generate_signal(market_data, historical_data)
                
                if signal:
                    signals.append((signal, weight))
        
        return signals
    
    def _calculate_ensemble_signal(self, primary_signals: List[Tuple[Signal, float]], 
                                 secondary_signals: List[Tuple[Signal, float]], 
                                 time_config: Dict,
                                 market_data: MarketData) -> Optional[Signal]:
        """
        앙상블 신호 계산
        
        Args:
            primary_signals: 주요 전략 신호들
            secondary_signals: 보조 전략 신호들
            time_config: 시간대 구성
            market_data: 시장 데이터
            
        Returns:
            앙상블 신호
        """
        if not primary_signals:
            return None
        
        # 신호 방향별 점수 계산
        buy_score = 0.0
        sell_score = 0.0
        total_weight = 0.0
        
        # 주요 전략 신호 처리
        for signal, weight in primary_signals:
            if signal.signal_type == 'BUY':
                buy_score += signal.strength * weight
            elif signal.signal_type == 'SELL':
                sell_score += signal.strength * weight
            total_weight += weight
        
        # 보조 전략 신호 처리 (가중치 절반 적용)
        for signal, weight in secondary_signals:
            adjusted_weight = weight * 0.5  # 보조 전략은 절반 가중치
            if signal.signal_type == 'BUY':
                buy_score += signal.strength * adjusted_weight
            elif signal.signal_type == 'SELL':
                sell_score += signal.strength * adjusted_weight
            total_weight += adjusted_weight
        
        # 최종 신호 결정
        if total_weight == 0:
            return None
        
        # 정규화
        buy_score /= total_weight
        sell_score /= total_weight
        
        # 신호 방향 및 강도 결정
        if buy_score > sell_score and buy_score > 0.3:  # 최소 임계값
            signal_type = 'BUY'
            strength = buy_score
        elif sell_score > buy_score and sell_score > 0.3:
            signal_type = 'SELL'
            strength = sell_score
        else:
            return None  # 신호 강도 부족
        
        # 신호 이유 생성
        strategy_names = [signal.strategy_name for signal, _ in primary_signals]
        reason = f"Ensemble signal from {', '.join(strategy_names)} during {time_config['description']}"
        
        # 최종 앙상블 신호 생성
        ensemble_signal = Signal(
            stock_code=market_data.stock_code,
            signal_type=signal_type,
            strength=strength,
            price=market_data.current_price,
            volume=market_data.volume,
            reason=reason,
            timestamp=market_data.timestamp,
            strategy_name=f"Ensemble_{time_config['description'].split(' - ')[0]}"
        )
        
        return ensemble_signal
    
    def _validate_ensemble_signal(self, signal: Signal, market_data: MarketData) -> bool:
        """
        앙상블 신호 검증
        
        Args:
            signal: 검증할 신호
            market_data: 시장 데이터
            
        Returns:
            신호 유효성 여부
        """
        # 기본 검증
        if signal.strength < 0.4:  # 앙상블 최소 임계값
            return False
        
        # 중복 신호 방지 (20분 내 같은 종목 중복 제거)
        if market_data.stock_code in self.last_signals:
            last_signal = self.last_signals[market_data.stock_code]
            time_diff = (market_data.timestamp - last_signal.timestamp).total_seconds()
            if time_diff < 1200:  # 20분
                return False
        
        return True
    
    def get_strategy_status(self) -> Dict:
        """전략 상태 반환"""
        status = {
            'current_time_config': self.current_time_config,
            'strategies': {},
            'last_signals_count': len(self.last_signals)
        }
        
        for name, strategy in self.strategies.items():
            status['strategies'][name] = strategy.get_status()
        
        return status
    
    def update_strategy_config(self, strategy_name: str, config: Dict):
        """전략 설정 업데이트"""
        if strategy_name in self.strategies:
            self.strategies[strategy_name].update_config(config)
    
    def activate_strategy(self, strategy_name: str):
        """전략 활성화"""
        if strategy_name in self.strategies:
            self.strategies[strategy_name].activate()
    
    def deactivate_strategy(self, strategy_name: str):
        """전략 비활성화"""
        if strategy_name in self.strategies:
            self.strategies[strategy_name].deactivate()
    
    def get_time_schedule(self) -> Dict:
        """시간대별 전략 스케줄 반환"""
        schedule = {}
        for name, config in self.time_configs.items():
            schedule[name] = {
                'time_range': f"{config['start_time']} - {config['end_time']}",
                'description': config['description'],
                'primary_strategies': config['primary_strategies'],
                'secondary_strategies': config['secondary_strategies']
            }
        return schedule
    
    def reload_config(self):
        """
        설정 파일 재로드 및 전략 재구성
        실시간으로 설정 변경을 반영할 때 사용
        """
        from utils.logger import setup_logger
        logger = setup_logger(__name__)
        
        try:
            # 설정 파일 재로드
            self.config_loader.reload_config()
            
            # 시간대별 전략 구성 재로드
            old_configs_count = len(self.time_configs)
            self.time_configs = self.config_loader.load_time_based_strategies()
            
            # 전략별 설정 재로드 및 업데이트
            strategy_configs = self.config_loader.get_all_strategy_configs()
            
            for strategy_name, config in strategy_configs.items():
                if strategy_name in self.strategies:
                    self.strategies[strategy_name].update_config(config)
            
            logger.info(f"설정 재로드 완료 (이전: {old_configs_count}개 → 현재: {len(self.time_configs)}개 시간대)")
            return True
            
        except Exception as e:
            logger.error(f"설정 재로드 실패: {e}")
            return False
    
    def get_config_status(self) -> Dict:
        """
        현재 설정 상태 반환
        
        Returns:
            설정 상태 정보
        """
        return {
            'config_path': self.config_loader.config_path,
            'time_configs_count': len(self.time_configs),
            'time_configs': list(self.time_configs.keys()),
            'strategy_configs': {
                name: strategy.config for name, strategy in self.strategies.items()
            },
            'current_time_config': self.current_time_config['description'] if self.current_time_config else None
        }
