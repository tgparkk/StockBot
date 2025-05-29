"""
포지션 관리자 (리팩토링 버전)
보유 종목 및 수익률 관리 전담
"""
import time
import threading
from typing import Dict, List, Optional
from utils.logger import setup_logger
from .kis_data_collector import KISDataCollector
from .trading_manager import TradingManager
from core.kis_market_api import get_disparity_rank, get_multi_period_disparity

logger = setup_logger(__name__)


class PositionManager:
    """간소화된 포지션 관리자"""

    def __init__(self, trading_manager: TradingManager):
        """초기화"""
        self.trading_manager = trading_manager
        self.data_collector = trading_manager.data_collector

        # 포지션 추적
        self.positions: Dict[str, Dict] = {}  # {stock_code: position_info}
        self.position_lock = threading.RLock()

        # 🎯 데이터 기반 손익 설정 계산
        self.profit_targets = self._calculate_scientific_targets()

        # 통계
        self.stats = {
            'total_positions': 0,
            'active_positions': 0,
            'profitable_positions': 0,
            'loss_positions': 0,
            'total_profit_loss': 0.0
        }

        logger.info("포지션 관리자 초기화 완료")

    def _calculate_scientific_targets(self) -> Dict:
        """📊 과학적 근거 기반 손익 목표 계산"""
        try:
            # 🔍 시장 변동성 분석
            market_volatility = self._analyze_market_volatility()
            
            # 📈 전략별 백테스팅 성과 분석  
            strategy_performance = self._analyze_strategy_performance()
            
            # ⚖️ 리스크 대비 수익률 최적화
            risk_adjusted_targets = self._calculate_risk_adjusted_targets(
                market_volatility, strategy_performance
            )
            
            logger.info(f"📊 과학적 손익 목표 설정 완료 - 시장변동성: {market_volatility:.2f}%")
            return risk_adjusted_targets
            
        except Exception as e:
            logger.warning(f"⚠️ 과학적 계산 실패, 기본값 사용: {e}")
            return self._get_default_targets()

    def _analyze_market_volatility(self) -> float:
        """📊 시장 전체 변동성 분석"""
        try:
            # KOSPI 지수 최근 20일 변동성 분석
            kospi_data = self.data_collector.get_market_index_data('001', period=20)
            if not kospi_data or len(kospi_data) < 10:
                return 2.5  # 기본값
            
            # 일일 변동률 계산
            daily_changes = []
            for i in range(1, len(kospi_data)):
                prev_price = float(kospi_data[i-1].get('bstp_nmix_prpr', 1))
                curr_price = float(kospi_data[i].get('bstp_nmix_prpr', 1))
                if prev_price > 0:
                    change_pct = abs((curr_price - prev_price) / prev_price * 100)
                    daily_changes.append(change_pct)
            
            if daily_changes:
                avg_volatility = sum(daily_changes) / len(daily_changes)
                logger.info(f"📊 시장 평균 변동성: {avg_volatility:.2f}%")
                return avg_volatility
            
            return 2.5  # 기본값
            
        except Exception as e:
            logger.debug(f"시장 변동성 분석 오류: {e}")
            return 2.5  # 기본값

    def _analyze_strategy_performance(self) -> Dict:
        """📈 전략별 과거 성과 분석"""
        try:
            # 데이터베이스에서 과거 거래 성과 조회
            if hasattr(self.trading_manager, 'trade_db'):
                performance_data = self.trading_manager.trade_db.get_strategy_performance_stats()
                
                # 전략별 성과 지표 계산
                strategy_stats = {}
                for strategy, data in performance_data.items():
                    if data['total_trades'] >= 10:  # 최소 10회 거래 필요
                        avg_profit = data['avg_profit_rate']
                        win_rate = data['win_rate']
                        max_drawdown = data['max_drawdown']
                        
                        # 샤프 비율 근사 계산
                        sharpe_ratio = (avg_profit - 1.0) / (data['volatility'] + 0.1)
                        
                        strategy_stats[strategy] = {
                            'avg_profit': avg_profit,
                            'win_rate': win_rate,
                            'max_drawdown': max_drawdown,
                            'sharpe_ratio': sharpe_ratio,
                            'sample_size': data['total_trades']
                        }
                
                return strategy_stats
            
            return {}
            
        except Exception as e:
            logger.debug(f"전략 성과 분석 오류: {e}")
            return {}

    def _calculate_risk_adjusted_targets(self, market_volatility: float, 
                                       strategy_performance: Dict) -> Dict:
        """⚖️ 리스크 조정 손익 목표 계산"""
        
        # 🎯 변동성 기반 기본 배수 계산
        volatility_multiplier = max(1.0, min(market_volatility / 2.0, 3.0))  # 1.0~3.0 범위
        
        # 📊 기본 리스크 비율 (변동성의 0.7~1.2배)
        base_stop_loss = -(market_volatility * 0.8)
        base_take_profit = market_volatility * 1.8
        base_trailing_trigger = market_volatility * 1.2
        
        # 🎯 전략별 조정 계산
        targets = {}
        
        strategy_configs = {
            'default': {'risk_factor': 1.0, 'profit_factor': 1.0, 'holding_factor': 1.0},
            'existing_holding': {'risk_factor': 1.3, 'profit_factor': 1.4, 'holding_factor': 2.0},
            'disparity_reversal': {'risk_factor': 0.9, 'profit_factor': 1.2, 'holding_factor': 1.5},
            'gap_trading': {'risk_factor': 0.7, 'profit_factor': 0.8, 'holding_factor': 0.7},
            'volume_breakout': {'risk_factor': 1.1, 'profit_factor': 1.3, 'holding_factor': 1.2},
            'momentum': {'risk_factor': 0.8, 'profit_factor': 1.6, 'holding_factor': 0.8}
        }
        
        for strategy, config in strategy_configs.items():
            # 🎯 과거 성과 반영
            performance = strategy_performance.get(strategy, {})
            performance_multiplier = 1.0
            
            if performance and performance.get('sample_size', 0) >= 10:
                # 윈율 기반 조정
                win_rate = performance.get('win_rate', 50)
                if win_rate > 60:
                    performance_multiplier = 1.2  # 고성과 전략은 더 공격적
                elif win_rate < 40:
                    performance_multiplier = 0.8  # 저성과 전략은 더 보수적
                
                # 샤프 비율 반영
                sharpe = performance.get('sharpe_ratio', 0)
                if sharpe > 1.0:
                    performance_multiplier *= 1.1
                elif sharpe < 0.5:
                    performance_multiplier *= 0.9
            
            # 🎯 최종 계산
            final_stop_loss = base_stop_loss * config['risk_factor'] * performance_multiplier
            final_take_profit = base_take_profit * config['profit_factor'] * performance_multiplier
            final_trailing_trigger = base_trailing_trigger * config['profit_factor'] * performance_multiplier
            final_trailing_gap = final_trailing_trigger * 0.5  # 트리거의 50%
            
            # 📏 합리적 범위 제한
            final_stop_loss = max(-8.0, min(final_stop_loss, -1.0))  # -1% ~ -8%
            final_take_profit = max(2.0, min(final_take_profit, 15.0))  # 2% ~ 15%
            final_trailing_trigger = max(1.5, min(final_trailing_trigger, 8.0))  # 1.5% ~ 8%
            final_trailing_gap = max(0.8, min(final_trailing_gap, 4.0))  # 0.8% ~ 4%
            
            # 🕐 보유 시간도 변동성 기반 계산
            base_holding_minutes = 30 + (market_volatility * 5)  # 변동성 높을수록 더 오래 보유
            final_holding_minutes = int(base_holding_minutes * config['holding_factor'])
            final_holding_minutes = max(10, min(final_holding_minutes, 120))  # 10분~2시간
            
            targets[strategy] = {
                'stop_loss': round(final_stop_loss, 1),
                'take_profit': round(final_take_profit, 1),
                'min_holding_minutes': final_holding_minutes,
                'early_stop_loss': round(final_stop_loss * 0.6, 1),  # 손절의 60%
                'early_stop_minutes': max(5, final_holding_minutes // 3),  # 보유시간의 1/3
                'trailing_stop_trigger': round(final_trailing_trigger, 1),
                'trailing_stop_gap': round(final_trailing_gap, 1),
                'dynamic_stop_loss': True,
                # 📊 계산 근거 로깅용
                '_calculation_basis': {
                    'market_volatility': market_volatility,
                    'risk_factor': config['risk_factor'],
                    'profit_factor': config['profit_factor'],
                    'performance_multiplier': performance_multiplier,
                    'sample_size': performance.get('sample_size', 0)
                }
            }
            
            logger.info(f"📊 {strategy}: 손절{final_stop_loss:.1f}%, 익절{final_take_profit:.1f}%, "
                       f"추격{final_trailing_trigger:.1f}% (변동성:{market_volatility:.1f}%)")
        
        return targets

    def _get_default_targets(self) -> Dict:
        """기본 손익 목표 (백업용)"""
        return {
            # 기본 전략: 안정적이고 수익성 있는 매도 (개선)
            'default': {
                'stop_loss': -3.5, 'take_profit': 5.5, 'min_holding_minutes': 30,
                'early_stop_loss': -2.0, 'early_stop_minutes': 15,
                'trailing_stop_trigger': 3.0, 'trailing_stop_gap': 1.5,
                'dynamic_stop_loss': True
            },
            
            # 기존 보유 종목: 더 여유있는 관리 (개선)
            'existing_holding': {
                'stop_loss': -4.5, 'take_profit': 7.5, 'min_holding_minutes': 60,
                'early_stop_loss': -3.0, 'early_stop_minutes': 30,
                'trailing_stop_trigger': 3.5, 'trailing_stop_gap': 2.0,
                'dynamic_stop_loss': True
            },
            
            # 🆕 이격도 반등: 과매도 반등 기대하며 여유 있게 (개선)
            'disparity_reversal': {
                'stop_loss': -3.0, 'take_profit': 6.5, 'min_holding_minutes': 45,
                'early_stop_loss': -2.0, 'early_stop_minutes': 20,
                'trailing_stop_trigger': 3.5, 'trailing_stop_gap': 1.8,
                'dynamic_stop_loss': True
            },
            
            # 갭 거래: 빠른 수익 실현 (개선)
            'gap_trading': {
                'stop_loss': -2.5, 'take_profit': 4.5, 'min_holding_minutes': 20,
                'early_stop_loss': -1.5, 'early_stop_minutes': 10,
                'trailing_stop_trigger': 2.5, 'trailing_stop_gap': 1.2,
                'dynamic_stop_loss': True
            },
            
            # 거래량 돌파: 트렌드 지속 기대 (개선)
            'volume_breakout': {
                'stop_loss': -3.2, 'take_profit': 6.8, 'min_holding_minutes': 35,
                'early_stop_loss': -2.2, 'early_stop_minutes': 18,
                'trailing_stop_trigger': 3.8, 'trailing_stop_gap': 2.0,
                'dynamic_stop_loss': True
            },
            
            # 모멘텀: 트렌드 최대한 활용 (개선)
            'momentum': {
                'stop_loss': -2.2, 'take_profit': 8.0, 'min_holding_minutes': 20,
                'early_stop_loss': -1.5, 'early_stop_minutes': 8,
                'trailing_stop_trigger': 4.5, 'trailing_stop_gap': 2.2,
                'dynamic_stop_loss': True
            }
        }

    # === 수익률 계산 헬퍼 메서드들 ===
    
    def _calculate_profit_rate(self, current_price: int, buy_price: int) -> float:
        """수익률 계산"""
        if buy_price <= 0:
            return 0.0
        return ((current_price - buy_price) / buy_price) * 100
    
    def _calculate_profit_loss(self, sell_price: int, buy_price: int, quantity: int) -> int:
        """손익 계산"""
        return (sell_price - buy_price) * quantity
    
    def _update_position_profit_info(self, position: Dict, current_price: int) -> None:
        """포지션 수익 정보 업데이트"""
        profit_rate = self._calculate_profit_rate(current_price, position['buy_price'])
        position['current_price'] = current_price
        position['profit_rate'] = profit_rate
        position['last_update'] = time.time()
        
        # 최대 수익률 업데이트
        if profit_rate > position.get('max_profit_rate', 0):
            position['max_profit_rate'] = profit_rate

    def add_position(self, stock_code: str, quantity: int, buy_price: int,
                    strategy_type: str = "manual") -> bool:
        """포지션 추가"""
        with self.position_lock:
            try:
                if stock_code in self.positions:
                    # 기존 포지션 업데이트 (평단가 계산)
                    existing = self.positions[stock_code]
                    total_qty = existing['quantity'] + quantity
                    total_amount = (existing['quantity'] * existing['buy_price']) + (quantity * buy_price)
                    avg_price = total_amount // total_qty

                    self.positions[stock_code].update({
                        'quantity': total_qty,
                        'buy_price': avg_price,
                        'last_update': time.time()
                    })

                    logger.info(f"✅ 포지션 추가매수: {stock_code} {quantity}주 평단가 {avg_price:,}원")
                else:
                    # 새 포지션
                    self.positions[stock_code] = {
                        'stock_code': stock_code,
                        'quantity': quantity,
                        'buy_price': buy_price,
                        'strategy_type': strategy_type,
                        'buy_time': time.time(),
                        'last_update': time.time(),
                        'max_profit_rate': 0.0,
                        'status': 'active'
                    }

                self.stats['total_positions'] += 1
                logger.info(f"✅ 새 포지션: {stock_code} {quantity}주 {buy_price:,}원")

                self.stats['active_positions'] = len([p for p in self.positions.values() if p['status'] == 'active'])
                return True

            except Exception as e:
                logger.error(f"포지션 추가 오류: {stock_code} - {e}")
                return False

    def remove_position(self, stock_code: str, quantity: int, sell_price: int) -> bool:
        """포지션 제거 (전체 또는 부분)"""
        with self.position_lock:
            try:
                if stock_code not in self.positions:
                    logger.warning(f"제거할 포지션이 없음: {stock_code}")
                    return False

                position = self.positions[stock_code]

                if quantity >= position['quantity']:
                    # 전체 매도
                    profit_loss = self._calculate_profit_loss(sell_price, position['buy_price'], position['quantity'])
                    profit_rate = self._calculate_profit_rate(sell_price, position['buy_price'])

                    position['status'] = 'closed'
                    position['sell_price'] = sell_price
                    position['sell_time'] = time.time()
                    position['profit_loss'] = profit_loss
                    position['profit_rate'] = profit_rate

                    # 통계 업데이트
                    if profit_loss > 0:
                        self.stats['profitable_positions'] += 1
                    else:
                        self.stats['loss_positions'] += 1

                    self.stats['total_profit_loss'] += profit_loss

                    del self.positions[stock_code]

                    logger.info(f"✅ 포지션 전체 매도: {stock_code} 수익률 {profit_rate:.2f}%")
                else:
                    # 부분 매도
                    profit_loss = self._calculate_profit_loss(sell_price, position['buy_price'], quantity)
                    position['quantity'] -= quantity

                    self.stats['total_profit_loss'] += profit_loss

                    logger.info(f"✅ 포지션 부분 매도: {stock_code} {quantity}주 (잔여 {position['quantity']}주)")

                self.stats['active_positions'] = len([p for p in self.positions.values() if p['status'] == 'active'])
                return True

            except Exception as e:
                logger.error(f"포지션 제거 오류: {stock_code} - {e}")
                return False

    def update_position_prices(self, force_rest_api: bool = False) -> None:
        """포지션 가격 업데이트"""
        if not self.positions:
            return

        current_time = time.time()

        # 웹소켓 연결 상태 확인 (REST API 강제 사용이 아닌 경우)
        websocket_available = False
        
        if not force_rest_api:
            websocket_available = self._check_websocket_connection()
            
            if not websocket_available:
                # 재연결 시도 빈도 제한 (5분 간격)
                if not hasattr(self, '_last_reconnect_attempt'):
                    self._last_reconnect_attempt = 0
                
                if current_time - self._last_reconnect_attempt > 300:  # 5분
                    logger.warning("🔴 웹소켓 연결 확인 불가 - 재연결 시도 중... (5분 간격)")
                    
                    # 재연결 시도
                    reconnected = self._attempt_websocket_reconnection_with_retry()
                    
                    if reconnected:
                        logger.info("✅ 웹소켓 필수 재연결 성공")
                        websocket_available = True
                    else:
                        logger.debug("재연결 실패 - REST API 백업 사용")
                    
                    self._last_reconnect_attempt = current_time
                else:
                    logger.debug("재연결 시도 쿨다운 중 - REST API 사용")

        # 웹소켓 우선 시도 (사용 가능한 경우)
        websocket_success = False
        if websocket_available:
            try:
                websocket_success = self._update_prices_via_websocket()
                if websocket_success:
                    logger.debug("✅ 웹소켓을 통한 가격 업데이트 성공")
                    return
                else:
                    logger.debug("웹소켓 가격 업데이트 실패 - REST API로 백업")
            except Exception as e:
                logger.error(f"웹소켓 가격 업데이트 오류: {e}")

        # REST API 백업 또는 강제 사용
        reason = "강제 모드" if force_rest_api else "웹소켓 재연결시도했으나 실패"
        logger.info(f"💾 REST API 백업 사용: {reason}")
        self._update_prices_via_rest_api()

    def _update_prices_via_rest_api(self):
        """REST API를 통한 가격 업데이트"""
        with self.position_lock:
            active_positions = [code for code, pos in self.positions.items() if pos['status'] == 'active']
            
            if not active_positions:
                return
            
            # REST API로 각 종목 가격 조회
            for stock_code in active_positions:
                try:
                    price_info = self.data_collector.get_fresh_price(stock_code)
                    if price_info.get('status') == 'success':
                        current_price = price_info.get('current_price', 0)
                        if current_price > 0:
                            self._update_position_price(stock_code, current_price)
                    time.sleep(0.05)  # API 호출 간격
                except Exception as e:
                    logger.error(f"REST API 가격 조회 실패: {stock_code} - {e}")

    def _update_prices_via_websocket(self) -> bool:
        """웹소켓을 통한 가격 업데이트"""
        try:
            with self.position_lock:
                active_positions = [code for code, pos in self.positions.items() if pos['status'] == 'active']
                
                if not active_positions:
                    return True
                
                # 웹소켓으로 배치 가격 조회
                price_data = self.data_collector.get_multiple_prices(active_positions, use_cache=True)
                
                success_count = 0
                for stock_code in active_positions:
                    if stock_code in price_data:
                        price_info = price_data[stock_code]
                        if price_info.get('status') == 'success':
                            current_price = price_info.get('current_price', 0)
                            if current_price > 0:
                                self._update_position_price(stock_code, current_price)
                                success_count += 1
                
                # 50% 이상 성공하면 성공으로 간주
                return success_count >= len(active_positions) * 0.5
                
        except Exception as e:
            logger.error(f"웹소켓 가격 업데이트 오류: {e}")
            return False

    def _update_position_price(self, stock_code: str, current_price: int):
        """개별 포지션 가격 업데이트"""
        with self.position_lock:
            if stock_code in self.positions:
                position = self.positions[stock_code]
                
                if position['status'] == 'active' and current_price > 0:
                    # 헬퍼 메서드를 사용하여 수익 정보 업데이트
                    self._update_position_profit_info(position, current_price)
                    
                    logger.debug(f"📊 포지션 업데이트: {stock_code} {current_price:,}원 "
                               f"(수익률: {position['profit_rate']:.2f}%)")

    def _attempt_websocket_reconnection(self) -> bool:
        """🎯 웹소켓 재연결 시도"""
        try:
            # 데이터 수집기를 통한 웹소켓 재연결
            if hasattr(self.data_collector, 'websocket'):
                websocket_manager = self.data_collector.websocket
                
                if websocket_manager:
                    # 현재 연결 상태 확인
                    is_connected = getattr(websocket_manager, 'is_connected', False)
                    
                    if not is_connected:
                        logger.info("🔄 웹소켓 재연결 시도...")
                        
                        # 웹소켓 재시작 시도
                        if hasattr(websocket_manager, 'ensure_connection'):
                            try:
                                websocket_manager.ensure_connection()
                                if getattr(websocket_manager, 'is_connected', False):
                                    logger.info("✅ 웹소켓 재연결 성공")
                                    return True
                                else:
                                    logger.warning("⚠️ 웹소켓 재연결 실패")
                                    return False
                            except Exception as e:
                                logger.warning(f"웹소켓 ensure_connection 오류: {e}")
                                return False
                        else:
                            logger.warning("웹소켓 ensure_connection 메서드 없음")
                            return False
                    else:
                        logger.debug("웹소켓이 이미 연결되어 있음")
                        return True
                else:
                    logger.warning("웹소켓 매니저 없음")
                    return False
            else:
                logger.warning("데이터 수집기에 웹소켓 없음")
                return False
                
        except Exception as e:
            logger.error(f"웹소켓 재연결 시도 중 오류: {e}")
            return False

    def _attempt_websocket_reconnection_with_retry(self) -> bool:
        """🎯 웹소켓 강력한 재연결 시도 (여러 번 재시도)"""
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"🔄 웹소켓 재연결 시도 {attempt}/{max_attempts}")
                
                if hasattr(self.data_collector, 'websocket'):
                    websocket_manager = self.data_collector.websocket
                    
                    if websocket_manager:
                        # ensure_connection 메서드 사용 (새로 추가된)
                        if hasattr(websocket_manager, 'ensure_connection'):
                            try:
                                websocket_manager.ensure_connection()
                                # 연결 상태 확인
                                if getattr(websocket_manager, 'is_connected', False):
                                    logger.info(f"✅ 웹소켓 재연결 성공 (시도 {attempt})")
                                    return True
                            except Exception as e:
                                logger.warning(f"⚠️ ensure_connection 실패 (시도 {attempt}): {e}")
                        
                        logger.warning(f"⚠️ 웹소켓 재연결 실패 - 시도 {attempt}/{max_attempts}")
                        
                        if attempt < max_attempts:
                            time.sleep(3 * attempt)  # 점진적 대기
                    else:
                        logger.error("웹소켓 매니저를 찾을 수 없음")
                        return False
                else:
                    logger.error("데이터 수집기에 웹소켓을 찾을 수 없음")
                    return False
                    
            except Exception as e:
                logger.error(f"웹소켓 재연결 시도 {attempt} 중 오류: {e}")
                if attempt < max_attempts:
                    time.sleep(3 * attempt)
                    
        logger.error("❌ 모든 웹소켓 재연결 시도 실패")
        return False

    def force_price_update_via_rest_api(self) -> int:
        """모든 포지션 현재가를 REST API로 강제 업데이트"""
        logger.info("🔄 REST API 강제 현재가 업데이트 시작")
        start_time = time.time()
        
        self.update_position_prices(force_rest_api=True)
        
        updated_count = len([p for p in self.positions.values() 
                           if p['status'] == 'active' and p.get('current_price', 0) > 0])
        
        elapsed = time.time() - start_time
        logger.info(f"✅ REST API 강제 업데이트 완료: {updated_count}개 종목, {elapsed:.1f}초 소요")
        
        return updated_count

    def check_exit_conditions(self) -> List[Dict]:
        """매도 조건 확인 - 개선된 수익성 중심 로직"""
        sell_signals = []

        with self.position_lock:
            for stock_code, position in self.positions.items():
                if position['status'] != 'active':
                    continue

                current_price = position.get('current_price')
                if not current_price:
                    continue

                # 개별 포지션 매도 조건 확인
                sell_signal = self._check_position_exit_conditions(position)
                if sell_signal:
                    sell_signals.append(sell_signal)

        return sell_signals

    def _check_position_exit_conditions(self, position: Dict) -> Optional[Dict]:
        """개별 포지션의 매도 조건 확인"""
        stock_code = position['stock_code']
        current_price = position.get('current_price')
        profit_rate = position.get('profit_rate', 0)
        max_profit_rate = position.get('max_profit_rate', 0)
        strategy_type = position.get('strategy_type', 'default')
        buy_time = position.get('buy_time', time.time())

        # 전략별 손익 기준
        targets = self.profit_targets.get(strategy_type, self.profit_targets['default'])
        holding_minutes = (time.time() - buy_time) / 60

        # 매도 조건 확인
        sell_reason = self._evaluate_sell_conditions(
            profit_rate, max_profit_rate, holding_minutes, targets, position
        )

        if sell_reason:
            logger.info(f"🚨 매도 신호 생성: {stock_code} - {sell_reason} (최고수익: {max_profit_rate:.2f}%)")
            
            return {
                'stock_code': stock_code,
                'quantity': position['quantity'],
                'current_price': current_price,
                'profit_rate': profit_rate,
                'max_profit_rate': max_profit_rate,
                'holding_minutes': holding_minutes,
                'reason': sell_reason,
                'strategy_type': strategy_type
            }

        return None

    def _evaluate_sell_conditions(self, profit_rate: float, max_profit_rate: float, 
                                 holding_minutes: float, targets: Dict, position: Dict = None) -> Optional[str]:
        """매도 조건 평가 (🎯 개선된 안전한 로직)"""
        stop_loss = targets['stop_loss']
        take_profit = targets['take_profit']
        min_holding_minutes = targets.get('min_holding_minutes', 30)
        trailing_stop_trigger = targets.get('trailing_stop_trigger', 3.0)
        trailing_stop_gap = targets.get('trailing_stop_gap', 1.5)
        
        # 🆕 새로운 안전 장치들
        early_stop_loss = targets.get('early_stop_loss', -2.0)
        early_stop_minutes = targets.get('early_stop_minutes', 15)
        dynamic_stop_loss = targets.get('dynamic_stop_loss', True)

        # 1. 극심한 손실 시 즉시 손절 (기존 유지)
        if profit_rate <= stop_loss - 2.5:  # 더 빠른 긴급손절
            return f"긴급손절 ({profit_rate:.2f}%)"
        
        # 2. 🆕 조기 손절 (시간 단축 + 안전성 강화)
        elif holding_minutes >= early_stop_minutes and profit_rate <= early_stop_loss:
            return f"조기손절 ({profit_rate:.2f}%, {holding_minutes:.0f}분)"
        
        # 3. 🆕 동적 손절 (최고점 기반 손절선 조정)
        elif dynamic_stop_loss and max_profit_rate > 2.0:
            # 최고점에서 이익이 났으면 손절선을 동적으로 조정
            dynamic_stop = max(stop_loss, early_stop_loss + (max_profit_rate * 0.3))
            if holding_minutes >= early_stop_minutes and profit_rate <= dynamic_stop:
                return f"동적손절 ({profit_rate:.2f}%, 최고:{max_profit_rate:.1f}%, {holding_minutes:.0f}분)"
        
        # 4. 🧠 지능형 추격매도 우선 체크 (충분한 수익 시)
        elif max_profit_rate >= trailing_stop_trigger and position:
            intelligent_signal = self._check_intelligent_trailing_stop(position)
            if intelligent_signal:
                return intelligent_signal
        
        # 5. 최소 홀딩 시간 후 정상 매도 조건
        elif holding_minutes >= min_holding_minutes:
            if profit_rate <= stop_loss:
                return f"손절 ({profit_rate:.2f}%, {holding_minutes:.0f}분)"
            elif profit_rate >= take_profit:
                return f"익절 ({profit_rate:.2f}%, {holding_minutes:.0f}분)"
            elif (max_profit_rate >= trailing_stop_trigger and 
                  profit_rate <= max_profit_rate - trailing_stop_gap):
                return f"기본추격매도 (최고 {max_profit_rate:.2f}% → {profit_rate:.2f}%, {holding_minutes:.0f}분)"
        
        # 6. 조기 익절 조건 (더 관대하게 조정)
        elif holding_minutes < min_holding_minutes and profit_rate >= take_profit + 1.5:
            return f"조기익절 ({profit_rate:.2f}%, {holding_minutes:.0f}분)"

        return None

    def execute_auto_sell(self, sell_signal: Dict) -> Optional[str]:
        """자동 매도 실행"""
        try:
            stock_code = sell_signal['stock_code']
            quantity = sell_signal['quantity']
            reason = sell_signal['reason']

            # 시장가로 매도
            order_no = self.trading_manager.execute_order(
                stock_code=stock_code,
                order_type="SELL",
                quantity=quantity,
                price=0,  # 시장가
                strategy_type=f"auto_sell_{reason}"
            )

            if order_no:
                logger.info(f"✅ 자동 매도 주문: {stock_code} {quantity}주 - {reason}")
                return order_no
            else:
                logger.error(f"❌ 자동 매도 실패: {stock_code} - {reason}")
                return None

        except Exception as e:
            logger.error(f"자동 매도 오류: {sell_signal['stock_code']} - {e}")
            return None

    def get_positions(self, status: str = 'active') -> Dict[str, Dict]:
        """포지션 목록 조회"""
        with self.position_lock:
            if status == 'all':
                return self.positions.copy()
            else:
                return {
                    code: pos for code, pos in self.positions.items()
                    if pos['status'] == status
                }

    def get_position_summary(self) -> Dict:
        """포지션 요약"""
        with self.position_lock:
            active_positions = [p for p in self.positions.values() if p['status'] == 'active']

            if not active_positions:
                return {
                    'total_positions': 0,
                    'total_value': 0,
                    'total_profit_loss': 0,
                    'total_profit_rate': 0,
                    'positions': []
                }

            total_value = 0
            total_profit_loss = 0
            total_investment = 0

            position_list = []

            for position in active_positions:
                current_price = position.get('current_price', position['buy_price'])
                quantity = position['quantity']

                position_value = current_price * quantity
                investment_amount = position['buy_price'] * quantity
                profit_loss = position_value - investment_amount
                profit_rate = position.get('profit_rate', 0)

                total_value += position_value
                total_profit_loss += profit_loss
                total_investment += investment_amount

                position_list.append({
                    'stock_code': position['stock_code'],
                    'quantity': quantity,
                    'buy_price': position['buy_price'],
                    'current_price': current_price,
                    'position_value': position_value,
                    'profit_loss': profit_loss,
                    'profit_rate': profit_rate,
                    'max_profit_rate': position.get('max_profit_rate', 0),
                    'strategy_type': position.get('strategy_type', 'manual')
                })

            total_profit_rate = (total_profit_loss / total_investment * 100) if total_investment > 0 else 0

            return {
                'total_positions': len(active_positions),
                'total_value': total_value,
                'total_investment': total_investment,
                'total_profit_loss': total_profit_loss,
                'total_profit_rate': total_profit_rate,
                'positions': position_list
            }

    def get_stats(self) -> Dict:
        """포지션 통계"""
        win_rate = (
            (self.stats['profitable_positions'] /
             (self.stats['profitable_positions'] + self.stats['loss_positions']) * 100)
            if (self.stats['profitable_positions'] + self.stats['loss_positions']) > 0 else 0
        )

        return {
            **self.stats.copy(),
            'win_rate': round(win_rate, 2)
        }

    def cleanup(self):
        """리소스 정리"""
        logger.info("포지션 관리자 정리 중...")

        # 활성 포지션 정보 로깅
        active_positions = self.get_positions('active')
        if active_positions:
            logger.info(f"정리 시점 활성 포지션: {len(active_positions)}개")
            for stock_code, position in active_positions.items():
                profit_rate = position.get('profit_rate', 0)
                logger.info(f"- {stock_code}: {position['quantity']}주, 수익률 {profit_rate:.2f}%")

        logger.info("포지션 관리자 정리 완료")

    def _check_disparity_sell_signal(self, position: Dict) -> Optional[Dict]:
        """🆕 고도화된 다중 이격도 기반 매도 신호 확인"""
        try:
            stock_code = position['stock_code']
            current_price = position.get('current_price', position['buy_price'])
            profit_rate = position.get('profit_rate', 0)
            
            # 🎯 다중 기간 이격도 데이터 조회 (5일, 20일, 60일)
            d5_data = get_disparity_rank(
                fid_input_iscd="0000",
                fid_hour_cls_code="5",
                fid_vol_cnt="10000"
            )
            d20_data = get_disparity_rank(
                fid_input_iscd="0000",
                fid_hour_cls_code="20",
                fid_vol_cnt="10000"
            )
            d60_data = get_disparity_rank(
                fid_input_iscd="0000",
                fid_hour_cls_code="60",
                fid_vol_cnt="10000"
            )
            
            # 해당 종목의 이격도 추출
            d5_val = d20_val = d60_val = None
            
            if d5_data is not None and not d5_data.empty:
                d5_row = d5_data[d5_data['mksc_shrn_iscd'] == stock_code]
                if not d5_row.empty:
                    d5_val = float(d5_row.iloc[0].get('d5_dsrt', 100))
            
            if d20_data is not None and not d20_data.empty:
                d20_row = d20_data[d20_data['mksc_shrn_iscd'] == stock_code]
                if not d20_row.empty:
                    d20_val = float(d20_row.iloc[0].get('d20_dsrt', 100))
            
            if d60_data is not None and not d60_data.empty:
                d60_row = d60_data[d60_data['mksc_shrn_iscd'] == stock_code]
                if not d60_row.empty:
                    d60_val = float(d60_row.iloc[0].get('d60_dsrt', 100))
            
            # 🎯 다중 이격도 기반 매도 전략
            if all(val is not None for val in [d5_val, d20_val, d60_val]):
                
                # 1. 🔥 극도 과매수 구간: 즉시 매도
                if d5_val >= 125 and d20_val >= 120:
                    if profit_rate >= 0.5:  # 0.5% 이상 수익시 즉시 매도
                        return {
                            'signal_type': 'SELL',
                            'reason': f'극도과매수 즉시매도 (D5:{d5_val:.1f}, D20:{d20_val:.1f}, 수익:{profit_rate:.1f}%)',
                            'urgency': 'URGENT',
                            'suggested_price': int(current_price * 0.992)  # 0.8% 할인 매도
                        }
                
                # 2. 🎯 과매수 구간: 수익 조건부 매도
                elif d5_val >= 115 and d20_val >= 110:
                    if profit_rate >= 1.5:  # 1.5% 이상 수익시 매도
                        return {
                            'signal_type': 'SELL',
                            'reason': f'다중과매수 수익매도 (D5:{d5_val:.1f}, D20:{d20_val:.1f}, 수익:{profit_rate:.1f}%)',
                            'urgency': 'HIGH',
                            'suggested_price': int(current_price * 0.995)  # 0.5% 할인 매도
                        }
                
                # 3. 🎯 Divergence 매도 신호: 장기 과열 + 단기 조정
                elif d60_val >= 110 and d20_val >= 105 and d5_val <= 100:
                    if profit_rate >= 2.0:  # 2% 이상 수익시 매도
                        return {
                            'signal_type': 'SELL',
                            'reason': f'하향Divergence 매도 (D60:{d60_val:.1f}↑ D5:{d5_val:.1f}↓, 수익:{profit_rate:.1f}%)',
                            'urgency': 'MEDIUM',
                            'suggested_price': int(current_price * 0.997)  # 0.3% 할인 매도
                        }
                
                # 4. 🛡️ 과매도 구간: 손절 완화 & 보유 연장
                elif d20_val <= 85 and d60_val <= 90:
                    # 과매도 구간에서는 보유 연장
                    targets = self.profit_targets.get(position.get('strategy_type', 'default'), {})
                    stop_loss = targets.get('stop_loss', -3.0)
                    
                    if profit_rate <= stop_loss and profit_rate >= stop_loss - 2.0:  # 손절 2% 완화
                        logger.info(f"🛡️ 다중과매도로 손절 완화: {stock_code} "
                                  f"D20:{d20_val:.1f} D60:{d60_val:.1f} 손실:{profit_rate:.1f}%")
                        return None  # 매도 신호 무시
                    
                    # 익절 기준도 상향 조정 (40% 완화)
                    take_profit = targets.get('take_profit', 5.0)
                    if profit_rate >= take_profit * 0.6:  # 익절 기준 40% 완화
                        logger.info(f"🛡️ 다중과매도로 익절 연장: {stock_code} "
                                  f"D20:{d20_val:.1f} D60:{d60_val:.1f} 수익:{profit_rate:.1f}%")
                        return None  # 익절 신호 무시하고 더 보유
                
                # 5. 🎯 특수 패턴: 단기 급등 후 조정 징후
                elif d5_val >= 110 and d20_val <= 105 and profit_rate >= 3.0:
                    return {
                        'signal_type': 'SELL',
                        'reason': f'단기급등 조정매도 (D5:{d5_val:.1f}↑ D20:{d20_val:.1f}, 수익:{profit_rate:.1f}%)',
                        'urgency': 'MEDIUM',
                        'suggested_price': int(current_price * 0.996)  # 0.4% 할인 매도
                    }
            
            # 단일 이격도 백업 로직 (20일 이격도만 확인 가능한 경우)
            elif d20_val is not None:
                if d20_val >= 120 and profit_rate >= 1.0:
                    return {
                        'signal_type': 'SELL',
                        'reason': f'20일과매수 매도 (D20:{d20_val:.1f}, 수익:{profit_rate:.1f}%)',
                        'urgency': 'HIGH',
                        'suggested_price': int(current_price * 0.995)
                    }
                elif d20_val <= 80:
                    # 과매도 구간에서는 보유 연장
                    logger.info(f"🛡️ 20일과매도로 보유연장: {stock_code} D20:{d20_val:.1f}%")
                    return None
            
            return None
            
        except Exception as e:
            logger.debug(f"다중 이격도 매도 신호 확인 오류 ({position.get('stock_code', 'Unknown')}): {e}")
            return None

    def check_auto_sell(self) -> List[str]:
        """자동 매도 체크 및 실행 - worker_manager 호환용"""
        try:
            executed_orders = []
            
            # 1. 포지션 현재가 업데이트
            self.update_position_prices()
            
            # 2. 매도 조건 확인
            sell_signals = self.check_exit_conditions()
            
            # 3. 매도 신호 실행
            for sell_signal in sell_signals:
                try:
                    order_no = self.execute_auto_sell(sell_signal)
                    if order_no:
                        executed_orders.append(order_no)
                        
                        # 포지션에서 제거
                        stock_code = sell_signal['stock_code']
                        quantity = sell_signal['quantity']
                        current_price = sell_signal['current_price']
                        
                        self.remove_position(stock_code, quantity, current_price)
                        
                        logger.info(f"✅ 자동 매도 완료: {stock_code} - {sell_signal['reason']}")
                    else:
                        logger.warning(f"⚠️ 자동 매도 주문 실패: {sell_signal['stock_code']}")
                        
                except Exception as e:
                    logger.error(f"❌ 자동 매도 실행 오류: {sell_signal['stock_code']} - {e}")
            
            return executed_orders
            
        except Exception as e:
            logger.error(f"❌ 자동 매도 체크 오류: {e}")
            return []

    def _check_websocket_connection(self) -> bool:
        """웹소켓 연결 상태 확인 (간소화 버전)"""
        try:
            # data_collector를 통해 웹소켓 매니저 접근
            websocket_manager = getattr(
                getattr(self.trading_manager, 'data_collector', None), 
                'websocket', 
                None
            )
            
            if not websocket_manager:
                logger.debug("웹소켓 매니저를 찾을 수 없음")
                return False

            # 기본 연결 상태 확인
            is_connected = getattr(websocket_manager, 'is_connected', False)
            is_running = getattr(websocket_manager, 'is_running', False)
            
            # 실제 연결 상태 확인
            actual_connected = websocket_manager._check_actual_connection_status()
            
            # 건강성 체크
            is_healthy = getattr(websocket_manager, 'is_healthy', lambda: False)()
            
            # 전체 상태 판단
            websocket_available = is_connected and is_running and actual_connected and is_healthy
            
            logger.debug(f"웹소켓 상태: connected={is_connected}, running={is_running}, "
                        f"actual={actual_connected}, healthy={is_healthy}")
            
            return websocket_available
                
        except Exception as e:
            logger.debug(f"웹소켓 연결 상태 확인 오류: {e}")
            return False

    def _check_intelligent_trailing_stop(self, position: Dict) -> Optional[str]:
        """🧠 지능형 추격매도 - 기술적 지표 활용"""
        try:
            stock_code = position['stock_code']
            current_price = position.get('current_price', position['buy_price'])
            profit_rate = position.get('profit_rate', 0)
            max_profit_rate = position.get('max_profit_rate', 0)
            
            # 🎯 추격매도 트리거 확인 (기본 3% 이상 수익)
            strategy_type = position.get('strategy_type', 'default')
            targets = self.profit_targets.get(strategy_type, self.profit_targets['default'])
            trailing_trigger = targets.get('trailing_stop_trigger', 3.0)
            
            if max_profit_rate < trailing_trigger:
                return None  # 아직 추격매도 조건 미달성
            
            # 🔍 기술적 지표 기반 매도 신호 확인
            try:
                # 최근 가격 데이터 조회 (간단한 버전)
                price_data = self.data_collector.get_historical_data(stock_code, period=20)
                if not price_data or len(price_data) < 10:
                    # 기술적 지표 없으면 기존 방식 사용
                    trailing_gap = targets.get('trailing_stop_gap', 1.5)
                    if profit_rate <= max_profit_rate - trailing_gap:
                        return f"기본추격매도 (최고 {max_profit_rate:.2f}% → {profit_rate:.2f}%)"
                    return None
                
                # 🧠 기술적 지표 계산
                from core.technical_indicators import TechnicalIndicators
                
                closes = [float(d.get('stck_clpr', 0)) for d in price_data[-20:]]
                closes.append(current_price)  # 현재가 포함
                
                # RSI 계산
                rsi_values = TechnicalIndicators.calculate_rsi(closes, period=14)
                current_rsi = rsi_values[-1] if rsi_values else 50.0
                
                # MACD 계산
                macd_data = TechnicalIndicators.calculate_macd(closes)
                current_macd = macd_data['macd'][-1] if macd_data['macd'] else 0.0
                current_signal = macd_data['signal'][-1] if macd_data['signal'] else 0.0
                
                # 볼린저 밴드 계산
                bb_data = TechnicalIndicators.calculate_bollinger_bands(closes)
                bb_position = bb_data['bandwidth'][-1] if bb_data['bandwidth'] else 50.0
                
                # 🎯 지능형 매도 신호 판단
                sell_signals = []
                
                # 1. RSI 과매수 신호 (70 이상)
                if current_rsi >= 70:
                    sell_signals.append(f"RSI과매수({current_rsi:.1f})")
                
                # 2. MACD 하향 전환
                if current_macd < current_signal and abs(current_macd - current_signal) > 0.5:
                    sell_signals.append("MACD하향전환")
                
                # 3. 볼린저 밴드 상단 터치 (과매수)
                if bb_position >= 80:
                    sell_signals.append(f"볼린저상단({bb_position:.1f}%)")
                
                # 4. 지지선 이탈 확인
                support_resistance = TechnicalIndicators.calculate_support_resistance(closes[-10:])
                if current_price < support_resistance['support'] * 1.02:  # 지지선 2% 근처
                    sell_signals.append("지지선근접")
                
                # 🚨 매도 신호 종합 판단
                if len(sell_signals) >= 2:  # 2개 이상 신호시 매도
                    return f"지능형추격매도 (최고:{max_profit_rate:.1f}%→{profit_rate:.1f}%, 신호:{'+'.join(sell_signals)})"
                elif len(sell_signals) == 1 and profit_rate <= max_profit_rate - 2.0:  # 1개 신호 + 2% 하락
                    return f"조건부추격매도 (최고:{max_profit_rate:.1f}%→{profit_rate:.1f}%, {sell_signals[0]})"
                else:
                    # 📈 기술적으로 아직 상승 여력 있음 → 기존 추격매도 기준 완화
                    relaxed_gap = targets.get('trailing_stop_gap', 1.5) + 0.5  # 0.5% 완화
                    if profit_rate <= max_profit_rate - relaxed_gap:
                        return f"완화추격매도 (최고:{max_profit_rate:.1f}%→{profit_rate:.1f}%, 기술적여력존재)"
                
                return None
                
            except Exception as e:
                logger.debug(f"기술적 지표 분석 오류 ({stock_code}): {e}")
                # 기술적 분석 실패시 기존 방식 사용
                trailing_gap = targets.get('trailing_stop_gap', 1.5)
                if profit_rate <= max_profit_rate - trailing_gap:
                    return f"백업추격매도 (최고 {max_profit_rate:.2f}% → {profit_rate:.2f}%)"
                return None
                
        except Exception as e:
            logger.error(f"지능형 추격매도 오류 ({position.get('stock_code', 'Unknown')}): {e}")
            return None
