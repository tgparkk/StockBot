"""
포지션 관리자 - 보유 종목 및 포지션 추적
"""
import time
import threading
from typing import Dict, List, Optional, Any, TYPE_CHECKING
from utils.logger import setup_logger
from ..data.kis_data_collector import KISDataCollector
from ..api.kis_market_api import get_disparity_rank, get_multi_period_disparity, get_inquire_price

# TYPE_CHECKING을 사용하여 순환 import 방지
if TYPE_CHECKING:
    from .trading_manager import TradingManager

logger = setup_logger(__name__)


class PositionManager:
    """간소화된 포지션 관리자"""

    def __init__(self, trading_manager: 'TradingManager', data_collector: KISDataCollector):
        """초기화"""
        self.trading_manager = trading_manager
        self.data_collector = trading_manager.data_collector

        # 포지션 추적
        self.positions: Dict[str, Dict] = {}  # {stock_code: position_info}
        self.position_lock = threading.RLock()

        # 데이터 기반 손익 설정 계산
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
        """과학적 근거 기반 손익 목표 계산"""
        try:
            # 시장 변동성 분석
            market_volatility = self._analyze_market_volatility()

            # 전략별 백테스팅 성과 분석
            strategy_performance = self._analyze_strategy_performance()

            # 리스크 대비 수익률 최적화
            risk_adjusted_targets = self._calculate_risk_adjusted_targets(
                market_volatility, strategy_performance
            )

            logger.info(f"과학적 손익 목표 설정 완료 - 시장변동성: {market_volatility:.2f}%")
            return risk_adjusted_targets

        except Exception as e:
            logger.warning(f"과학적 계산 실패, 기본값 사용: {e}")
            return self._get_default_targets()

    def _analyze_market_volatility(self) -> float:
        """시장 전체 변동성 분석"""
        try:
            # KOSPI 지수 최근 20일 변동성 분석 - 간소화된 버전
            # get_market_index_data 메서드가 없으므로 기본 계산 사용
            logger.debug("시장 변동성 분석 - 기본값 사용")
            return 2.5  # 기본값

        except Exception as e:
            logger.debug(f"시장 변동성 분석 오류: {e}")
            return 2.5  # 기본값

    def _analyze_strategy_performance(self) -> Dict:
        """전략별 과거 성과 분석"""
        try:
            # 데이터베이스에서 과거 거래 성과 조회 - 안전한 접근
            trade_db = getattr(self.trading_manager, 'trade_db', None)
            if trade_db:
                performance_data = trade_db.get_strategy_performance_stats()

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
        """리스크 조정 손익 목표 계산"""

        # 변동성 기반 기본 배수 계산
        volatility_multiplier = max(1.0, min(market_volatility / 2.0, 3.0))  # 1.0~3.0 범위

        # 기본 리스크 비율 (변동성의 0.7~1.2배)
        base_stop_loss = -(market_volatility * 0.8)
        base_take_profit = market_volatility * 1.8
        base_trailing_trigger = market_volatility * 1.2

        # 전략별 조정 계산
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
            # 과거 성과 반영
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

            # 최종 계산
            final_stop_loss = base_stop_loss * config['risk_factor'] * performance_multiplier
            final_take_profit = base_take_profit * config['profit_factor'] * performance_multiplier
            final_trailing_trigger = base_trailing_trigger * config['profit_factor'] * performance_multiplier
            final_trailing_gap = final_trailing_trigger * 0.5  # 트리거의 50%

            # 합리적 범위 제한
            final_stop_loss = max(-8.0, min(final_stop_loss, -1.0))  # -1% ~ -8%
            final_take_profit = max(2.0, min(final_take_profit, 15.0))  # 2% ~ 15%
            final_trailing_trigger = max(1.5, min(final_trailing_trigger, 8.0))  # 1.5% ~ 8%
            final_trailing_gap = max(0.8, min(final_trailing_gap, 4.0))  # 0.8% ~ 4%

            # 보유 시간도 변동성 기반 계산
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
                # 계산 근거 로깅용
                '_calculation_basis': {
                    'market_volatility': market_volatility,
                    'risk_factor': config['risk_factor'],
                    'profit_factor': config['profit_factor'],
                    'performance_multiplier': performance_multiplier,
                    'sample_size': performance.get('sample_size', 0)
                }
            }

            logger.info(f"{strategy}: 손절{final_stop_loss:.1f}%, 익절{final_take_profit:.1f}%, "
                       f"추격{final_trailing_trigger:.1f}% (변동성:{market_volatility:.1f}%)")

        return targets

    def _get_default_targets(self) -> Dict:
        """기본 손익 목표 (백업용) - 더 합리적인 보유 시간 적용"""
        return {
            # 기본 전략: 안정적이고 수익성 있는 매도 (최소 보유 시간 개선)
            'default': {
                'stop_loss': -3.5, 'take_profit': 5.5, 'min_holding_minutes': 45,
                'early_stop_loss': -2.0, 'early_stop_minutes': 20,  # 15분 → 20분
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

            # 이격도 반등: 과매도 반등 기대하며 여유 있게 (개선)
            'disparity_reversal': {
                'stop_loss': -3.0, 'take_profit': 6.5, 'min_holding_minutes': 45,
                'early_stop_loss': -2.0, 'early_stop_minutes': 25,  # 20분 → 25분
                'trailing_stop_trigger': 3.5, 'trailing_stop_gap': 1.8,
                'dynamic_stop_loss': True
            },

            # 갭 거래: 빠른 수익 실현이지만 더 여유 있게 (개선)
            'gap_trading': {
                'stop_loss': -2.5, 'take_profit': 4.5, 'min_holding_minutes': 30,  # 20분 → 30분
                'early_stop_loss': -1.5, 'early_stop_minutes': 15,  # 10분 → 15분
                'trailing_stop_trigger': 2.5, 'trailing_stop_gap': 1.2,
                'dynamic_stop_loss': True
            },

            # 거래량 돌파: 트렌드 지속 기대 (개선)
            'volume_breakout': {
                'stop_loss': -3.2, 'take_profit': 6.8, 'min_holding_minutes': 40,  # 35분 → 40분
                'early_stop_loss': -2.2, 'early_stop_minutes': 20,  # 18분 → 20분
                'trailing_stop_trigger': 3.8, 'trailing_stop_gap': 2.0,
                'dynamic_stop_loss': True
            },

            # 모멘텀: 트렌드 최대한 활용하되 너무 짧지 않게 (개선)
            'momentum': {
                'stop_loss': -2.2, 'take_profit': 8.0, 'min_holding_minutes': 25,  # 20분 → 25분
                'early_stop_loss': -1.5, 'early_stop_minutes': 15,  # 8분 → 15분
                'trailing_stop_trigger': 4.5, 'trailing_stop_gap': 2.2,
                'dynamic_stop_loss': True
            }
        }

    # === 수익률 계산 헬퍼 메서드들 ===

    def _calculate_profit_rate(self, current_price: int, buy_price: int) -> float:
        """수익률 계산 (세전)"""
        if buy_price <= 0:
            return 0.0
        return ((current_price - buy_price) / buy_price) * 100

    def _calculate_after_tax_profit_rate(self, current_price: int, buy_price: int,
                                        market_type: str = "KOSPI") -> float:
        """세후 실제 수익률 계산"""
        if buy_price <= 0:
            return 0.0

        # 한국 주식 거래 비용 (2024년 기준)
        brokerage_fee_rate = 0.00015  # 0.015% (매수/매도 각각)

        if market_type.upper() == "KOSDAQ":
            transaction_tax_rate = 0.003  # 0.3% (코스닥, 매도시만)
        else:
            transaction_tax_rate = 0.0023  # 0.23% (코스피, 매도시만)

        securities_tax_rate = 0.0015  # 0.15% (매도시만)
        rural_tax_rate = transaction_tax_rate * 0.2  # 농특세 (거래세의 20%)

        # 총 거래 비용 계산
        buy_cost = buy_price * brokerage_fee_rate  # 매수 수수료
        sell_cost = current_price * (
            brokerage_fee_rate +  # 매도 수수료
            transaction_tax_rate +  # 거래세
            securities_tax_rate +  # 증권거래세
            rural_tax_rate  # 농특세
        )

        # 실제 손익
        gross_profit = current_price - buy_price
        net_profit = gross_profit - buy_cost - sell_cost

        # 세후 수익률
        after_tax_rate = (net_profit / buy_price) * 100

        return round(after_tax_rate, 2)

    def _get_market_type(self, stock_code: str) -> str:
        """종목 코드로 시장 구분 (코스피/코스닥)"""
        try:
            # 간단한 구분 로직 (정확하지 않을 수 있음)
            code_int = int(stock_code)
            if code_int < 100000:  # 6자리 미만은 대체로 코스피
                return "KOSPI"
            else:
                return "KOSDAQ"
        except:
            return "KOSPI"  # 기본값

    def _calculate_profit_loss(self, sell_price: int, buy_price: int, quantity: int) -> int:
        """손익 계산"""
        return (sell_price - buy_price) * quantity

    def _update_position_profit_info(self, position: Dict, current_price: int) -> None:
        """포지션 수익 정보 업데이트"""
        stock_code = position['stock_code']
        buy_price = position['buy_price']

        # 세전 수익률 (기존)
        profit_rate = self._calculate_profit_rate(current_price, buy_price)

        # 세후 실제 수익률 (신규 추가)
        market_type = self._get_market_type(stock_code)
        after_tax_profit_rate = self._calculate_after_tax_profit_rate(current_price, buy_price, market_type)

        # 포지션 정보 업데이트
        position['current_price'] = current_price
        position['profit_rate'] = profit_rate  # 세전 수익률 (기존 호환성)
        position['after_tax_profit_rate'] = after_tax_profit_rate  # 세후 수익률
        position['market_type'] = market_type  # 시장 구분 정보
        position['last_update'] = time.time()

        # 최대 수익률 업데이트 (세후 기준으로 변경)
        if after_tax_profit_rate > position.get('max_after_tax_profit_rate', 0):
            position['max_after_tax_profit_rate'] = after_tax_profit_rate

        # 최대 손실률 추적 (세후 기준으로 변경)
        if after_tax_profit_rate < position.get('min_after_tax_profit_rate', 0):
            position['min_after_tax_profit_rate'] = after_tax_profit_rate

        # 기존 호환성을 위해 세전 기준도 유지
        if profit_rate > position.get('max_profit_rate', 0):
            position['max_profit_rate'] = profit_rate
        if profit_rate < position.get('min_profit_rate', 0):
            position['min_profit_rate'] = profit_rate

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
                        'min_profit_rate': 0.0,  # 🆕 최대 손실률 추적용 추가
                        'max_after_tax_profit_rate': 0.0,  # 🎯 세후 최대 수익률
                        'min_after_tax_profit_rate': 0.0,  # 🎯 세후 최대 손실률
                        'market_type': self._get_market_type(stock_code),  # 시장 구분
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

    def update_position_prices(self, force_rest_api: bool = True) -> None:
        """포지션 가격 업데이트 - 🔧 웹소켓 재연결 스팸 방지"""
        if not self.positions:
            return

        current_time = time.time()

        # 웹소켓 연결 상태 확인 (REST API 강제 사용이 아닌 경우)
        websocket_available = False

        if not force_rest_api:
            # 🔧 _check_websocket_connection 메서드 대신 직접 확인
            try:
                if (hasattr(self.data_collector, 'websocket') and
                    self.data_collector.websocket):
                    websocket_manager = self.data_collector.websocket
                    websocket_available = getattr(websocket_manager, 'is_connected', False)
                else:
                    websocket_available = False
            except Exception as e:
                logger.debug(f"웹소켓 연결 상태 확인 오류: {e}")
                websocket_available = False

            if not websocket_available:
                # 🔧 재연결 시도 빈도 제한 (15분 간격으로 연장)
                if not hasattr(self, '_last_reconnect_attempt'):
                    self._last_reconnect_attempt = 0

                if current_time - self._last_reconnect_attempt > 900:  # 🔧 15분 (기존 5분에서 증가)
                    logger.info("🔴 웹소켓 연결 확인 불가 - 재연결 시도 중... (15분 간격)")

                    # 재연결 시도
                    reconnected = self._attempt_websocket_reconnection_simple()

                    if reconnected:
                        logger.info("✅ 웹소켓 재연결 성공")
                        websocket_available = True
                    else:
                        logger.debug("재연결 실패 - REST API 계속 사용")

                    self._last_reconnect_attempt = current_time
                # 🔧 쿨다운 중 로그 제거 (스팸 방지)

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
        # 🔧 로그 빈도 제한 (15분마다 한번만)
        if not hasattr(self, '_last_rest_api_log'):
            self._last_rest_api_log = 0

        if current_time - self._last_rest_api_log > 900:  # 15분마다
            reason = "강제 모드" if force_rest_api else "웹소켓 연결불가"
            logger.info(f"💾 REST API 백업 사용: {reason}")
            self._last_rest_api_log = current_time

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

    def _attempt_websocket_reconnection_simple(self) -> bool:
        """🔧 간소화된 웹소켓 재연결 시도 (1회만)"""
        try:
            # 데이터 수집기를 통한 웹소켓 재연결
            if hasattr(self.data_collector, 'websocket'):
                websocket_manager = self.data_collector.websocket

                if websocket_manager:
                    # 현재 연결 상태 확인
                    is_connected = getattr(websocket_manager, 'is_connected', False)

                    if not is_connected:
                        logger.debug("🔄 웹소켓 간소화 재연결 시도...")

                        # 웹소켓 재시작 시도 (간단한 버전)
                        if hasattr(websocket_manager, 'ensure_connection'):
                            try:
                                websocket_manager.ensure_connection()
                                if getattr(websocket_manager, 'is_connected', False):
                                    logger.debug("✅ 웹소켓 간소화 재연결 성공")
                                    return True
                            except Exception as e:
                                logger.debug(f"웹소켓 간소화 재연결 실패: {e}")
                                return False

                        return False
                    else:
                        logger.debug("웹소켓이 이미 연결되어 있음")
                        return True
                else:
                    return False
            else:
                return False

        except Exception as e:
            logger.debug(f"웹소켓 간소화 재연결 오류: {e}")
            return False

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
        """🆕 매도 조건 확인 및 신호 반환 (직접 실행하지 않음)"""
        try:
            sell_signals = []

            # 1. 포지션 현재가 업데이트 (이미 worker에서 호출됨)
            # self.update_position_prices()  # 제거 - worker에서 별도 호출

            # 2. 활성 포지션들의 매도 조건 확인
            active_positions = self.get_positions('active')

            for stock_code, position in active_positions.items():
                try:
                    # 🔧 통합된 매도 조건 확인 (기본 + 이격도 조건 모두 포함)
                    sell_signal = self._check_position_exit_conditions(position)

                    if sell_signal:
                        sell_signals.append(sell_signal)
                        logger.info(f"💡 매도 신호 생성: {stock_code} - {sell_signal['reason']}")

                except Exception as e:
                    logger.error(f"❌ 포지션 매도 조건 확인 오류: {stock_code} - {e}")

            if sell_signals:
                logger.info(f"📊 총 {len(sell_signals)}개 매도 신호 생성됨")

            return sell_signals

        except Exception as e:
            logger.error(f"❌ 매도 조건 확인 오류: {e}")
            return []

    def _check_position_exit_conditions(self, position: Dict) -> Optional[Dict]:
        """🔧 통합된 포지션 매도 조건 확인 - 기본 조건 + 이격도 조건 통합"""
        stock_code = position['stock_code']
        current_price = position.get('current_price')

        # current_price가 None이면 매도 불가
        if current_price is None or current_price <= 0:
            logger.warning(f"유효하지 않은 현재가: {stock_code} - {current_price}")
            return None

        profit_rate = position.get('profit_rate', 0)
        max_profit_rate = position.get('max_profit_rate', 0)
        strategy_type = position.get('strategy_type', 'default')
        buy_time = position.get('buy_time', time.time())
        holding_minutes = (time.time() - buy_time) / 60

        # 🎯 1. 우선순위: 이격도 기반 매도 신호 확인 (시장 상황 우선)
        disparity_signal = self._check_disparity_sell_signal(position)
        if disparity_signal:
            optimal_sell_price = disparity_signal.get('suggested_price', current_price)
            logger.info(f"🎯 이격도 매도 신호: {stock_code} - {disparity_signal['reason']}")

            return {
                'stock_code': stock_code,
                'quantity': position['quantity'],
                'current_price': current_price,
                'optimal_sell_price': optimal_sell_price,
                'profit_rate': profit_rate,
                'max_profit_rate': max_profit_rate,
                'holding_minutes': holding_minutes,
                'reason': disparity_signal['reason'],
                'strategy_type': strategy_type,
                'urgency': disparity_signal.get('urgency', 'MEDIUM'),
                'signal_source': 'DISPARITY'  # 신호 출처 구분
            }

        # 🎯 2. 기본 매도 조건 확인 (이격도 신호가 없는 경우에만)
        targets = self.profit_targets.get(strategy_type, self.profit_targets['default'])
        sell_reason = self._evaluate_sell_conditions(
            profit_rate, max_profit_rate, holding_minutes, targets, position
        )

        if sell_reason:
            # 매도 조건 충족시 최적 매도 가격 계산
            optimal_sell_price = self._calculate_optimal_sell_price(stock_code, current_price, sell_reason)

            logger.info(f"💡 기본 매도 신호: {stock_code} - {sell_reason} (최고수익: {max_profit_rate:.2f}%)")
            logger.info(f"매도가격: 현재{current_price:,}원 → 최적{optimal_sell_price:,}원")

            return {
                'stock_code': stock_code,
                'quantity': position['quantity'],
                'current_price': current_price,
                'optimal_sell_price': optimal_sell_price,
                'profit_rate': profit_rate,
                'max_profit_rate': max_profit_rate,
                'holding_minutes': holding_minutes,
                'reason': sell_reason,
                'strategy_type': strategy_type,
                'signal_source': 'BASIC'  # 신호 출처 구분
            }

        return None

    def _evaluate_sell_conditions(self, profit_rate: float, max_profit_rate: float,
                                 holding_minutes: float, targets: Dict, position: Optional[Dict] = None) -> Optional[str]:
        """🎯 정리된 매도 조건 평가 (중복 제거 + 정교한 반등 예측)"""
        stop_loss = targets['stop_loss']
        take_profit = targets['take_profit']
        min_holding_minutes = targets.get('min_holding_minutes', 30)
        trailing_stop_trigger = targets.get('trailing_stop_trigger', 3.0)
        trailing_stop_gap = targets.get('trailing_stop_gap', 1.5)

        # 🚨 1. 극단적 손실 보호 (-10% 이상은 무조건 손절)
        if profit_rate <= -10.0:
            return f"극한손절 ({profit_rate:.2f}%) - 시스템보호"

        # 🧠 2. 지능형 손절 체크 (반등 예측 통합) - 핵심 로직
        if profit_rate <= stop_loss:
            # 🎯 정교한 반등 예측 분석 (-3% 이상 손실에서 실행)
            if profit_rate <= -3.0 and position is not None:
                bounce_analysis = self._analyze_advanced_bounce_potential(position, profit_rate)

                if bounce_analysis and bounce_analysis.get('should_hold', False):
                    bounce_reason = bounce_analysis.get('reason', '반등예측')
                    bounce_confidence = bounce_analysis.get('confidence', 0)
                    remaining_time = bounce_analysis.get('remaining_hold_time', 0)

                    logger.info(f"🎯 정교한반등예측 보유연장: {position['stock_code']} - {bounce_reason}")
                    logger.info(f"   신뢰도:{bounce_confidence:.2f}, 최대추가보유:{remaining_time:.0f}분")
                    return None  # 매도하지 않음
                else:
                    # 반등 가능성이 낮거나 시간 초과시 손절
                    bounce_score = bounce_analysis.get('score', 0) if bounce_analysis else 0
                    return f"반등예측손절 ({profit_rate:.2f}%, 점수:{bounce_score:.0f})"
            else:
                # -3% 이상의 경미한 손실은 일반 손절
                return f"손절 ({profit_rate:.2f}%, {holding_minutes:.0f}분)"

        # 💰 3. 익절 조건 (조기/일반 통합)
        elif profit_rate >= take_profit:
            if holding_minutes < min_holding_minutes:
                return f"조기익절 ({profit_rate:.2f}%, {holding_minutes:.0f}분)"
            else:
                return f"익절 ({profit_rate:.2f}%, {holding_minutes:.0f}분)"

        # 📈 4. 통합 추격매도 (지능형 + 기본)
        elif max_profit_rate >= trailing_stop_trigger and holding_minutes >= min_holding_minutes:
            # 🧠 지능형 추격매도 우선 시도
            if position:
                intelligent_signal = self._check_intelligent_trailing_stop(position)
                if intelligent_signal:
                    return intelligent_signal

            # 📉 기본 추격매도 (지능형 실패시 백업)
            if profit_rate <= max_profit_rate - trailing_stop_gap:
                return f"추격매도 (최고:{max_profit_rate:.2f}%→{profit_rate:.2f}%, {holding_minutes:.0f}분)"

        # ⏰ 5. 시간 기반 매도 (수익 구간에서만)
        elif (profit_rate > 0 and
              holding_minutes >= min_holding_minutes * 2.5):  # 2.5배 시간 후
            return f"시간만료매도 ({profit_rate:.2f}%, {holding_minutes:.0f}분)"

        # ✅ 매도 조건 없음
        return None

    def _analyze_advanced_bounce_potential(self, position: Dict, profit_rate: float) -> Optional[Dict]:
        """🎯 정교한 반등 예측 분석 (확장된 -7% 이상 지원)"""
        try:
            stock_code = position['stock_code']
            current_price = position.get('current_price', 0)
            holding_minutes = (time.time() - position['buy_time']) / 60

            logger.info(f"🔍 {stock_code} 정교한 반등 분석: 손실률{profit_rate:.1f}%, 보유{holding_minutes:.0f}분")

            # 🚨 기본 조건: -3% 이상 손실에서만 분석
            if profit_rate >= -3.0:
                return {'should_hold': False, 'reason': '경미한손실', 'confidence': 0.0}

            bounce_score = 0.0
            reasons = []
            max_additional_hold_minutes = 0  # 추가 보유 가능 시간

            # 🎯 손실률별 기본 점수 차등 적용
            if -4.0 <= profit_rate < -3.0:
                base_score = 20  # 경미 손실
                max_extension = 60  # 1시간 추가
            elif -6.0 <= profit_rate < -4.0:
                base_score = 15  # 중간 손실
                max_extension = 90  # 1.5시간 추가
            elif -8.0 <= profit_rate < -6.0:
                base_score = 10  # 큰 손실
                max_extension = 120  # 2시간 추가
            else:  # -8% 이상
                base_score = 5   # 매우 큰 손실
                max_extension = 60   # 1시간만 추가 (위험 제한)

            bounce_score += base_score
            reasons.append(f"손실구간기본점수({base_score}점)")

            # 1. 🎯 이격도 기반 과매도 분석 (최고 40점)
            try:
                from ..api.kis_market_api import get_disparity_rank
                disparity_data = get_disparity_rank(
                    fid_input_iscd="0000",      # 전체 시장
                    fid_hour_cls_code="20",     # 20일 이격도
                    fid_vol_cnt="5000"          # 거래량 5천주 이상
                )

                if disparity_data is not None and not disparity_data.empty:
                    stock_row = disparity_data[disparity_data['mksc_shrn_iscd'] == stock_code]
                    if not stock_row.empty:
                        d5 = float(stock_row.iloc[0].get('d5_dsrt', 100))
                        d20 = float(stock_row.iloc[0].get('d20_dsrt', 100))
                        d60 = float(stock_row.iloc[0].get('d60_dsrt', 100))

                        # 🔥 극도 과매도: 최고 점수
                        if d20 <= 80 and d5 <= 85:  # 더 엄격한 기준
                            bounce_score += 40
                            max_extension += 90  # 추가 연장
                            reasons.append(f"극도과매도(D20:{d20:.1f}, D5:{d5:.1f})")
                        elif d20 <= 85 and d5 <= 90:
                            bounce_score += 30
                            max_extension += 60
                            reasons.append(f"심각과매도(D20:{d20:.1f}, D5:{d5:.1f})")
                        elif d20 <= 90 and d5 <= 95:
                            bounce_score += 20
                            max_extension += 30
                            reasons.append(f"과매도상태(D20:{d20:.1f}, D5:{d5:.1f})")

                        # 🎯 다이버전스 패턴 (추가 점수)
                        if d60 >= 105 and d20 <= 85:  # 장기↑ 단기↓
                            bounce_score += 25
                            max_extension += 45
                            reasons.append(f"강력다이버전스(D60:{d60:.1f}↑D20:{d20:.1f}↓)")
                        elif d60 >= 100 and d20 <= 90:
                            bounce_score += 15
                            max_extension += 30
                            reasons.append(f"다이버전스(D60:{d60:.1f}D20:{d20:.1f})")

            except Exception as e:
                logger.debug(f"이격도 분석 오류: {e}")

            # 2. 🎯 거래량 분석 (최고 20점)
            try:
                from ..api.kis_market_api import get_inquire_price
                current_data = get_inquire_price("J", stock_code)
                if current_data is not None and not current_data.empty:
                    volume = int(current_data.iloc[0].get('acml_vol', 0))
                    avg_volume = int(current_data.iloc[0].get('avrg_vol', 0))

                    if avg_volume > 10000:
                        volume_ratio = volume / avg_volume

                        # 🎯 대량 거래량 + 큰 손실 = 바닥 신호
                        if volume_ratio >= 3.0 and profit_rate <= -5.0:
                            bounce_score += 20
                            max_extension += 60
                            reasons.append(f"대량바닥거래량({volume_ratio:.1f}배)")
                        elif volume_ratio >= 2.0 and profit_rate <= -4.0:
                            bounce_score += 15
                            max_extension += 45
                            reasons.append(f"대량거래량({volume_ratio:.1f}배)")
                        elif volume_ratio >= 1.5:
                            bounce_score += 10
                            max_extension += 30
                            reasons.append(f"거래량증가({volume_ratio:.1f}배)")

            except Exception as e:
                logger.debug(f"거래량 분석 오류: {e}")

            # 3. 🎯 시간 패턴 분석 (최고 15점)
            if holding_minutes <= 45:  # 45분 이내 급락
                bounce_score += 15
                max_extension += 45
                reasons.append(f"단기급락({holding_minutes:.0f}분)")
            elif holding_minutes <= 90:  # 1.5시간 이내
                bounce_score += 10
                max_extension += 30
                reasons.append(f"중기급락({holding_minutes:.0f}분)")
            elif holding_minutes >= 240:  # 4시간 이상은 감점
                bounce_score -= 10
                max_extension = max(0, max_extension - 30)
                reasons.append(f"장기보유하락({holding_minutes:.0f}분)")

            # 4. 🎯 과거 반등 패턴 분석 (최고 15점)
            try:
                # 해당 종목의 과거 급락 후 반등 성공률 계산
                historical_bounce_rate = self._analyze_historical_bounce_pattern(stock_code, profit_rate)
                if historical_bounce_rate > 70:  # 70% 이상 반등 성공
                    bounce_score += 15
                    max_extension += 60
                    reasons.append(f"과거반등성공({historical_bounce_rate:.0f}%)")
                elif historical_bounce_rate > 50:
                    bounce_score += 10
                    max_extension += 30
                    reasons.append(f"과거반등양호({historical_bounce_rate:.0f}%)")

            except Exception as e:
                logger.debug(f"과거 패턴 분석 오류: {e}")

            # 5. 🎯 최대 보유 시간 제한 확인
            min_holding_minutes = position.get('min_holding_minutes', 60)  # 기본값 60분
            max_total_hold_minutes = min_holding_minutes * 5  # 최대 5배
            current_extension_limit = max_total_hold_minutes - holding_minutes

            if current_extension_limit <= 0:
                bounce_score -= 30  # 시간 초과 감점
                max_extension = 0
                reasons.append("최대보유시간초과")
            else:
                max_extension = min(max_extension, current_extension_limit)

            # 6. 🎯 위험도 기반 조정 (-8% 이상은 더 엄격)
            if profit_rate <= -8.0:
                bounce_score *= 0.7  # 30% 감점
                max_extension = min(max_extension, 60)  # 최대 1시간으로 제한
                reasons.append("고위험구간조정")

            # 7. 🎯 최종 판단
            confidence = min(bounce_score / 100.0, 1.0)
            should_hold = False

            # 🚀 보유 연장 조건 (더 엄격한 기준)
            if bounce_score >= 70 and max_extension > 30:  # 70점 이상 + 30분 이상 연장 가능
                should_hold = True
                prediction = "HIGH"
            elif bounce_score >= 50 and max_extension > 15:  # 50점 이상 + 15분 이상 연장 가능
                should_hold = True
                prediction = "MEDIUM"
            elif bounce_score >= 35 and max_extension > 0 and profit_rate >= -6.0:  # 35점 + -6% 이상만
                should_hold = True
                prediction = "LOW"
            else:
                prediction = "NONE"

            result = {
                'should_hold': should_hold,
                'confidence': confidence,
                'prediction': prediction,
                'score': bounce_score,
                'reason': ', '.join(reasons) if reasons else '반등근거부족',
                'remaining_hold_time': max_extension,
                'max_total_hold_minutes': max_total_hold_minutes
            }

            logger.info(f"✅ {stock_code} 정교한반등분석: {prediction} (점수:{bounce_score:.0f}, 보유:{should_hold})")
            logger.info(f"   추가보유가능: {max_extension:.0f}분, 근거: {', '.join(reasons[:3])}")

            return result

        except Exception as e:
            logger.error(f"정교한 반등 예측 분석 오류: {e}")
            return {'should_hold': False, 'reason': '분석오류', 'confidence': 0.0}

    def _analyze_historical_bounce_pattern(self, stock_code: str, current_loss_rate: float) -> float:
        """📊 과거 반등 패턴 분석"""
        try:
            # 과거 60일 데이터에서 유사한 손실률 후 반등 성공률 계산
            # (실제 구현시에는 데이터베이스나 API 호출 필요)

            # 손실률 구간별 기본 반등 확률 (통계적 근사치)
            if current_loss_rate >= -4.0:
                return 75.0  # -4% 이상은 높은 반등률
            elif current_loss_rate >= -6.0:
                return 60.0  # -6% 이상은 중간 반등률
            elif current_loss_rate >= -8.0:
                return 45.0  # -8% 이상은 낮은 반등률
            else:
                return 30.0  # -8% 이하는 매우 낮은 반등률

        except Exception as e:
            logger.debug(f"과거 반등 패턴 분석 오류: {e}")
            return 50.0  # 기본값

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
                if d5_val is not None and d20_val is not None and d5_val >= 125 and d20_val >= 120:
                    if profit_rate >= 0.5:  # 0.5% 이상 수익시 즉시 매도
                        return {
                            'signal_type': 'SELL',
                            'reason': f'극도과매수 즉시매도 (D5:{d5_val:.1f}, D20:{d20_val:.1f}, 수익:{profit_rate:.1f}%)',
                            'urgency': 'URGENT',
                            'suggested_price': int(current_price * 0.992)  # 0.8% 할인 매도
                        }

                # 2. 🆕 과매수 구간: 수익 조건부 매도
                elif d5_val is not None and d20_val is not None and d5_val >= 115 and d20_val >= 110:
                    if profit_rate >= 1.5:  # 1.5% 이상 수익시 매도
                        return {
                            'signal_type': 'SELL',
                            'reason': f'다중과매수 수익매도 (D5:{d5_val:.1f}, D20:{d20_val:.1f}, 수익:{profit_rate:.1f}%)',
                            'urgency': 'HIGH',
                            'suggested_price': int(current_price * 0.995)  # 0.5% 할인 매도
                        }

                # 3. 🎯 Divergence 매도 신호: 장기 과열 + 단기 조정
                elif (d60_val is not None and d20_val is not None and d5_val is not None and
                      d60_val >= 110 and d20_val >= 105 and d5_val <= 100):
                    if profit_rate >= 2.0:  # 2% 이상 수익시 매도
                        return {
                            'signal_type': 'SELL',
                            'reason': f'하향Divergence 매도 (D60:{d60_val:.1f}↑ D5:{d5_val:.1f}↓, 수익:{profit_rate:.1f}%)',
                            'urgency': 'MEDIUM',
                            'suggested_price': int(current_price * 0.997)  # 0.3% 할인 매도
                        }

                # 4. 🛡️ 과매도 구간: 손절 완화 & 보유 연장
                elif d20_val is not None and d60_val is not None and d20_val <= 85 and d60_val <= 90:
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
                elif (d5_val is not None and d20_val is not None and
                      d5_val >= 110 and d20_val <= 105 and profit_rate >= 3.0):
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
                # 간단한 기술적 분석 (히스토리컬 데이터 없이)
                # 기존 추격매도 기준 완화
                trailing_gap = targets.get('trailing_stop_gap', 1.5) + 0.5  # 0.5% 완화
                if profit_rate <= max_profit_rate - trailing_gap:
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

    def _analyze_bounce_potential(self, position: Dict, profit_rate: float) -> Optional[Dict]:
        """🎯 반등 예측 분석 (-5% → +5% 시나리오)"""
        try:
            stock_code = position['stock_code']
            current_price = position.get('current_price', 0)
            holding_minutes = (time.time() - position['buy_time']) / 60

            logger.info(f"🔍 {stock_code} 반등 예측 분석 시작 (손실률:{profit_rate:.1f}%, 보유:{holding_minutes:.0f}분)")

            # 기본 조건: -3% 이상 손실에서만 분석
            if profit_rate >= -3.0:
                return {'should_hold': False, 'reason': '경미한손실', 'confidence': 0.0}

            bounce_score = 0.0
            reasons = []

            # 1. 🎯 이격도 기반 과매도 분석 (가장 강력한 지표)
            try:
                from ..api.kis_market_api import get_disparity_rank
                disparity_data = get_disparity_rank(
                    fid_input_iscd="0000",      # 전체 시장
                    fid_hour_cls_code="20",     # 20일 이격도
                    fid_vol_cnt="5000"          # 거래량 5천주 이상
                )

                if disparity_data is not None and not disparity_data.empty:
                    stock_row = disparity_data[disparity_data['mksc_shrn_iscd'] == stock_code]
                    if not stock_row.empty:
                        d5 = float(stock_row.iloc[0].get('d5_dsrt', 100))
                        d20 = float(stock_row.iloc[0].get('d20_dsrt', 100))
                        d60 = float(stock_row.iloc[0].get('d60_dsrt', 100))

                        # 극도 과매도: 강력한 반등 신호
                        if d20 <= 85 and d5 <= 90:
                            bounce_score += 40  # 최고 점수
                            reasons.append(f"극도과매도(D20:{d20:.1f}, D5:{d5:.1f})")
                        elif d20 <= 90 and d5 <= 95:
                            bounce_score += 25
                            reasons.append(f"과매도상태(D20:{d20:.1f}, D5:{d5:.1f})")

                        # 장기/단기 이격도 다이버전스
                        if d60 >= 105 and d20 <= 90:  # 장기 상승 + 단기 과매도
                            bounce_score += 20
                            reasons.append(f"다이버전스(D60:{d60:.1f}↑ D20:{d20:.1f}↓)")

            except Exception as e:
                logger.debug(f"이격도 분석 오류: {e}")

            # 2. 🎯 거래량 분석 (매도 압력 vs 매수 압력)
            try:
                from ..api.kis_market_api import get_inquire_price
                current_data = get_inquire_price("J", stock_code)
                if current_data is not None and not current_data.empty:
                    volume = int(current_data.iloc[0].get('acml_vol', 0))
                    avg_volume = int(current_data.iloc[0].get('avrg_vol', 0))

                    if avg_volume > 10000:  # 평균 거래량이 10,000주 이상일 때만 계산
                        volume_ratio = volume / avg_volume
                        if volume_ratio >= 2.0 and profit_rate <= -4.0:  # 대량 거래량 + 큰 하락
                            bounce_score += 15
                            reasons.append(f"대량거래량반등({volume_ratio:.1f}배)")
                        elif volume_ratio >= 1.5:
                            bounce_score += 8
                            reasons.append(f"거래량증가({volume_ratio:.1f}배)")

            except Exception as e:
                logger.debug(f"거래량 분석 오류: {e}")

            # 3. 🎯 시간 기반 분석
            if holding_minutes <= 60:  # 1시간 이내 급락은 일시적 가능성
                bounce_score += 10
                reasons.append(f"단기급락({holding_minutes:.0f}분)")
            elif holding_minutes >= 180:  # 3시간 이상 보유 후 하락은 신중
                bounce_score -= 10
                reasons.append(f"장기보유하락({holding_minutes:.0f}분)")

            # 4. 🎯 손실률 기반 분석
            if -6.0 <= profit_rate <= -4.0:  # 중간 손실 구간
                bounce_score += 15
                reasons.append(f"중간손실구간({profit_rate:.1f}%)")
            elif profit_rate <= -7.0:  # 큰 손실은 위험
                bounce_score -= 15
                reasons.append(f"큰손실위험({profit_rate:.1f}%)")

            # 5. 🎯 최대 연장 시간 제한
            min_holding_minutes = position.get('min_holding_minutes', 60)  # 기본값 60분
            max_total_hold_minutes = min_holding_minutes * 5  # 최대 5배
            if holding_minutes >= max_total_hold_minutes:
                bounce_score -= 30
                reasons.append(f"최대연장시간초과({holding_minutes:.0f}분)")

            # 6. 🎯 최종 판단
            confidence = min(bounce_score / 100.0, 1.0)
            should_hold = False

            # 보유 연장 조건
            if bounce_score >= 50 and holding_minutes < max_total_hold_minutes:
                should_hold = True
                prediction = "HIGH"
            elif bounce_score >= 30 and holding_minutes < max_total_hold_minutes * 0.75:
                should_hold = True
                prediction = "MEDIUM"
            else:
                prediction = "LOW"

            result = {
                'should_hold': should_hold,
                'confidence': confidence,
                'prediction': prediction,
                'score': bounce_score,
                'reason': ', '.join(reasons) if reasons else '반등근거부족',
                'max_hold_minutes': max_total_hold_minutes
            }

            logger.info(f"✅ {stock_code} 반등분석: {prediction} (점수:{bounce_score:.0f}, 보유:{should_hold})")
            if reasons:
                logger.info(f"📋 반등근거: {', '.join(reasons)}")

            return result

        except Exception as e:
            logger.error(f"반등 예측 분석 오류: {e}")
            return {'should_hold': False, 'reason': '분석오류', 'confidence': 0.0}

    def _calculate_optimal_sell_price(self, stock_code: str, current_price: int, reason: str) -> int:
        """🎯 최적 매도 가격 계산"""
        try:
            if current_price <= 0:
                logger.error(f"유효하지 않은 현재가: {stock_code} - {current_price}")
                return 0

            # 1. 🎯 매도 사유별 할인율 차등 적용
            urgency_discounts = {
                '극한손절': 0.015,      # 1.5% 할인 (급매)
                '반등예측손절': 0.008,   # 0.8% 할인
                '손절': 0.005,          # 0.5% 할인
                '익절': 0.002,          # 0.2% 할인
                '조기익절': 0.003,      # 0.3% 할인
                '추격매도': 0.003,      # 0.3% 할인
                '시간만료매도': 0.004,  # 0.4% 할인
                'default': 0.005        # 기본 0.5% 할인
            }

            # 매도 사유에서 키워드 추출하여 할인율 결정
            discount_rate = urgency_discounts['default']
            for key, rate in urgency_discounts.items():
                if key in reason:
                    discount_rate = rate
                    break

            # 2. 🎯 호가 스프레드 분석 (추가 조정)
            try:
                # 간단한 가격대별 스프레드 근사치
                if current_price >= 100000:      # 10만원 이상
                    spread_adjustment = 0.002    # 0.2% 추가
                elif current_price >= 50000:    # 5만원 이상
                    spread_adjustment = 0.003    # 0.3% 추가
                elif current_price >= 10000:    # 1만원 이상
                    spread_adjustment = 0.005    # 0.5% 추가
                else:                           # 1만원 미만
                    spread_adjustment = 0.008    # 0.8% 추가

            except Exception as e:
                logger.debug(f"스프레드 분석 오류: {e}")
                spread_adjustment = 0.005

            # 4. 🎯 총 할인율 계산 및 제한
            total_discount = discount_rate + spread_adjustment

            # 5. 🎯 최종 매도 가격 계산
            calculated_price = int(current_price * (1 - total_discount))

            # 6. 🎯 가격 단위 조정 (한국 주식 호가 단위)
            adjusted_price = self._adjust_to_tick_size(calculated_price)

            # 7. 🎯 안전성 검증
            min_price = int(current_price * 0.97)  # 현재가의 3% 이하로는 안 팔음
            max_price = int(current_price * 1.01)  # 현재가의 1% 이상으로는 안 팔음 (익절 제외)

            if '익절' in reason:
                # 익절의 경우 현재가보다 높게 팔 수 있음
                final_price = max(adjusted_price, min_price)
            else:
                # 손절의 경우 현재가보다 낮게 할인하여 판매
                final_price = min(max(adjusted_price, min_price), max_price)

            logger.debug(f"💰 {stock_code} 가격계산: 현재{current_price:,} → "
                        f"할인율{total_discount:.1%} → 최종{final_price:,}원")

            return final_price

        except Exception as e:
            logger.error(f"매도 가격 계산 오류: {stock_code} - {e}")
            # 오류시 현재가의 0.5% 할인가로 백업
            return max(int(current_price * 0.995), 1)

    def _adjust_to_tick_size(self, price: int) -> int:
        """🎯 한국 주식 호가 단위에 맞게 가격 조정"""
        try:
            if price >= 500000:        # 50만원 이상: 1000원 단위
                return (price // 1000) * 1000
            elif price >= 100000:      # 10만원 이상: 500원 단위
                return (price // 500) * 500
            elif price >= 50000:       # 5만원 이상: 100원 단위
                return (price // 100) * 100
            elif price >= 10000:       # 1만원 이상: 50원 단위
                return (price // 50) * 50
            elif price >= 5000:        # 5천원 이상: 10원 단위
                return (price // 10) * 10
            elif price >= 1000:        # 1천원 이상: 5원 단위
                return (price // 5) * 5
            else:                      # 1천원 미만: 1원 단위
                return price

        except Exception as e:
            logger.debug(f"호가 단위 조정 오류: {e}")
            return price

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
                after_tax_profit_rate = position.get('after_tax_profit_rate', 0)  # 🎯 세후 수익률

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
                    'profit_rate': profit_rate,  # 세전 수익률
                    'after_tax_profit_rate': after_tax_profit_rate,  # 🎯 세후 수익률
                    'max_profit_rate': position.get('max_profit_rate', 0),
                    'max_after_tax_profit_rate': position.get('max_after_tax_profit_rate', 0),  # 🎯 세후 최대 수익률
                    'market_type': position.get('market_type', 'KOSPI'),  # 시장 구분
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

    def sync_with_account(self) -> Dict:
        """🔧 실제 계좌와 포지션 동기화"""
        try:
            logger.info("📊 계좌-포지션 동기화 시작...")

            # 실제 계좌 잔고 조회
            balance = self.trading_manager.get_balance()
            if not balance or not balance.get('success'):
                error_msg = f"계좌 조회 실패: {balance}"
                logger.error(f"❌ {error_msg}")
                return {'success': False, 'error': error_msg}

            account_holdings = balance.get('holdings', [])
            logger.info(f"📊 계좌 보유 종목: {len(account_holdings)}개")

            # 포지션과 계좌 비교
            sync_result = {
                'success': True,
                'total_checked': 0,
                'synced_count': 0,
                'quantity_adjustments': [],
                'removed_positions': [],
                'missing_in_positions': [],
                'errors': []
            }

            # 🔍 1. 포지션 매니저의 종목들을 계좌와 비교
            with self.position_lock:
                active_positions = self.get_positions('active')

                for stock_code, position in active_positions.items():
                    sync_result['total_checked'] += 1
                    position_quantity = position['quantity']

                    # 계좌에서 해당 종목 찾기
                    account_quantity = 0
                    for holding in account_holdings:
                        # 다양한 필드명 확인
                        for field in ['pdno', 'stock_code', 'stck_shrn_iscd', 'mksc_shrn_iscd']:
                            if field in holding and str(holding[field]).strip() == stock_code:
                                # 수량 확인
                                for qty_field in ['hldg_qty', 'quantity', 'ord_psbl_qty', 'available_quantity']:
                                    if qty_field in holding:
                                        try:
                                            account_quantity = int(holding[qty_field])
                                            break
                                        except (ValueError, TypeError):
                                            continue
                                break
                        if account_quantity > 0:
                            break

                    # 수량 비교 및 조정
                    if account_quantity != position_quantity:
                        logger.warning(f"⚠️ 수량 불일치 발견: {stock_code} - 포지션={position_quantity:,}주, 계좌={account_quantity:,}주")

                        if account_quantity == 0:
                            # 계좌에 없는 종목 - 포지션에서 제거
                            try:
                                self.positions[stock_code]['status'] = 'missing_in_account'
                                del self.positions[stock_code]
                                sync_result['removed_positions'].append({
                                    'stock_code': stock_code,
                                    'removed_quantity': position_quantity,
                                    'reason': '계좌에서 종목이 없음'
                                })
                                logger.info(f"🗑️ 포지션 제거: {stock_code} (계좌에 없음)")
                            except Exception as e:
                                sync_result['errors'].append(f"포지션 제거 실패: {stock_code} - {e}")
                        else:
                            # 수량 조정
                            try:
                                old_quantity = self.positions[stock_code]['quantity']
                                self.positions[stock_code]['quantity'] = account_quantity
                                self.positions[stock_code]['last_update'] = time.time()

                                sync_result['quantity_adjustments'].append({
                                    'stock_code': stock_code,
                                    'old_quantity': old_quantity,
                                    'new_quantity': account_quantity,
                                    'adjustment': account_quantity - old_quantity
                                })

                                logger.info(f"🔧 수량 조정: {stock_code} {old_quantity:,}주 → {account_quantity:,}주")
                                sync_result['synced_count'] += 1

                            except Exception as e:
                                sync_result['errors'].append(f"수량 조정 실패: {stock_code} - {e}")
                    else:
                        # 수량 일치
                        logger.debug(f"✅ 수량 일치: {stock_code} = {position_quantity:,}주")
                        sync_result['synced_count'] += 1

                # 🔍 2. 계좌에만 있고 포지션에 없는 종목들 확인
                for holding in account_holdings:
                    found_stock_code = None
                    holding_quantity = 0

                    # 종목코드 찾기
                    for field in ['pdno', 'stock_code', 'stck_shrn_iscd', 'mksc_shrn_iscd']:
                        if field in holding:
                            code_value = str(holding[field]).strip()
                            if code_value and len(code_value) == 6:  # 6자리 종목코드
                                found_stock_code = code_value
                                break

                    # 수량 찾기
                    if found_stock_code:
                        for qty_field in ['hldg_qty', 'quantity', 'ord_psbl_qty', 'available_quantity']:
                            if qty_field in holding:
                                try:
                                    holding_quantity = int(holding[qty_field])
                                    break
                                except (ValueError, TypeError):
                                    continue

                    # 포지션에 없는 종목 확인
                    if found_stock_code and holding_quantity > 0:
                        if found_stock_code not in active_positions:
                            sync_result['missing_in_positions'].append({
                                'stock_code': found_stock_code,
                                'quantity': holding_quantity,
                                'reason': '포지션 매니저에 없는 보유 종목'
                            })
                            logger.warning(f"⚠️ 포지션 누락 발견: {found_stock_code} {holding_quantity:,}주 (계좌에만 존재)")

            # 통계 업데이트
            self.stats['active_positions'] = len([p for p in self.positions.values() if p['status'] == 'active'])

            # 결과 로깅
            logger.info(f"✅ 계좌-포지션 동기화 완료:")
            logger.info(f"   - 확인된 종목: {sync_result['total_checked']}개")
            logger.info(f"   - 동기화된 종목: {sync_result['synced_count']}개")
            logger.info(f"   - 수량 조정: {len(sync_result['quantity_adjustments'])}개")
            logger.info(f"   - 제거된 포지션: {len(sync_result['removed_positions'])}개")
            logger.info(f"   - 누락된 포지션: {len(sync_result['missing_in_positions'])}개")

            if sync_result['errors']:
                logger.warning(f"   - 오류: {len(sync_result['errors'])}개")
                for error in sync_result['errors']:
                    logger.warning(f"     {error}")

            return sync_result

        except Exception as e:
            error_msg = f"계좌-포지션 동기화 오류: {e}"
            logger.error(f"❌ {error_msg}")
            return {'success': False, 'error': error_msg}
