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

        # 손익 설정 (설정 파일에서 로드해야 함)
        self.profit_targets = {
            'default': {'stop_loss': -3.0, 'take_profit': 5.0},
            'gap_trading': {'stop_loss': -2.0, 'take_profit': 3.0},
            'volume_breakout': {'stop_loss': -2.5, 'take_profit': 4.0}
        }

        # 통계
        self.stats = {
            'total_positions': 0,
            'active_positions': 0,
            'profitable_positions': 0,
            'loss_positions': 0,
            'total_profit_loss': 0.0
        }

        logger.info("포지션 관리자 초기화 완료")

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
                    profit_loss = (sell_price - position['buy_price']) * position['quantity']
                    profit_rate = ((sell_price - position['buy_price']) / position['buy_price']) * 100

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
                    profit_loss = (sell_price - position['buy_price']) * quantity
                    position['quantity'] -= quantity

                    self.stats['total_profit_loss'] += profit_loss

                    logger.info(f"✅ 포지션 부분 매도: {stock_code} {quantity}주 (잔여 {position['quantity']}주)")

                self.stats['active_positions'] = len([p for p in self.positions.values() if p['status'] == 'active'])
                return True

            except Exception as e:
                logger.error(f"포지션 제거 오류: {stock_code} - {e}")
                return False

    def update_position_prices(self) -> None:
        """포지션별 현재가 및 수익률 업데이트"""
        with self.position_lock:
            active_positions = [code for code, pos in self.positions.items() if pos['status'] == 'active']

            if not active_positions:
                return

            # 배치로 현재가 조회 (캐시 우선)
            price_data = self.data_collector.get_multiple_prices(active_positions, use_cache=True)

            for stock_code, position in self.positions.items():
                if position['status'] != 'active':
                    continue

                if stock_code in price_data:
                    price_info = price_data[stock_code]
                    if price_info.get('status') == 'success':
                        current_price = price_info.get('current_price', 0)
                        if current_price > 0:
                            # 수익률 계산
                            profit_rate = ((current_price - position['buy_price']) / position['buy_price']) * 100

                            # 최대 수익률 업데이트
                            position['max_profit_rate'] = max(position['max_profit_rate'], profit_rate)
                            position['current_price'] = current_price
                            position['profit_rate'] = profit_rate
                            position['last_update'] = time.time()

    def check_exit_conditions(self) -> List[Dict]:
        """매도 조건 확인"""
        sell_signals = []

        with self.position_lock:
            for stock_code, position in self.positions.items():
                if position['status'] != 'active':
                    continue

                current_price = position.get('current_price')
                if not current_price:
                    continue

                profit_rate = position.get('profit_rate', 0)
                max_profit_rate = position.get('max_profit_rate', 0)
                strategy_type = position.get('strategy_type', 'default')

                # 전략별 손익 기준
                targets = self.profit_targets.get(strategy_type, self.profit_targets['default'])
                stop_loss = targets['stop_loss']
                take_profit = targets['take_profit']

                # 매도 조건 확인
                sell_reason = None

                if profit_rate <= stop_loss:
                    sell_reason = f"손절 ({profit_rate:.2f}%)"
                elif profit_rate >= take_profit:
                    sell_reason = f"익절 ({profit_rate:.2f}%)"
                elif max_profit_rate >= 2.0 and profit_rate <= max_profit_rate - 1.5:
                    sell_reason = f"추격매도 (최고 {max_profit_rate:.2f}% → {profit_rate:.2f}%)"

                if sell_reason:
                    sell_signals.append({
                        'stock_code': stock_code,
                        'quantity': position['quantity'],
                        'current_price': current_price,
                        'profit_rate': profit_rate,
                        'reason': sell_reason,
                        'strategy_type': strategy_type
                    })

        return sell_signals

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
