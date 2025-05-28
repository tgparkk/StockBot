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

        # 손익 설정 (🎯 더 수익성 있고 현실적인 기준으로 개선)
        self.profit_targets = {
            # 기본 전략: 안정적이고 수익성 있는 매도
            'default': {
                'stop_loss': -4.5, 'take_profit': 6.0, 'min_holding_minutes': 45,
                'trailing_stop_trigger': 3.5, 'trailing_stop_gap': 2.0  # 3.5% 수익 후 2% 하락시 매도
            },
            
            # 기존 보유 종목: 더 여유있는 관리
            'existing_holding': {
                'stop_loss': -5.5, 'take_profit': 8.0, 'min_holding_minutes': 90,
                'trailing_stop_trigger': 4.0, 'trailing_stop_gap': 2.5  # 4% 수익 후 2.5% 하락시 매도
            },
            
            # 🆕 이격도 반등: 과매도 반등 기대하며 여유 있게
            'disparity_reversal': {
                'stop_loss': -3.5, 'take_profit': 7.0, 'min_holding_minutes': 60,
                'trailing_stop_trigger': 4.0, 'trailing_stop_gap': 2.0  # 4% 수익 후 2% 하락시 매도
            },
            
            # 갭 거래: 빠른 수익 실현, 하지만 여유 있게
            'gap_trading': {
                'stop_loss': -3.5, 'take_profit': 5.0, 'min_holding_minutes': 30,
                'trailing_stop_trigger': 3.0, 'trailing_stop_gap': 1.8  # 3% 수익 후 1.8% 하락시 매도
            },
            
            # 거래량 돌파: 트렌드 지속 기대
            'volume_breakout': {
                'stop_loss': -4.0, 'take_profit': 7.0, 'min_holding_minutes': 40,
                'trailing_stop_trigger': 4.0, 'trailing_stop_gap': 2.2  # 4% 수익 후 2.2% 하락시 매도
            },
            
            # 모멘텀: 트렌드 최대한 활용
            'momentum': {
                'stop_loss': -3.0, 'take_profit': 8.5, 'min_holding_minutes': 25,
                'trailing_stop_trigger': 5.0, 'trailing_stop_gap': 2.5  # 5% 수익 후 2.5% 하락시 매도
            }
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

    def update_position_prices(self, force_rest_api: bool = False) -> None:
        """포지션별 현재가 및 수익률 업데이트 - 🎯 웹소켓 우선 사용 정책"""
        with self.position_lock:
            active_positions = [code for code, pos in self.positions.items() if pos['status'] == 'active']

            if not active_positions:
                return

            # 🎯 웹소켓 우선 사용 정책
            websocket_available = False
            websocket_retry_needed = False
            
            if hasattr(self.data_collector, 'websocket') and self.data_collector.websocket:
                websocket_available = getattr(self.data_collector.websocket, 'is_connected', False)
                
                # 🎯 웹소켓이 연결되지 않은 경우 재연결 시도
                if not websocket_available and not force_rest_api:
                    logger.info("🔄 웹소켓 연결 끊김 감지 - 재연결 시도 중...")
                    websocket_retry_needed = True
                    
                    # 웹소켓 재연결 시도
                    if hasattr(self.data_collector, 'data_manager') and self.data_collector.data_manager:
                        try:
                            reconnect_success = self._attempt_websocket_reconnection()
                            if reconnect_success:
                                websocket_available = True
                                logger.info("✅ 웹소켓 재연결 성공 - 웹소켓으로 가격 조회")
                            else:
                                logger.warning("⚠️ 웹소켓 재연결 실패 - REST API로 백업")
                        except Exception as e:
                            logger.error(f"웹소켓 재연결 시도 오류: {e}")

            # 데이터 수집 방식 결정
            if force_rest_api or not websocket_available:
                if websocket_retry_needed:
                    logger.info(f"💾 REST API 백업 사용: 웹소켓 재연결{'시도했으나 실패' if websocket_retry_needed else '불가'}")
                else:
                    logger.info(f"💾 REST API 강제 사용: force={force_rest_api}, websocket_connected={websocket_available}")
                
                # REST API 강제 사용 - 캐시 비활성화
                price_data = {}
                for stock_code in active_positions:
                    price_data[stock_code] = self.data_collector.get_fresh_price(stock_code)
                    time.sleep(0.05)  # API 호출 간격
            else:
                # 🎯 웹소켓 우선 사용
                logger.info(f"📡 웹소켓 우선 사용: {len(active_positions)}개 종목 실시간 조회")
                # 배치로 현재가 조회 (캐시 우선, 웹소켓→REST API 자동 백업)
                price_data = self.data_collector.get_multiple_prices(active_positions, use_cache=True)

            # 실패한 종목에 대해서는 REST API 재시도
            failed_stocks = []
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
                        else:
                            failed_stocks.append(stock_code)
                    else:
                        failed_stocks.append(stock_code)
                else:
                    failed_stocks.append(stock_code)

            # 실패한 종목들 REST API로 재시도
            if failed_stocks and not force_rest_api:
                logger.warning(f"가격 조회 실패 종목 {len(failed_stocks)}개 - REST API 재시도: {failed_stocks}")
                for stock_code in failed_stocks:
                    try:
                        fresh_data = self.data_collector.get_fresh_price(stock_code)
                        if fresh_data.get('status') == 'success':
                            current_price = fresh_data.get('current_price', 0)
                            if current_price > 0 and stock_code in self.positions:
                                position = self.positions[stock_code]
                                profit_rate = ((current_price - position['buy_price']) / position['buy_price']) * 100
                                position['max_profit_rate'] = max(position['max_profit_rate'], profit_rate)
                                position['current_price'] = current_price
                                position['profit_rate'] = profit_rate
                                position['last_update'] = time.time()
                                logger.info(f"✅ REST API 백업 성공: {stock_code} {current_price:,}원")
                        time.sleep(0.05)
                    except Exception as e:
                        logger.error(f"REST API 백업 실패: {stock_code} - {e}")

    def _attempt_websocket_reconnection(self) -> bool:
        """🎯 웹소켓 재연결 시도"""
        try:
            # 데이터 매니저를 통한 웹소켓 재연결
            if hasattr(self.data_collector, 'data_manager'):
                data_manager = self.data_collector.data_manager
                
                # 웹소켓 매니저 확인
                if hasattr(data_manager, 'websocket_manager') and data_manager.websocket_manager:
                    websocket_manager = data_manager.websocket_manager
                    
                    # 현재 연결 상태 확인
                    is_connected = getattr(websocket_manager, 'is_connected', False)
                    
                    if not is_connected:
                        logger.info("🔄 웹소켓 재연결 시도...")
                        
                        # 웹소켓 재시작 시도
                        if hasattr(data_manager, '_start_websocket_if_needed'):
                            success = data_manager._start_websocket_if_needed()
                            if success:
                                logger.info("✅ 웹소켓 재연결 성공")
                                return True
                            else:
                                logger.warning("⚠️ 웹소켓 재연결 실패")
                                return False
                        else:
                            logger.warning("웹소켓 재시작 메서드 없음")
                            return False
                    else:
                        logger.debug("웹소켓이 이미 연결되어 있음")
                        return True
                else:
                    logger.warning("웹소켓 매니저 없음")
                    return False
            else:
                logger.warning("데이터 매니저 없음")
                return False
                
        except Exception as e:
            logger.error(f"웹소켓 재연결 시도 중 오류: {e}")
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
        """매도 조건 확인 - 🎯 개선된 수익성 중심 로직"""
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
                buy_time = position.get('buy_time', time.time())

                # 전략별 손익 기준
                targets = self.profit_targets.get(strategy_type, self.profit_targets['default'])
                stop_loss = targets['stop_loss']
                take_profit = targets['take_profit']
                min_holding_minutes = targets.get('min_holding_minutes', 45)
                trailing_stop_trigger = targets.get('trailing_stop_trigger', 3.5)
                trailing_stop_gap = targets.get('trailing_stop_gap', 2.0)

                # 홀딩 시간 확인 (분 단위)
                holding_minutes = (time.time() - buy_time) / 60

                # 🎯 개선된 매도 조건 확인
                sell_reason = None

                # 1. 💥 극심한 손실 시 즉시 손절 (홀딩시간 무관)
                if profit_rate <= stop_loss - 3.0:  # 기준보다 3% 더 하락
                    sell_reason = f"긴급손절 ({profit_rate:.2f}%)"
                
                # 2. ⏰ 최소 홀딩 시간 후 매도 조건
                elif holding_minutes >= min_holding_minutes:
                    # 2-1. 손절 조건
                    if profit_rate <= stop_loss:
                        sell_reason = f"손절 ({profit_rate:.2f}%, {holding_minutes:.0f}분)"
                    
                    # 2-2. 고수익 익절 조건 (기준보다 높을 때만)
                    elif profit_rate >= take_profit:
                        sell_reason = f"익절 ({profit_rate:.2f}%, {holding_minutes:.0f}분)"
                    
                    # 2-3. 🎯 개선된 트레일링 스톱 (더 관대하게)
                    elif max_profit_rate >= trailing_stop_trigger and profit_rate <= max_profit_rate - trailing_stop_gap:
                        sell_reason = f"추격매도 (최고 {max_profit_rate:.2f}% → {profit_rate:.2f}%, {holding_minutes:.0f}분)"
                
                # 3. 📈 조기 익절 조건 (매우 높은 수익시만)
                elif holding_minutes < min_holding_minutes and profit_rate >= take_profit + 2.0:  # 익절 기준보다 2% 더 수익
                    sell_reason = f"조기익절 ({profit_rate:.2f}%, {holding_minutes:.0f}분)"
                
                # 4. ⚡ 장마감 30분 전 강제 매도 방지 (당일매매 아니므로 제거)
                # 현재는 스윙 트레이딩이므로 장마감 강제 매도 없음

                if sell_reason:
                    sell_signals.append({
                        'stock_code': stock_code,
                        'quantity': position['quantity'],
                        'current_price': current_price,
                        'profit_rate': profit_rate,
                        'max_profit_rate': max_profit_rate,
                        'holding_minutes': holding_minutes,
                        'reason': sell_reason,
                        'strategy_type': strategy_type
                    })
                    
                    logger.info(f"🚨 매도 신호 생성: {stock_code} - {sell_reason} (최고수익: {max_profit_rate:.2f}%)")

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

    def check_sell_signals(self) -> List[Dict]:
        """매도 신호 확인"""
        sell_signals = []
        
        with self.position_lock:
            for position in list(self.positions.values()):
                if position['status'] != 'active':
                    continue
                
                # 🆕 이격도 기반 매도 신호 우선 확인
                disparity_signal = self._check_disparity_sell_signal(position)
                if disparity_signal:
                    sell_signals.append({
                        'stock_code': position['stock_code'],
                        'signal': disparity_signal,
                        'position': position
                    })
                    continue  # 이격도 신호가 있으면 다른 신호 체크 생략
                
                # 기존 매도 신호 체크
                try:
                    # 손익 기반 매도 신호
                    profit_signal = self._check_profit_loss_signal(position)
                    if profit_signal:
                        sell_signals.append({
                            'stock_code': position['stock_code'],
                            'signal': profit_signal,
                            'position': position
                        })
                        continue
                    
                    # 시간 기반 매도 신호
                    time_signal = self._check_time_based_signal(position)
                    if time_signal:
                        sell_signals.append({
                            'stock_code': position['stock_code'],
                            'signal': time_signal,
                            'position': position
                        })
                        continue
                    
                    # 트레일링 스톱 신호
                    trailing_signal = self._check_trailing_stop_signal(position)
                    if trailing_signal:
                        sell_signals.append({
                            'stock_code': position['stock_code'],
                            'signal': trailing_signal,
                            'position': position
                        })
                
                except Exception as e:
                    logger.error(f"매도 신호 확인 오류 ({position['stock_code']}): {e}")
        
        return sell_signals

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
