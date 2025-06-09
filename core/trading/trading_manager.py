"""
거래 관리자 - 주문 실행 및 포지션 관리
"""
import time
from typing import Dict, List, Optional, Any
from utils.logger import setup_logger
from ..api.rest_api_manager import KISRestAPIManager
from ..data.kis_data_collector import KISDataCollector
from ..websocket.kis_websocket_manager import KISWebSocketManager
import pytz
from datetime import datetime

logger = setup_logger(__name__)


class TradingManager:
    """간소화된 주문 실행 관리자"""

    def __init__(self, websocket_manager: KISWebSocketManager, rest_api_manager: KISRestAPIManager, data_collector: KISDataCollector):
        """초기화"""
        self.rest_api = rest_api_manager
        self.data_collector = data_collector

        # 주문 추적
        self.pending_orders: Dict[str, Dict] = {}  # {order_no: order_info}
        self.order_history: List[Dict] = []

        # 통계
        self.stats = {
            'total_orders': 0,
            'successful_orders': 0,
            'failed_orders': 0,
            'buy_orders': 0,
            'sell_orders': 0
        }

    def execute_order(self, stock_code: str, order_type: str, quantity: int,
                     price: int = 0, strategy_type: str = "manual") -> Optional[str]:
        """주문 실행 (간소화 버전)"""
        self.stats['total_orders'] += 1

        try:
            # 장시간 체크
            market_status = self._check_market_status()
            if not market_status.get('is_trading_time', False):
                logger.warning(f"⚠️ 장외시간 주문 취소: {stock_code} {order_type} - "
                             f"현재 상태: {market_status.get('status', '확인불가')} "
                             f"({market_status.get('current_time', 'N/A')})")
                self.stats['failed_orders'] += 1
                return None

            # 1. 현재가 확인 (시장가 주문시)
            if price == 0:
                price_data = self.data_collector.get_current_price(stock_code, use_cache=True)
                if price_data.get('status') != 'success':
                    logger.error(f"현재가 조회 실패: {stock_code}")
                    self.stats['failed_orders'] += 1
                    return None

                # 시장가는 현재가 기준으로 설정
                current_price = price_data.get('current_price', 0)
                if order_type.upper() == "BUY":
                    price = int(current_price * 1.002)  # 0.2% 위에서 매수
                else:
                    price = int(current_price * 0.998)  # 0.2% 아래에서 매도

            # 2. 주문 실행
            if order_type.upper() == "BUY":
                result = self.rest_api.buy_order(stock_code, quantity, price)
                self.stats['buy_orders'] += 1
            else:
                result = self.rest_api.sell_order(stock_code, quantity, price)
                self.stats['sell_orders'] += 1

            # 3. 결과 처리 - rest_api는 status 필드를 사용
            if result and result.get('status') == 'success':
                # 🔧 order_no 검증 및 폴백 처리
                order_no = result.get('order_no', '')
                if not order_no or order_no.strip() == '':
                    # 빈 문자열이면 타임스탬프 기반 주문번호 생성
                    order_no = f"order_{int(time.time() * 1000)}"  # 밀리초 포함
                    logger.warning(f"⚠️ {stock_code} API에서 주문번호 누락 - 임시번호 생성: {order_no}")

                # 주문 정보 저장
                order_info = {
                    'order_no': order_no,
                    'stock_code': stock_code,
                    'order_type': order_type,
                    'quantity': quantity,
                    'price': price,
                    'strategy_type': strategy_type,
                    'order_time': time.time(),
                    'status': 'pending'
                }

                self.pending_orders[order_no] = order_info
                self.order_history.append(order_info.copy())

                self.stats['successful_orders'] += 1
                logger.info(f"✅ 주문 성공: {stock_code} {order_type} {quantity}주 {price:,}원 → {order_no}")

                return order_no
            else:
                error_msg = result.get('message', '알 수 없는 오류') if result else '응답 없음'
                error_code = result.get('error_code', 'UNKNOWN') if result else 'NO_RESPONSE'
                
                # 🔧 구체적인 오류 정보 구성
                detailed_error = f"{error_code}: {error_msg}"
                logger.error(f"❌ 주문 실패: {stock_code} {order_type} - {detailed_error}")
                
                self.stats['failed_orders'] += 1
                # 🆕 오류 정보를 포함한 딕셔너리 반환 (None 대신)
                return {
                    'success': False,
                    'error_code': error_code,
                    'error_message': error_msg,
                    'detailed_error': detailed_error
                }

        except Exception as e:
            logger.error(f"주문 실행 오류: {stock_code} {order_type} - {e}")
            self.stats['failed_orders'] += 1
            # 🆕 예외 정보를 포함한 딕셔너리 반환
            return {
                'success': False,
                'error_code': 'EXCEPTION',
                'error_message': str(e),
                'detailed_error': f"EXCEPTION: {str(e)}"
            }

    def cancel_order(self, order_no: str) -> bool:
        """주문 취소"""
        try:
            if order_no not in self.pending_orders:
                logger.warning(f"취소할 주문이 없음: {order_no}")
                return False

            order_info = self.pending_orders[order_no]
            stock_code = order_info['stock_code']
            quantity = order_info['quantity']

            # REST API로 취소 (메서드가 없으면 스킵)
            try:
                result = self.rest_api.cancel_order(order_no, stock_code, quantity)

                if result and result.get('success'):
                    # 주문 상태 업데이트
                    order_info['status'] = 'cancelled'
                    del self.pending_orders[order_no]

                    logger.info(f"✅ 주문 취소 성공: {order_no}")
                    return True
                else:
                    error_msg = result.get('message', '취소 실패') if result else '응답 없음'
                    logger.error(f"❌ 주문 취소 실패: {order_no} - {error_msg}")
                    return False

            except AttributeError:
                logger.warning(f"cancel_order 메서드가 없음 - 수동 취소 필요: {order_no}")
                return False

        except Exception as e:
            logger.error(f"주문 취소 오류: {order_no} - {e}")
            return False

    def get_order_status(self, order_no: str) -> Optional[str]:
        """주문 상태 조회"""
        try:
            # REST API로 조회 (메서드가 없으면 스킵)
            try:
                result = self.rest_api.get_order_status(order_no)

                if result and result.get('success'):
                    status = result.get('status', 'unknown')

                    # 주문 정보 업데이트
                    if order_no in self.pending_orders:
                        self.pending_orders[order_no]['status'] = status

                        # 체결 완료시 pending에서 제거
                        if status in ['완전체결', '취소']:
                            del self.pending_orders[order_no]

                    return status
                else:
                    logger.warning(f"주문 상태 조회 실패: {order_no}")
                    return None

            except AttributeError:
                logger.warning(f"get_order_status 메서드가 없음: {order_no}")
                return 'unknown'

        except Exception as e:
            logger.error(f"주문 상태 조회 오류: {order_no} - {e}")
            return None

    def get_balance(self) -> Dict:
        """계좌 잔고 조회"""
        try:
            balance = self.rest_api.get_balance()
            if balance and balance.get('status') == 'success':
                summary = balance.get('summary', {})
                holdings = balance.get('holdings', [])

                # KIS API 응답 구조에 맞게 매핑
                total_evaluation = int(summary.get('tot_evlu_amt', 0))        # 총평가금액
                available_cash = int(summary.get('prvs_rcdl_excc_amt', 0))        # 익일정산금액(가용현금)
                stock_evaluation = int(summary.get('scts_evlu_amt', 0))      # 유가증권평가금액
                profit_loss = int(summary.get('evlu_pfls_smtl_amt', 0))      # 평가손익합계금액

                #cash_balance = int(summary.get('dnca_tot_amt', 0))

                logger.debug(f"잔고 조회 결과: 총평가={total_evaluation:,}원, 가용현금={available_cash:,}원, 주식평가={stock_evaluation:,}원")

                return {
                    'success': True,
                    'total_assets': total_evaluation,
                    'available_cash': available_cash,
                    'stock_evaluation': stock_evaluation,
                    'profit_loss': profit_loss,
                    'holdings': holdings
                }
            else:
                logger.error(f"잔고 조회 실패: {balance}")
                return {'success': False, 'message': '잔고 조회 실패'}

        except Exception as e:
            logger.error(f"잔고 조회 오류: {e}")
            return {'success': False, 'message': f'오류: {e}'}

    def calculate_order_size(self, stock_code: str, order_type: str,
                           available_cash: int, risk_percent: float = 2.0) -> Dict:
        """주문 수량 계산"""
        try:
            # 현재가 조회
            price_data = self.data_collector.get_current_price(stock_code, use_cache=True)
            if price_data.get('status') != 'success':
                return {'success': False, 'message': '현재가 조회 실패'}

            current_price = price_data.get('current_price', 0)
            if current_price <= 0:
                return {'success': False, 'message': '현재가 정보 없음'}

            if order_type.upper() == "BUY":
                # 매수: 리스크 비율 기반 계산
                risk_amount = int(available_cash * (risk_percent / 100))
                max_quantity = risk_amount // current_price

                return {
                    'success': True,
                    'quantity': max_quantity,
                    'estimated_price': current_price,
                    'estimated_amount': max_quantity * current_price,
                    'risk_amount': risk_amount
                }
            else:
                # 매도: 보유 수량 기준
                balance = self.get_balance()
                if not balance.get('success'):
                    return {'success': False, 'message': '잔고 조회 실패'}

                holdings = balance.get('holdings', [])
                for holding in holdings:
                    if holding.get('stock_code') == stock_code:
                        available_qty = holding.get('available_quantity', 0)
                        return {
                            'success': True,
                            'quantity': available_qty,
                            'estimated_price': current_price,
                            'estimated_amount': available_qty * current_price
                        }

                return {'success': False, 'message': '보유 종목 없음'}

        except Exception as e:
            logger.error(f"주문 수량 계산 오류: {stock_code} - {e}")
            return {'success': False, 'message': f'오류: {e}'}

    def get_pending_orders(self) -> Dict[str, Dict]:
        """대기 중인 주문 목록"""
        return self.pending_orders.copy()

    def get_order_history(self, limit: int = 100) -> List[Dict]:
        """주문 이력"""
        return self.order_history[-limit:] if limit > 0 else self.order_history

    def get_stats(self) -> Dict:
        """거래 통계"""
        success_rate = (
            (self.stats['successful_orders'] / self.stats['total_orders'] * 100)
            if self.stats['total_orders'] > 0 else 0
        )

        return {
            **self.stats.copy(),
            'success_rate': round(success_rate, 2),
            'pending_orders_count': len(self.pending_orders),
            'order_history_count': len(self.order_history)
        }

    def _check_market_status(self) -> dict:
        """시장 상태 확인"""
        try:
            # 한국 시간대 사용
            kst = pytz.timezone('Asia/Seoul')
            now = datetime.now(kst)

            # 평일 체크 (월~금: 0~4)
            is_weekday = now.weekday() < 5

            # 시장 시간 체크 (9:00~15:30)
            current_time = now.time()
            market_open = datetime.strptime("09:00", "%H:%M").time()
            market_close = datetime.strptime("15:30", "%H:%M").time()
            is_market_hours = market_open <= current_time <= market_close

            # 프리마켓 시간 체크 (8:30~9:00)
            premarket_open = datetime.strptime("08:30", "%H:%M").time()
            is_premarket = premarket_open <= current_time < market_open

            # 전체 거래 가능 시간 (프리마켓 + 정규장)
            is_trading_time = is_weekday and (is_premarket or is_market_hours)

            status_text = "휴장"
            if is_weekday:
                if is_premarket:
                    status_text = "프리마켓"
                elif is_market_hours:
                    status_text = "정규장"
                else:
                    status_text = "장외시간"
            else:
                status_text = "주말"

            return {
                'is_open': is_market_hours,
                'is_trading_time': is_trading_time,
                'is_premarket': is_premarket,
                'is_weekday': is_weekday,
                'current_time': now.strftime('%H:%M:%S'),
                'current_date': now.strftime('%Y-%m-%d'),
                'status': status_text,
                'kst_time': now
            }
        except Exception as e:
            logger.error(f"시장 상태 확인 오류: {e}")
            return {'is_open': False, 'status': '확인불가', 'is_trading_time': False}

    def cleanup(self):
        """리소스 정리"""
        logger.info("주문 관리자 정리 중...")

        # 대기 중인 주문들 상태 최종 확인
        pending_orders = list(self.pending_orders.keys())
        for order_no in pending_orders:
            try:
                status = self.get_order_status(order_no)
                logger.info(f"최종 주문 상태: {order_no} → {status}")
            except Exception as e:
                logger.error(f"최종 상태 확인 오류: {order_no} - {e}")

        logger.info("주문 관리자 정리 완료")
