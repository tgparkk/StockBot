#!/usr/bin/env python3
"""
웹소켓 NOTICE 기반 주문 실행 결과 처리 관리자
실제 체결 통보를 받아 포지션과 거래 기록을 업데이트
"""
import time
import asyncio
from typing import Dict, List, Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass
from utils.logger import setup_logger


logger = setup_logger(__name__)


@dataclass
class PendingOrder:
    """🔔 대기 중인 주문 - 실전투자 전용"""
    order_id: str
    stock_code: str
    order_type: str  # 'BUY' or 'SELL'
    quantity: int
    price: int
    strategy_type: str
    timestamp: datetime
    timeout_seconds: int = 300  # 5분 타임아웃
    account_no: str = ""        # 계좌번호 (체결통보 검증용)
    
    # 🆕 패턴 정보 추가
    pattern_type: str = ""      # 사용된 패턴 (HAMMER/BULLISH_ENGULFING/BEARISH_ENGULFING)
    pattern_confidence: float = 0.0  # 패턴 신뢰도 (0.0-1.0)
    pattern_strength: int = 0   # 패턴 강도 (0-100)
    
    # 🆕 기술적 지표 정보 추가
    rsi_value: float = None     # RSI 값
    macd_value: float = None    # MACD 값
    volume_ratio: float = None  # 거래량 비율
    
    # 🆕 투자 정보 추가
    investment_amount: int = 0  # 실제 투자금액
    investment_ratio: float = None  # 포트폴리오 대비 투자 비율

    def is_expired(self) -> bool:
        """주문 타임아웃 여부"""
        try:
            if self.timestamp is None or self.timeout_seconds is None:
                return True  # None 값이면 만료된 것으로 처리
            
            elapsed_seconds = (datetime.now() - self.timestamp).total_seconds()
            return elapsed_seconds > self.timeout_seconds
        except Exception as e:
            # 오류 발생시 만료된 것으로 처리
            return True


class OrderExecutionManager:
    """🎯 웹소켓 NOTICE 기반 주문 실행 결과 처리 관리자"""

    def __init__(self, trade_db, async_logger):
        """초기화"""
        self.trade_db = trade_db
        self.async_logger = async_logger

        # 🎯 대기 중인 주문들 (주문ID로 관리)
        self.pending_orders: Dict[str, PendingOrder] = {}

        # 📊 통계
        self.stats = {
            'orders_sent': 0,
            'orders_filled': 0,
            'orders_timeout': 0,
            'orders_error': 0,
            'last_execution_time': None
        }

        # 콜백 함수들
        self.execution_callbacks: List[Callable] = []

        logger.info("✅ 주문 실행 관리자 초기화 완료 (KIS API 직접 사용)")

    def add_pending_order(self, order_id: str, stock_code: str, order_type: str,
                         quantity: int, price: int, strategy_type: str,
                         pattern_type: str = "", pattern_confidence: float = 0.0,
                         pattern_strength: int = 0, rsi_value: float = None,
                         macd_value: float = None, volume_ratio: float = None,
                         investment_amount: int = 0, investment_ratio: float = None) -> bool:
        """대기 중인 주문 추가 - 패턴 정보 포함"""
        try:
            if not order_id:
                logger.error("❌ 주문ID가 없습니다")
                return False

            pending_order = PendingOrder(
                order_id=order_id,
                stock_code=stock_code,
                order_type=order_type,
                quantity=quantity,
                price=price,
                strategy_type=strategy_type,
                timestamp=datetime.now(),
                pattern_type=pattern_type,
                pattern_confidence=pattern_confidence,
                pattern_strength=pattern_strength,
                rsi_value=rsi_value,
                macd_value=macd_value,
                volume_ratio=volume_ratio,
                investment_amount=investment_amount,
                investment_ratio=investment_ratio
            )

            self.pending_orders[order_id] = pending_order
            self.stats['orders_sent'] += 1

            logger.info(f"📝 대기 주문 등록: {order_type} {stock_code} {quantity:,}주 @{price:,}원 "
                       f"패턴:{pattern_type} 신뢰도:{pattern_confidence:.2f} (ID: {order_id})")
            return True

        except Exception as e:
            logger.error(f"❌ 대기 주문 등록 실패: {e}")
            return False

    async def handle_execution_notice(self, notice_data: Dict) -> bool:
        """🔔 웹소켓 NOTICE 체결통보 처리"""
        try:
            # 체결통보 데이터 파싱
            execution_info = self._parse_notice_data(notice_data)
            if not execution_info:
                logger.warning("⚠️ 체결통보 데이터 파싱 실패")
                return False

            order_id = execution_info.get('order_id', '')
            if not order_id:
                logger.warning("⚠️ 체결통보에 주문ID가 없습니다")
                return False

            # 🆕 대기 중인 주문 확인 (임시 주문ID 매칭 개선)
            pending_order = None
            matched_order_id = None

            # 1. 정확한 주문ID 매칭 시도
            if order_id in self.pending_orders:
                pending_order = self.pending_orders[order_id]
                matched_order_id = order_id
                logger.info(f"✅ 정확한 주문ID 매칭: {order_id}")
            else:
                # 2. 임시 주문ID 매칭 시도 (종목코드, 타입, 시간 기준)
                stock_code = execution_info.get('stock_code', '')
                order_type = execution_info.get('order_type', '')
                
                logger.info(f"🔍 임시 주문ID 매칭 시도: 종목={stock_code}, 타입={order_type}")
                
                for temp_order_id, temp_order in self.pending_orders.items():
                    if (temp_order.stock_code == stock_code and 
                        temp_order.order_type == order_type):
                        
                        # 시간 범위 확인 (10분 이내)
                        elapsed_seconds = (datetime.now() - temp_order.timestamp).total_seconds()
                        if elapsed_seconds <= 600:  # 10분 이내
                            pending_order = temp_order
                            matched_order_id = temp_order_id
                            logger.info(f"✅ 임시 주문ID 매칭 성공: {temp_order_id} → {order_id}")
                            logger.info(f"✅ 매칭 조건: 종목={stock_code}, 타입={order_type}, 경과시간={elapsed_seconds:.1f}초")
                            break
                        else:
                            logger.debug(f"⏰ 시간 초과로 매칭 제외: {temp_order_id} ({elapsed_seconds:.1f}초)")

            if not pending_order:
                logger.warning(f"⚠️ 매칭되는 대기 주문 없음: {order_id}")
                logger.warning(f"⚠️ 현재 대기 주문 목록: {list(self.pending_orders.keys())}")
                return False

            # 체결 정보 검증
            if not self._validate_execution(pending_order, execution_info):
                logger.error(f"❌ 체결 정보 검증 실패: {order_id}")
                return False

            # 체결 처리
            success = await self._process_execution(pending_order, execution_info)

            if success:
                # 🆕 대기 목록에서 제거 (매칭된 주문ID 사용)
                if matched_order_id and matched_order_id in self.pending_orders:
                    del self.pending_orders[matched_order_id]
                    logger.info(f"✅ 대기 목록에서 제거: {matched_order_id}")
                
                self.stats['orders_filled'] += 1
                self.stats['last_execution_time'] = datetime.now()

                logger.info(f"✅ 체결 처리 완료: {pending_order.order_type} {pending_order.stock_code}")

                # 콜백 실행
                await self._execute_callbacks(pending_order, execution_info)

            return success

        except Exception as e:
            logger.error(f"❌ 체결통보 처리 오류: {e}")
            return False

    def _parse_notice_data(self, notice_data) -> Optional[Dict]:
        """🔔 체결통보 데이터 파싱 - KIS 공식 문서 기준 (실전투자 전용) - 개선된 버전"""
        try:
            # 🆕 상세 디버깅 로그 추가
            logger.info(f"🔍 체결통보 파싱 시작: 데이터 타입={type(notice_data)}")
            
            # 🚨 입력 데이터 타입 처리 (문자열 또는 딕셔너리)
            if isinstance(notice_data, str):
                # 웹소켓에서 직접 문자열로 전달된 경우
                data = notice_data
                logger.info(f"📋 문자열 데이터 수신: 길이={len(data)}")
            elif isinstance(notice_data, dict):
                # 딕셔너리 구조로 전달된 경우
                logger.info(f"📋 딕셔너리 데이터 수신: 키={list(notice_data.keys())}")
                data = notice_data.get('data', notice_data.get('body', ''))
                if not data:
                    # 🆕 다른 가능한 키들도 확인
                    for key in ['message', 'content', 'payload', 'result']:
                        if key in notice_data:
                            data = notice_data[key]
                            logger.info(f"📋 대체 키에서 데이터 발견: {key}")
                            break
                    
                    if not data:
                        logger.warning(f"⚠️ 체결통보 데이터가 없습니다. 전체 구조: {notice_data}")
                        return None
                logger.info(f"📋 추출된 데이터: 길이={len(str(data))}")
            else:
                logger.error(f"❌ 지원하지 않는 데이터 타입: {type(notice_data)}")
                logger.error(f"❌ 데이터 내용: {str(notice_data)[:200]}...")
                return None

            # 🆕 데이터 형식 확인 및 전처리
            data_str = str(data).strip()
            logger.info(f"📋 파싱할 데이터 (처음 100자): {data_str[:100]}...")

            # 🔒 체결통보 데이터는 암호화되어 전송됨 - 복호화 필요
            if data_str.startswith('encrypt:') or data_str.startswith('1|'):
                logger.warning("⚠️ 암호화된 체결통보 데이터 감지 - 복호화 처리 필요")
                logger.warning(f"⚠️ 암호화 데이터 샘플: {data_str[:50]}...")
                return None

            # 🆕 웹소켓 응답 형식 처리 (|로 구분되는 경우)
            if '|' in data_str:
                # 웹소켓 응답: 암호화여부|TR_ID|데이터건수|응답데이터
                websocket_parts = data_str.split('|')
                logger.info(f"📋 웹소켓 형식 감지: {len(websocket_parts)}개 부분")
                
                if len(websocket_parts) >= 4:
                    encryption_flag = websocket_parts[0]
                    tr_id = websocket_parts[1]
                    data_count = websocket_parts[2]
                    actual_data = websocket_parts[3]
                    
                    logger.info(f"📋 웹소켓 파싱: 암호화={encryption_flag}, TR_ID={tr_id}, 건수={data_count}")
                    
                    if encryption_flag == '1':
                        logger.warning("⚠️ 암호화된 데이터 (flag=1) - 복호화 필요")
                        return None
                    
                    data_str = actual_data
                    logger.info(f"📋 실제 데이터 추출: {data_str[:100]}...")

            # KIS 공식 문서에 따른 '^' 구분자로 필드 분리
            parts = data_str.split('^')
            
            # 🆕 상세 디버깅 정보
            logger.info(f"📋 체결통보 파싱 정보: 전체필드={len(parts)}개")
            logger.info(f"📋 처음 10개 필드: {parts[:10]}")
            
            if len(parts) < 15:  # 🆕 최소 필드 수를 15개로 완화 (더 유연하게)
                logger.warning(f"⚠️ 체결통보 데이터 필드 부족: {len(parts)}개 (최소 15개 필요)")
                logger.warning(f"📋 전체 필드 내용: {parts}")
                logger.warning(f"📋 원본 데이터: {data_str}")
                return None

            # 🎯 KIS 공식 문서에 따른 정확한 필드 매핑 (안전한 인덱스 접근)
            def safe_get(index: int, default: str = '') -> str:
                """안전한 배열 접근"""
                return parts[index] if index < len(parts) else default
            
            execution_info = {
                'cust_id': safe_get(0),                    # CUST_ID: 고객 ID
                'account_no': safe_get(1),                 # ACNT_NO: 계좌번호
                'order_id': safe_get(2),                   # ODER_NO: 주문번호 (핵심!)
                'original_order_id': safe_get(3),          # OODER_NO: 원주문번호
                'buy_sell_code': safe_get(4),              # SELN_BYOV_CLS: 매도매수구분
                'modify_code': safe_get(5),                # RCTF_CLS: 정정구분
                'order_kind': safe_get(6),                 # ODER_KIND: 주문종류
                'order_condition': safe_get(7),            # ODER_COND: 주문조건
                'stock_code': safe_get(8),                 # STCK_SHRN_ISCD: 주식 단축 종목코드
                'executed_quantity': safe_get(9),          # CNTG_QTY: 체결 수량
                'executed_price': safe_get(10),            # CNTG_UNPR: 체결단가
                'execution_time': safe_get(11),            # STCK_CNTG_HOUR: 주식 체결 시간
                'reject_yn': safe_get(12),                 # RFUS_YN: 거부여부
                'execution_yn': safe_get(13),              # CNTG_YN: 체결여부 (중요!)
                'accept_yn': safe_get(14),                 # ACPT_YN: 접수여부
                'branch_no': safe_get(15),                 # BRNC_NO: 지점번호
                'order_quantity': safe_get(16),            # ODER_QTY: 주문수량
                'account_name': safe_get(17),              # ACNT_NAME: 계좌명
                'order_condition_price': safe_get(18),     # ORD_COND_PRC: 호가조건가격
                'order_exchange_code': safe_get(19),       # ORD_EXG_GB: 주문거래소 구분
                'popup_yn': safe_get(20),                  # POPUP_YN: 실시간체결창 표시여부
                'filler': safe_get(21),                    # FILLER: 필러
                'credit_code': safe_get(22),               # CRDT_CLS: 신용구분
                'credit_loan_date': safe_get(23),          # CRDT_LOAN_DATE: 신용대출일자
                'stock_name': safe_get(24),                # CNTG_ISNM40: 체결종목명
                'order_price': safe_get(25),               # ODER_PRC: 주문가격
                'timestamp': notice_data.get('timestamp', datetime.now()) if isinstance(notice_data, dict) else datetime.now()
            }

            # 🎯 체결여부 검증 (가장 중요!)
            execution_yn = execution_info['execution_yn']
            if execution_yn != '2':
                logger.debug(f"📋 체결통보가 아님 (CNTG_YN={execution_yn}): 1=접수통보, 2=체결통보")
                return None  # 체결통보가 아니면 처리하지 않음

            # 🔍 거부 여부 확인
            if execution_info['reject_yn'] == 'Y':
                logger.warning(f"❌ 주문 거부됨: {execution_info['order_id']}")
                return None

            # 📊 숫자 필드 변환 및 검증
            try:
                execution_info['executed_quantity'] = int(execution_info['executed_quantity']) if execution_info['executed_quantity'] else 0
                execution_info['executed_price'] = int(execution_info['executed_price']) if execution_info['executed_price'] else 0
                execution_info['order_quantity'] = int(execution_info['order_quantity']) if execution_info['order_quantity'] else 0
                execution_info['order_price'] = int(execution_info['order_price']) if execution_info['order_price'] else 0
            except (ValueError, TypeError) as e:
                logger.error(f"❌ 숫자 필드 변환 오류: {e}")
                return None

            # 💰 매매구분 변환 (KIS 코드 -> 표준 형식)
            buy_sell_code = execution_info['buy_sell_code']
            if buy_sell_code == '01':
                execution_info['order_type'] = 'SELL'
            elif buy_sell_code == '02':
                execution_info['order_type'] = 'BUY'
            else:
                logger.warning(f"⚠️ 알 수 없는 매매구분: {buy_sell_code}")
                execution_info['order_type'] = 'UNKNOWN'

            # ✅ 체결 수량 검증
            if execution_info['executed_quantity'] <= 0:
                logger.warning(f"⚠️ 체결수량이 0 이하: {execution_info['executed_quantity']}")
                return None

            # ✅ 체결 가격 검증
            if execution_info['executed_price'] <= 0:
                logger.warning(f"⚠️ 체결가격이 0 이하: {execution_info['executed_price']}")
                return None

            logger.info(f"✅ 체결통보 파싱 성공: {execution_info['order_type']} {execution_info['stock_code']} "
                       f"{execution_info['executed_quantity']:,}주 @{execution_info['executed_price']:,}원 "
                       f"(주문ID: {execution_info['order_id']})")

            return execution_info

        except Exception as e:
            logger.error(f"❌ 체결통보 파싱 오류: {e}")
            return None

    def _validate_execution(self, pending_order: PendingOrder, execution_info: Dict) -> bool:
        """🔍 체결 정보 검증 - KIS 공식 문서 기준 (임시 주문ID 지원)"""
        try:
            pending_order_id = pending_order.order_id
            execution_order_id = execution_info.get('order_id', '')

            # 🎯 주문번호 일치 확인 (임시 주문ID 처리 개선)
            if pending_order_id.startswith('order_'):
                # 🆕 임시 주문ID인 경우: 종목코드, 시간, 수량으로 매칭
                logger.info(f"🔄 임시 주문ID 검증: {pending_order_id}")
                logger.info(f"🔄 체결통보 주문번호: {execution_order_id}")

                # 1. 종목코드가 일치하는지 확인
                if pending_order.stock_code != execution_info.get('stock_code', ''):
                    logger.error(f"❌ 임시주문 종목코드 불일치: {pending_order.stock_code} vs {execution_info.get('stock_code')}")
                    return False

                # 2. 주문 타입 확인
                if pending_order.order_type != execution_info.get('order_type', ''):
                    logger.error(f"❌ 임시주문 타입 불일치: {pending_order.order_type} vs {execution_info.get('order_type')}")
                    return False

                # 3. 시간 범위 확인 (임시 주문ID 생성 후 10분 이내로 확장)
                try:
                    temp_timestamp = int(pending_order_id.split('_')[-1]) / 1000  # 밀리초를 초로 변환
                    current_timestamp = datetime.now().timestamp()
                    elapsed_seconds = current_timestamp - temp_timestamp
                    
                    if elapsed_seconds > 600:  # 10분 초과
                        logger.warning(f"⚠️ 임시주문 시간 초과: {elapsed_seconds:.1f}초")
                        return False
                    
                    logger.info(f"✅ 시간 검증 통과: {elapsed_seconds:.1f}초 경과")
                except (ValueError, IndexError) as e:
                    logger.warning(f"⚠️ 임시주문ID 시간 파싱 오류: {pending_order_id}, 오류: {e}")
                    # 시간 파싱 실패해도 다른 조건으로 매칭 시도

                # 4. 🆕 수량 확인 (추가 검증)
                pending_qty = pending_order.quantity
                executed_qty = execution_info.get('executed_quantity', 0)
                
                if executed_qty != pending_qty:
                    logger.warning(f"⚠️ 수량 불일치하지만 부분체결 가능: 주문={pending_qty}주, 체결={executed_qty}주")
                    # 부분체결도 허용

                logger.info(f"✅ 임시 주문ID 검증 통과: {pending_order_id} → {execution_order_id}")
                logger.info(f"✅ 매칭 조건: 종목={pending_order.stock_code}, 타입={pending_order.order_type}, 수량={pending_qty}→{executed_qty}")

            elif pending_order_id.startswith('TEMP_'):
                # 기존 TEMP_ 형식 지원 (하위 호환성)
                logger.info(f"🔄 TEMP 주문ID 검증: {pending_order_id}")

                if pending_order.stock_code != execution_info.get('stock_code', ''):
                    logger.error(f"❌ TEMP주문 종목코드 불일치: {pending_order.stock_code} vs {execution_info.get('stock_code')}")
                    return False

                try:
                    temp_timestamp = int(pending_order_id.split('_')[-1])
                    current_timestamp = int(datetime.now().timestamp())
                    if current_timestamp - temp_timestamp > 600:  # 10분 초과
                        logger.warning(f"⚠️ TEMP주문 시간 초과: {current_timestamp - temp_timestamp}초")
                        return False
                except (ValueError, IndexError):
                    logger.warning(f"⚠️ TEMP주문ID 형식 오류: {pending_order_id}")

                logger.info(f"✅ TEMP 주문ID 검증 통과: {pending_order_id} → {execution_order_id}")

            else:
                # 일반 주문ID인 경우: 정확히 일치해야 함
                if pending_order_id != execution_order_id:
                    logger.error(f"❌ 주문번호 불일치: {pending_order_id} vs {execution_order_id}")
                    return False

            # 🎯 종목코드 일치 확인
            if pending_order.stock_code != execution_info.get('stock_code', ''):
                logger.error(f"❌ 종목코드 불일치: {pending_order.stock_code} vs {execution_info.get('stock_code')}")
                return False

            # 🎯 주문구분 일치 확인
            if pending_order.order_type != execution_info.get('order_type', ''):
                logger.error(f"❌ 주문구분 불일치: {pending_order.order_type} vs {execution_info.get('order_type')}")
                return False

            # 🎯 체결수량 검증
            executed_quantity = execution_info.get('executed_quantity', 0)
            if executed_quantity <= 0:
                logger.error(f"❌ 체결수량 오류: {executed_quantity}")
                return False

            # 🎯 체결수량이 주문수량을 초과하지 않는지 확인
            if executed_quantity > pending_order.quantity:
                logger.error(f"❌ 체결수량 초과: {executed_quantity} > {pending_order.quantity}")
                return False

            # 🎯 체결가격 검증
            executed_price = execution_info.get('executed_price', 0)
            if executed_price <= 0:
                logger.error(f"❌ 체결가격 오류: {executed_price}")
                return False

            # 🎯 주문가격과 체결가격 비교 (합리적 범위 내인지)
            if pending_order.price > 0:
                price_diff_pct = abs(executed_price - pending_order.price) / pending_order.price
                if price_diff_pct > 0.1:  # 10% 이상 차이나면 경고
                    logger.warning(f"⚠️ 주문가격과 체결가격 차이 큼: 주문={pending_order.price:,}원, 체결={executed_price:,}원 ({price_diff_pct:.1%})")
                    # 하지만 체결은 유효하므로 계속 진행

            # 🎯 거부 상태 재확인 (파싱에서도 확인했지만 이중 검증)
            if execution_info.get('reject_yn', 'N') == 'Y':
                logger.error(f"❌ 거부된 주문: {execution_info.get('order_id')}")
                return False

            # 🎯 계좌번호 검증 (선택사항 - TradingManager에서 계좌번호 가져올 수 있으면)
            expected_account = getattr(pending_order, 'account_no', None)
            if expected_account and expected_account != execution_info.get('account_no', ''):
                logger.warning(f"⚠️ 계좌번호 불일치: {expected_account} vs {execution_info.get('account_no')}")
                # 경고만 출력하고 계속 진행

            # 🎯 체결시간 검증 (너무 오래된 체결통보는 무시)
            execution_time = execution_info.get('execution_time', '')
            if execution_time and len(execution_time) == 6:  # HHMMSS 형식
                try:
                    from datetime import time as dt_time
                    current_time = datetime.now()
                    exec_hour = int(execution_time[0:2])
                    exec_minute = int(execution_time[2:4])
                    exec_second = int(execution_time[4:6])

                    # 오늘 날짜의 체결시간 생성
                    execution_datetime = datetime.combine(
                        current_time.date(),
                        dt_time(exec_hour, exec_minute, exec_second)
                    )

                    # 체결시간이 현재시간보다 미래이거나 1시간 이상 과거면 경고
                    time_diff = (current_time - execution_datetime).total_seconds()
                    if time_diff < -60:  # 미래 시간
                        logger.warning(f"⚠️ 미래 체결시간: {execution_time}")
                    elif time_diff > 3600:  # 1시간 이상 과거
                        logger.warning(f"⚠️ 오래된 체결통보: {execution_time} ({time_diff/60:.1f}분 전)")

                except ValueError:
                    logger.warning(f"⚠️ 체결시간 형식 오류: {execution_time}")

            logger.debug(f"✅ 체결 정보 검증 통과: {pending_order.order_type} {pending_order.stock_code} "
                        f"{executed_quantity:,}주 @{executed_price:,}원")
            return True

        except Exception as e:
            logger.error(f"❌ 체결 검증 오류: {e}")
            return False

    async def _process_execution(self, pending_order: PendingOrder, execution_info: Dict) -> bool:
        """체결 처리"""
        try:
            executed_quantity = execution_info['executed_quantity']
            executed_price = execution_info['executed_price']

            if pending_order.order_type == 'BUY':
                # 📈 매수 체결 처리
                success = await self._process_buy_execution(pending_order, execution_info)
            else:
                # 📉 매도 체결 처리
                success = await self._process_sell_execution(pending_order, execution_info)

            return success

        except Exception as e:
            logger.error(f"❌ 체결 처리 오류: {e}")
            return False

    async def _process_buy_execution(self, pending_order: PendingOrder, execution_info: Dict) -> bool:
        """매수 체결 처리 - 패턴 정보 포함"""
        try:
            executed_quantity = execution_info['executed_quantity']
            executed_price = execution_info['executed_price']

            # 1. 포지션 관리는 KIS API로 처리
            logger.debug(f"💡 KIS API로 포지션 관리: {pending_order.stock_code}")

            # 2. 거래 기록 저장 - 패턴 정보 포함
            trade_id = self.trade_db.record_buy_trade(
                stock_code=pending_order.stock_code,
                stock_name=pending_order.stock_code,  # 실제로는 종목명 조회
                quantity=executed_quantity,
                price=executed_price,
                total_amount=executed_quantity * executed_price,
                strategy_type=pending_order.strategy_type,
                order_id=pending_order.order_id,
                status='FILLED',
                # 🆕 패턴 정보 추가
                pattern_type=pending_order.pattern_type,
                pattern_confidence=pending_order.pattern_confidence,
                pattern_strength=pending_order.pattern_strength,
                # 🆕 기술적 지표 정보 추가
                rsi_value=pending_order.rsi_value,
                macd_value=pending_order.macd_value,
                volume_ratio=pending_order.volume_ratio,
                # 🆕 투자 정보 추가
                investment_amount=pending_order.investment_amount or (executed_quantity * executed_price),
                investment_ratio=pending_order.investment_ratio,
                # 기존 정보
                market_conditions={
                    'execution_time': execution_info.get('execution_time', ''),
                    'original_order_price': pending_order.price,
                    'price_difference': executed_price - pending_order.price
                },
                notes=f"웹소켓 체결통보 기반 매수 완료"
            )

            # 3. 비동기 로깅
            from .async_data_logger import log_buy_success
            log_buy_success(
                stock_code=pending_order.stock_code,
                buy_price=executed_price,
                quantity=executed_quantity,
                strategy=pending_order.strategy_type,
                signal_data={
                    'execution_method': 'websocket_notice',
                    'order_id': pending_order.order_id,
                    'execution_info': execution_info,
                    'pattern_type': pending_order.pattern_type,
                    'pattern_confidence': pending_order.pattern_confidence
                }
            )

            logger.info(f"✅ 매수 체결 완료: {pending_order.stock_code} {executed_quantity:,}주 @{executed_price:,}원 "
                       f"패턴:{pending_order.pattern_type} (거래ID: {trade_id})")
            return True

        except Exception as e:
            logger.error(f"❌ 매수 체결 처리 오류: {e}")
            return False

    async def _process_sell_execution(self, pending_order: PendingOrder, execution_info: Dict) -> bool:
        """매도 체결 처리 - 패턴 정보 포함"""
        try:
            executed_quantity = execution_info['executed_quantity']
            executed_price = execution_info['executed_price']

            # 1. 포지션 관리는 KIS API로 처리
            logger.debug(f"💡 KIS API로 포지션 관리: {pending_order.stock_code}")

            # 2. 거래 기록 저장 - 패턴 정보 포함
            buy_trade_id = self.trade_db.find_buy_trade_for_sell(
                pending_order.stock_code,
                executed_quantity
            )

            trade_id = self.trade_db.record_sell_trade(
                stock_code=pending_order.stock_code,
                stock_name=pending_order.stock_code,  # 실제로는 종목명 조회
                quantity=executed_quantity,
                price=executed_price,
                total_amount=executed_quantity * executed_price,
                strategy_type=pending_order.strategy_type,
                buy_trade_id=buy_trade_id,
                order_id=pending_order.order_id,
                status='FILLED',
                # 🆕 패턴 정보 추가 (매도 시에는 매도 사유 패턴)
                pattern_type=pending_order.pattern_type,
                pattern_confidence=pending_order.pattern_confidence,
                pattern_strength=pending_order.pattern_strength,
                # 🆕 기술적 지표 정보 추가
                rsi_value=pending_order.rsi_value,
                macd_value=pending_order.macd_value,
                volume_ratio=pending_order.volume_ratio,
                # 기존 정보
                market_conditions={
                    'execution_time': execution_info.get('execution_time', ''),
                    'original_order_price': pending_order.price,
                    'price_difference': executed_price - pending_order.price
                },
                notes=f"웹소켓 체결통보 기반 매도 완료"
            )

            logger.info(f"✅ 매도 체결 완료: {pending_order.stock_code} {executed_quantity:,}주 @{executed_price:,}원 "
                       f"패턴:{pending_order.pattern_type} (거래ID: {trade_id})")
            return True

        except Exception as e:
            logger.error(f"❌ 매도 체결 처리 오류: {e}")
            return False

    async def _execute_callbacks(self, pending_order: PendingOrder, execution_info: Dict):
        """콜백 함수들 실행"""
        try:
            for callback in self.execution_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(pending_order, execution_info)
                    else:
                        callback(pending_order, execution_info)
                except Exception as e:
                    logger.error(f"❌ 콜백 실행 오류: {e}")
        except Exception as e:
            logger.error(f"❌ 콜백 실행 중 오류: {e}")

    def add_execution_callback(self, callback: Callable):
        """체결 콜백 함수 추가"""
        self.execution_callbacks.append(callback)

    def cleanup_expired_orders(self) -> int:
        """🆕 만료된 대기 주문 정리 및 상태 복원"""
        try:
            current_time = datetime.now()
            expired_orders = []

            for order_id, pending_order in self.pending_orders.items():
                if pending_order.is_expired():
                    expired_orders.append(order_id)

            cleanup_count = 0
            for order_id in expired_orders:
                pending_order = self.pending_orders.pop(order_id)
                self.stats['orders_timeout'] += 1
                cleanup_count += 1

                logger.warning(f"⏰ 주문 타임아웃: {pending_order.order_type} {pending_order.stock_code} (ID: {order_id})")

                # 🆕 타임아웃 콜백 실행 (CandleTradeManager에서 상태 복원)
                try:
                    self._execute_timeout_callbacks(pending_order)
                except Exception as cb_error:
                    logger.error(f"❌ 타임아웃 콜백 오류: {cb_error}")

            if cleanup_count > 0:
                logger.info(f"🧹 만료된 주문 정리 완료: {cleanup_count}개")

            return cleanup_count

        except Exception as e:
            logger.error(f"❌ 만료 주문 정리 오류: {e}")
            return 0

    def _execute_timeout_callbacks(self, expired_order: PendingOrder):
        """🆕 타임아웃 콜백 실행 (종목 상태 복원용)"""
        try:
            # 🔧 안전한 elapsed_seconds 계산
            elapsed_seconds = 0
            try:
                if expired_order.timestamp:
                    elapsed_seconds = (datetime.now() - expired_order.timestamp).total_seconds()
            except Exception:
                elapsed_seconds = 0
            
            timeout_data = {
                'action': 'order_timeout',
                'order_id': expired_order.order_id,
                'stock_code': expired_order.stock_code,
                'order_type': expired_order.order_type,
                'quantity': expired_order.quantity,
                'price': expired_order.price,
                'strategy_type': expired_order.strategy_type,
                'timeout_reason': 'order_expired',
                'elapsed_seconds': elapsed_seconds
            }
            
            # 🎯 중요: 동기 콜백으로 처리 (CandleTradeManager 상태 복원)
            for callback in self.execution_callbacks:
                try:
                    if hasattr(callback, '__call__'):
                        callback(timeout_data)
                    else:
                        logger.warning(f"⚠️ 유효하지 않은 콜백: {callback}")
                except Exception as cb_error:
                    logger.error(f"❌ 개별 타임아웃 콜백 오류: {cb_error}")
                    
        except Exception as e:
            logger.error(f"❌ 타임아웃 콜백 실행 오류: {e}")

    def get_pending_orders_count(self) -> int:
        """대기 중인 주문 수"""
        return len(self.pending_orders)

    def get_stats(self) -> Dict:
        """통계 정보"""
        try:
            pending_orders_list = []
            for order in self.pending_orders.values():
                # 🔧 안전한 elapsed_seconds 계산
                elapsed_seconds = 0
                try:
                    if order.timestamp:
                        elapsed_seconds = (datetime.now() - order.timestamp).total_seconds()
                except Exception:
                    elapsed_seconds = 0
                
                pending_orders_list.append({
                    'order_id': order.order_id,
                    'stock_code': order.stock_code,
                    'order_type': order.order_type,
                    'quantity': order.quantity,
                    'price': order.price,
                    'elapsed_seconds': elapsed_seconds
                })
            
            return {
                **self.stats,
                'pending_orders_count': len(self.pending_orders),
                'pending_orders': pending_orders_list
            }
        except Exception as e:
            logger.error(f"❌ 통계 조회 오류: {e}")
            return {
                **self.stats,
                'pending_orders_count': len(self.pending_orders),
                'pending_orders': []
            }
