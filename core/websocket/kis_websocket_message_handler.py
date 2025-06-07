#!/usr/bin/env python3
"""
KIS 웹소켓 메시지 처리 전담 클래스
"""
import asyncio
import json
from typing import Dict, Callable, TYPE_CHECKING
from datetime import datetime
from enum import Enum
from utils.logger import setup_logger

if TYPE_CHECKING:
    from .kis_websocket_data_parser import KISWebSocketDataParser
    from .kis_websocket_subscription_manager import KISWebSocketSubscriptionManager

logger = setup_logger(__name__)


class KIS_WSReq(Enum):
    """웹소켓 요청 타입"""
    BID_ASK = 'H0STASP0'     # 실시간 국내주식 호가
    CONTRACT = 'H0STCNT0'    # 실시간 국내주식 체결
    NOTICE = 'H0STCNI0'      # 실시간 계좌체결발생통보 (실전)
    NOTICE_DEMO = 'H0STCNI9' # 실시간 계좌체결발생통보 (모의)
    MARKET_INDEX = 'H0UPCNT0' # 실시간 시장지수


class DataType(Enum):
    """데이터 타입"""
    STOCK_PRICE = 'stock_price'          # 주식체결가
    STOCK_ORDERBOOK = 'stock_orderbook'  # 주식호가
    STOCK_EXECUTION = 'stock_execution'  # 주식체결통보
    MARKET_INDEX = 'market_index'        # 시장지수


class KISWebSocketMessageHandler:
    """KIS 웹소켓 메시지 처리 전담 클래스"""

    def __init__(self, data_parser: "KISWebSocketDataParser",
                 subscription_manager: "KISWebSocketSubscriptionManager"):
        self.data_parser = data_parser
        self.subscription_manager = subscription_manager

        # 🆕 체결통보 직접 처리를 위한 OrderExecutionManager
        self.execution_manager = None

        # 🎯 CandleTradeManager 설정 - _all_stocks 상태 업데이트용
        self.candle_trade_manager = None

        # 통계
        self.stats = {
            'messages_received': 0,
            'last_message_time': None,
            'ping_pong_count': 0,
            'last_ping_pong_time': None,
            'errors': 0
        }

    def set_execution_manager(self, execution_manager):
        """🎯 OrderExecutionManager 설정"""
        self.execution_manager = execution_manager
        logger.info("✅ OrderExecutionManager 설정 완료 - 직접 체결통보 처리 가능")

    def set_candle_trade_manager(self, candle_trade_manager):
        """🎯 CandleTradeManager 설정 - _all_stocks 상태 업데이트용"""
        self.candle_trade_manager = candle_trade_manager
        logger.info("✅ CandleTradeManager 설정 완료 - _all_stocks 상태 업데이트 처리 가능")

    async def handle_realtime_data(self, data: str):
        """실시간 데이터 처리 - 🎯 KIS 공식 문서 기준 개선"""
        try:
            # 🔧 디버그: 실시간 데이터 수신 확인
            #logger.info(f"🔔 실시간 데이터 수신: {data[:100]}...")  # 첫 100자만 로그

            parts = data.split('|')
            if len(parts) < 4:
                logger.debug(f"⚠️ 데이터 파트 수 부족: {len(parts)}")
                return

            # 🎯 KIS 공식 구조: 암호화유무|TR_ID|데이터건수|응답데이터
            encryption_flag = parts[0]  # 0: 암호화없음, 1: 암호화됨
            tr_id = parts[1]
            data_count = parts[2] if len(parts) > 2 else "001"  # 데이터 건수
            raw_data = parts[3]

            #logger.info(f"📋 TR_ID: {tr_id}, 암호화: {encryption_flag}, 데이터건수: {data_count}, 길이: {len(raw_data)}")

            if tr_id == KIS_WSReq.CONTRACT.value:
                # 실시간 체결
                #logger.info(f"📈 실시간 체결 데이터 처리: {tr_id} ({data_count}건)")

                # 🔍 암호화 여부 확인
                is_encrypted = encryption_flag == '1'

                if is_encrypted:
                    # 암호화된 경우 복호화 필요
                    decrypted_data = self.data_parser.decrypt_notice_data(raw_data)
                    if decrypted_data:
                        parsed_data = self.data_parser.parse_contract_data(decrypted_data)
                        #logger.debug(f"🔓 체결 데이터 복호화 성공: {len(decrypted_data)}자")
                    else:
                        logger.warning("❌ 체결 데이터 복호화 실패")
                        parsed_data = None
                else:
                    # 암호화되지 않은 경우 직접 파싱
                    parsed_data = self.data_parser.parse_contract_data(raw_data)

                if parsed_data:
                    stock_code = parsed_data['stock_code']
                    total_records = parsed_data.get('total_data_count', 1)
                    #logger.info(f"✅ 체결 데이터 파싱 성공: {stock_code} "
                    #           f"(암호화: {'예' if is_encrypted else '아니오'}, "
                    #           f"처리건수: {total_records}건)")
                    await self._execute_callbacks(DataType.STOCK_PRICE.value, parsed_data)
                else:
                    logger.warning("❌ 체결 데이터 파싱 실패")

            elif tr_id == KIS_WSReq.BID_ASK.value:
                # 실시간 호가
                #logger.info(f"📊 실시간 호가 데이터 처리: {tr_id} ({data_count}건)")

                # 🔍 암호화 여부 확인
                is_encrypted = encryption_flag == '1'

                if is_encrypted:
                    # 암호화된 경우 복호화 필요
                    decrypted_data = self.data_parser.decrypt_notice_data(raw_data)
                    if decrypted_data:
                        parsed_data = self.data_parser.parse_bid_ask_data(decrypted_data)
                        #logger.debug(f"🔓 호가 데이터 복호화 성공: {len(decrypted_data)}자")
                    else:
                        logger.warning("❌ 호가 데이터 복호화 실패")
                        parsed_data = None
                else:
                    # 암호화되지 않은 경우 직접 파싱
                    parsed_data = self.data_parser.parse_bid_ask_data(raw_data)

                if parsed_data:
                    stock_code = parsed_data['stock_code']
                    #logger.info(f"✅ 호가 데이터 파싱 성공: {stock_code} "
                    #           f"(암호화: {'예' if is_encrypted else '아니오'})")
                    await self._execute_callbacks(DataType.STOCK_ORDERBOOK.value, parsed_data)
                else:
                    logger.warning("❌ 호가 데이터 파싱 실패")

            elif tr_id in [KIS_WSReq.NOTICE.value]:
                # 체결통보 (실전투자는 NOTICE만 사용)
                #logger.info(f"📢 체결통보 처리: {tr_id} ({data_count}건)")

                # 🔍 체결통보는 항상 암호화됨
                decrypted_data = self.data_parser.decrypt_notice_data(raw_data)
                if decrypted_data:
                    #logger.info(f"✅ 체결통보 수신: {decrypted_data[:100]}...")

                    # 🆕 직접 OrderExecutionManager 호출
                    await self._handle_execution_notice_direct(decrypted_data)

                    # 기존 콜백 시스템도 유지 (다른 용도)
                    await self._execute_callbacks(DataType.STOCK_EXECUTION.value,
                                                {'data': decrypted_data, 'timestamp': datetime.now()})
                else:
                    logger.warning("❌ 체결통보 복호화 실패")

            else:
                logger.warning(f"⚠️ 알 수 없는 TR_ID: {tr_id}")

        except Exception as e:
            logger.error(f"실시간 데이터 처리 오류: {e}")
            import traceback
            logger.error(f"스택 트레이스: {traceback.format_exc()}")
            self.stats['errors'] += 1

    async def handle_system_message(self, data: str):
        """시스템 메시지 처리"""
        try:
            # 이벤트 루프 안전성 미리 확인
            try:
                current_loop = asyncio.get_running_loop()
                if current_loop.is_closed():
                    logger.debug("시스템 메시지 처리 - 이벤트 루프가 닫혀있음, 건너뜀")
                    return
            except RuntimeError:
                logger.debug("시스템 메시지 처리 - 실행 중인 이벤트 루프가 없음, 건너뜀")
                return

            json_data = json.loads(data)
            tr_id = json_data.get('header', {}).get('tr_id', '')

            if tr_id == "PINGPONG":
                # 🎯 KIS PINGPONG 처리 (JSON 메시지)
                logger.debug(f"### RECV [PINGPONG] [{data[:100]}...]")
                self.stats['ping_pong_count'] = self.stats.get('ping_pong_count', 0) + 1
                self.stats['last_ping_pong_time'] = datetime.now()

                # 🎯 동일한 PINGPONG 메시지를 그대로 반환 (KIS 방식)
                return 'PINGPONG', data
            else:
                body = json_data.get('body', {})
                rt_cd = body.get('rt_cd', '')
                msg = body.get('msg1', '')

                if rt_cd == '0':  # 성공
                    logger.debug(f"시스템 메시지: {msg}")

                    # 체결통보 암호화 키 저장
                    output = body.get('output', {})
                    if 'key' in output and 'iv' in output:
                        self.data_parser.set_encryption_keys(output['key'], output['iv'])

                elif rt_cd == '1':  # 오류
                    logger.error(f"시스템 오류: {msg}")
                    self.stats['errors'] += 1

        except asyncio.CancelledError:
            logger.debug("시스템 메시지 처리 - 태스크 취소됨")
            raise  # CancelledError는 다시 발생시켜야 함
        except Exception as e:
            # 오류 로깅 최소화 (너무 많은 로그 방지)
            if "Event loop is closed" in str(e):
                logger.debug(f"시스템 메시지 처리 - 이벤트 루프 닫힘: {e}")
            elif "PINGPONG" in data:
                logger.debug(f"PINGPONG 처리 중 오류: {e}")
            else:
                logger.error(f"시스템 메시지 처리 오류: {e}")
            self.stats['errors'] += 1

    async def process_message(self, message: str):
        """메시지 처리 메인 함수"""
        try:
            self.stats['messages_received'] += 1
            self.stats['last_message_time'] = datetime.now()

            # 디버그: 수신된 메시지 로그
            #logger.info(f"📨 웹소켓 메시지 수신 (길이: {len(message)}, 첫 문자: '{message[0] if message else 'None'}')")

            if message[0] in ('0', '1'):
                # 실시간 데이터
                #logger.info(f"🔔 실시간 데이터로 분류하여 처리")
                await self.handle_realtime_data(message)
            else:
                # 시스템 메시지
                #logger.info(f"🔧 시스템 메시지로 분류하여 처리")
                result = await self.handle_system_message(message)
                return result  # PINGPONG 등 특별한 처리가 필요한 경우 반환

        except Exception as e:
            logger.error(f"메시지 처리 오류: {e}")
            self.stats['errors'] += 1

    async def _execute_callbacks(self, data_type: str, data: Dict):
        """콜백 함수들 실행 - 🆕 data_type 정보 전달"""
        try:
            # 글로벌 콜백 실행
            global_callbacks = self.subscription_manager.get_global_callbacks(data_type)
            for callback in global_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(data_type, data)  # 🆕 data_type 추가
                    else:
                        callback(data_type, data)  # 🆕 data_type 추가
                except Exception as e:
                    logger.error(f"글로벌 콜백 실행 오류 ({data_type}): {e}")

            # 종목별 콜백 실행 (stock_code가 있는 경우)
            stock_code = data.get('stock_code')
            if stock_code:
                stock_callbacks = self.subscription_manager.get_callbacks_for_stock(stock_code)
                for callback in stock_callbacks:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(data_type, stock_code, data)  # 🆕 data_type 추가
                        else:
                            callback(data_type, stock_code, data)  # 🆕 data_type 추가
                    except Exception as e:
                        logger.error(f"종목별 콜백 실행 오류 ({stock_code}): {e}")

        except Exception as e:
            logger.error(f"콜백 실행 오류: {e}")

    def get_stats(self) -> Dict:
        """메시지 처리 통계 반환"""
        return self.stats.copy()

    async def _handle_execution_notice_direct(self, decrypted_data: str):
        """🎯 체결통보 직접 처리 - OrderExecutionManager 연동"""
        try:
            # 🔍 OrderExecutionManager 찾기
            execution_manager = self._find_execution_manager()
            if not execution_manager:
                logger.warning("⚠️ OrderExecutionManager를 찾을 수 없음 - 체결통보 처리 불가")
                return

            # 🎯 체결통보 데이터 구조 생성
            notice_data = {
                'data': decrypted_data,  # KIS에서 복호화된 '^' 구분 데이터
                'timestamp': datetime.now(),
                'source': 'kis_websocket_direct'
            }

            # 🚀 OrderExecutionManager로 직접 전달
            logger.info(f"🎯 체결통보 직접 처리: {decrypted_data[:100]}...")
            await execution_manager.handle_execution_notice(notice_data)

            # 🎯 CandleTradeManager의 _all_stocks 상태 업데이트도 처리
            if self.candle_trade_manager:
                logger.info("🔄 CandleTradeManager _all_stocks 상태 업데이트 처리")
                await self.candle_trade_manager.handle_execution_confirmation(decrypted_data)
            else:
                logger.debug("💡 CandleTradeManager가 설정되지 않음 - _all_stocks 업데이트 생략")

        except Exception as e:
            logger.error(f"❌ 체결통보 직접 처리 오류: {e}")

    def _find_execution_manager(self):
        """OrderExecutionManager 인스턴스 찾기"""
        try:
            # 🎯 직접 설정된 execution_manager 사용
            if self.execution_manager and hasattr(self.execution_manager, 'handle_execution_notice'):
                return self.execution_manager

            logger.debug("💡 OrderExecutionManager가 설정되지 않음 - 콜백 시스템 사용")
            return None

        except Exception as e:
            logger.error(f"❌ OrderExecutionManager 검색 오류: {e}")
            return None
