"""
데이터베이스 매니저
시간대별 종목 선택, 거래 기록, 성과 분석 등 모든 데이터베이스 작업 관리
"""
import json
import pytz
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any
from peewee import DoesNotExist, IntegrityError
from utils.logger import setup_logger
from database.db_models import (
    database, ALL_TABLES, KST,
    GoldenTimeStock, MorningLeadersStock, LunchTimeStock, ClosingTrendStock,
    TradeRecord, Position, StrategyPerformance, MarketScreeningLog,
    SystemLog, APICallStats,
    StrategyType, TimeSlot, OrderType, OrderStatus, PositionStatus, DataPriority
)

logger = setup_logger(__name__)

class DatabaseManager:
    """데이터베이스 매니저"""

    def __init__(self):
        self.db = database

        # 시간대별 테이블 매핑
        self.time_slot_tables = {
            TimeSlot.GOLDEN_TIME: GoldenTimeStock,
            TimeSlot.MORNING_LEADERS: MorningLeadersStock,
            TimeSlot.LUNCH_TIME: LunchTimeStock,
            TimeSlot.CLOSING_TREND: ClosingTrendStock,
        }

        logger.info("📊 데이터베이스 매니저 초기화")

    def initialize_database(self):
        """데이터베이스 및 테이블 초기화"""
        try:
            # 데이터베이스 연결
            self.db.connect(reuse_if_open=True)

            # 모든 테이블 생성 (존재하지 않는 경우만)
            self.db.create_tables(ALL_TABLES, safe=True)

            logger.info("✅ 데이터베이스 초기화 완료")

            # 테이블 정보 로깅
            for table in ALL_TABLES:
                count = table.select().count()
                logger.info(f"📋 {table._meta.table_name}: {count}개 레코드")

        except Exception as e:
            logger.error(f"❌ 데이터베이스 초기화 실패: {e}")
            raise

    def close_database(self):
        """데이터베이스 연결 종료"""
        try:
            if not self.db.is_closed():
                self.db.close()
                logger.info("🔒 데이터베이스 연결 종료")
        except Exception as e:
            logger.error(f"데이터베이스 종료 중 오류: {e}")

    # ========== 시간대별 종목 기록 ==========

    def record_selected_stock(self, time_slot: TimeSlot, stock_data: Dict) -> bool:
        """선택된 종목 기록"""
        try:
            table_class = self.time_slot_tables.get(time_slot)
            if not table_class:
                logger.error(f"알 수 없는 시간대: {time_slot}")
                return False

            now_kst = datetime.now(KST)

            # 기본 데이터 준비
            record_data = {
                'stock_code': stock_data['stock_code'],
                'stock_name': stock_data.get('stock_name', ''),
                'strategy_type': stock_data['strategy_type'],
                'score': Decimal(str(stock_data.get('score', 0))),
                'reason': stock_data.get('reason', ''),
                'priority': stock_data.get('priority', 'MEDIUM'),
                'current_price': int(stock_data.get('current_price', 0)),
                'change_rate': Decimal(str(stock_data.get('change_rate', 0))),
                'volume': int(stock_data.get('volume', 0)),
                'selection_date': now_kst.date(),
                'selection_time': now_kst.time(),
                'is_active': True,
                'activation_success': stock_data.get('activation_success', False)
            }

            # 시간대별 특화 데이터 추가
            if time_slot == TimeSlot.GOLDEN_TIME:
                record_data.update({
                    'volume_ratio': Decimal(str(stock_data.get('volume_ratio', 0))) if stock_data.get('volume_ratio') else None,
                    'gap_rate': Decimal(str(stock_data.get('gap_rate', 0))) if stock_data.get('gap_rate') else None,
                })
            elif time_slot == TimeSlot.MORNING_LEADERS:
                record_data.update({
                    'volume_ratio': Decimal(str(stock_data.get('volume_ratio', 0))) if stock_data.get('volume_ratio') else None,
                    'breakout_direction': stock_data.get('breakout_direction'),
                    'buying_pressure': stock_data.get('buying_pressure'),
                })
            elif time_slot in [TimeSlot.LUNCH_TIME, TimeSlot.CLOSING_TREND]:
                record_data.update({
                    'momentum_strength': Decimal(str(stock_data.get('momentum_strength', 0))) if stock_data.get('momentum_strength') else None,
                    'trend_quality': stock_data.get('trend_quality'),
                })

                if time_slot == TimeSlot.LUNCH_TIME:
                    record_data['consecutive_up_days'] = stock_data.get('consecutive_up_days')
                elif time_slot == TimeSlot.CLOSING_TREND:
                    record_data['ma_position'] = stock_data.get('ma_position')

            # 기존 레코드 확인 (중복 방지)
            existing = table_class.select().where(
                (table_class.stock_code == stock_data['stock_code']) &
                (table_class.selection_date == now_kst.date()) &
                (table_class.strategy_type == stock_data['strategy_type'])
            ).first()

            if existing:
                # 기존 레코드 업데이트
                for key, value in record_data.items():
                    setattr(existing, key, value)
                existing.save()
                logger.debug(f"종목 기록 업데이트: {stock_data['stock_code']} ({time_slot.value})")
            else:
                # 새 레코드 생성
                table_class.create(**record_data)
                logger.info(f"📝 종목 기록 저장: {stock_data['stock_code']} ({time_slot.value})")

            return True

        except Exception as e:
            logger.error(f"종목 기록 저장 실패: {e}")
            return False

    def update_stock_activation_status(self, time_slot: TimeSlot, stock_code: str, success: bool):
        """종목 활성화 상태 업데이트"""
        try:
            table_class = self.time_slot_tables.get(time_slot)
            if not table_class:
                return False

            today = datetime.now(KST).date()

            # 오늘의 해당 종목 찾기
            stocks = table_class.update(
                activation_success=success,
                is_active=success
            ).where(
                (table_class.stock_code == stock_code) &
                (table_class.selection_date == today)
            )

            updated_count = stocks.execute()

            if updated_count > 0:
                logger.info(f"📊 종목 활성화 상태 업데이트: {stock_code} → {'성공' if success else '실패'}")
                return True

            return False

        except Exception as e:
            logger.error(f"종목 활성화 상태 업데이트 실패: {e}")
            return False

    def get_today_selected_stocks(self, time_slot: TimeSlot = None) -> List[Dict]:
        """오늘 선택된 종목들 조회"""
        try:
            today = datetime.now(KST).date()
            results = []

            if time_slot:
                # 특정 시간대만 조회
                table_class = self.time_slot_tables.get(time_slot)
                if table_class:
                    stocks = table_class.select().where(
                        table_class.selection_date == today
                    ).order_by(table_class.score.desc())

                    for stock in stocks:
                        results.append(self._stock_record_to_dict(stock, time_slot))
            else:
                # 모든 시간대 조회
                for slot, table_class in self.time_slot_tables.items():
                    stocks = table_class.select().where(
                        table_class.selection_date == today
                    ).order_by(table_class.score.desc())

                    for stock in stocks:
                        results.append(self._stock_record_to_dict(stock, slot))

            return results

        except Exception as e:
            logger.error(f"오늘 선택 종목 조회 실패: {e}")
            return []

    def _stock_record_to_dict(self, stock: Any, time_slot: TimeSlot) -> Dict:
        """종목 레코드를 딕셔너리로 변환"""
        base_dict = {
            'time_slot': time_slot.value,
            'stock_code': stock.stock_code,
            'stock_name': stock.stock_name,
            'strategy_type': stock.strategy_type,
            'score': float(stock.score),
            'reason': stock.reason,
            'priority': stock.priority,
            'current_price': stock.current_price,
            'change_rate': float(stock.change_rate),
            'volume': stock.volume,
            'selection_date': stock.selection_date.isoformat(),
            'selection_time': stock.selection_time.isoformat(),
            'is_active': stock.is_active,
            'activation_success': stock.activation_success,
        }

        # 시간대별 특화 필드 추가
        if hasattr(stock, 'volume_ratio') and stock.volume_ratio:
            base_dict['volume_ratio'] = float(stock.volume_ratio)
        if hasattr(stock, 'gap_rate') and stock.gap_rate:
            base_dict['gap_rate'] = float(stock.gap_rate)
        if hasattr(stock, 'breakout_direction') and stock.breakout_direction:
            base_dict['breakout_direction'] = stock.breakout_direction
        if hasattr(stock, 'buying_pressure') and stock.buying_pressure:
            base_dict['buying_pressure'] = stock.buying_pressure
        if hasattr(stock, 'momentum_strength') and stock.momentum_strength:
            base_dict['momentum_strength'] = float(stock.momentum_strength)
        if hasattr(stock, 'trend_quality') and stock.trend_quality:
            base_dict['trend_quality'] = stock.trend_quality
        if hasattr(stock, 'consecutive_up_days') and stock.consecutive_up_days:
            base_dict['consecutive_up_days'] = stock.consecutive_up_days
        if hasattr(stock, 'ma_position') and stock.ma_position:
            base_dict['ma_position'] = stock.ma_position

        return base_dict

    # ========== 거래 기록 ==========

    def record_execution_to_db(self, execution_data: Dict, current_slot_name: str = None, pending_orders: Dict = None) -> bool:
        """체결 데이터를 데이터베이스에 기록"""
        try:
            # 시간대 정보 처리
            time_slot = current_slot_name if current_slot_name else "unknown"

            # 대기 주문에서 전략 정보 찾기
            order_no = execution_data.get('order_no', '')
            pending_order = pending_orders.get(order_no, {}) if pending_orders else {}

            trade_data = {
                'order_id': order_no,
                'stock_code': execution_data.get('stock_code', ''),
                'stock_name': execution_data.get('stock_name', ''),
                'order_type': OrderType.BUY.value if execution_data.get('order_type') == '매수' else OrderType.SELL.value,
                'order_status': OrderStatus.FILLED.value,
                'order_quantity': int(execution_data.get('execution_qty', 0)),
                'executed_quantity': int(execution_data.get('execution_qty', 0)),
                'order_price': int(execution_data.get('execution_price', 0)),
                'executed_price': int(execution_data.get('execution_price', 0)),
                'time_slot': time_slot,
                'strategy_type': pending_order.get('strategy_type', 'manual'),
                'trade_amount': int(execution_data.get('execution_qty', 0)) * int(execution_data.get('execution_price', 0)),
                'fee': execution_data.get('fee', 0),
                'tax': execution_data.get('tax', 0),
                'net_amount': execution_data.get('net_amount', 0),
            }

            # DB에 기록
            success = self.record_trade(trade_data)
            if success:
                logger.debug(f"💾 거래 DB 기록 완료: {order_no}")
            else:
                logger.warning(f"⚠️ 거래 DB 기록 실패: {order_no}")

            return success

        except Exception as e:
            logger.error(f"거래 DB 기록 중 오류: {e}")
            return False

    def record_trade(self, trade_data: Dict) -> bool:
        """거래 기록"""
        try:
            # 시간 처리
            order_time = trade_data.get('order_time')
            if isinstance(order_time, str):
                order_time = datetime.fromisoformat(order_time)
            elif order_time is None:
                order_time = datetime.now(KST)

            executed_time = trade_data.get('executed_time')
            if isinstance(executed_time, str):
                executed_time = datetime.fromisoformat(executed_time)

            record_data = {
                'order_id': trade_data['order_id'],
                'stock_code': trade_data['stock_code'],
                'stock_name': trade_data.get('stock_name', ''),
                'order_type': trade_data['order_type'],
                'order_status': trade_data['order_status'],
                'order_quantity': int(trade_data['order_quantity']),
                'executed_quantity': int(trade_data.get('executed_quantity', 0)),
                'order_price': int(trade_data['order_price']),
                'executed_price': int(trade_data['executed_price']) if trade_data.get('executed_price') else None,
                'time_slot': trade_data['time_slot'],
                'strategy_type': trade_data['strategy_type'],
                'order_time': order_time,
                'executed_time': executed_time,
                'trade_amount': int(trade_data['trade_amount']) if trade_data.get('trade_amount') else None,
                'fee': int(trade_data.get('fee', 0)),
                'tax': int(trade_data.get('tax', 0)),
                'net_amount': int(trade_data['net_amount']) if trade_data.get('net_amount') else None,
                'error_message': trade_data.get('error_message'),
                'retry_count': int(trade_data.get('retry_count', 0)),
            }

            # 기존 거래 확인
            existing = TradeRecord.select().where(
                TradeRecord.order_id == trade_data['order_id']
            ).first()

            if existing:
                # 기존 거래 업데이트
                for key, value in record_data.items():
                    setattr(existing, key, value)
                existing.save()
                logger.debug(f"거래 기록 업데이트: {trade_data['order_id']}")
            else:
                # 새 거래 생성
                TradeRecord.create(**record_data)
                logger.info(f"💰 거래 기록 저장: {trade_data['stock_code']} {trade_data['order_type']}")

            return True

        except Exception as e:
            logger.error(f"거래 기록 저장 실패: {e}")
            return False

    def get_trade_history(self, stock_code: str = None, days: int = 7) -> List[Dict]:
        """거래 기록 조회"""
        try:
            query = TradeRecord.select()

            if stock_code:
                query = query.where(TradeRecord.stock_code == stock_code)

            if days > 0:
                start_date = datetime.now(KST) - timedelta(days=days)
                query = query.where(TradeRecord.order_time >= start_date)

            trades = query.order_by(TradeRecord.order_time.desc())

            results = []
            for trade in trades:
                results.append({
                    'order_id': trade.order_id,
                    'stock_code': trade.stock_code,
                    'stock_name': trade.stock_name,
                    'order_type': trade.order_type,
                    'order_status': trade.order_status,
                    'order_quantity': trade.order_quantity,
                    'executed_quantity': trade.executed_quantity,
                    'order_price': trade.order_price,
                    'executed_price': trade.executed_price,
                    'time_slot': trade.time_slot,
                    'strategy_type': trade.strategy_type,
                    'order_time': trade.order_time.isoformat(),
                    'executed_time': trade.executed_time.isoformat() if trade.executed_time else None,
                    'trade_amount': trade.trade_amount,
                    'fee': trade.fee,
                    'tax': trade.tax,
                    'net_amount': trade.net_amount,
                    'error_message': trade.error_message,
                    'retry_count': trade.retry_count,
                })

            return results

        except Exception as e:
            logger.error(f"거래 기록 조회 실패: {e}")
            return []

    # ========== 포지션 관리 ==========

    def create_position(self, position_data: Dict) -> bool:
        """포지션 생성"""
        try:
            entry_time = position_data.get('entry_time')
            if isinstance(entry_time, str):
                entry_time = datetime.fromisoformat(entry_time)
            elif entry_time is None:
                entry_time = datetime.now(KST)

            record_data = {
                'stock_code': position_data['stock_code'],
                'stock_name': position_data.get('stock_name', ''),
                'status': PositionStatus.OPEN.value,
                'quantity': int(position_data['quantity']),
                'avg_buy_price': int(position_data['avg_buy_price']),
                'total_buy_amount': int(position_data['total_buy_amount']),
                'entry_time_slot': position_data['time_slot'],
                'entry_strategy': position_data['strategy_type'],
                'entry_time': entry_time,
            }

            Position.create(**record_data)
            logger.info(f"📈 포지션 생성: {position_data['stock_code']} {position_data['quantity']}주")
            return True

        except Exception as e:
            logger.error(f"포지션 생성 실패: {e}")
            return False

    def update_position(self, stock_code: str, update_data: Dict) -> bool:
        """포지션 업데이트"""
        try:
            position = Position.select().where(
                (Position.stock_code == stock_code) &
                (Position.status == PositionStatus.OPEN.value)
            ).first()

            if not position:
                logger.warning(f"업데이트할 포지션을 찾을 수 없음: {stock_code}")
                return False

            # 업데이트 데이터 적용
            for key, value in update_data.items():
                if hasattr(position, key):
                    setattr(position, key, value)

            # 종료 시간 처리
            if 'exit_time' in update_data:
                exit_time = update_data['exit_time']
                if isinstance(exit_time, str):
                    exit_time = datetime.fromisoformat(exit_time)
                elif exit_time is None:
                    exit_time = datetime.now(KST)

                position.exit_time = exit_time

                # 보유 시간 계산
                if position.entry_time:
                    duration = exit_time - position.entry_time
                    position.holding_duration = int(duration.total_seconds() / 60)  # 분 단위

            position.save()
            logger.info(f"📊 포지션 업데이트: {stock_code}")
            return True

        except Exception as e:
            logger.error(f"포지션 업데이트 실패: {e}")
            return False

    def close_position(self, stock_code: str, sell_data: Dict) -> bool:
        """포지션 종료"""
        try:
            position = Position.select().where(
                (Position.stock_code == stock_code) &
                (Position.status == PositionStatus.OPEN.value)
            ).first()

            if not position:
                logger.warning(f"종료할 포지션을 찾을 수 없음: {stock_code}")
                return False

            # 매도 정보 업데이트
            position.status = PositionStatus.CLOSED.value
            position.sell_price = int(sell_data['sell_price'])
            position.sell_amount = int(sell_data['sell_amount'])
            position.realized_pnl = int(sell_data['realized_pnl'])
            position.realized_pnl_rate = Decimal(str(sell_data['realized_pnl_rate']))
            position.exit_strategy = sell_data.get('exit_strategy', 'manual')

            # 종료 시간
            exit_time = sell_data.get('exit_time')
            if isinstance(exit_time, str):
                exit_time = datetime.fromisoformat(exit_time)
            elif exit_time is None:
                exit_time = datetime.now(KST)

            position.exit_time = exit_time

            # 보유 시간 계산
            if position.entry_time:
                duration = exit_time - position.entry_time
                position.holding_duration = int(duration.total_seconds() / 60)

            position.save()
            logger.info(f"✅ 포지션 종료: {stock_code} 손익:{sell_data['realized_pnl']:+,}원")
            return True

        except Exception as e:
            logger.error(f"포지션 종료 실패: {e}")
            return False

    def get_open_positions(self) -> List[Dict]:
        """보유 중인 포지션 조회"""
        try:
            positions = Position.select().where(
                Position.status == PositionStatus.OPEN.value
            ).order_by(Position.entry_time.desc())

            results = []
            for pos in positions:
                results.append({
                    'stock_code': pos.stock_code,
                    'stock_name': pos.stock_name,
                    'quantity': pos.quantity,
                    'avg_buy_price': pos.avg_buy_price,
                    'total_buy_amount': pos.total_buy_amount,
                    'current_price': pos.current_price,
                    'current_value': pos.current_value,
                    'unrealized_pnl': pos.unrealized_pnl,
                    'unrealized_pnl_rate': float(pos.unrealized_pnl_rate) if pos.unrealized_pnl_rate else None,
                    'entry_time_slot': pos.entry_time_slot,
                    'entry_strategy': pos.entry_strategy,
                    'entry_time': pos.entry_time.isoformat(),
                    'holding_duration': pos.holding_duration,
                })

            return results

        except Exception as e:
            logger.error(f"보유 포지션 조회 실패: {e}")
            return []

    # ========== 시장 스크리닝 로그 ==========

    def record_market_screening(self, screening_data: Dict) -> bool:
        """시장 스크리닝 로그 기록"""
        try:
            record_data = {
                'screening_type': screening_data['screening_type'],
                'stock_code': screening_data['stock_code'],
                'stock_name': screening_data.get('stock_name', ''),
                'rank': int(screening_data['rank']),
                'current_price': int(screening_data['current_price']),
                'change_rate': Decimal(str(screening_data['change_rate'])),
                'volume': int(screening_data['volume']),
                'volume_ratio': Decimal(str(screening_data['volume_ratio'])) if screening_data.get('volume_ratio') else None,
                'market_cap': int(screening_data['market_cap']) if screening_data.get('market_cap') else None,
                'amount': int(screening_data['amount']) if screening_data.get('amount') else None,
                'bid_quantity': int(screening_data['bid_quantity']) if screening_data.get('bid_quantity') else None,
                'ask_quantity': int(screening_data['ask_quantity']) if screening_data.get('ask_quantity') else None,
                'buying_pressure': screening_data.get('buying_pressure'),
                'criteria': screening_data.get('criteria', ''),
                'was_activated': screening_data.get('was_activated', False),
                'priority_assigned': screening_data.get('priority_assigned'),
            }

            MarketScreeningLog.create(**record_data)
            logger.debug(f"📊 시장 스크리닝 기록: {screening_data['stock_code']} ({screening_data['screening_type']})")
            return True

        except Exception as e:
            logger.error(f"시장 스크리닝 기록 실패: {e}")
            return False

    # ========== 시스템 로그 ==========

    def record_system_log(self, log_level: str, component: str, message: str,
                         details: Dict = None, error_traceback: str = None,
                         performance_data: Dict = None) -> bool:
        """시스템 로그 기록"""
        try:
            record_data = {
                'log_level': log_level,
                'component': component,
                'message': message,
                'details': json.dumps(details, ensure_ascii=False) if details else None,
                'error_traceback': error_traceback,
            }

            # 성능 데이터 추가
            if performance_data:
                record_data.update({
                    'memory_usage': performance_data.get('memory_usage'),
                    'cpu_usage': Decimal(str(performance_data['cpu_usage'])) if performance_data.get('cpu_usage') else None,
                    'api_call_count': performance_data.get('api_call_count'),
                    'websocket_connections': performance_data.get('websocket_connections'),
                })

            SystemLog.create(**record_data)
            return True

        except Exception as e:
            logger.error(f"시스템 로그 기록 실패: {e}")
            return False

    # ========== 성과 분석 ==========

    def calculate_daily_performance(self, target_date: date = None) -> Dict:
        """일일 성과 계산"""
        try:
            if target_date is None:
                target_date = datetime.now(KST).date()

            # 당일 완료된 거래들
            completed_trades = TradeRecord.select().where(
                (TradeRecord.order_time >= target_date) &
                (TradeRecord.order_time < target_date + timedelta(days=1)) &
                (TradeRecord.order_status == OrderStatus.FILLED.value)
            )

            # 매수/매도 분리
            buy_trades = [t for t in completed_trades if t.order_type == OrderType.BUY.value]
            sell_trades = [t for t in completed_trades if t.order_type == OrderType.SELL.value]

            # 기본 통계
            total_buy_amount = sum(t.net_amount or 0 for t in buy_trades)
            total_sell_amount = sum(t.net_amount or 0 for t in sell_trades)
            total_fee = sum(t.fee or 0 for t in completed_trades)
            total_tax = sum(t.tax or 0 for t in completed_trades)

            # 실현 손익 (종료된 포지션들)
            closed_positions = Position.select().where(
                (Position.exit_time >= target_date) &
                (Position.exit_time < target_date + timedelta(days=1)) &
                (Position.status == PositionStatus.CLOSED.value)
            )

            realized_pnl = sum(pos.realized_pnl or 0 for pos in closed_positions)
            win_count = sum(1 for pos in closed_positions if (pos.realized_pnl or 0) > 0)
            lose_count = sum(1 for pos in closed_positions if (pos.realized_pnl or 0) < 0)

            return {
                'date': target_date.isoformat(),
                'total_trades': len(completed_trades),
                'buy_trades': len(buy_trades),
                'sell_trades': len(sell_trades),
                'total_buy_amount': total_buy_amount,
                'total_sell_amount': total_sell_amount,
                'total_fee': total_fee,
                'total_tax': total_tax,
                'closed_positions': len(closed_positions),
                'realized_pnl': realized_pnl,
                'win_count': win_count,
                'lose_count': lose_count,
                'win_rate': (win_count / len(closed_positions) * 100) if closed_positions else 0,
            }

        except Exception as e:
            logger.error(f"일일 성과 계산 실패: {e}")
            return {}

    def get_strategy_statistics(self, days: int = 30) -> Dict:
        """전략별 통계"""
        try:
            start_date = datetime.now(KST) - timedelta(days=days)

            stats = {}

            for time_slot in TimeSlot:
                for strategy_type in StrategyType:
                    # 해당 전략의 완료된 포지션들
                    positions = Position.select().where(
                        (Position.entry_time >= start_date) &
                        (Position.entry_time_slot == time_slot.value) &
                        (Position.entry_strategy == strategy_type.value) &
                        (Position.status == PositionStatus.CLOSED.value)
                    )

                    if not positions:
                        continue

                    total_pnl = sum(pos.realized_pnl or 0 for pos in positions)
                    win_positions = [pos for pos in positions if (pos.realized_pnl or 0) > 0]
                    lose_positions = [pos for pos in positions if (pos.realized_pnl or 0) < 0]

                    avg_holding_time = sum(pos.holding_duration or 0 for pos in positions) / len(positions)

                    key = f"{time_slot.value}_{strategy_type.value}"
                    stats[key] = {
                        'time_slot': time_slot.value,
                        'strategy_type': strategy_type.value,
                        'total_positions': len(positions),
                        'win_positions': len(win_positions),
                        'lose_positions': len(lose_positions),
                        'win_rate': len(win_positions) / len(positions) * 100,
                        'total_pnl': total_pnl,
                        'avg_pnl': total_pnl / len(positions),
                        'max_profit': max((pos.realized_pnl or 0) for pos in positions),
                        'max_loss': min((pos.realized_pnl or 0) for pos in positions),
                        'avg_holding_time': avg_holding_time,
                    }

            return stats

        except Exception as e:
            logger.error(f"전략 통계 계산 실패: {e}")
            return {}

# 전역 데이터베이스 매니저 인스턴스
db_manager = DatabaseManager()
