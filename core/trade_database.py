"""
거래 기록 데이터베이스 관리자
매수/매도 기록 저장 및 성과 분석
"""
import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from utils.logger import setup_logger

logger = setup_logger(__name__)

class TradeDatabase:
    """거래 기록 데이터베이스 관리자"""

    def __init__(self, db_path: str = "data/trades.db"):
        """초기화"""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True)
        
        # 데이터베이스 초기화
        self._init_database()
        logger.info(f"거래 데이터베이스 초기화 완료: {self.db_path}")

    def _init_database(self):
        """데이터베이스 테이블 생성"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 거래 기록 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_type TEXT NOT NULL,           -- BUY/SELL
                    stock_code TEXT NOT NULL,           -- 종목코드
                    stock_name TEXT,                    -- 종목명
                    quantity INTEGER NOT NULL,          -- 수량
                    price INTEGER NOT NULL,             -- 가격
                    total_amount INTEGER NOT NULL,      -- 총 거래금액
                    strategy_type TEXT,                 -- 전략 타입
                    timestamp DATETIME NOT NULL,        -- 거래 시간
                    order_id TEXT,                      -- 주문번호
                    status TEXT NOT NULL,               -- SUCCESS/FAILED
                    error_message TEXT,                 -- 오류 메시지
                    
                    -- 매도시 수익 정보
                    buy_trade_id INTEGER,               -- 연결된 매수 거래 ID
                    profit_loss INTEGER,                -- 손익 금액
                    profit_rate REAL,                   -- 수익률 (%)
                    holding_duration INTEGER,           -- 보유 시간 (분)
                    
                    -- 추가 정보
                    market_conditions TEXT,             -- 시장 상황 (JSON)
                    technical_indicators TEXT,          -- 기술적 지표 (JSON)
                    notes TEXT,                         -- 기타 메모
                    
                    FOREIGN KEY (buy_trade_id) REFERENCES trades(id)
                )
            """)
            
            # 일별 거래 요약 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_summary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date DATE NOT NULL UNIQUE,
                    total_trades INTEGER DEFAULT 0,
                    buy_trades INTEGER DEFAULT 0,
                    sell_trades INTEGER DEFAULT 0,
                    total_profit_loss INTEGER DEFAULT 0,
                    total_profit_rate REAL DEFAULT 0,
                    winning_trades INTEGER DEFAULT 0,
                    losing_trades INTEGER DEFAULT 0,
                    largest_profit INTEGER DEFAULT 0,
                    largest_loss INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 🆕 시간대별 종목 선정 기록 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS selected_stocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    selection_date DATE NOT NULL,        -- 선정 날짜
                    time_slot TEXT NOT NULL,             -- 시간대 (golden_time, morning_leaders, etc.)
                    slot_start_time TIME,                -- 시간대 시작 시간
                    slot_end_time TIME,                  -- 시간대 종료 시간
                    
                    stock_code TEXT NOT NULL,            -- 종목코드
                    stock_name TEXT,                     -- 종목명  
                    strategy_type TEXT NOT NULL,         -- 전략 타입 (gap_trading, volume_breakout, etc.)
                    score REAL NOT NULL,                 -- 종목 점수
                    reason TEXT,                         -- 선정 이유
                    rank_in_strategy INTEGER,            -- 전략 내 순위
                    
                    -- 선정 당시 시장 데이터
                    current_price INTEGER,               -- 현재가
                    change_rate REAL,                    -- 변화율 (%)
                    volume INTEGER,                      -- 거래량
                    volume_ratio REAL,                   -- 거래량 비율
                    market_cap INTEGER,                  -- 시가총액
                    
                    -- 추가 지표 (전략별로 다름)
                    gap_rate REAL,                       -- 갭 비율 (gap_trading)
                    momentum_strength REAL,              -- 모멘텀 강도 (momentum)
                    breakout_volume REAL,                -- 돌파 거래량 (volume_breakout)
                    technical_signals TEXT,              -- 기술적 신호 (JSON)
                    
                    -- 활성화 및 결과
                    is_activated BOOLEAN DEFAULT FALSE,  -- 실시간 모니터링 활성화 여부
                    activation_success BOOLEAN DEFAULT FALSE, -- 활성화 성공 여부
                    trade_executed BOOLEAN DEFAULT FALSE, -- 실제 거래 실행 여부
                    trade_id INTEGER,                    -- 연결된 거래 ID
                    
                    -- 메타 정보
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    notes TEXT,                          -- 기타 메모
                    
                    FOREIGN KEY (trade_id) REFERENCES trades(id)
                )
            """)
            
            # 시간대별 요약 테이블 (일별 통계용)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS time_slot_summary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    summary_date DATE NOT NULL,
                    time_slot TEXT NOT NULL,
                    
                    total_candidates INTEGER DEFAULT 0,  -- 총 후보 종목 수
                    activated_stocks INTEGER DEFAULT 0,  -- 활성화된 종목 수
                    traded_stocks INTEGER DEFAULT 0,     -- 실제 거래된 종목 수
                    
                    -- 전략별 통계
                    gap_trading_count INTEGER DEFAULT 0,
                    volume_breakout_count INTEGER DEFAULT 0,
                    momentum_count INTEGER DEFAULT 0,
                    
                    -- 성과 통계  
                    total_trades INTEGER DEFAULT 0,
                    successful_trades INTEGER DEFAULT 0,
                    total_profit_loss INTEGER DEFAULT 0,
                    avg_score REAL DEFAULT 0,
                    
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    
                    UNIQUE(summary_date, time_slot)
                )
            """)
            
            # 인덱스 생성
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_stock_code ON trades(stock_code)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_type ON trades(trade_type)")
            
            # 🆕 시간대별 종목 선정 인덱스
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_selected_date_slot ON selected_stocks(selection_date, time_slot)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_selected_stock_code ON selected_stocks(stock_code)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_selected_strategy ON selected_stocks(strategy_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_selected_score ON selected_stocks(score DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_time_slot_summary_date ON time_slot_summary(summary_date)")
            
            conn.commit()

    def record_buy_trade(self, stock_code: str, stock_name: str, quantity: int, 
                        price: int, total_amount: int, strategy_type: str, 
                        order_id: str = "", status: str = "SUCCESS", 
                        error_message: str = "", **kwargs) -> int:
        """매수 거래 기록"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 시장 상황 및 기술적 지표 JSON 직렬화
                market_conditions = json.dumps(kwargs.get('market_conditions', {}))
                technical_indicators = json.dumps(kwargs.get('technical_indicators', {}))
                
                cursor.execute("""
                    INSERT INTO trades (
                        trade_type, stock_code, stock_name, quantity, price, 
                        total_amount, strategy_type, timestamp, order_id, 
                        status, error_message, market_conditions, 
                        technical_indicators, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    'BUY', stock_code, stock_name, quantity, price,
                    total_amount, strategy_type, datetime.now(), order_id,
                    status, error_message, market_conditions,
                    technical_indicators, kwargs.get('notes', '')
                ))
                
                trade_id = cursor.lastrowid
                conn.commit()
                
                logger.info(f"💾 매수 기록 저장: {stock_code} {quantity}주 @{price:,}원 (ID: {trade_id})")
                
                # 일별 요약 업데이트
                self._update_daily_summary()
                
                return trade_id
                
        except Exception as e:
            logger.error(f"매수 기록 저장 오류: {e}")
            return -1

    def record_sell_trade(self, stock_code: str, stock_name: str, quantity: int, 
                         price: int, total_amount: int, strategy_type: str,
                         buy_trade_id: int = None, order_id: str = "", 
                         status: str = "SUCCESS", error_message: str = "", 
                         **kwargs) -> int:
        """매도 거래 기록"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 매수 거래 정보 조회 (수익률 계산용)
                profit_loss = 0
                profit_rate = 0.0
                holding_duration = 0
                
                if buy_trade_id:
                    cursor.execute("""
                        SELECT price, total_amount, timestamp 
                        FROM trades 
                        WHERE id = ? AND trade_type = 'BUY'
                    """, (buy_trade_id,))
                    
                    buy_record = cursor.fetchone()
                    if buy_record:
                        buy_price, buy_total, buy_time = buy_record
                        
                        # 손익 계산
                        profit_loss = total_amount - buy_total
                        profit_rate = (profit_loss / buy_total) * 100 if buy_total > 0 else 0
                        
                        # 보유 시간 계산 (분 단위)
                        buy_datetime = datetime.fromisoformat(buy_time)
                        holding_duration = int((datetime.now() - buy_datetime).total_seconds() / 60)
                
                # 시장 상황 및 기술적 지표 JSON 직렬화
                market_conditions = json.dumps(kwargs.get('market_conditions', {}))
                technical_indicators = json.dumps(kwargs.get('technical_indicators', {}))
                
                cursor.execute("""
                    INSERT INTO trades (
                        trade_type, stock_code, stock_name, quantity, price, 
                        total_amount, strategy_type, timestamp, order_id, 
                        status, error_message, buy_trade_id, profit_loss, 
                        profit_rate, holding_duration, market_conditions, 
                        technical_indicators, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    'SELL', stock_code, stock_name, quantity, price,
                    total_amount, strategy_type, datetime.now(), order_id,
                    status, error_message, buy_trade_id, profit_loss,
                    profit_rate, holding_duration, market_conditions,
                    technical_indicators, kwargs.get('notes', '')
                ))
                
                trade_id = cursor.lastrowid
                conn.commit()
                
                logger.info(f"💾 매도 기록 저장: {stock_code} {quantity}주 @{price:,}원 "
                          f"손익{profit_loss:+,}원 ({profit_rate:+.1f}%) (ID: {trade_id})")
                
                # 일별 요약 업데이트
                self._update_daily_summary()
                
                return trade_id
                
        except Exception as e:
            logger.error(f"매도 기록 저장 오류: {e}")
            return -1

    def get_open_positions(self) -> List[Dict]:
        """미결제 포지션 조회 (매수했지만 매도하지 않은 종목)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT b.id, b.stock_code, b.stock_name, b.quantity, 
                           b.price, b.total_amount, b.strategy_type, b.timestamp
                    FROM trades b
                    LEFT JOIN trades s ON b.id = s.buy_trade_id AND s.trade_type = 'SELL'
                    WHERE b.trade_type = 'BUY' 
                      AND b.status = 'SUCCESS'
                      AND s.id IS NULL
                    ORDER BY b.timestamp DESC
                """)
                
                positions = []
                for row in cursor.fetchall():
                    positions.append({
                        'buy_trade_id': row[0],
                        'stock_code': row[1],
                        'stock_name': row[2],
                        'quantity': row[3],
                        'buy_price': row[4],
                        'buy_amount': row[5],
                        'strategy_type': row[6],
                        'buy_time': row[7]
                    })
                
                return positions
                
        except Exception as e:
            logger.error(f"미결제 포지션 조회 오류: {e}")
            return []

    def find_buy_trade_for_sell(self, stock_code: str, quantity: int) -> Optional[int]:
        """매도할 종목의 해당하는 매수 거래 ID 찾기 (FIFO 방식)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 해당 종목의 미결제 매수 거래를 시간순으로 조회
                cursor.execute("""
                    SELECT b.id, b.quantity
                    FROM trades b
                    LEFT JOIN trades s ON b.id = s.buy_trade_id AND s.trade_type = 'SELL'
                    WHERE b.stock_code = ? 
                      AND b.trade_type = 'BUY' 
                      AND b.status = 'SUCCESS'
                      AND s.id IS NULL
                    ORDER BY b.timestamp ASC
                """, (stock_code,))
                
                remaining_quantity = quantity
                
                for buy_id, buy_quantity in cursor.fetchall():
                    if remaining_quantity <= 0:
                        break
                    
                    if buy_quantity >= remaining_quantity:
                        # 이 매수 거래로 충분함
                        return buy_id
                    else:
                        # 부분 매도 (복잡한 로직이므로 일단 첫 번째 매수 거래 반환)
                        remaining_quantity -= buy_quantity
                        return buy_id
                
                return None
                
        except Exception as e:
            logger.error(f"매수 거래 찾기 오류: {e}")
            return None

    def get_daily_summary(self, days: int = 7) -> List[Dict]:
        """최근 N일간 거래 요약"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=days-1)
                
                cursor.execute("""
                    SELECT * FROM daily_summary 
                    WHERE trade_date BETWEEN ? AND ?
                    ORDER BY trade_date DESC
                """, (start_date, end_date))
                
                columns = [desc[0] for desc in cursor.description]
                summaries = []
                
                for row in cursor.fetchall():
                    summary = dict(zip(columns, row))
                    summaries.append(summary)
                
                return summaries
                
        except Exception as e:
            logger.error(f"일별 요약 조회 오류: {e}")
            return []

    def get_trade_history(self, stock_code: str = None, days: int = 30, 
                         trade_type: str = None) -> List[Dict]:
        """거래 내역 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 조건 구성
                conditions = ["timestamp >= ?"]
                params = [datetime.now() - timedelta(days=days)]
                
                if stock_code:
                    conditions.append("stock_code = ?")
                    params.append(stock_code)
                
                if trade_type:
                    conditions.append("trade_type = ?")
                    params.append(trade_type.upper())
                
                query = f"""
                    SELECT * FROM trades 
                    WHERE {' AND '.join(conditions)}
                    ORDER BY timestamp DESC
                """
                
                cursor.execute(query, params)
                
                columns = [desc[0] for desc in cursor.description]
                trades = []
                
                for row in cursor.fetchall():
                    trade = dict(zip(columns, row))
                    # JSON 필드 파싱
                    if trade['market_conditions']:
                        trade['market_conditions'] = json.loads(trade['market_conditions'])
                    if trade['technical_indicators']:
                        trade['technical_indicators'] = json.loads(trade['technical_indicators'])
                    trades.append(trade)
                
                return trades
                
        except Exception as e:
            logger.error(f"거래 내역 조회 오류: {e}")
            return []

    def get_performance_stats(self, days: int = 30) -> Dict:
        """거래 성과 통계"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)
                
                # 기본 통계
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN trade_type = 'BUY' THEN 1 ELSE 0 END) as buy_trades,
                        SUM(CASE WHEN trade_type = 'SELL' THEN 1 ELSE 0 END) as sell_trades,
                        SUM(CASE WHEN trade_type = 'BUY' THEN total_amount ELSE 0 END) as total_buy_amount,
                        SUM(CASE WHEN trade_type = 'SELL' THEN total_amount ELSE 0 END) as total_sell_amount
                    FROM trades 
                    WHERE timestamp BETWEEN ? AND ? AND status = 'SUCCESS'
                """, (start_date, end_date))
                
                basic_stats = cursor.fetchone()
                
                # 매도 거래의 수익 통계
                cursor.execute("""
                    SELECT 
                        COUNT(*) as completed_trades,
                        SUM(profit_loss) as total_profit_loss,
                        AVG(profit_rate) as avg_profit_rate,
                        SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) as winning_trades,
                        SUM(CASE WHEN profit_loss < 0 THEN 1 ELSE 0 END) as losing_trades,
                        MAX(profit_loss) as max_profit,
                        MIN(profit_loss) as max_loss,
                        AVG(holding_duration) as avg_holding_minutes
                    FROM trades 
                    WHERE timestamp BETWEEN ? AND ? 
                      AND trade_type = 'SELL' 
                      AND status = 'SUCCESS'
                      AND profit_loss IS NOT NULL
                """, (start_date, end_date))
                
                profit_stats = cursor.fetchone()
                
                # 전략별 통계
                cursor.execute("""
                    SELECT 
                        strategy_type,
                        COUNT(*) as trade_count,
                        SUM(profit_loss) as strategy_profit,
                        AVG(profit_rate) as avg_profit_rate
                    FROM trades 
                    WHERE timestamp BETWEEN ? AND ? 
                      AND trade_type = 'SELL' 
                      AND status = 'SUCCESS'
                      AND profit_loss IS NOT NULL
                    GROUP BY strategy_type
                    ORDER BY strategy_profit DESC
                """, (start_date, end_date))
                
                strategy_stats = cursor.fetchall()
                
                # 결과 조합
                stats = {
                    'period_days': days,
                    'total_trades': basic_stats[0] or 0,
                    'buy_trades': basic_stats[1] or 0,
                    'sell_trades': basic_stats[2] or 0,
                    'total_buy_amount': basic_stats[3] or 0,
                    'total_sell_amount': basic_stats[4] or 0,
                    'completed_trades': profit_stats[0] or 0,
                    'total_profit_loss': profit_stats[1] or 0,
                    'avg_profit_rate': round(profit_stats[2] or 0, 2),
                    'winning_trades': profit_stats[3] or 0,
                    'losing_trades': profit_stats[4] or 0,
                    'max_profit': profit_stats[5] or 0,
                    'max_loss': profit_stats[6] or 0,
                    'avg_holding_minutes': round(profit_stats[7] or 0, 1),
                    'win_rate': 0,
                    'profit_factor': 0,
                    'strategy_performance': []
                }
                
                # 승률 계산
                if stats['completed_trades'] > 0:
                    stats['win_rate'] = round((stats['winning_trades'] / stats['completed_trades']) * 100, 1)
                
                # 수익 팩터 계산 (총 수익 / 총 손실)
                if stats['max_loss'] < 0:
                    total_profit = sum(row[2] for row in strategy_stats if row[2] > 0)
                    total_loss = abs(sum(row[2] for row in strategy_stats if row[2] < 0))
                    if total_loss > 0:
                        stats['profit_factor'] = round(total_profit / total_loss, 2)
                
                # 전략별 성과
                for row in strategy_stats:
                    stats['strategy_performance'].append({
                        'strategy': row[0],
                        'trade_count': row[1],
                        'total_profit': row[2],
                        'avg_profit_rate': round(row[3], 2)
                    })
                
                return stats
                
        except Exception as e:
            logger.error(f"성과 통계 조회 오류: {e}")
            return {}

    def _update_daily_summary(self):
        """일별 요약 업데이트"""
        try:
            today = datetime.now().date()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 오늘의 거래 통계 계산
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN trade_type = 'BUY' THEN 1 ELSE 0 END) as buy_trades,
                        SUM(CASE WHEN trade_type = 'SELL' THEN 1 ELSE 0 END) as sell_trades,
                        COALESCE(SUM(CASE WHEN trade_type = 'SELL' AND profit_loss IS NOT NULL THEN profit_loss ELSE 0 END), 0) as total_profit_loss,
                        COALESCE(AVG(CASE WHEN trade_type = 'SELL' AND profit_rate IS NOT NULL THEN profit_rate END), 0) as avg_profit_rate,
                        SUM(CASE WHEN trade_type = 'SELL' AND profit_loss > 0 THEN 1 ELSE 0 END) as winning_trades,
                        SUM(CASE WHEN trade_type = 'SELL' AND profit_loss < 0 THEN 1 ELSE 0 END) as losing_trades,
                        COALESCE(MAX(CASE WHEN trade_type = 'SELL' THEN profit_loss END), 0) as largest_profit,
                        COALESCE(MIN(CASE WHEN trade_type = 'SELL' THEN profit_loss END), 0) as largest_loss
                    FROM trades 
                    WHERE DATE(timestamp) = ? AND status = 'SUCCESS'
                """, (today,))
                
                stats = cursor.fetchone()
                
                # UPSERT (존재하면 업데이트, 없으면 삽입)
                cursor.execute("""
                    INSERT OR REPLACE INTO daily_summary (
                        trade_date, total_trades, buy_trades, sell_trades,
                        total_profit_loss, total_profit_rate, winning_trades, 
                        losing_trades, largest_profit, largest_loss, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    today, stats[0], stats[1], stats[2], stats[3], 
                    stats[4], stats[5], stats[6], stats[7], stats[8],
                    datetime.now()
                ))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"일별 요약 업데이트 오류: {e}")

    def export_trades_to_csv(self, filepath: str, days: int = 30) -> bool:
        """거래 내역 CSV 내보내기"""
        try:
            import csv
            
            trades = self.get_trade_history(days=days)
            
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                if not trades:
                    return False
                
                fieldnames = [
                    'timestamp', 'trade_type', 'stock_code', 'stock_name',
                    'quantity', 'price', 'total_amount', 'strategy_type',
                    'profit_loss', 'profit_rate', 'holding_duration',
                    'order_id', 'status'
                ]
                
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for trade in trades:
                    row = {field: trade.get(field, '') for field in fieldnames}
                    writer.writerow(row)
            
            logger.info(f"거래 내역 CSV 내보내기 완료: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"CSV 내보내기 오류: {e}")
            return False

    def cleanup_old_trades(self, days: int = 90):
        """오래된 거래 기록 정리"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    DELETE FROM trades 
                    WHERE timestamp < ? AND status != 'SUCCESS'
                """, (cutoff_date,))
                
                deleted_count = cursor.rowcount
                conn.commit()
                
                logger.info(f"오래된 거래 기록 {deleted_count}개 정리 완료")
                
        except Exception as e:
            logger.error(f"거래 기록 정리 오류: {e}")

    def get_database_stats(self) -> Dict:
        """데이터베이스 통계"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("SELECT COUNT(*) FROM trades")
                total_trades = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM daily_summary")
                total_days = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT MIN(timestamp), MAX(timestamp) 
                    FROM trades WHERE status = 'SUCCESS'
                """)
                date_range = cursor.fetchone()
                
                return {
                    'total_trades': total_trades,
                    'total_days': total_days,
                    'first_trade': date_range[0] if date_range[0] else None,
                    'last_trade': date_range[1] if date_range[1] else None,
                    'database_size': self.db_path.stat().st_size if self.db_path.exists() else 0
                }
                
        except Exception as e:
            logger.error(f"데이터베이스 통계 조회 오류: {e}")
            return {}

    # ========== 🆕 시간대별 종목 선정 관련 메서드들 ==========

    def record_selected_stocks(self, time_slot: str, slot_start_time: str, slot_end_time: str, 
                              stock_candidates: List[Dict]) -> List[int]:
        """시간대별 선정된 종목들을 기록"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                today = datetime.now().date()
                recorded_ids = []
                
                for i, candidate in enumerate(stock_candidates):
                    try:
                        # 기술적 신호 JSON 직렬화
                        technical_signals = json.dumps(candidate.get('technical_signals', {}))
                        
                        cursor.execute("""
                            INSERT INTO selected_stocks (
                                selection_date, time_slot, slot_start_time, slot_end_time,
                                stock_code, stock_name, strategy_type, score, reason, rank_in_strategy,
                                current_price, change_rate, volume, volume_ratio, market_cap,
                                gap_rate, momentum_strength, breakout_volume, technical_signals,
                                notes
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            today, time_slot, slot_start_time, slot_end_time,
                            candidate.get('stock_code', ''),
                            candidate.get('stock_name', ''),
                            candidate.get('strategy_type', ''),
                            candidate.get('score', 0.0),
                            candidate.get('reason', ''),
                            i + 1,  # rank_in_strategy
                            candidate.get('current_price', 0),
                            candidate.get('change_rate', 0.0),
                            candidate.get('volume', 0),
                            candidate.get('volume_ratio', 0.0),
                            candidate.get('market_cap', 0),
                            candidate.get('gap_rate', 0.0),
                            candidate.get('momentum_strength', 0.0),
                            candidate.get('breakout_volume', 0.0),
                            technical_signals,
                            candidate.get('notes', '')
                        ))
                        
                        recorded_ids.append(cursor.lastrowid)
                        
                    except Exception as e:
                        logger.error(f"종목 선정 기록 오류 ({candidate.get('stock_code', 'Unknown')}): {e}")
                        continue
                
                conn.commit()
                
                logger.info(f"💾 {time_slot} 시간대 종목 선정 기록: {len(recorded_ids)}개 종목")
                
                # 시간대별 요약 업데이트
                self._update_time_slot_summary(today, time_slot)
                
                return recorded_ids
                
        except Exception as e:
            logger.error(f"시간대별 종목 선정 기록 오류: {e}")
            return []

    def update_stock_activation(self, stock_code: str, is_activated: bool, activation_success: bool = False):
        """종목 활성화 상태 업데이트"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                today = datetime.now().date()
                
                cursor.execute("""
                    UPDATE selected_stocks 
                    SET is_activated = ?, activation_success = ?
                    WHERE stock_code = ? AND selection_date = ?
                """, (is_activated, activation_success, stock_code, today))
                
                conn.commit()
                
                if cursor.rowcount > 0:
                    logger.debug(f"종목 활성화 상태 업데이트: {stock_code} (활성화: {is_activated})")
                
        except Exception as e:
            logger.error(f"종목 활성화 상태 업데이트 오류: {e}")

    def link_trade_to_selected_stock(self, stock_code: str, trade_id: int):
        """거래와 선정된 종목 연결"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                today = datetime.now().date()
                
                cursor.execute("""
                    UPDATE selected_stocks 
                    SET trade_executed = TRUE, trade_id = ?
                    WHERE stock_code = ? AND selection_date = ?
                """, (trade_id, stock_code, today))
                
                conn.commit()
                
                if cursor.rowcount > 0:
                    logger.info(f"거래 연결 완료: {stock_code} → 거래 ID {trade_id}")
                
        except Exception as e:
            logger.error(f"거래 연결 오류: {e}")

    def get_selected_stocks_by_date(self, target_date: str = None, time_slot: str = None) -> List[Dict]:
        """날짜별/시간대별 선정된 종목 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 기본값: 오늘
                if target_date is None:
                    target_date = datetime.now().date()
                
                # 쿼리 조건 구성
                query = """
                    SELECT 
                        id, selection_date, time_slot, slot_start_time, slot_end_time,
                        stock_code, stock_name, strategy_type, score, reason, rank_in_strategy,
                        current_price, change_rate, volume, volume_ratio, market_cap,
                        gap_rate, momentum_strength, breakout_volume, technical_signals,
                        is_activated, activation_success, trade_executed, trade_id,
                        created_at, notes
                    FROM selected_stocks 
                    WHERE selection_date = ?
                """
                params = [target_date]
                
                if time_slot:
                    query += " AND time_slot = ?"
                    params.append(time_slot)
                
                query += " ORDER BY time_slot, strategy_type, rank_in_strategy"
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                results = []
                for row in rows:
                    try:
                        technical_signals = json.loads(row[19]) if row[19] else {}
                    except:
                        technical_signals = {}
                    
                    results.append({
                        'id': row[0],
                        'selection_date': row[1],
                        'time_slot': row[2],
                        'slot_start_time': row[3],
                        'slot_end_time': row[4],
                        'stock_code': row[5],
                        'stock_name': row[6],
                        'strategy_type': row[7],
                        'score': row[8],
                        'reason': row[9],
                        'rank_in_strategy': row[10],
                        'current_price': row[11],
                        'change_rate': row[12],
                        'volume': row[13],
                        'volume_ratio': row[14],
                        'market_cap': row[15],
                        'gap_rate': row[16],
                        'momentum_strength': row[17],
                        'breakout_volume': row[18],
                        'technical_signals': technical_signals,
                        'is_activated': bool(row[20]),
                        'activation_success': bool(row[21]),
                        'trade_executed': bool(row[22]),
                        'trade_id': row[23],
                        'created_at': row[24],
                        'notes': row[25]
                    })
                
                return results
                
        except Exception as e:
            logger.error(f"선정 종목 조회 오류: {e}")
            return []

    def get_time_slot_performance(self, days: int = 7) -> List[Dict]:
        """시간대별 성과 분석"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                start_date = datetime.now() - timedelta(days=days)
                
                cursor.execute("""
                    SELECT 
                        s.time_slot,
                        s.strategy_type,
                        COUNT(*) as total_candidates,
                        SUM(CASE WHEN s.is_activated THEN 1 ELSE 0 END) as activated_count,
                        SUM(CASE WHEN s.trade_executed THEN 1 ELSE 0 END) as traded_count,
                        AVG(s.score) as avg_score,
                        COUNT(t.id) as completed_trades,
                        COALESCE(SUM(t.profit_loss), 0) as total_profit,
                        COALESCE(AVG(t.profit_rate), 0) as avg_profit_rate,
                        SUM(CASE WHEN t.profit_loss > 0 THEN 1 ELSE 0 END) as winning_trades
                    FROM selected_stocks s
                    LEFT JOIN trades t ON s.trade_id = t.id AND t.trade_type = 'SELL'
                    WHERE s.selection_date >= ?
                    GROUP BY s.time_slot, s.strategy_type
                    ORDER BY s.time_slot, total_profit DESC
                """, (start_date.date(),))
                
                rows = cursor.fetchall()
                
                results = []
                for row in rows:
                    win_rate = (row[9] / row[6] * 100) if row[6] > 0 else 0
                    activation_rate = (row[4] / row[2] * 100) if row[2] > 0 else 0
                    execution_rate = (row[5] / row[4] * 100) if row[4] > 0 else 0
                    
                    results.append({
                        'time_slot': row[0],
                        'strategy_type': row[1],
                        'total_candidates': row[2],
                        'activated_count': row[3],
                        'traded_count': row[4],
                        'avg_score': round(row[5], 2),
                        'completed_trades': row[6],
                        'total_profit': row[7],
                        'avg_profit_rate': round(row[8], 2),
                        'winning_trades': row[9],
                        'win_rate': round(win_rate, 1),
                        'activation_rate': round(activation_rate, 1),
                        'execution_rate': round(execution_rate, 1)
                    })
                
                return results
                
        except Exception as e:
            logger.error(f"시간대별 성과 분석 오류: {e}")
            return []

    def _update_time_slot_summary(self, target_date: str, time_slot: str):
        """시간대별 요약 통계 업데이트"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 해당 날짜/시간대의 통계 계산
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_candidates,
                        SUM(CASE WHEN is_activated THEN 1 ELSE 0 END) as activated_stocks,
                        SUM(CASE WHEN trade_executed THEN 1 ELSE 0 END) as traded_stocks,
                        SUM(CASE WHEN strategy_type = 'gap_trading' THEN 1 ELSE 0 END) as gap_trading_count,
                        SUM(CASE WHEN strategy_type = 'volume_breakout' THEN 1 ELSE 0 END) as volume_breakout_count,
                        SUM(CASE WHEN strategy_type = 'momentum' THEN 1 ELSE 0 END) as momentum_count,
                        AVG(score) as avg_score
                    FROM selected_stocks 
                    WHERE selection_date = ? AND time_slot = ?
                """, (target_date, time_slot))
                
                stats = cursor.fetchone()
                
                # 해당 시간대에서 실행된 거래의 성과
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) as successful_trades,
                        COALESCE(SUM(profit_loss), 0) as total_profit_loss
                    FROM selected_stocks s
                    JOIN trades t ON s.trade_id = t.id
                    WHERE s.selection_date = ? AND s.time_slot = ? AND t.trade_type = 'SELL'
                """, (target_date, time_slot))
                
                trade_stats = cursor.fetchone()
                
                # UPSERT 실행
                cursor.execute("""
                    INSERT OR REPLACE INTO time_slot_summary (
                        summary_date, time_slot, total_candidates, activated_stocks, traded_stocks,
                        gap_trading_count, volume_breakout_count, momentum_count,
                        total_trades, successful_trades, total_profit_loss, avg_score, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    target_date, time_slot, stats[0], stats[1], stats[2],
                    stats[3], stats[4], stats[5],
                    trade_stats[0], trade_stats[1], trade_stats[2], 
                    round(stats[6] or 0, 2), datetime.now()
                ))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"시간대별 요약 업데이트 오류: {e}")

    def export_selected_stocks_to_csv(self, filepath: str, days: int = 7) -> bool:
        """선정된 종목 내역 CSV 내보내기"""
        try:
            import csv
            
            start_date = datetime.now() - timedelta(days=days)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT 
                        selection_date, time_slot, stock_code, stock_name, strategy_type,
                        score, reason, rank_in_strategy, current_price, change_rate,
                        volume, volume_ratio, gap_rate, momentum_strength, breakout_volume,
                        is_activated, activation_success, trade_executed, created_at
                    FROM selected_stocks 
                    WHERE selection_date >= ?
                    ORDER BY selection_date DESC, time_slot, rank_in_strategy
                """, (start_date.date(),))
                
                rows = cursor.fetchall()
                
                if not rows:
                    return False
                
                with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = [
                        'selection_date', 'time_slot', 'stock_code', 'stock_name', 'strategy_type',
                        'score', 'reason', 'rank_in_strategy', 'current_price', 'change_rate',
                        'volume', 'volume_ratio', 'gap_rate', 'momentum_strength', 'breakout_volume',
                        'is_activated', 'activation_success', 'trade_executed', 'created_at'
                    ]
                    
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for row in rows:
                        writer.writerow(dict(zip(fieldnames, row)))
                
                logger.info(f"선정 종목 CSV 내보내기 완료: {filepath}")
                return True
                
        except Exception as e:
            logger.error(f"선정 종목 CSV 내보내기 오류: {e}")
            return False

    def check_existing_position_recorded(self, stock_code: str) -> bool:
        """해당 종목의 기존 보유 기록이 오늘 이미 있는지 확인"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                today = datetime.now().date()
                
                cursor.execute("""
                    SELECT COUNT(*) FROM trades 
                    WHERE stock_code = ? 
                      AND trade_type = 'BUY' 
                      AND strategy_type = 'existing_holding'
                      AND order_id = 'EXISTING_POSITION'
                      AND DATE(timestamp) = ?
                """, (stock_code, today))
                
                count = cursor.fetchone()[0]
                return count > 0
                
        except Exception as e:
            logger.error(f"기존 보유 기록 확인 오류: {e}")
            return False

    def record_existing_position_if_not_exists(self, stock_code: str, stock_name: str, 
                                              quantity: int, avg_price: int, current_price: int) -> int:
        """기존 보유 종목을 중복 체크 후 기록"""
        try:
            # 오늘 이미 기록된 기존 보유 종목인지 확인
            if self.check_existing_position_recorded(stock_code):
                logger.debug(f"기존 보유 종목 이미 기록됨: {stock_code}")
                return -1
            
            # 🆕 순수 기존 보유 종목으로 기록
            total_amount = quantity * avg_price
            trade_id = self.record_buy_trade(
                stock_code=stock_code,
                stock_name=stock_name,
                quantity=quantity,
                price=avg_price,
                total_amount=total_amount,
                strategy_type="existing_holding",  # 🆕 단순히 기존 보유로 기록
                order_id="EXISTING_POSITION",
                status='SUCCESS',
                market_conditions={
                    'current_price': current_price,
                    'source': 'existing_position_setup',
                    'setup_time': datetime.now().isoformat(),
                    'avg_price': avg_price
                },
                notes=f"프로그램 시작시 기존 보유 종목 등록 (평균가: {avg_price:,}원, 현재가: {current_price:,}원)"
            )
            
            return trade_id
            
        except Exception as e:
            logger.error(f"기존 보유 종목 기록 오류: {e}")
            return -1

    def get_existing_positions(self) -> List[Dict]:
        """기존 보유 종목들 조회 (existing_holding 전략의 미결제 포지션)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT b.id, b.stock_code, b.stock_name, b.quantity, 
                           b.price as avg_buy_price, b.total_amount, b.timestamp
                    FROM trades b
                    LEFT JOIN trades s ON b.id = s.buy_trade_id AND s.trade_type = 'SELL'
                    WHERE b.trade_type = 'BUY' 
                      AND b.status = 'SUCCESS'
                      AND b.strategy_type = 'existing_holding'
                      AND s.id IS NULL
                    ORDER BY b.timestamp DESC
                """)
                
                positions = []
                for row in cursor.fetchall():
                    positions.append({
                        'buy_trade_id': row[0],
                        'stock_code': row[1],
                        'stock_name': row[2],
                        'quantity': row[3],
                        'avg_buy_price': row[4],
                        'total_investment': row[5],
                        'buy_time': row[6],
                        # 실시간 수익률은 별도 계산 필요 (현재가 정보 필요)
                        'unrealized_pnl': 0,
                        'unrealized_pnl_rate': 0.0
                    })
                
                return positions
                
        except Exception as e:
            logger.error(f"기존 보유 종목 조회 오류: {e}")
            return [] 