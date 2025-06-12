"""
비동기 데이터 저장 시스템
매수 시도/실패, 신호 분석 데이터를 머신러닝용으로 비동기 저장
"""
import asyncio
import sqlite3
import json
import time
import threading
from datetime import datetime
from typing import Dict, List, Optional, Any
from queue import Queue, Empty
from pathlib import Path
from utils.logger import setup_logger

logger = setup_logger(__name__)

class AsyncDataLogger:
    """💾 비동기 데이터 저장 시스템 (머신러닝용)"""

    def __init__(self, db_path: str = "data/ml_training_data.db", max_queue_size: int = 10000):
        """초기화"""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True)
        
        # 🚀 비동기 처리용 큐들
        self.signal_queue = Queue(maxsize=max_queue_size)
        self.buy_attempt_queue = Queue(maxsize=max_queue_size)
        self.market_state_queue = Queue(maxsize=max_queue_size)
        
        # 🔧 설정
        self.max_queue_size = max_queue_size
        self.batch_size = 100  # 배치 단위로 DB 저장
        self.flush_interval = 30  # 30초마다 강제 플러시
        
        # 🎯 상태 관리
        self.is_running = False
        self.worker_threads = []
        self.stats = {
            'signals_logged': 0,
            'buy_attempts_logged': 0,
            'market_states_logged': 0,
            'db_writes': 0,
            'errors': 0
        }
        
        # 데이터베이스 초기화
        self._init_database()
        
        # 워커 스레드 시작
        self.start_workers()
        
        logger.info(f"🚀 비동기 데이터 로거 초기화 완료: {self.db_path}")

    def _init_database(self):
        """🤖 머신러닝용 데이터베이스 테이블 생성 - 확장된 버전"""
        try:
            with sqlite3.connect(str(self.db_path), timeout=30.0) as conn:
                cursor = conn.cursor()
                
                # 1. 📊 신호 분석 데이터 (매수 조건 못 미친 종목들) - 확장
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS signal_analysis (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        stock_code TEXT NOT NULL,
                        stock_name TEXT,
                        
                        -- 신호 정보
                        strategy_type TEXT NOT NULL,
                        signal_strength REAL NOT NULL,
                        signal_threshold REAL NOT NULL,
                        signal_passed BOOLEAN NOT NULL,
                        signal_reason TEXT,
                        
                        -- 🆕 기본 가격 정보
                        current_price INTEGER NOT NULL,
                        open_price INTEGER,
                        high_price INTEGER,
                        low_price INTEGER,
                        prev_close INTEGER,
                        price_change INTEGER,
                        price_change_pct REAL,
                        
                        -- 🆕 확장된 거래량 정보
                        volume INTEGER,
                        volume_power REAL,             -- 체결강도
                        avg_volume_5 INTEGER,          -- 5일 평균 거래량
                        avg_volume_20 INTEGER,         -- 20일 평균 거래량
                        avg_volume_60 INTEGER,         -- 60일 평균 거래량
                        volume_ratio_5d REAL,          -- 5일 대비 거래량 비율
                        volume_ratio_20d REAL,         -- 20일 대비 거래량 비율
                        
                        -- 🆕 확장된 기술적 지표
                        rsi REAL,
                        rsi_9 REAL,                    -- 9일 RSI
                        rsi_14 REAL,                   -- 14일 RSI
                        macd REAL,
                        macd_signal REAL,
                        macd_histogram REAL,
                        bb_upper REAL,                 -- 볼린저 밴드 상단
                        bb_middle REAL,                -- 볼린저 밴드 중앙
                        bb_lower REAL,                 -- 볼린저 밴드 하단
                        bb_position REAL,              -- 볼린저 밴드 내 위치 (0~1)
                        bb_width REAL,                 -- 볼린저 밴드 폭
                        
                        -- 🆕 이동평균선
                        ma5 INTEGER,
                        ma10 INTEGER,
                        ma20 INTEGER,
                        ma60 INTEGER,
                        ma120 INTEGER,
                        
                        -- 🆕 이격도 (다중 기간)
                        disparity_5d REAL,
                        disparity_10d REAL,
                        disparity_20d REAL,
                        disparity_60d REAL,
                        disparity_120d REAL,
                        
                        -- 🆕 모멘텀 지표
                        momentum_5d REAL,              -- 5일 모멘텀
                        momentum_10d REAL,             -- 10일 모멘텀
                        momentum_20d REAL,             -- 20일 모멘텀
                        rate_of_change REAL,           -- 변화율
                        
                        -- 🆕 변동성 지표
                        volatility_5d REAL,            -- 5일 변동성
                        volatility_20d REAL,           -- 20일 변동성
                        atr REAL,                      -- Average True Range
                        
                        -- 🆕 시장 정보
                        market_cap INTEGER,
                        sector TEXT,
                        market_type TEXT,              -- KOSPI/KOSDAQ
                        listing_date TEXT,             -- 상장일
                        foreign_ownership_pct REAL,    -- 외국인 지분율
                        
                        -- 🆕 호가 정보
                        bid_ask_spread REAL,           -- 호가 스프레드
                        bid_volume INTEGER,            -- 매수 호가량
                        ask_volume INTEGER,            -- 매도 호가량
                        bid_ask_ratio REAL,            -- 매수/매도 호가 비율
                        
                        -- 🆕 시간대 정보
                        hour_of_day INTEGER,
                        minute_of_hour INTEGER,
                        day_of_week INTEGER,           -- 요일 (0=월요일)
                        is_opening_hour BOOLEAN,       -- 시가 시간대
                        is_closing_hour BOOLEAN,       -- 종가 시간대
                        
                        -- 🆕 과거 성과 정보
                        performance_1d REAL,           -- 1일 전 수익률
                        performance_3d REAL,           -- 3일 전 수익률
                        performance_1w REAL,           -- 1주 전 수익률
                        performance_1m REAL,           -- 1개월 전 수익률
                        
                        -- 결과 (나중에 업데이트)
                        price_1h_later INTEGER,
                        price_4h_later INTEGER,
                        price_1d_later INTEGER,
                        price_1w_later INTEGER,
                        max_price_24h INTEGER,
                        min_price_24h INTEGER,
                        
                        -- 메타 데이터
                        raw_data_json TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 2. 📈 매수 시도 기록 (성공/실패 모두) - 확장
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS buy_attempts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        stock_code TEXT NOT NULL,
                        stock_name TEXT,
                        
                        -- 시도 정보
                        attempt_result TEXT NOT NULL,  -- SUCCESS, FAILED_VALIDATION, FAILED_FUNDS, etc.
                        failure_reason TEXT,
                        
                        -- 신호 정보
                        signal_strength REAL,
                        strategy_type TEXT,
                        signal_data_json TEXT,
                        
                        -- 매수 정보 (성공한 경우)
                        buy_price INTEGER,
                        quantity INTEGER,
                        total_amount INTEGER,
                        
                        -- 🆕 상세 검증 정보
                        validation_checks TEXT,        -- JSON으로 각 검증 단계 결과
                        disparity_check_passed BOOLEAN,
                        volume_check_passed BOOLEAN,
                        price_check_passed BOOLEAN,
                        balance_check_passed BOOLEAN,
                        
                        -- 🆕 시장 상황
                        market_condition TEXT,
                        kospi_value REAL,
                        kosdaq_value REAL,
                        market_volatility REAL,
                        portfolio_status TEXT,
                        available_cash INTEGER,
                        current_positions_count INTEGER,
                        
                        -- 🆕 경쟁 종목 정보
                        competing_signals_count INTEGER,  -- 동시에 발생한 다른 신호 개수
                        signal_priority_rank INTEGER,     -- 신호 우선순위 순위
                        
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 3. 🌍 시장 상태 스냅샷 - 확장
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS market_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        
                        -- 지수 정보
                        kospi_value REAL,
                        kosdaq_value REAL,
                        kospi_change_pct REAL,
                        kosdaq_change_pct REAL,
                        
                        -- 거래량
                        kospi_volume INTEGER,
                        kosdaq_volume INTEGER,
                        
                        -- 상승/하락 종목 수
                        rising_stocks INTEGER,
                        falling_stocks INTEGER,
                        unchanged_stocks INTEGER,
                        
                        -- 🆕 확장된 시장 지표
                        market_volatility REAL,
                        vix_korea REAL,
                        fear_greed_index REAL,         -- 공포/탐욕 지수
                        
                        -- 🆕 섹터별 정보
                        sector_performance_json TEXT,   -- 섹터별 수익률
                        hot_sectors JSON,               -- 상승 섹터들
                        cold_sectors JSON,              -- 하락 섹터들
                        
                        -- 🆕 외국인/기관 동향
                        foreign_net_buying INTEGER,    -- 외국인 순매수
                        institution_net_buying INTEGER, -- 기관 순매수
                        individual_net_buying INTEGER,  -- 개인 순매수
                        
                        -- 🆕 거래 동향
                        top_volume_stocks JSON,        -- 거래량 상위 종목
                        top_rising_stocks JSON,        -- 상승률 상위 종목
                        top_falling_stocks JSON,       -- 하락률 상위 종목
                        
                        -- 시간대
                        hour_of_day INTEGER,
                        is_opening BOOLEAN,
                        is_closing BOOLEAN,
                        
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 4. 📋 라벨링된 데이터 (학습용) - 확장
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS labeled_features (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        signal_analysis_id INTEGER,
                        
                        -- 피처 벡터 (정규화된 값들)
                        features_json TEXT NOT NULL,
                        features_version TEXT DEFAULT 'v2.0',
                        
                        -- 🆕 다양한 기간의 라벨 
                        label_30m REAL,                -- 30분 후 수익률
                        label_1h REAL,                 -- 1시간 후 수익률
                        label_2h REAL,                 -- 2시간 후 수익률
                        label_4h REAL,                 -- 4시간 후 수익률  
                        label_1d REAL,                 -- 1일 후 수익률
                        label_3d REAL,                 -- 3일 후 수익률
                        label_1w REAL,                 -- 1주 후 수익률
                        
                        -- 🆕 최대/최소 수익률
                        label_max_1h REAL,             -- 1시간 내 최대 수익률
                        label_max_4h REAL,             -- 4시간 내 최대 수익률
                        label_max_1d REAL,             -- 1일 내 최대 수익률
                        label_min_1h REAL,             -- 1시간 내 최소 수익률 (최대 손실)
                        label_min_4h REAL,             -- 4시간 내 최소 수익률
                        label_min_1d REAL,             -- 1일 내 최소 수익률
                        
                        -- 🆕 다양한 임계점 분류 라벨
                        is_profitable_1h BOOLEAN,      -- 1시간 후 수익
                        is_profitable_4h BOOLEAN,      -- 4시간 후 수익
                        is_profitable_1d BOOLEAN,      -- 1일 후 수익
                        
                        is_target_2pct_1h BOOLEAN,     -- 1시간 내 2% 달성
                        is_target_5pct_1d BOOLEAN,     -- 1일 내 5% 달성
                        is_target_10pct_1w BOOLEAN,    -- 1주 내 10% 달성
                        
                        is_stoploss_3pct BOOLEAN,      -- 3% 손절 히트
                        is_stoploss_5pct BOOLEAN,      -- 5% 손절 히트
                        
                        -- 🆕 거래 품질 라벨
                        trade_quality_score REAL,      -- 거래 품질 점수 (0~1)
                        risk_adjusted_return REAL,     -- 위험 조정 수익률
                        sharpe_ratio REAL,             -- 샤프 비율
                        max_drawdown REAL,             -- 최대 낙폭
                        
                        is_good_trade BOOLEAN,          -- 종합 판정
                        is_excellent_trade BOOLEAN,    -- 우수 거래 판정
                        
                        -- 메타 정보
                        labeled_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        
                        FOREIGN KEY (signal_analysis_id) REFERENCES signal_analysis(id)
                    )
                """)
                
                # 🆕 5. 종목별 히스토리 테이블 (시계열 데이터)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS stock_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stock_code TEXT NOT NULL,
                        date DATE NOT NULL,
                        
                        -- OHLCV
                        open_price INTEGER,
                        high_price INTEGER,
                        low_price INTEGER,
                        close_price INTEGER,
                        volume INTEGER,
                        
                        -- 기술적 지표 (일봉 기준)
                        rsi_14 REAL,
                        macd REAL,
                        macd_signal REAL,
                        bb_upper REAL,
                        bb_lower REAL,
                        ma20 REAL,
                        ma60 REAL,
                        
                        -- 거래 정보
                        foreign_net INTEGER,           -- 외국인 순매수
                        institution_net INTEGER,       -- 기관 순매수
                        
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(stock_code, date)
                    )
                """)
                
                # 🆕 6. 뉴스/이벤트 테이블
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS news_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        stock_code TEXT,
                        
                        -- 뉴스 정보
                        news_type TEXT,                -- EARNINGS, NEWS, DISCLOSURE, etc.
                        title TEXT,
                        content TEXT,
                        sentiment_score REAL,          -- 감정 점수 (-1~1)
                        importance_score REAL,        -- 중요도 점수 (0~1)
                        
                        -- 영향도
                        price_impact_1h REAL,         -- 1시간 후 가격 영향
                        price_impact_1d REAL,         -- 1일 후 가격 영향
                        volume_impact REAL,           -- 거래량 영향
                        
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 인덱스 생성 (성능 최적화)
                indexes = [
                    # 기존 인덱스
                    "CREATE INDEX IF NOT EXISTS idx_signal_timestamp ON signal_analysis(timestamp)",
                    "CREATE INDEX IF NOT EXISTS idx_signal_stock ON signal_analysis(stock_code)",
                    "CREATE INDEX IF NOT EXISTS idx_signal_strategy ON signal_analysis(strategy_type)",
                    "CREATE INDEX IF NOT EXISTS idx_signal_passed ON signal_analysis(signal_passed)",
                    "CREATE INDEX IF NOT EXISTS idx_buy_timestamp ON buy_attempts(timestamp)",
                    "CREATE INDEX IF NOT EXISTS idx_buy_stock ON buy_attempts(stock_code)",
                    "CREATE INDEX IF NOT EXISTS idx_buy_result ON buy_attempts(attempt_result)",
                    "CREATE INDEX IF NOT EXISTS idx_market_timestamp ON market_snapshots(timestamp)",
                    "CREATE INDEX IF NOT EXISTS idx_labeled_signal_id ON labeled_features(signal_analysis_id)",
                    
                    # 🆕 새 인덱스들
                    "CREATE INDEX IF NOT EXISTS idx_signal_hour ON signal_analysis(hour_of_day)",
                    "CREATE INDEX IF NOT EXISTS idx_signal_disparity ON signal_analysis(disparity_20d)",
                    "CREATE INDEX IF NOT EXISTS idx_signal_volume_ratio ON signal_analysis(volume_ratio_20d)",
                    "CREATE INDEX IF NOT EXISTS idx_buy_strategy ON buy_attempts(strategy_type)",
                    "CREATE INDEX IF NOT EXISTS idx_stock_history_code_date ON stock_history(stock_code, date)",
                    "CREATE INDEX IF NOT EXISTS idx_news_stock_time ON news_events(stock_code, timestamp)",
                    "CREATE INDEX IF NOT EXISTS idx_labeled_quality ON labeled_features(trade_quality_score)",
                    "CREATE INDEX IF NOT EXISTS idx_labeled_profitable ON labeled_features(is_good_trade)",
                ]
                
                for index_sql in indexes:
                    cursor.execute(index_sql)
                
                conn.commit()
                logger.info("✅ 머신러닝용 확장 데이터베이스 테이블 생성 완료")
                
        except Exception as e:
            logger.error(f"❌ 데이터베이스 초기화 오류: {e}")
            raise

    def start_workers(self):
        """워커 스레드들 시작"""
        if self.is_running:
            return
            
        self.is_running = True
        
        # 📊 신호 데이터 처리 워커
        signal_worker = threading.Thread(
            target=self._signal_worker,
            name="SignalDataWorker",
            daemon=True
        )
        
        # 📈 매수 시도 처리 워커  
        buy_worker = threading.Thread(
            target=self._buy_attempt_worker,
            name="BuyAttemptWorker",
            daemon=True
        )
        
        # 🌍 시장 상태 처리 워커
        market_worker = threading.Thread(
            target=self._market_worker,
            name="MarketDataWorker", 
            daemon=True
        )
        
        self.worker_threads = [signal_worker, buy_worker, market_worker]
        
        for worker in self.worker_threads:
            worker.start()
            
        logger.info("🚀 비동기 데이터 로거 워커 스레드 시작")

    def log_signal_analysis(self, signal_data: Dict[str, Any]):
        """📊 신호 분석 데이터 로깅 (비동기)"""
        try:
            # 타임스탬프 추가
            signal_data['logged_at'] = time.time()
            
            # 큐에 추가 (논블로킹)
            if not self.signal_queue.full():
                self.signal_queue.put_nowait(signal_data)
                logger.debug(f"🔍 신호 분석 데이터 큐 추가: {signal_data.get('stock_code', 'Unknown')}")
            else:
                logger.warning(f"⚠️ 신호 분석 큐 가득참 - 데이터 무시: {signal_data.get('stock_code', 'Unknown')}")
                self.stats['errors'] += 1
                
        except Exception as e:
            logger.error(f"❌ 신호 분석 데이터 로깅 오류: {e}")
            self.stats['errors'] += 1

    def log_buy_attempt(self, attempt_data: Dict[str, Any]):
        """📈 매수 시도 데이터 로깅 (비동기)"""
        try:
            # 타임스탬프 추가
            attempt_data['logged_at'] = time.time()
            
            # 큐에 추가 (논블로킹)
            if not self.buy_attempt_queue.full():
                self.buy_attempt_queue.put_nowait(attempt_data)
                logger.debug(f"💰 매수 시도 데이터 큐 추가: {attempt_data.get('stock_code', 'Unknown')}")
            else:
                logger.warning(f"⚠️ 매수 시도 큐 가득참 - 데이터 무시: {attempt_data.get('stock_code', 'Unknown')}")
                self.stats['errors'] += 1
                
        except Exception as e:
            logger.error(f"❌ 매수 시도 데이터 로깅 오류: {e}")
            self.stats['errors'] += 1

    def log_market_snapshot(self, market_data: Dict[str, Any]):
        """🌍 시장 상태 스냅샷 로깅 (비동기)"""
        try:
            # 타임스탬프 추가
            market_data['logged_at'] = time.time()
            
            # 큐에 추가 (논블로킹)
            if not self.market_state_queue.full():
                self.market_state_queue.put_nowait(market_data)
                logger.debug("🌍 시장 상태 스냅샷 큐 추가")
            else:
                logger.warning("⚠️ 시장 상태 큐 가득참 - 데이터 무시")
                self.stats['errors'] += 1
                
        except Exception as e:
            logger.error(f"❌ 시장 상태 스냅샷 로깅 오류: {e}")
            self.stats['errors'] += 1

    def _signal_worker(self):
        """📊 신호 데이터 처리 워커"""
        batch = []
        last_flush = time.time()
        
        while self.is_running:
            try:
                # 큐에서 데이터 가져오기 (타임아웃 1초)
                try:
                    data = self.signal_queue.get(timeout=1.0)
                    batch.append(data)
                except Empty:
                    pass
                
                # 배치 크기 도달 또는 시간 초과시 DB 저장
                current_time = time.time()
                should_flush = (
                    len(batch) >= self.batch_size or 
                    (batch and current_time - last_flush >= self.flush_interval)
                )
                
                if should_flush:
                    self._save_signal_batch(batch)
                    batch.clear()
                    last_flush = current_time
                    
            except Exception as e:
                logger.error(f"❌ 신호 워커 오류: {e}")
                time.sleep(1)
        
        # 종료시 남은 데이터 처리
        if batch:
            self._save_signal_batch(batch)

    def _buy_attempt_worker(self):
        """📈 매수 시도 처리 워커"""
        batch = []
        last_flush = time.time()
        
        while self.is_running:
            try:
                # 큐에서 데이터 가져오기 (타임아웃 1초)
                try:
                    data = self.buy_attempt_queue.get(timeout=1.0)
                    batch.append(data)
                except Empty:
                    pass
                
                # 배치 크기 도달 또는 시간 초과시 DB 저장
                current_time = time.time()
                should_flush = (
                    len(batch) >= self.batch_size or 
                    (batch and current_time - last_flush >= self.flush_interval)
                )
                
                if should_flush:
                    self._save_buy_attempt_batch(batch)
                    batch.clear()
                    last_flush = current_time
                    
            except Exception as e:
                logger.error(f"❌ 매수 시도 워커 오류: {e}")
                time.sleep(1)
        
        # 종료시 남은 데이터 처리
        if batch:
            self._save_buy_attempt_batch(batch)

    def _market_worker(self):
        """🌍 시장 상태 처리 워커"""
        batch = []
        last_flush = time.time()
        
        while self.is_running:
            try:
                # 큐에서 데이터 가져오기 (타임아웃 1초)
                try:
                    data = self.market_state_queue.get(timeout=1.0)
                    batch.append(data)
                except Empty:
                    pass
                
                # 배치 크기 도달 또는 시간 초과시 DB 저장
                current_time = time.time()
                should_flush = (
                    len(batch) >= self.batch_size or 
                    (batch and current_time - last_flush >= self.flush_interval)
                )
                
                if should_flush:
                    self._save_market_batch(batch)
                    batch.clear()
                    last_flush = current_time
                    
            except Exception as e:
                logger.error(f"❌ 시장 상태 워커 오류: {e}")
                time.sleep(1)
        
        # 종료시 남은 데이터 처리
        if batch:
            self._save_market_batch(batch)

    def _save_signal_batch(self, batch: List[Dict]):
        """📊 신호 분석 배치 저장"""
        if not batch:
            return
            
        try:
            with sqlite3.connect(str(self.db_path), timeout=30.0) as conn:
                cursor = conn.cursor()
                
                for data in batch:
                    # JSON 직렬화
                    raw_data_json = json.dumps(data.get('raw_data', {}))
                    
                    cursor.execute("""
                        INSERT INTO signal_analysis (
                            timestamp, stock_code, stock_name, strategy_type,
                            signal_strength, signal_threshold, signal_passed, signal_reason,
                            current_price, open_price, high_price, low_price, prev_close, price_change,
                            price_change_pct,
                            volume, volume_power, avg_volume_5, avg_volume_20, avg_volume_60,
                            volume_ratio_5d, volume_ratio_20d,
                            rsi, rsi_9, rsi_14,
                            macd, macd_signal, macd_histogram,
                            bb_upper, bb_middle, bb_lower, bb_position, bb_width,
                            ma5, ma10, ma20, ma60, ma120,
                            disparity_5d, disparity_10d, disparity_20d, disparity_60d, disparity_120d,
                            momentum_5d, momentum_10d, momentum_20d, rate_of_change,
                            volatility_5d, volatility_20d, atr,
                            market_cap, sector, market_type, listing_date, foreign_ownership_pct,
                            bid_ask_spread, bid_volume, ask_volume, bid_ask_ratio,
                            hour_of_day, minute_of_hour, day_of_week, is_opening_hour, is_closing_hour,
                            performance_1d, performance_3d, performance_1w, performance_1m,
                            price_1h_later, price_4h_later, price_1d_later, price_1w_later, max_price_24h, min_price_24h,
                            raw_data_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        datetime.fromtimestamp(data.get('timestamp', time.time())),  # 1
                        data.get('stock_code', ''),                                 # 2
                        data.get('stock_name', ''),                                 # 3
                        data.get('strategy_type', ''),                              # 4
                        data.get('signal_strength', 0.0),                           # 5
                        data.get('signal_threshold', 0.0),                          # 6
                        data.get('signal_passed', False),                           # 7
                        data.get('signal_reason', ''),                              # 8
                        data.get('current_price', 0),                               # 9
                        data.get('open_price', 0),                                  # 10
                        data.get('high_price', 0),                                  # 11
                        data.get('low_price', 0),                                   # 12
                        data.get('prev_close', 0),                                  # 13
                        data.get('price_change', 0),                                # 14
                        data.get('price_change_pct', 0.0),                          # 15
                        data.get('volume', 0),                                      # 16
                        data.get('volume_power', 0.0),                              # 17
                        data.get('avg_volume_5', 0),                                # 18
                        data.get('avg_volume_20', 0),                               # 19
                        data.get('avg_volume_60', 0),                               # 20
                        data.get('volume_ratio_5d', 0.0),                           # 21
                        data.get('volume_ratio_20d', 0.0),                          # 22
                        data.get('rsi', 50.0),                                      # 23
                        data.get('rsi_9', 50.0),                                    # 24
                        data.get('rsi_14', 50.0),                                   # 25
                        data.get('macd', 0.0),                                      # 26
                        data.get('macd_signal', 0.0),                               # 27
                        data.get('macd_histogram', 0.0),                            # 28
                        data.get('bb_upper', 0),                                    # 29
                        data.get('bb_middle', 0),                                   # 30
                        data.get('bb_lower', 0),                                    # 31
                        data.get('bb_position', 0.5),                               # 32
                        data.get('bb_width', 0.0),                                  # 33
                        data.get('ma5', 0),                                         # 34
                        data.get('ma10', 0),                                        # 35
                        data.get('ma20', 0),                                        # 36
                        data.get('ma60', 0),                                        # 37
                        data.get('ma120', 0),                                       # 38
                        data.get('disparity_5d', 100.0),                            # 39
                        data.get('disparity_10d', 100.0),                           # 40
                        data.get('disparity_20d', 100.0),                           # 41
                        data.get('disparity_60d', 100.0),                           # 42
                        data.get('disparity_120d', 100.0),                          # 43
                        data.get('momentum_5d', 0.0),                               # 44
                        data.get('momentum_10d', 0.0),                              # 45
                        data.get('momentum_20d', 0.0),                              # 46
                        data.get('rate_of_change', 0.0),                            # 47
                        data.get('volatility_5d', 0.0),                             # 48
                        data.get('volatility_20d', 0.0),                            # 49
                        data.get('atr', 0.0),                                       # 50
                        data.get('market_cap', 0),                                  # 51
                        data.get('sector', ''),                                     # 52
                        data.get('market_type', ''),                                # 53
                        data.get('listing_date', ''),                               # 54
                        data.get('foreign_ownership_pct', 0.0),                     # 55
                        data.get('bid_ask_spread', 0.0),                            # 56
                        data.get('bid_volume', 0),                                  # 57
                        data.get('ask_volume', 0),                                  # 58
                        data.get('bid_ask_ratio', 0.0),                             # 59
                        data.get('hour_of_day', 0),                                 # 60
                        data.get('minute_of_hour', 0),                              # 61
                        data.get('day_of_week', 0),                                 # 62
                        data.get('is_opening_hour', False),                         # 63
                        data.get('is_closing_hour', False),                         # 64
                        data.get('performance_1d', 0.0),                            # 65
                        data.get('performance_3d', 0.0),                            # 66
                        data.get('performance_1w', 0.0),                            # 67
                        data.get('performance_1m', 0.0),                            # 68
                        data.get('price_1h_later', 0),                              # 69
                        data.get('price_4h_later', 0),                              # 70
                        data.get('price_1d_later', 0),                              # 71
                        data.get('price_1w_later', 0),                              # 72
                        data.get('max_price_24h', 0),                               # 73
                        data.get('min_price_24h', 0),                               # 74
                        raw_data_json                                               # 75
                    ))
                
                conn.commit()
                self.stats['signals_logged'] += len(batch)
                self.stats['db_writes'] += 1
                
                logger.debug(f"💾 신호 분석 배치 저장 완료: {len(batch)}개")
                
        except Exception as e:
            logger.error(f"❌ 신호 분석 배치 저장 오류: {e}")
            self.stats['errors'] += 1

    def _save_buy_attempt_batch(self, batch: List[Dict]):
        """📈 매수 시도 배치 저장"""
        if not batch:
            return
            
        try:
            with sqlite3.connect(str(self.db_path), timeout=30.0) as conn:
                cursor = conn.cursor()
                
                for data in batch:
                    # JSON 직렬화 (datetime 객체 처리)
                    signal_data_json = json.dumps(data.get('signal_data', {}), default=str)
                    validation_checks = json.dumps(data.get('validation_checks', {}), default=str)
                    
                    cursor.execute("""
                        INSERT INTO buy_attempts (
                            timestamp, stock_code, stock_name, attempt_result, failure_reason,
                            signal_strength, strategy_type, signal_data_json,
                            buy_price, quantity, total_amount, validation_checks,
                            market_condition, portfolio_status, available_cash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        datetime.fromtimestamp(data.get('timestamp', time.time())),
                        data.get('stock_code', ''),
                        data.get('stock_name', ''),
                        data.get('attempt_result', ''),
                        data.get('failure_reason', ''),
                        data.get('signal_strength', 0.0),
                        data.get('strategy_type', ''),
                        signal_data_json,
                        data.get('buy_price', 0),
                        data.get('quantity', 0),
                        data.get('total_amount', 0),
                        validation_checks,
                        data.get('market_condition', ''),
                        data.get('portfolio_status', ''),
                        data.get('available_cash', 0)
                    ))
                
                conn.commit()
                self.stats['buy_attempts_logged'] += len(batch)
                self.stats['db_writes'] += 1
                
                logger.debug(f"💾 매수 시도 배치 저장 완료: {len(batch)}개")
                
        except Exception as e:
            logger.error(f"❌ 매수 시도 배치 저장 오류: {e}")
            self.stats['errors'] += 1

    def _save_market_batch(self, batch: List[Dict]):
        """🌍 시장 상태 배치 저장"""
        if not batch:
            return
            
        try:
            with sqlite3.connect(str(self.db_path), timeout=30.0) as conn:
                cursor = conn.cursor()
                
                for data in batch:
                    cursor.execute("""
                        INSERT INTO market_snapshots (
                            timestamp, kospi_value, kosdaq_value, kospi_change_pct, kosdaq_change_pct,
                            kospi_volume, kosdaq_volume, rising_stocks, falling_stocks, unchanged_stocks,
                            market_volatility, vix_korea, hour_of_day, is_opening, is_closing
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        datetime.fromtimestamp(data.get('timestamp', time.time())),
                        data.get('kospi_value', 0.0),
                        data.get('kosdaq_value', 0.0),
                        data.get('kospi_change_pct', 0.0),
                        data.get('kosdaq_change_pct', 0.0),
                        data.get('kospi_volume', 0),
                        data.get('kosdaq_volume', 0),
                        data.get('rising_stocks', 0),
                        data.get('falling_stocks', 0),
                        data.get('unchanged_stocks', 0),
                        data.get('market_volatility', 0.0),
                        data.get('vix_korea', 0.0),
                        data.get('hour_of_day', 0),
                        data.get('is_opening', False),
                        data.get('is_closing', False)
                    ))
                
                conn.commit()
                self.stats['market_states_logged'] += len(batch)
                self.stats['db_writes'] += 1
                
                logger.debug(f"💾 시장 상태 배치 저장 완료: {len(batch)}개")
                
        except Exception as e:
            logger.error(f"❌ 시장 상태 배치 저장 오류: {e}")
            self.stats['errors'] += 1

    def get_stats(self) -> Dict[str, Any]:
        """📊 통계 정보 반환"""
        return {
            **self.stats,
            'queue_sizes': {
                'signal_queue': self.signal_queue.qsize(),
                'buy_attempt_queue': self.buy_attempt_queue.qsize(),
                'market_state_queue': self.market_state_queue.qsize()
            },
            'is_running': self.is_running,
            'worker_threads_alive': [t.is_alive() for t in self.worker_threads]
        }

    def shutdown(self):
        """시스템 종료"""
        logger.info("🛑 비동기 데이터 로거 종료 시작...")
        
        self.is_running = False
        
        # 워커 스레드 종료 대기 (최대 30초)
        for thread in self.worker_threads:
            thread.join(timeout=30)
            
        # 남은 큐 데이터 강제 플러시
        self._emergency_flush()
        
        logger.info("✅ 비동기 데이터 로거 종료 완료")

    def _emergency_flush(self):
        """비상 데이터 플러시"""
        try:
            # 모든 큐의 남은 데이터 처리
            signal_batch = []
            while not self.signal_queue.empty():
                try:
                    signal_batch.append(self.signal_queue.get_nowait())
                except Empty:
                    break
            
            buy_batch = []
            while not self.buy_attempt_queue.empty():
                try:
                    buy_batch.append(self.buy_attempt_queue.get_nowait())
                except Empty:
                    break
            
            market_batch = []
            while not self.market_state_queue.empty():
                try:
                    market_batch.append(self.market_state_queue.get_nowait())
                except Empty:
                    break
            
            # 배치 저장
            if signal_batch:
                self._save_signal_batch(signal_batch)
            if buy_batch:
                self._save_buy_attempt_batch(buy_batch)
            if market_batch:
                self._save_market_batch(market_batch)
                
            logger.info(f"🚨 비상 플러시 완료: 신호={len(signal_batch)}, 매수={len(buy_batch)}, 시장={len(market_batch)}")
            
        except Exception as e:
            logger.error(f"❌ 비상 플러시 오류: {e}")


# 🌐 글로벌 인스턴스 (싱글톤 패턴)
_async_logger = None

def get_async_logger() -> AsyncDataLogger:
    """비동기 데이터 로거 싱글톤 인스턴스 반환"""
    global _async_logger
    if _async_logger is None:
        _async_logger = AsyncDataLogger()
    return _async_logger


# 🚀 편의 함수들
def log_signal_failed(stock_code: str, strategy: str, signal_strength: float, 
                     threshold: float, reason: str, market_data: Dict = None):
    """신호 조건 못 미친 종목 로깅 (편의 함수)"""
    logger_instance = get_async_logger()
    
    signal_data = {
        'timestamp': time.time(),
        'stock_code': stock_code,
        'strategy_type': strategy,
        'signal_strength': signal_strength,
        'signal_threshold': threshold,
        'signal_passed': False,
        'signal_reason': reason,
        'raw_data': market_data or {}
    }
    
    # 시장 데이터에서 추가 정보 추출
    if market_data:
        signal_data.update({
            'stock_name': market_data.get('stock_name', ''),
            'current_price': market_data.get('current_price', 0),
            'volume': market_data.get('volume', 0),
            'price_change_pct': market_data.get('price_change_pct', 0.0),
            # ... 기타 필드들
        })
    
    logger_instance.log_signal_analysis(signal_data)


def log_buy_failed(stock_code: str, reason: str, signal_data: Dict = None, 
                  validation_details: Dict = None):
    """매수 실패 로깅 (편의 함수)"""
    logger_instance = get_async_logger()
    
    attempt_data = {
        'timestamp': time.time(),
        'stock_code': stock_code,
        'attempt_result': 'FAILED',
        'failure_reason': reason,
        'signal_data': signal_data or {},
        'validation_checks': validation_details or {}
    }
    
    logger_instance.log_buy_attempt(attempt_data)


def log_buy_success(stock_code: str, buy_price: int, quantity: int, 
                   strategy: str, signal_data: Dict = None):
    """매수 성공 로깅 (편의 함수)"""
    logger_instance = get_async_logger()
    
    attempt_data = {
        'timestamp': time.time(),
        'stock_code': stock_code,
        'attempt_result': 'SUCCESS',
        'buy_price': buy_price,
        'quantity': quantity,
        'total_amount': buy_price * quantity,
        'strategy_type': strategy,
        'signal_data': signal_data or {}
    }
    
    logger_instance.log_buy_attempt(attempt_data) 