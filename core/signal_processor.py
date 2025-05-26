"""
신호 처리기 (리팩토링 버전)
전략별 매매 신호 분석 및 처리 전담
"""
import time
import threading
from typing import Dict, List, Optional, Any
from utils.logger import setup_logger
from .kis_data_collector import KISDataCollector
# 임시 전략 관리자 (추후 실제 구현 필요)
class StrategyManager:
    def get_active_strategies(self):
        return ["gap_trading", "volume_breakout", "price_momentum", "support_resistance"]

logger = setup_logger(__name__)


class SignalProcessor:
    """간소화된 신호 처리기"""

    def __init__(self, data_collector: KISDataCollector, is_demo: bool = False):
        """초기화"""
        self.data_collector = data_collector
        self.is_demo = is_demo

        # 전략 관리자
        self.strategy_manager = StrategyManager()

        # 신호 처리
        self.signals: List[Dict] = []
        self.signal_lock = threading.RLock()
        self.last_signal_time: Dict[str, float] = {}  # {stock_code: timestamp}

        # 신호 필터링 설정
        self.signal_cooldown = 300  # 5분 (같은 종목 신호 간격)
        self.min_signal_strength = 0.6  # 최소 신호 강도

        # 통계
        self.stats = {
            'total_signals': 0,
            'buy_signals': 0,
            'sell_signals': 0,
            'filtered_signals': 0,
            'processed_signals': 0
        }

        logger.info("신호 처리기 초기화 완료")

    def process_market_data(self, market_data: Dict[str, Any]) -> List[Dict]:
        """시장 데이터로부터 신호 생성"""
        signals = []

        try:
            with self.signal_lock:
                # 전략별 신호 분석
                for strategy_name in self.strategy_manager.get_active_strategies():
                    strategy_signals = self._analyze_strategy_signals(market_data, strategy_name)
                    signals.extend(strategy_signals)

                # 신호 필터링 및 처리
                filtered_signals = self._filter_signals(signals)

                # 처리된 신호 저장
                for signal in filtered_signals:
                    self.signals.append(signal)
                    self.stats['processed_signals'] += 1

                # 통계 업데이트
                self.stats['total_signals'] += len(signals)
                self.stats['filtered_signals'] += len(signals) - len(filtered_signals)

                return filtered_signals

        except Exception as e:
            logger.error(f"신호 처리 오류: {e}")
            return []

    def _analyze_strategy_signals(self, market_data: Dict, strategy_name: str) -> List[Dict]:
        """전략별 신호 분석"""
        signals = []

        try:
            # 기본 데이터 확인
            stock_code = market_data.get('stock_code')
            if not stock_code:
                return signals

            current_price = market_data.get('current_price', 0)
            volume = market_data.get('volume', 0)

            if current_price <= 0 or volume <= 0:
                return signals

            # 전략별 신호 생성
            if strategy_name == "gap_trading":
                gap_signals = self._analyze_gap_signals(market_data)
                signals.extend(gap_signals)

            elif strategy_name == "volume_breakout":
                volume_signals = self._analyze_volume_signals(market_data)
                signals.extend(volume_signals)

            elif strategy_name == "price_momentum":
                momentum_signals = self._analyze_momentum_signals(market_data)
                signals.extend(momentum_signals)

            elif strategy_name == "support_resistance":
                sr_signals = self._analyze_support_resistance_signals(market_data)
                signals.extend(sr_signals)

            return signals

        except Exception as e:
            logger.error(f"전략 신호 분석 오류 ({strategy_name}): {e}")
            return []

    def _analyze_gap_signals(self, market_data: Dict) -> List[Dict]:
        """갭 거래 신호 분석"""
        signals = []

        try:
            stock_code = market_data['stock_code']
            current_price = market_data['current_price']
            open_price = market_data.get('open_price', current_price)
            prev_close = market_data.get('prev_close', current_price)

            if prev_close <= 0:
                return signals

            # 갭 비율 계산
            gap_ratio = ((open_price - prev_close) / prev_close) * 100

            # 상승 갭 (2% 이상)
            if gap_ratio >= 2.0:
                # 갭 하락 시 매수 신호
                price_change_from_open = ((current_price - open_price) / open_price) * 100
                if price_change_from_open <= -1.0:  # 시가 대비 1% 이상 하락
                    signals.append({
                        'stock_code': stock_code,
                        'signal_type': 'BUY',
                        'strategy': 'gap_trading',
                        'strength': min(0.8, abs(price_change_from_open) / 3.0),
                        'price': current_price,
                        'reason': f'상승갭({gap_ratio:.1f}%) 후 하락반전',
                        'timestamp': time.time()
                    })

            # 하락 갭 (2% 이상)
            elif gap_ratio <= -2.0:
                # 갭 상승 시 매도 또는 공매도 신호
                price_change_from_open = ((current_price - open_price) / open_price) * 100
                if price_change_from_open >= 1.0:  # 시가 대비 1% 이상 상승
                    signals.append({
                        'stock_code': stock_code,
                        'signal_type': 'SELL',
                        'strategy': 'gap_trading',
                        'strength': min(0.8, price_change_from_open / 3.0),
                        'price': current_price,
                        'reason': f'하락갭({gap_ratio:.1f}%) 후 상승반전',
                        'timestamp': time.time()
                    })

            return signals

        except Exception as e:
            logger.error(f"갭 신호 분석 오류: {e}")
            return []

    def _analyze_volume_signals(self, market_data: Dict) -> List[Dict]:
        """거래량 돌파 신호 분석"""
        signals = []

        try:
            stock_code = market_data['stock_code']
            current_price = market_data['current_price']
            volume = market_data['volume']
            avg_volume = market_data.get('avg_volume_20', volume)

            if avg_volume <= 0:
                return signals

            # 거래량 비율
            volume_ratio = volume / avg_volume

            # 가격 변화
            prev_close = market_data.get('prev_close', current_price)
            price_change_pct = ((current_price - prev_close) / prev_close) * 100 if prev_close > 0 else 0

            # 거래량 급증 + 가격 상승
            if volume_ratio >= 3.0 and price_change_pct >= 2.0:
                signals.append({
                    'stock_code': stock_code,
                    'signal_type': 'BUY',
                    'strategy': 'volume_breakout',
                    'strength': min(0.9, (volume_ratio / 5.0) * (price_change_pct / 5.0)),
                    'price': current_price,
                    'reason': f'거래량급증({volume_ratio:.1f}x) + 가격상승({price_change_pct:.1f}%)',
                    'timestamp': time.time()
                })

            # 거래량 급증 + 가격 하락 (매도 신호)
            elif volume_ratio >= 3.0 and price_change_pct <= -2.0:
                signals.append({
                    'stock_code': stock_code,
                    'signal_type': 'SELL',
                    'strategy': 'volume_breakout',
                    'strength': min(0.9, (volume_ratio / 5.0) * (abs(price_change_pct) / 5.0)),
                    'price': current_price,
                    'reason': f'거래량급증({volume_ratio:.1f}x) + 가격하락({price_change_pct:.1f}%)',
                    'timestamp': time.time()
                })

            return signals

        except Exception as e:
            logger.error(f"거래량 신호 분석 오류: {e}")
            return []

    def _analyze_momentum_signals(self, market_data: Dict) -> List[Dict]:
        """모멘텀 신호 분석"""
        signals = []

        try:
            stock_code = market_data['stock_code']
            current_price = market_data['current_price']

            # 이동평균 기반 모멘텀
            ma5 = market_data.get('ma5', current_price)
            ma20 = market_data.get('ma20', current_price)

            if ma5 <= 0 or ma20 <= 0:
                return signals

            # 골든크로스 (5일선이 20일선 돌파)
            ma_ratio = (ma5 / ma20 - 1) * 100
            price_above_ma5 = ((current_price / ma5 - 1) * 100) if ma5 > 0 else 0

            if ma_ratio >= 1.0 and price_above_ma5 >= 0.5:
                signals.append({
                    'stock_code': stock_code,
                    'signal_type': 'BUY',
                    'strategy': 'price_momentum',
                    'strength': min(0.7, ma_ratio / 3.0),
                    'price': current_price,
                    'reason': f'골든크로스 돌파 (MA비율: {ma_ratio:.1f}%)',
                    'timestamp': time.time()
                })

            # 데드크로스 (5일선이 20일선 하향 돌파)
            elif ma_ratio <= -1.0 and price_above_ma5 <= -0.5:
                signals.append({
                    'stock_code': stock_code,
                    'signal_type': 'SELL',
                    'strategy': 'price_momentum',
                    'strength': min(0.7, abs(ma_ratio) / 3.0),
                    'price': current_price,
                    'reason': f'데드크로스 하락 (MA비율: {ma_ratio:.1f}%)',
                    'timestamp': time.time()
                })

            return signals

        except Exception as e:
            logger.error(f"모멘텀 신호 분석 오류: {e}")
            return []

    def _analyze_support_resistance_signals(self, market_data: Dict) -> List[Dict]:
        """지지/저항 신호 분석"""
        signals = []

        try:
            stock_code = market_data['stock_code']
            current_price = market_data['current_price']

            # 일일 고가/저가
            high_price = market_data.get('high_price', current_price)
            low_price = market_data.get('low_price', current_price)

            # 가격 위치 비율 (저가=0, 고가=1)
            if high_price > low_price:
                price_position = (current_price - low_price) / (high_price - low_price)
            else:
                price_position = 0.5

            # 저가 근처에서 반등 (지지선 터치)
            if price_position <= 0.1:  # 하위 10% 구간
                prev_close = market_data.get('prev_close', current_price)
                price_change = ((current_price - prev_close) / prev_close) * 100 if prev_close > 0 else 0

                if price_change >= 1.0:  # 1% 이상 반등
                    signals.append({
                        'stock_code': stock_code,
                        'signal_type': 'BUY',
                        'strategy': 'support_resistance',
                        'strength': min(0.6, price_change / 2.0),
                        'price': current_price,
                        'reason': f'지지선 터치 후 반등 ({price_change:.1f}%)',
                        'timestamp': time.time()
                    })

            # 고가 근처에서 하락 (저항선 터치)
            elif price_position >= 0.9:  # 상위 10% 구간
                prev_close = market_data.get('prev_close', current_price)
                price_change = ((current_price - prev_close) / prev_close) * 100 if prev_close > 0 else 0

                if price_change <= -1.0:  # 1% 이상 하락
                    signals.append({
                        'stock_code': stock_code,
                        'signal_type': 'SELL',
                        'strategy': 'support_resistance',
                        'strength': min(0.6, abs(price_change) / 2.0),
                        'price': current_price,
                        'reason': f'저항선 터치 후 하락 ({price_change:.1f}%)',
                        'timestamp': time.time()
                    })

            return signals

        except Exception as e:
            logger.error(f"지지/저항 신호 분석 오류: {e}")
            return []

    def _filter_signals(self, signals: List[Dict]) -> List[Dict]:
        """신호 필터링"""
        filtered = []
        current_time = time.time()

        for signal in signals:
            stock_code = signal['stock_code']
            signal_strength = signal['strength']

            # 1. 최소 신호 강도 체크
            if signal_strength < self.min_signal_strength:
                continue

            # 2. 쿨다운 체크 (같은 종목 연속 신호 방지)
            last_time = self.last_signal_time.get(stock_code, 0)
            if current_time - last_time < self.signal_cooldown:
                continue

            # 3. 필터링 통과
            filtered.append(signal)
            self.last_signal_time[stock_code] = current_time

            # 통계 업데이트
            if signal['signal_type'] == 'BUY':
                self.stats['buy_signals'] += 1
            else:
                self.stats['sell_signals'] += 1

        return filtered

    def get_recent_signals(self, minutes: int = 60) -> List[Dict]:
        """최근 신호 조회"""
        cutoff_time = time.time() - (minutes * 60)

        with self.signal_lock:
            return [
                signal for signal in self.signals
                if signal['timestamp'] >= cutoff_time
            ]

    def get_signals_by_strategy(self, strategy: str, minutes: int = 60) -> List[Dict]:
        """전략별 신호 조회"""
        cutoff_time = time.time() - (minutes * 60)

        with self.signal_lock:
            return [
                signal for signal in self.signals
                if signal['strategy'] == strategy and signal['timestamp'] >= cutoff_time
            ]

    def get_top_signals(self, signal_type: str = 'BUY', limit: int = 10) -> List[Dict]:
        """강도 순 상위 신호"""
        recent_signals = self.get_recent_signals(30)  # 최근 30분

        filtered_signals = [
            signal for signal in recent_signals
            if signal['signal_type'] == signal_type
        ]

        # 강도 순 정렬
        sorted_signals = sorted(filtered_signals, key=lambda x: x['strength'], reverse=True)

        return sorted_signals[:limit]

    def get_stats(self) -> Dict:
        """신호 처리 통계"""
        filter_rate = (
            (self.stats['filtered_signals'] / self.stats['total_signals'] * 100)
            if self.stats['total_signals'] > 0 else 0
        )

        return {
            **self.stats.copy(),
            'filter_rate': round(filter_rate, 2),
            'signal_history_count': len(self.signals)
        }

    def cleanup(self):
        """리소스 정리"""
        logger.info("신호 처리기 정리 중...")

        # 최근 신호 통계
        recent_signals = self.get_recent_signals(60)
        if recent_signals:
            buy_count = len([s for s in recent_signals if s['signal_type'] == 'BUY'])
            sell_count = len([s for s in recent_signals if s['signal_type'] == 'SELL'])
            logger.info(f"최근 1시간 신호: 매수 {buy_count}개, 매도 {sell_count}개")

        logger.info("신호 처리기 정리 완료")
