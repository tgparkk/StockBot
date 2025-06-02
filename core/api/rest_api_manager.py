"""
KIS REST API 통합 관리자 (리팩토링 버전)
공식 문서 기반 + 모듈화
"""
import time
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Union, Any
from utils.logger import setup_logger

# KIS 모듈 import
from . import kis_auth as kis
from . import kis_order_api as order_api
from . import kis_account_api as account_api
from . import kis_market_api as market_api
from .kis_market_api import (
    get_gap_trading_candidates, get_volume_breakout_candidates, get_momentum_candidates,
    get_fluctuation_rank, get_volume_rank, get_volume_power_rank
)

# 데이터 모델은 필요할 때 지연 import (순환 import 방지)

# 설정 import (settings.py에서 .env 파일을 읽어서 제공)
from config.settings import (
    KIS_BASE_URL, APP_KEY, SECRET_KEY,
    ACCOUNT_NUMBER, HTS_ID
)

logger = setup_logger(__name__)


class KISRestAPIManager:
    """KIS REST API 통합 관리자 (간소화 버전)"""

    def __init__(self):
        """초기화"""
        # 인증 초기화
        svr = 'prod'
        logger.info("🔑 KIS API 인증 시작...")

        if not kis.auth(svr):
            logger.error("❌ KIS API 인증 실패!")
            logger.error("📋 문제 해결 방법:")
            logger.error("  1. .env 파일이 프로젝트 루트에 있는지 확인")
            logger.error("  2. .env 파일에 실제 KIS API 키가 입력되어 있는지 확인")
            logger.error("  3. KIS_APP_KEY, KIS_APP_SECRET 값이 정확한지 확인")
            logger.error("  4. 계좌번호와 HTS ID가 올바른지 확인")
            raise ValueError("KIS API 인증 실패 - 설정을 확인해주세요")

        logger.info("✅ KIS API 인증 성공!")


    # === 인증 관련 ===

    def get_token_info(self) -> Dict:
        """토큰 정보 조회"""
        env = kis.getTREnv()
        if not env:
            return {"status": "error", "message": "인증되지 않음"}

        return {
            "status": "success",
            "app_key": env.my_app[:10] + "...",  # 일부만 표시
            "account": env.my_acct,
            "product": env.my_prod,
            "url": env.my_url
        }

    def force_token_refresh(self) -> bool:
        """토큰 강제 갱신"""
        svr = 'prod'
        return kis.auth(svr)

    # === 주문 관련 ===

    def buy_order(self, stock_code: str, quantity: int, price: int = 0) -> Dict:
        """매수 주문"""
        if price == 0:
            logger.warning("시장가 매수 주문")

        result = order_api.get_order_cash("buy", stock_code, quantity, price)

        if result is not None and not result.empty:
            return {
                "status": "success",
                "order_no": result.iloc[0].get('odno', ''),
                "message": "매수 주문 완료"
            }
        else:
            return {
                "status": "error",
                "message": "매수 주문 실패"
            }

    def sell_order(self, stock_code: str, quantity: int, price: int = 0) -> Dict:
        """매도 주문"""
        if price == 0:
            logger.warning("시장가 매도 주문")

        result = order_api.get_order_cash("sell", stock_code, quantity, price)

        if result is not None and not result.empty:
            return {
                "status": "success",
                "order_no": result.iloc[0].get('odno', ''),
                "message": "매도 주문 완료"
            }
        else:
            return {
                "status": "error",
                "message": "매도 주문 실패"
            }

    def cancel_order(self, order_no: str, stock_code: str, quantity: int) -> Dict:
        """주문 취소"""
        # 실제 주문 취소는 정정취소 API를 사용해야 하며 추가 정보가 필요
        logger.warning("주문 취소는 정정취소 API를 직접 사용하세요")
        return {
            "status": "error",
            "message": "주문 취소는 kis_order_api.get_order_rvsecncl() 사용 권장"
        }

    def get_today_orders(self) -> List[Dict]:
        """당일 주문 내역"""
        result = order_api.get_inquire_daily_ccld_lst()

        if result is not None and not result.empty:
            return result.to_dict('records')
        else:
            return []

    # === 계좌 관련 ===

    def get_balance(self) -> Dict:
        """계좌 잔고 조회"""
        # 계좌 요약 정보
        balance_obj = account_api.get_inquire_balance_obj()
        # 보유 종목 목록
        balance_lst = account_api.get_inquire_balance_lst()

        if balance_obj is not None and not balance_obj.empty:
            summary = balance_obj.iloc[0].to_dict()
        else:
            summary = {}

        if balance_lst is not None and not balance_lst.empty:
            holdings = balance_lst.to_dict('records')
        else:
            holdings = []

        return {
            "status": "success",
            "summary": summary,
            "holdings": holdings,
            "total_count": len(holdings)
        }

    def get_buy_possible(self, stock_code: str, price: int = 0) -> Dict:
        """매수 가능 조회"""
        result = account_api.get_inquire_psbl_order(stock_code, price)

        if result is not None and not result.empty:
            data = result.iloc[0].to_dict()
            return {
                "status": "success",
                "stock_code": stock_code,
                "max_buy_amount": int(data.get('max_buy_amt', 0)),
                "max_buy_qty": int(data.get('max_buy_qty', 0)),
                "available_cash": int(data.get('ord_psbl_cash', 0))
            }
        else:
            return {
                "status": "error",
                "message": "매수가능조회 실패"
            }

    # === 시세 관련 ===

    def get_current_price(self, stock_code: str) -> Dict:
        """현재가 조회"""
        result = market_api.get_inquire_price("J", stock_code)

        if result is not None and not result.empty:
            data = result.iloc[0].to_dict()
            return {
                "status": "success",
                "stock_code": stock_code,
                "current_price": int(data.get('stck_prpr', 0)),
                "change_rate": float(data.get('prdy_ctrt', 0)),
                "volume": int(data.get('acml_vol', 0)),
                "high_price": int(data.get('stck_hgpr', 0)),
                "low_price": int(data.get('stck_lwpr', 0)),
                "open_price": int(data.get('stck_oprc', 0))
            }
        else:
            return {
                "status": "error",
                "message": "현재가 조회 실패"
            }

    def get_orderbook(self, stock_code: str) -> Dict:
        """호가 조회"""
        result = market_api.get_inquire_asking_price_exp_ccn("1", "J", stock_code)

        if result is not None and not result.empty:
            data = result.iloc[0].to_dict()

            # 호가 데이터 파싱
            asks = []  # 매도호가
            bids = []  # 매수호가

            for i in range(1, 11):  # 1~10호가
                ask_price = int(data.get(f'askp{i}', 0))
                ask_volume = int(data.get(f'askp_rsqn{i}', 0))
                bid_price = int(data.get(f'bidp{i}', 0))
                bid_volume = int(data.get(f'bidp_rsqn{i}', 0))

                if ask_price > 0:
                    asks.append({"price": ask_price, "volume": ask_volume})
                if bid_price > 0:
                    bids.append({"price": bid_price, "volume": bid_volume})

            return {
                "status": "success",
                "stock_code": stock_code,
                "asks": asks,
                "bids": bids,
                "total_ask_volume": int(data.get('total_askp_rsqn', 0)),
                "total_bid_volume": int(data.get('total_bidp_rsqn', 0))
            }
        else:
            return {
                "status": "error",
                "message": "호가 조회 실패"
            }

    def get_daily_prices(self, stock_code: str, period_type: str = "D") -> List[Dict]:
        """일봉 데이터 조회"""
        result = market_api.get_inquire_daily_itemchartprice("2", "J", stock_code, period_code=period_type)

        if result is not None and not result.empty:
            return result.to_dict('records')
        else:
            return []

    # === 시장 스크리닝 관련 ===
    def get_market_screening_candidates(self, market_type: str = "all") -> Dict:
        """🎯 최적화된 시장 스크리닝 - 중복 제거 버전"""
        logger.info(f"📊 최적화된 시장 스크리닝 시작: {market_type}")

        candidates = {
            'gap': [],
            'volume': [],
            'momentum': [],
            'technical': []
        }

        markets = ["0000", "0001", "1001"] if market_type == "all" else [market_type]

        # 🎯 시간대별 적응형 전략
        from datetime import datetime
        current_time = datetime.now()
        is_pre_market = current_time.hour < 9 or (current_time.hour == 9 and current_time.minute < 30)
        is_early_market = current_time.hour < 11

        # 🆕 중복 방지를 위한 종목 캐시
        analyzed_stocks = {}  # {stock_code: {price_data, current_data, technical_analysis}}
        collected_stocks = set()  # 수집된 모든 종목 코드

        for market in markets:
            try:
                logger.info(f"🔍 [{market}] 시장 분석 시작...")

                # === 1단계: 모든 소스에서 종목 수집 (중복 제거) ===
                market_stocks = self._collect_all_market_stocks(market, is_pre_market)
                collected_stocks.update(market_stocks)
                logger.info(f"📊 [{market}] 종목 수집 완료: {len(market_stocks)}개")

                # === 2단계: 수집된 종목들에 대해 일괄 분석 (한 번만) ===
                batch_analysis = self._batch_analyze_stocks(list(market_stocks)[:100], analyzed_stocks)
                logger.info(f"📈 [{market}] 일괄 분석 완료: {len(batch_analysis)}개")

                # === 3단계: 분석 결과를 전략별로 분류 ===
                market_candidates = self._classify_candidates_by_strategy(batch_analysis, market)

                # 결과 합산
                for category, items in market_candidates.items():
                    candidates[category].extend(items)

                logger.info(f"✅ [{market}] 완료 - 갭:{len(market_candidates['gap'])} 거래량:{len(market_candidates['volume'])} 모멘텀:{len(market_candidates['momentum'])} 기술:{len(market_candidates['technical'])}")

                # API 제한 방지
                time.sleep(0.2)

            except Exception as e:
                logger.error(f"❌ 시장 {market} 스크리닝 오류: {e}")
                continue

        # === 최종 정리 및 정렬 ===
        total_candidates = sum(len(v) for v in candidates.values())
        logger.info(f"🎯 최적화된 스크리닝 완료: 총 {total_candidates}개 후보")
        logger.info(f"📊 분석된 종목: {len(analyzed_stocks)}개 (중복 제거)")

        # 카테고리별 상위 후보로 제한
        for category in candidates:
            if candidates[category]:
                candidates[category].sort(key=lambda x: x.get('technical_score', 0), reverse=True)
                candidates[category] = candidates[category][:30]  # 상위 30개로 제한

        return candidates

    # 종목 수집 함수
    def _collect_all_market_stocks(self, market: str, is_pre_market: bool) -> set:
        """🆕 모든 소스에서 종목 수집 (중복 제거)"""
        collected_stocks = set()

        try:
            # 기본 스크리닝 방법들
            screening_methods = [
                # 갭 관련
                lambda: market_api.get_gap_trading_candidates(market),
                lambda: market_api.get_fluctuation_rank(fid_input_iscd=market, fid_rank_sort_cls_code="0"),
                lambda: market_api.get_fluctuation_rank(fid_input_iscd=market, fid_rank_sort_cls_code="1"),

                # 거래량 관련
                lambda: market_api.get_volume_breakout_candidates(market),
                lambda: market_api.get_volume_rank(fid_input_iscd=market, fid_blng_cls_code="1"),
                lambda: market_api.get_bulk_trans_num_rank(fid_input_iscd=market),

                # 모멘텀 관련
                lambda: market_api.get_momentum_candidates(market),
                lambda: market_api.get_volume_power_rank(fid_input_iscd=market),

                # 기술적 지표 관련
                lambda: market_api.get_disparity_rank(fid_input_iscd=market, fid_rank_sort_cls_code="1", fid_hour_cls_code="20"),
                lambda: market_api.get_quote_balance_rank(fid_input_iscd=market)
            ]

            for method in screening_methods:
                try:
                    data = method()
                    if data is not None and not data.empty:
                        for _, row in data.head(20).iterrows():  # 각 방법에서 20개씩
                            stock_code = row.get('stck_shrn_iscd', '')
                            if stock_code and len(stock_code) == 6:  # 유효한 종목코드
                                collected_stocks.add(stock_code)
                except Exception as e:
                    logger.debug(f"종목 수집 방법 오류: {e}")
                    continue

                time.sleep(0.05)  # API 제한 방지

            logger.info(f"📊 [{market}] 종목 수집 완료: {len(collected_stocks)}개 (중복 제거됨)")
            return collected_stocks

        except Exception as e:
            logger.error(f"종목 수집 오류 ({market}): {e}")
            return set()

    def _batch_analyze_stocks(self, stock_codes: List[str], cache: Dict) -> Dict:
        """🆕 종목 일괄 분석 (캐시 활용으로 중복 방지)"""
        try:
            from ..analysis.technical_indicators import TechnicalIndicators

            batch_results = {}

            for stock_code in stock_codes:
                try:
                    # 캐시 확인
                    if stock_code in cache:
                        batch_results[stock_code] = cache[stock_code]
                        continue

                    # 🎯 한 번에 모든 데이터 수집
                    price_data = market_api.get_inquire_daily_price("J", stock_code)
                    current_data = market_api.get_inquire_price("J", stock_code)

                    if (price_data is None or price_data.empty or
                        current_data is None or current_data.empty):
                        continue

                    current_info = current_data.iloc[0]
                    current_price = int(current_info.get('stck_prpr', 0))
                    change_rate = float(current_info.get('prdy_ctrt', 0))
                    volume = int(current_info.get('acml_vol', 0))

                    # 종가 데이터 추출 (한 번만)
                    if 'stck_clpr' in price_data.columns:
                        closes = price_data['stck_clpr'].astype(int).tolist()
                    elif 'close' in price_data.columns:
                        closes = price_data['close'].astype(int).tolist()
                    else:
                        continue

                    if len(closes) < 10 or current_price <= 0:
                        continue

                    # 🎯 모든 기술적 지표를 한 번에 계산
                    technical_analysis = self._comprehensive_technical_analysis(closes, current_price)

                    # 결과 캐시 저장
                    analysis_result = {
                        'stock_code': stock_code,
                        'current_price': current_price,
                        'change_rate': change_rate,
                        'volume': volume,
                        'technical_analysis': technical_analysis,
                        'price_data': price_data.to_dict('records')[:5],  # 최근 5일만 저장
                        'current_data': current_info.to_dict()
                    }

                    cache[stock_code] = analysis_result
                    batch_results[stock_code] = analysis_result

                except Exception as e:
                    logger.debug(f"종목 {stock_code} 분석 오류: {e}")
                    continue

                # API 제한 방지
                time.sleep(0.03)

            logger.info(f"📈 일괄 분석 완료: {len(batch_results)}개 성공")
            return batch_results

        except Exception as e:
            logger.error(f"일괄 분석 오류: {e}")
            return {}

    def _comprehensive_technical_analysis(self, closes: List[int], current_price: int) -> Dict:
        """🆕 포괄적 기술적 분석 (한 번에 모든 지표 계산)"""
        try:
            from ..analysis.technical_indicators import TechnicalIndicators

            # 🎯 모든 기술적 지표를 한 번에 계산
            rsi = TechnicalIndicators.calculate_rsi(closes)[-1]
            macd_data = TechnicalIndicators.calculate_macd(closes)
            ma_data = TechnicalIndicators.calculate_moving_averages(closes, [5, 20, 60])

            # 기술적 점수 계산
            technical_score = 0
            signals = []

            # RSI 분석
            if 20 <= rsi <= 50:
                technical_score += 25
                signals.append(f"RSI적정({rsi:.1f})")
            elif rsi < 30:
                technical_score += 20
                signals.append(f"RSI과매도({rsi:.1f})")
            elif rsi > 70:
                technical_score -= 10
                signals.append(f"RSI과매수({rsi:.1f})")

            # MACD 분석
            macd_line = macd_data['macd'][-1]
            macd_signal = macd_data['signal'][-1]
            macd_histogram = macd_data['histogram'][-1]

            if macd_line > macd_signal and macd_histogram > 0:
                technical_score += 25
                signals.append("MACD상승")
            elif macd_histogram > 0 and len(macd_data['histogram']) > 1:
                if macd_data['histogram'][-2] <= 0:
                    technical_score += 30
                    signals.append("MACD전환")

            # 이동평균 분석
            ma_5 = ma_data['ma_5'][-1] if ma_data['ma_5'] else current_price
            ma_20 = ma_data['ma_20'][-1] if ma_data['ma_20'] else current_price
            ma_60 = ma_data['ma_60'][-1] if ma_data['ma_60'] else current_price

            if current_price > ma_5 > ma_20 > ma_60:
                technical_score += 35
                signals.append("완벽상승배열")
            elif current_price > ma_5 > ma_20:
                technical_score += 20
                signals.append("단기상승배열")
            elif ma_5 > ma_20:
                technical_score += 15
                signals.append("골든크로스")

            return {
                'rsi': rsi,
                'macd_line': macd_line,
                'macd_signal': macd_signal,
                'macd_histogram': macd_histogram,
                'ma_5': ma_5,
                'ma_20': ma_20,
                'ma_60': ma_60,
                'technical_score': technical_score,
                'signals': signals
            }

        except Exception as e:
            logger.error(f"포괄적 기술적 분석 오류: {e}")
            return {
                'technical_score': 0,
                'signals': []
            }

    def _classify_candidates_by_strategy(self, batch_analysis: Dict, market: str) -> Dict:
        """🆕 분석 결과를 전략별로 분류"""
        try:
            classified = {
                'gap': [],
                'volume': [],
                'momentum': [],
                'technical': []
            }

            for stock_code, analysis in batch_analysis.items():
                try:
                    change_rate = analysis['change_rate']
                    volume = analysis['volume']
                    technical_score = analysis['technical_analysis']['technical_score']

                    # 기본 종목 정보
                    base_info = {
                        'stock_code': stock_code,
                        'current_price': analysis['current_price'],
                        'change_rate': change_rate,
                        'volume': volume,
                        'technical_score': technical_score,
                        'signals': analysis['technical_analysis']['signals']
                    }

                    # 🎯 전략별 분류 (중복 허용 - 하나의 종목이 여러 전략에 포함될 수 있음)

                    # 갭 트레이딩 (3% 이상 갭 + 기술적 점수 30점 이상)
                    if abs(change_rate) >= 3.0 and technical_score >= 30:
                        gap_candidate = {
                            **base_info,
                            'gap_rate': change_rate,
                            'strategy': 'gap_trading',
                            'reason': f"갭{change_rate:.1f}% + 기술적{technical_score}점"
                        }
                        classified['gap'].append(gap_candidate)

                    # 거래량 돌파 (2배 이상 거래량 추정 + 기술적 점수 25점 이상)
                    avg_volume = volume // 2  # 간단한 추정
                    if volume > avg_volume * 1.5 and technical_score >= 25:
                        volume_candidate = {
                            **base_info,
                            'volume_ratio': volume / max(avg_volume, 1),
                            'strategy': 'volume_breakout',
                            'reason': f"거래량{volume:,} + 기술적{technical_score}점"
                        }
                        classified['volume'].append(volume_candidate)

                    # 모멘텀 (1% 이상 상승 + RSI 적정 + 기술적 점수 20점 이상)
                    rsi = analysis['technical_analysis'].get('rsi', 50)
                    if change_rate >= 1.0 and 30 <= rsi <= 70 and technical_score >= 20:
                        momentum_candidate = {
                            **base_info,
                            'momentum_score': technical_score + (change_rate * 5),
                            'rsi': rsi,
                            'strategy': 'momentum',
                            'reason': f"모멘텀{change_rate:.1f}% + RSI{rsi:.0f} + 기술적{technical_score}점"
                        }
                        classified['momentum'].append(momentum_candidate)

                    # 순수 기술적 (기술적 점수 50점 이상)
                    if technical_score >= 50:
                        technical_candidate = {
                            **base_info,
                            'strategy': 'technical_priority',
                            'reason': f"기술적우선{technical_score}점"
                        }
                        classified['technical'].append(technical_candidate)

                except Exception as e:
                    logger.debug(f"종목 {stock_code} 분류 오류: {e}")
                    continue

            return classified

        except Exception as e:
            logger.error(f"전략 분류 오류: {e}")
            return {'gap': [], 'volume': [], 'momentum': [], 'technical': []}

    # === 편의 메서드 ===

    def is_market_open(self) -> bool:
        """장 운영 시간 확인"""
        now = datetime.now()

        # 주말 체크
        if now.weekday() >= 5:  # 토요일(5), 일요일(6)
            return False

        # 장 운영 시간: 09:00 ~ 15:30
        current_time = now.time()
        market_open = datetime.strptime("09:00", "%H:%M").time()
        market_close = datetime.strptime("15:30", "%H:%M").time()

        return market_open <= current_time <= market_close

    def get_account_info(self) -> Dict:
        """계좌 정보 요약"""
        balance_info = self.get_balance()
        token_info = self.get_token_info()

        return {
            "account": token_info.get("account", ""),
            "product": token_info.get("product", ""),
            "is_market_open": self.is_market_open(),
            "total_holdings": balance_info.get("total_count", 0),
            "status": "active" if token_info.get("status") == "success" else "inactive"
        }

    # === 통계 메서드 ===

    @staticmethod
    def get_api_stats() -> Dict:
        """API 호출 통계 (간소화)"""
        return {
            "message": "API 통계는 kis_auth 모듈에서 확인하세요",
            "status": "info"
        }

    # === 기존 호환성 메서드들 ===

    def get_websocket_approval_key(self) -> str:
        """웹소켓 접속키 발급 (호환성)"""
        logger.warning("웹소켓 접속키는 kis_websocket_client에서 자동 관리됩니다")
        return ""

    def _make_request(self, method: str, url: str, **kwargs) -> Optional[Dict]:
        """내부 요청 메서드 (호환성용)"""
        logger.warning("_make_request는 사용하지 마세요. 직접 kis_auth._url_fetch 사용 권장")
        return None

    # === Rate Limiting ===

    def wait_for_rate_limit(self, seconds: float = 0.1) -> None:
        """API 호출 간격 조절"""
        time.sleep(seconds)

    # === 공식 API 직접 접근 ===

    def call_order_api(self, function_name: str, **kwargs):
        """주문 API 직접 호출"""
        if hasattr(order_api, function_name):
            return getattr(order_api, function_name)(**kwargs)
        else:
            raise ValueError(f"주문 API 함수 '{function_name}'를 찾을 수 없습니다")

    def call_account_api(self, function_name: str, **kwargs):
        """계좌 API 직접 호출"""
        if hasattr(account_api, function_name):
            return getattr(account_api, function_name)(**kwargs)
        else:
            raise ValueError(f"계좌 API 함수 '{function_name}'를 찾을 수 없습니다")

    def call_market_api(self, function_name: str, **kwargs):
        """시세 API 직접 호출"""
        if hasattr(market_api, function_name):
            return getattr(market_api, function_name)(**kwargs)
        else:
            raise ValueError(f"시세 API 함수 '{function_name}'를 찾을 수 없습니다")

    # === 기존 호환성 속성들 ===

    @property
    def base_url(self) -> Optional[str]:
        """기본 URL (호환성)"""
        env = kis.getTREnv()
        return env.my_url if env else None

    @property
    def account_no(self) -> Optional[str]:
        """계좌번호 (호환성)"""
        env = kis.getTREnv()
        return f"{env.my_acct}{env.my_prod}" if env else None

    def _calculate_gap_info(self, row) -> Dict:
        """갭 정보 계산"""
        try:
            stock_code = str(row.get('stck_shrn_iscd', ''))
            stock_name = str(row.get('hts_kor_isnm', ''))

            try:
                current_price = int(row.get('stck_prpr', 0)) if row.get('stck_prpr', 0) != '' else 0
            except (ValueError, TypeError):
                current_price = 0

            try:
                gap_rate = float(row.get('gap_rate', 0)) if row.get('gap_rate', 0) != '' else 0.0
            except (ValueError, TypeError):
                gap_rate = 0.0

            try:
                change_rate = float(row.get('prdy_ctrt', 0)) if row.get('prdy_ctrt', 0) != '' else 0.0
            except (ValueError, TypeError):
                change_rate = 0.0

            if stock_code and current_price > 0:
                return {
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'current_price': current_price,
                    'gap_rate': gap_rate,
                    'change_rate': change_rate,
                    'reason': f"갭{gap_rate:.1f}% 변동{change_rate:.1f}%"
                }
            return None
        except Exception as e:
            logger.debug(f"갭 정보 계산 오류: {e}")
            return None

    def _calculate_volume_info(self, row) -> Dict:
        """거래량 정보 계산"""
        try:
            stock_code = str(row.get('mksc_shrn_iscd', ''))
            stock_name = str(row.get('hts_kor_isnm', ''))

            try:
                current_price = int(row.get('stck_prpr', 0)) if row.get('stck_prpr', 0) != '' else 0
            except (ValueError, TypeError):
                current_price = 0

            try:
                volume = int(row.get('acml_vol', 0)) if row.get('acml_vol', 0) != '' else 0
            except (ValueError, TypeError):
                volume = 0

            try:
                volume_ratio = float(row.get('vol_inrt', 0)) if row.get('vol_inrt', 0) != '' else 0.0
            except (ValueError, TypeError):
                volume_ratio = 0.0

            if stock_code and current_price > 0:
                return {
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'current_price': current_price,
                    'volume': volume,
                    'volume_ratio': volume_ratio,
                    'reason': f"거래량{volume:,}주 비율{volume_ratio:.1f}배"
                }
            return None
        except Exception as e:
            logger.debug(f"거래량 정보 계산 오류: {e}")
            return None

    def _calculate_momentum_info(self, row) -> Dict:
        """모멘텀 정보 계산"""
        try:
            stock_code = str(row.get('mksc_shrn_iscd', ''))
            stock_name = str(row.get('hts_kor_isnm', ''))

            try:
                current_price = int(row.get('stck_prpr', 0)) if row.get('stck_prpr', 0) != '' else 0
            except (ValueError, TypeError):
                current_price = 0

            try:
                power = float(row.get('cttr', 0)) if row.get('cttr', 0) != '' else 0.0
            except (ValueError, TypeError):
                power = 0.0

            try:
                volume = int(row.get('acml_vol', 0)) if row.get('acml_vol', 0) != '' else 0
            except (ValueError, TypeError):
                volume = 0

            if stock_code and current_price > 0:
                return {
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'current_price': current_price,
                    'power': power,
                    'volume': volume,
                    'reason': f"체결강도{power:.1f} 거래량{volume:,}주"
                }
            return None
        except Exception as e:
            logger.debug(f"모멘텀 정보 계산 오류: {e}")
            return None

    def _get_backup_gap_candidates(self, market: str, is_pre_market: bool) -> List[Dict]:
        """🔄 갭 트레이딩 백업 후보 조회 (매우 관대한 기준)"""
        try:
            logger.info(f"🔄 갭 백업 후보 조회 시작 ({market})")

            # 🎯 매우 관대한 기준으로 등락률 상위 조회
            if is_pre_market:
                min_fluctuation = "0.1"  # 0.1% 이상
            else:
                min_fluctuation = "0.3"  # 0.3% 이상

            backup_data = market_api.get_fluctuation_rank(
                fid_input_iscd=market,
                fid_rank_sort_cls_code="0",  # 상승률순
                fid_rsfl_rate1=min_fluctuation
            )

            if backup_data is None or backup_data.empty:
                logger.warning(f"갭 백업 조회에서도 데이터 없음 ({market})")
                return []

            backup_candidates = []
            for idx, row in backup_data.head(10).iterrows():  # 상위 10개만
                try:
                    stock_code = row.get('stck_shrn_iscd', '')
                    stock_name = row.get('hts_kor_isnm', '')
                    current_price = int(row.get('stck_prpr', 0))
                    change_rate = float(row.get('prdy_ctrt', 0))

                    if stock_code and current_price > 500 and change_rate > 0:  # 최소 조건
                        backup_candidates.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'current_price': current_price,
                            'change_rate': change_rate,
                            'strategy': 'gap_backup',
                            'reason': f"백업갭 변동{change_rate:.1f}%",
                            'technical_score': 0  # 기본값
                        })

                except Exception as e:
                    logger.debug(f"갭 백업 후보 처리 오류: {e}")
                    continue

            logger.info(f"🔄 갭 백업 후보: {len(backup_candidates)}개 발견")
            return backup_candidates

        except Exception as e:
            logger.error(f"갭 백업 후보 조회 오류: {e}")
            return []

    def _get_backup_volume_candidates(self, market: str, is_pre_market: bool) -> List[Dict]:
        """🔄 거래량 백업 후보 조회 (매우 관대한 기준)"""
        try:
            logger.info(f"🔄 거래량 백업 후보 조회 시작 ({market})")

            # 🎯 매우 관대한 거래량 기준으로 조회
            if is_pre_market:
                volume_threshold = "1000"  # 1천주
            else:
                volume_threshold = "5000"  # 5천주

            backup_data = market_api.get_volume_rank(
                fid_input_iscd=market,
                fid_blng_cls_code="1",  # 거래증가율
                fid_vol_cnt=volume_threshold
            )

            if backup_data is None or backup_data.empty:
                logger.warning(f"거래량 백업 조회에서도 데이터 없음 ({market})")
                return []

            backup_candidates = []
            for idx, row in backup_data.head(8).iterrows():  # 상위 8개만
                try:
                    stock_code = row.get('mksc_shrn_iscd', '')
                    stock_name = row.get('hts_kor_isnm', '')
                    current_price = int(row.get('stck_prpr', 0))
                    volume = int(row.get('acml_vol', 0))

                    if stock_code and current_price > 500 and volume > 1000:  # 최소 조건
                        backup_candidates.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'current_price': current_price,
                            'volume': volume,
                            'strategy': 'volume_backup',
                            'reason': f"백업거래량 {volume:,}주",
                            'technical_score': 0  # 기본값
                        })
                except Exception as e:
                    logger.debug(f"거래량 백업 후보 처리 오류: {e}")
                    continue

            logger.info(f"🔄 거래량 백업 후보: {len(backup_candidates)}개 발견")
            return backup_candidates

        except Exception as e:
            logger.error(f"거래량 백업 후보 조회 오류: {e}")
            return []

    def _get_backup_momentum_candidates(self, market: str, is_pre_market: bool) -> List[Dict]:
        """🔄 모멘텀 백업 후보 조회 (매우 관대한 기준)"""
        try:
            logger.info(f"🔄 모멘텀 백업 후보 조회 시작 ({market})")

            # 🎯 매우 관대한 체결강도 기준으로 조회
            if is_pre_market:
                power_threshold = "500"  # 500주
            else:
                power_threshold = "2000"  # 2천주

            backup_data = market_api.get_volume_power_rank(
                fid_input_iscd=market,
                fid_vol_cnt=power_threshold
            )

            if backup_data is None or backup_data.empty:
                logger.warning(f"모멘텀 백업 조회에서도 데이터 없음 ({market})")
                return []

            backup_candidates = []
            for idx, row in backup_data.head(6).iterrows():  # 상위 6개만
                try:
                    stock_code = row.get('mksc_shrn_iscd', '')
                    stock_name = row.get('hts_kor_isnm', '')
                    current_price = int(row.get('stck_prpr', 0))
                    power = float(row.get('cttr', 0))

                    if stock_code and current_price > 500 and power > 50:  # 최소 조건
                        backup_candidates.append({
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'current_price': current_price,
                            'power': power,
                            'strategy': 'momentum_backup',
                            'reason': f"백업체결강도 {power:.1f}",
                            'technical_score': 0  # 기본값
                        })

                except Exception as e:
                    logger.debug(f"모멘텀 백업 후보 처리 오류: {e}")
                    continue

            logger.info(f"🔄 모멘텀 백업 후보: {len(backup_candidates)}개 발견")
            return backup_candidates

        except Exception as e:
            logger.error(f"모멘텀 백업 후보 조회 오류: {e}")
            return []
