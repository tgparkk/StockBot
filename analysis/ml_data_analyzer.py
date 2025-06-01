"""
머신러닝 데이터 분석 도구
수집된 신호/매수 데이터를 분석하여 패턴과 인사이트 도출
"""
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
from pathlib import Path

# 한글 폰트 설정
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

class MLDataAnalyzer:
    """💡 머신러닝 데이터 분석기"""

    def __init__(self, db_path: str = "data/ml_training_data.db"):
        """초기화"""
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"데이터베이스 파일을 찾을 수 없습니다: {self.db_path}")
        
        print(f"🔍 ML 데이터 분석기 초기화: {self.db_path}")

    def get_basic_stats(self) -> Dict:
        """📊 기본 통계 정보"""
        with sqlite3.connect(str(self.db_path)) as conn:
            stats = {}
            
            # 신호 분석 데이터 통계
            signal_query = """
                SELECT 
                    COUNT(*) as total_signals,
                    COUNT(CASE WHEN signal_passed = 1 THEN 1 END) as passed_signals,
                    COUNT(CASE WHEN signal_passed = 0 THEN 1 END) as failed_signals,
                    AVG(signal_strength) as avg_strength,
                    COUNT(DISTINCT stock_code) as unique_stocks,
                    COUNT(DISTINCT strategy_type) as unique_strategies,
                    MIN(timestamp) as earliest_signal,
                    MAX(timestamp) as latest_signal
                FROM signal_analysis
            """
            signal_stats = pd.read_sql_query(signal_query, conn).iloc[0].to_dict()
            stats['signals'] = signal_stats
            
            # 매수 시도 데이터 통계
            buy_query = """
                SELECT 
                    COUNT(*) as total_attempts,
                    COUNT(CASE WHEN attempt_result = 'SUCCESS' THEN 1 END) as successful_buys,
                    COUNT(CASE WHEN attempt_result = 'FAILED' THEN 1 END) as failed_buys,
                    AVG(signal_strength) as avg_buy_strength,
                    SUM(total_amount) as total_buy_amount,
                    AVG(total_amount) as avg_buy_amount
                FROM buy_attempts
            """
            buy_stats = pd.read_sql_query(buy_query, conn).iloc[0].to_dict()
            stats['buy_attempts'] = buy_stats
            
            # 성공률 계산
            if signal_stats['total_signals'] > 0:
                stats['signal_pass_rate'] = signal_stats['passed_signals'] / signal_stats['total_signals']
            else:
                stats['signal_pass_rate'] = 0
                
            if buy_stats['total_attempts'] > 0:
                stats['buy_success_rate'] = buy_stats['successful_buys'] / buy_stats['total_attempts']
            else:
                stats['buy_success_rate'] = 0
        
        return stats

    def analyze_signal_failures(self) -> pd.DataFrame:
        """🚫 신호 실패 원인 분석"""
        with sqlite3.connect(str(self.db_path)) as conn:
            query = """
                SELECT 
                    signal_reason,
                    strategy_type,
                    COUNT(*) as failure_count,
                    AVG(signal_strength) as avg_strength,
                    AVG(signal_threshold) as avg_threshold,
                    AVG(signal_strength - signal_threshold) as avg_strength_gap
                FROM signal_analysis 
                WHERE signal_passed = 0
                GROUP BY signal_reason, strategy_type
                ORDER BY failure_count DESC
            """
            return pd.read_sql_query(query, conn)

    def analyze_strength_distribution(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """📈 신호 강도 분포 분석"""
        with sqlite3.connect(str(self.db_path)) as conn:
            # 전체 신호 강도 분포
            overall_query = """
                SELECT 
                    ROUND(signal_strength, 1) as strength_range,
                    COUNT(*) as count,
                    AVG(CASE WHEN signal_passed = 1 THEN 1.0 ELSE 0.0 END) as pass_rate
                FROM signal_analysis
                GROUP BY ROUND(signal_strength, 1)
                ORDER BY strength_range
            """
            overall_dist = pd.read_sql_query(overall_query, conn)
            
            # 전략별 신호 강도 분포
            strategy_query = """
                SELECT 
                    strategy_type,
                    ROUND(signal_strength, 1) as strength_range,
                    COUNT(*) as count,
                    AVG(CASE WHEN signal_passed = 1 THEN 1.0 ELSE 0.0 END) as pass_rate
                FROM signal_analysis
                GROUP BY strategy_type, ROUND(signal_strength, 1)
                ORDER BY strategy_type, strength_range
            """
            strategy_dist = pd.read_sql_query(strategy_query, conn)
            
        return overall_dist, strategy_dist

    def analyze_disparity_patterns(self) -> pd.DataFrame:
        """📊 이격도 패턴 분석"""
        with sqlite3.connect(str(self.db_path)) as conn:
            query = """
                SELECT 
                    CASE 
                        WHEN disparity_20d <= 90 THEN '과매도 (≤90%)'
                        WHEN disparity_20d <= 110 THEN '중립 (90-110%)'
                        WHEN disparity_20d <= 125 THEN '약간과매수 (110-125%)'
                        ELSE '과매수 (>125%)'
                    END as disparity_range,
                    strategy_type,
                    COUNT(*) as signal_count,
                    AVG(CASE WHEN signal_passed = 1 THEN 1.0 ELSE 0.0 END) as pass_rate,
                    AVG(signal_strength) as avg_strength
                FROM signal_analysis 
                WHERE disparity_20d IS NOT NULL
                GROUP BY 
                    CASE 
                        WHEN disparity_20d <= 90 THEN '과매도 (≤90%)'
                        WHEN disparity_20d <= 110 THEN '중립 (90-110%)'
                        WHEN disparity_20d <= 125 THEN '약간과매수 (110-125%)'
                        ELSE '과매수 (>125%)'
                    END,
                    strategy_type
                ORDER BY pass_rate DESC
            """
            return pd.read_sql_query(query, conn)

    def analyze_time_patterns(self) -> pd.DataFrame:
        """⏰ 시간대별 패턴 분석"""
        with sqlite3.connect(str(self.db_path)) as conn:
            query = """
                SELECT 
                    strftime('%H', timestamp) as hour,
                    COUNT(*) as signal_count,
                    AVG(CASE WHEN signal_passed = 1 THEN 1.0 ELSE 0.0 END) as pass_rate,
                    AVG(signal_strength) as avg_strength
                FROM signal_analysis
                GROUP BY strftime('%H', timestamp)
                ORDER BY hour
            """
            return pd.read_sql_query(query, conn)

    def generate_feature_dataset(self, lookback_hours: int = 24) -> pd.DataFrame:
        """🤖 머신러닝용 피처 데이터셋 생성"""
        with sqlite3.connect(str(self.db_path)) as conn:
            query = """
                SELECT 
                    stock_code,
                    timestamp,
                    strategy_type,
                    signal_strength,
                    signal_threshold,
                    signal_passed,
                    current_price,
                    volume,
                    volume_ratio,
                    rsi,
                    macd,
                    bb_position,
                    disparity_5d,
                    disparity_20d,
                    disparity_60d,
                    price_change_pct,
                    signal_reason
                FROM signal_analysis
                ORDER BY timestamp
            """
            df = pd.read_sql_query(query, conn)
            
            if df.empty:
                return df
                
            # 시간 변환
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # 카테고리 변수 인코딩
            df['strategy_encoded'] = pd.Categorical(df['strategy_type']).codes
            
            # 파생 피처 생성
            df['strength_threshold_ratio'] = df['signal_strength'] / (df['signal_threshold'] + 0.001)
            df['disparity_spread'] = df['disparity_5d'] - df['disparity_20d']
            df['hour'] = df['timestamp'].dt.hour
            df['is_morning'] = (df['hour'] >= 9) & (df['hour'] <= 11)
            df['is_afternoon'] = (df['hour'] >= 13) & (df['hour'] <= 15)
            
            return df

    def create_visualizations(self, save_dir: str = "analysis/plots"):
        """📊 시각화 생성"""
        save_path = Path(save_dir)
        save_path.mkdir(exist_ok=True, parents=True)
        
        # 1. 기본 통계
        stats = self.get_basic_stats()
        self._plot_basic_stats(stats, save_path)
        
        # 2. 신호 실패 원인
        failure_analysis = self.analyze_signal_failures()
        if not failure_analysis.empty:
            self._plot_failure_reasons(failure_analysis, save_path)
        
        # 3. 신호 강도 분포
        overall_dist, strategy_dist = self.analyze_strength_distribution()
        if not overall_dist.empty:
            self._plot_strength_distribution(overall_dist, strategy_dist, save_path)
        
        # 4. 이격도 패턴
        disparity_patterns = self.analyze_disparity_patterns()
        if not disparity_patterns.empty:
            self._plot_disparity_patterns(disparity_patterns, save_path)
        
        # 5. 시간대 패턴
        time_patterns = self.analyze_time_patterns()
        if not time_patterns.empty:
            self._plot_time_patterns(time_patterns, save_path)
        
        print(f"📊 시각화 저장 완료: {save_path}")

    def _plot_basic_stats(self, stats: Dict, save_path: Path):
        """기본 통계 시각화"""
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('🎯 기본 통계 현황', fontsize=16, fontweight='bold')
        
        # 신호 성공/실패 비율
        signal_data = [stats['signals']['passed_signals'], stats['signals']['failed_signals']]
        signal_labels = ['성공', '실패']
        axes[0, 0].pie(signal_data, labels=signal_labels, autopct='%1.1f%%', startangle=90)
        axes[0, 0].set_title(f"신호 통과율 ({stats['signal_pass_rate']:.1%})")
        
        # 매수 성공/실패 비율
        buy_data = [stats['buy_attempts']['successful_buys'], stats['buy_attempts']['failed_buys']]
        buy_labels = ['성공', '실패']
        if sum(buy_data) > 0:
            axes[0, 1].pie(buy_data, labels=buy_labels, autopct='%1.1f%%', startangle=90)
            axes[0, 1].set_title(f"매수 성공률 ({stats['buy_success_rate']:.1%})")
        else:
            axes[0, 1].text(0.5, 0.5, '매수 시도 없음', ha='center', va='center')
            axes[0, 1].set_title('매수 성공률')
        
        # 신호 강도 히스토그램 (간단한 버전)
        axes[1, 0].bar(['평균 신호 강도'], [stats['signals']['avg_strength']], color='skyblue')
        axes[1, 0].set_title('평균 신호 강도')
        axes[1, 0].set_ylim(0, 1)
        
        # 종목 및 전략 수
        categories = ['고유 종목 수', '고유 전략 수']
        counts = [stats['signals']['unique_stocks'], stats['signals']['unique_strategies']]
        axes[1, 1].bar(categories, counts, color=['orange', 'green'])
        axes[1, 1].set_title('데이터 다양성')
        
        plt.tight_layout()
        plt.savefig(save_path / 'basic_stats.png', dpi=300, bbox_inches='tight')
        plt.close()

    def _plot_failure_reasons(self, failure_df: pd.DataFrame, save_path: Path):
        """실패 원인 시각화"""
        fig, axes = plt.subplots(1, 2, figsize=(20, 8))
        
        # 실패 원인 순위 (상위 10개)
        top_failures = failure_df.groupby('signal_reason')['failure_count'].sum().nlargest(10)
        axes[0].barh(range(len(top_failures)), top_failures.values)
        axes[0].set_yticks(range(len(top_failures)))
        axes[0].set_yticklabels([reason[:50] + '...' if len(reason) > 50 else reason 
                                for reason in top_failures.index])
        axes[0].set_xlabel('실패 횟수')
        axes[0].set_title('🚫 신호 실패 원인 Top 10')
        
        # 전략별 실패 분포
        strategy_failures = failure_df.groupby('strategy_type')['failure_count'].sum()
        axes[1].pie(strategy_failures.values, labels=strategy_failures.index, autopct='%1.1f%%')
        axes[1].set_title('📈 전략별 실패 분포')
        
        plt.tight_layout()
        plt.savefig(save_path / 'failure_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()

    def _plot_strength_distribution(self, overall_df: pd.DataFrame, strategy_df: pd.DataFrame, save_path: Path):
        """신호 강도 분포 시각화"""
        fig, axes = plt.subplots(2, 1, figsize=(15, 12))
        
        # 전체 신호 강도 분포와 통과율
        ax1 = axes[0]
        ax2 = ax1.twinx()
        
        bars = ax1.bar(overall_df['strength_range'], overall_df['count'], alpha=0.7, color='skyblue')
        line = ax2.plot(overall_df['strength_range'], overall_df['pass_rate'] * 100, 
                       color='red', marker='o', linewidth=2)
        
        ax1.set_xlabel('신호 강도')
        ax1.set_ylabel('신호 개수', color='blue')
        ax2.set_ylabel('통과율 (%)', color='red')
        ax1.set_title('📊 신호 강도별 분포 및 통과율')
        
        # 전략별 신호 강도 분포 (히트맵)
        pivot_data = strategy_df.pivot(index='strategy_type', columns='strength_range', values='count').fillna(0)
        sns.heatmap(pivot_data, annot=True, fmt='.0f', cmap='YlOrRd', ax=axes[1])
        axes[1].set_title('🎯 전략별 신호 강도 분포')
        axes[1].set_xlabel('신호 강도')
        axes[1].set_ylabel('전략')
        
        plt.tight_layout()
        plt.savefig(save_path / 'strength_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

    def _plot_disparity_patterns(self, disparity_df: pd.DataFrame, save_path: Path):
        """이격도 패턴 시각화"""
        fig, axes = plt.subplots(1, 2, figsize=(20, 8))
        
        # 이격도 구간별 통과율
        overall_disparity = disparity_df.groupby('disparity_range').agg({
            'signal_count': 'sum',
            'pass_rate': 'mean'
        }).reset_index()
        
        ax1 = axes[0]
        ax2 = ax1.twinx()
        
        bars = ax1.bar(overall_disparity['disparity_range'], overall_disparity['signal_count'], 
                      alpha=0.7, color='lightgreen')
        line = ax2.plot(overall_disparity['disparity_range'], overall_disparity['pass_rate'] * 100, 
                       color='red', marker='o', linewidth=2)
        
        ax1.set_ylabel('신호 개수', color='green')
        ax2.set_ylabel('통과율 (%)', color='red')
        ax1.set_title('📈 이격도 구간별 신호 분포 및 통과율')
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
        
        # 전략별 이격도 패턴 히트맵
        pivot_data = disparity_df.pivot(index='strategy_type', columns='disparity_range', values='pass_rate')
        sns.heatmap(pivot_data, annot=True, fmt='.2f', cmap='RdYlGn', ax=axes[1], 
                   vmin=0, vmax=1, cbar_kws={'label': '통과율'})
        axes[1].set_title('🎯 전략별 이격도 구간 통과율')
        axes[1].set_xlabel('이격도 구간')
        axes[1].set_ylabel('전략')
        
        plt.tight_layout()
        plt.savefig(save_path / 'disparity_patterns.png', dpi=300, bbox_inches='tight')
        plt.close()

    def _plot_time_patterns(self, time_df: pd.DataFrame, save_path: Path):
        """시간대 패턴 시각화"""
        fig, axes = plt.subplots(2, 1, figsize=(15, 10))
        
        # 시간대별 신호 개수
        axes[0].bar(time_df['hour'], time_df['signal_count'], color='lightblue', alpha=0.7)
        axes[0].set_xlabel('시간 (24시간)')
        axes[0].set_ylabel('신호 개수')
        axes[0].set_title('⏰ 시간대별 신호 발생 빈도')
        axes[0].set_xticks(range(0, 24, 2))
        
        # 시간대별 통과율과 평균 강도
        ax1 = axes[1]
        ax2 = ax1.twinx()
        
        line1 = ax1.plot(time_df['hour'], time_df['pass_rate'] * 100, 
                        color='red', marker='o', linewidth=2, label='통과율')
        line2 = ax2.plot(time_df['hour'], time_df['avg_strength'], 
                        color='blue', marker='s', linewidth=2, label='평균 강도')
        
        ax1.set_xlabel('시간 (24시간)')
        ax1.set_ylabel('통과율 (%)', color='red')
        ax2.set_ylabel('평균 신호 강도', color='blue')
        ax1.set_title('📊 시간대별 신호 품질')
        ax1.set_xticks(range(0, 24, 2))
        
        # 범례
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
        
        plt.tight_layout()
        plt.savefig(save_path / 'time_patterns.png', dpi=300, bbox_inches='tight')
        plt.close()

    def export_ml_dataset(self, output_path: str = "analysis/ml_dataset.csv"):
        """🤖 머신러닝용 데이터셋 내보내기"""
        df = self.generate_feature_dataset()
        
        if df.empty:
            print("⚠️ 내보낼 데이터가 없습니다.")
            return
        
        # 결측값 처리
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].median())
        
        # 파일 저장
        output_file = Path(output_path)
        output_file.parent.mkdir(exist_ok=True, parents=True)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"📁 ML 데이터셋 저장 완료: {output_file}")
        print(f"📊 데이터 크기: {df.shape}")
        print(f"📈 Target 분포: {df['signal_passed'].value_counts().to_dict()}")

    def print_summary_report(self):
        """📋 요약 보고서 출력"""
        print("=" * 80)
        print("🎯 머신러닝 데이터 분석 보고서")
        print("=" * 80)
        
        # 기본 통계
        stats = self.get_basic_stats()
        print(f"\n📊 기본 통계:")
        print(f"  • 총 신호 수: {stats['signals']['total_signals']:,}개")
        print(f"  • 신호 통과율: {stats['signal_pass_rate']:.1%}")
        print(f"  • 매수 성공률: {stats['buy_success_rate']:.1%}")
        print(f"  • 고유 종목 수: {stats['signals']['unique_stocks']:,}개")
        print(f"  • 고유 전략 수: {stats['signals']['unique_strategies']:,}개")
        
        # 주요 실패 원인
        failure_analysis = self.analyze_signal_failures()
        if not failure_analysis.empty:
            print(f"\n🚫 주요 실패 원인 Top 5:")
            top_failures = failure_analysis.groupby('signal_reason')['failure_count'].sum().nlargest(5)
            for i, (reason, count) in enumerate(top_failures.items(), 1):
                print(f"  {i}. {reason}: {count:,}회")
        
        # 이격도 분석
        disparity_patterns = self.analyze_disparity_patterns()
        if not disparity_patterns.empty:
            print(f"\n📈 이격도 구간별 통과율:")
            overall_disparity = disparity_patterns.groupby('disparity_range').agg({
                'signal_count': 'sum',
                'pass_rate': 'mean'
            }).round(3)
            for range_name, row in overall_disparity.iterrows():
                print(f"  • {range_name}: {row['pass_rate']:.1%} ({row['signal_count']:,}개)")
        
        print("\n" + "=" * 80)

    def analyze_enhanced_patterns(self) -> pd.DataFrame:
        """🆕 확장된 패턴 분석"""
        with sqlite3.connect(str(self.db_path)) as conn:
            query = """
                SELECT 
                    strategy_type,
                    CASE 
                        WHEN hour_of_day BETWEEN 9 AND 11 THEN '오전'
                        WHEN hour_of_day BETWEEN 12 AND 14 THEN '오후' 
                        ELSE '기타'
                    END as time_period,
                    CASE 
                        WHEN disparity_20d <= 90 THEN '과매도'
                        WHEN disparity_20d <= 110 THEN '중립'
                        ELSE '과매수'
                    END as disparity_zone,
                    CASE 
                        WHEN volume_ratio_20d >= 2.0 THEN '고거래량'
                        WHEN volume_ratio_20d >= 1.5 THEN '중거래량'
                        ELSE '저거래량'
                    END as volume_zone,
                    COUNT(*) as signal_count,
                    AVG(CASE WHEN signal_passed = 1 THEN 1.0 ELSE 0.0 END) as pass_rate,
                    AVG(signal_strength) as avg_strength,
                    AVG(rsi) as avg_rsi,
                    AVG(volatility_20d) as avg_volatility
                FROM signal_analysis 
                WHERE signal_strength IS NOT NULL
                GROUP BY strategy_type, time_period, disparity_zone, volume_zone
                HAVING signal_count >= 5
                ORDER BY pass_rate DESC, signal_count DESC
            """
            return pd.read_sql_query(query, conn)

    def analyze_market_conditions_impact(self) -> pd.DataFrame:
        """🆕 시장 상황별 성과 분석"""
        with sqlite3.connect(str(self.db_path)) as conn:
            query = """
                SELECT 
                    CASE 
                        WHEN market_volatility <= 0.1 THEN '안정'
                        WHEN market_volatility <= 0.2 THEN '보통'
                        ELSE '고변동'
                    END as volatility_regime,
                    CASE 
                        WHEN kospi_change_pct >= 1.0 THEN '강세장'
                        WHEN kospi_change_pct >= -1.0 THEN '보합장'
                        ELSE '약세장'
                    END as market_trend,
                    strategy_type,
                    COUNT(*) as attempt_count,
                    AVG(CASE WHEN attempt_result = 'SUCCESS' THEN 1.0 ELSE 0.0 END) as success_rate,
                    AVG(signal_strength) as avg_signal_strength
                FROM buy_attempts ba
                WHERE kospi_change_pct IS NOT NULL AND market_volatility IS NOT NULL
                GROUP BY volatility_regime, market_trend, strategy_type
                HAVING attempt_count >= 3
                ORDER BY success_rate DESC
            """
            return pd.read_sql_query(query, conn)

    def get_feature_importance_data(self) -> pd.DataFrame:
        """🆕 피처 중요도 분석용 데이터"""
        with sqlite3.connect(str(self.db_path)) as conn:
            query = """
                SELECT 
                    signal_strength, disparity_20d, volume_ratio_20d, rsi, macd,
                    bb_position, volatility_20d, momentum_20d, hour_of_day,
                    CASE WHEN day_of_week IN (0,1,2,3,4) THEN 1 ELSE 0 END as is_weekday,
                    CASE WHEN is_opening_hour = 1 THEN 1 ELSE 0 END as is_opening,
                    CASE WHEN is_closing_hour = 1 THEN 1 ELSE 0 END as is_closing,
                    signal_passed as target
                FROM signal_analysis 
                WHERE signal_strength IS NOT NULL 
                AND disparity_20d IS NOT NULL 
                AND volume_ratio_20d IS NOT NULL
            """
            return pd.read_sql_query(query, conn)


if __name__ == "__main__":
    # 사용 예시
    try:
        analyzer = MLDataAnalyzer()
        
        # 요약 보고서 출력
        analyzer.print_summary_report()
        
        # 시각화 생성
        analyzer.create_visualizations()
        
        # 머신러닝 데이터셋 내보내기
        analyzer.export_ml_dataset()
        
        print("\n✅ 분석 완료!")
        
    except FileNotFoundError as e:
        print(f"❌ 오류: {e}")
        print("💡 힌트: 시스템을 실행하여 데이터를 수집한 후 다시 시도해주세요.")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}") 