# StockBot - 한국투자증권 API 자동매매 봇

한국투자증권 Open API를 활용한 주식 자동매매 봇 프로젝트입니다.

## 📋 주요 기능

- 실시간 웹소켓 데이터 수신
- 앙상블 전략 기반 매매 신호 생성
- 텔레그램을 통한 원격 제어
- 리스크 관리 시스템
- 거래 내역 데이터베이스 저장
- **실전투자 전용** (모의투자 미지원)

## WebSocket
https://wikidocs.net/164058

## Api 호출 유량 안내
# REST API
실전투자: 1초당 20건

※ 계좌 단위로 유량 제한(실전투자의 경우, 1계좌당 1초당 20건)

# WEBSOCKET : 1세션

실시간체결가 + 호가 + 예상체결 + 체결통보 등 실시간 데이터 합산 41건까지 등록 가능
※ 국내주식/해외주식/국내파생/해외파생 모든 상품 실시간 합산 41건
※ 체결통보의 경우 HTS ID 단위로 등록하여, ID에 연결된 모든 계좌의 체결 통보 수신
※ 계좌(앱키) 단위로 유량 제한(1개의 계좌(앱키)당 1세션)
※ 1개의 PC에서 여러 개의 계좌(앱키)로 세션 연결 가능

https://apiportal.koreainvestment.com/community/10000000-0000-0011-0000-000000000002/post/32f34d68-d43a-4da9-9a83-c6fb278a05b0

## 기능 서술

당일 매수 매매 프로그램을 구현하려고 하고요.
텔레그램 api 를 이용해서 외부에서도 프로그램을 제어하고 싶어요. 생각 해본 기능으로는 '강제 종료', '일시 정지', '재개', '계좌 상황 보기', '오늘의 수익률 보기' 입니다.
에러, 특정상황(거래성공, 실패 등 다양항 에러 상황) 은 날짜폴더별로 로그 파일을 만들려고 합니다.
api key 정보와 같은 중요정보는 .env 파일에 기록하려고합니다.
손절 임계값, 이익 임계값, 최대손실율, 최대 허용 변동성, 일일 최대 리스크 금액, 최대 포지션 크기 등 주식거래에 대한 정보는 ini 파일에 담는게 좋을거  같고요.

메인 함수에서는 특별한 로직(이익이 날거같은)을 통해 종목을 30개 정도 정하고, 장시간을 체크해서 장 시간동안은 무한루프를 돌면서
종목갯수만큼 스레드를 만들어서 특정 조건( 여러 단타전략)  에 부합하면 먼저 계좌 상황과 해당 종목은 많이 매수해도 되는지 등 다양한 조건으로 판별해서 매수하려고 하면 좋을거 같네요.
매수한 종목은 sqlite 에 저장하면 좋을거 같네요.( 나중에 ml 을 이용해서 분석할 예정)
n 개의 스레드로 매수 판단을 하지만 실제 매수는 (매수시그널이 나온 종목들은) 순차적으로 매수 하는게 좋을거 같아요.

매도프로세스는 매수한 종목 만큼 스레드를 만들어서 주식사고적인 과정을 통해서 매수를 했으면 합니다.
마찬가지로 거래 내용을 sqlite 에 저장하려고 합니다.

## 환경 설정

### 1. API 인증 정보 설정

`config/.env` 파일을 생성하고 다음 정보를 입력하세요:

```bash
# 한국투자증권 API 설정
KIS_BASE_URL=https://openapi.koreainvestment.com:9443
KIS_APP_KEY=your_app_key_here
KIS_APP_SECRET=your_app_secret_here
KIS_ACCOUNT_NO=your_account_number_here

# 웹소켓 실시간 데이터용 HTS ID (주식체결통보 구독시 필요)
KIS_HTS_ID=your_hts_id_here
```

### 2. HTS_ID 확인 방법

**HTS_ID**는 한국투자증권 HTS(영웅문) 로그인 ID입니다:

1. **HTS 프로그램에서 확인**
   - 한국투자증권 HTS(영웅문) 로그인 화면의 사용자 ID
   - 또는 로그인 후 [도구] → [환경설정] → [사용자정보]에서 확인

2. **홈페이지에서 확인**
   - https://securities.koreainvestment.com 로그인
   - [마이페이지] → [개인정보]에서 확인

3. **고객센터 문의**
   - 한국투자증권 고객센터: 1544-5000

**주의사항**:
- HTS_ID는 주식체결통보(H0STCNI9) 구독시에만 필요합니다
- 주식체결가, 주식호가 구독시에는 필요하지 않습니다
- 실제 계좌가 없으면 체결통보 데이터를 받을 수 없습니다

## 사용법

### 기본 API 테스트
```bash
python examples/test_websocket_approval_key.py
```

### 웹소켓 실시간 데이터 테스트
```bash
# 간단한 테스트 (주식체결가, 호가만)
python examples/simple_websocket_test.py

# 전체 기능 테스트
python examples/websocket_example.py
```

## 설치 방법

1. 가상환경 생성 및 활성화
```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
```

2. 의존성 설치
```bash
pip install -r requirements.txt
```

3. 환경 변수 설정
`config/.env` 파일을 실제 API 키로 수정

4. 실행
```bash
python main.py
```

## 프로젝트 구조

```
D:\GIT\StockBot\
├── .gitignore
├── README.md
├── requirements.txt
├── main.py
├── config/
│   ├── .env                # API 키 설정 (실제 값으로 수정 필요)
│   └── settings.ini        # 거래 설정
├── core/
│   ├── __init__.py
│   ├── broker.py           # 증권사 API 래퍼 (기본 API 호출)
│   ├── trader.py           # 거래 로직
│   ├── risk_manager.py     # 리스크 관리
│   ├── websocket_manager.py # 웹소켓 관리
│   ├── data/               # 데이터 모듈
│   │   ├── __init__.py
│   │   ├── kis_api_manager.py      # KIS API 매니저 (broker.py 확장)
│   │   ├── kis_data_models.py      # KIS 데이터 모델
│   │   └── strategy_data_adapter.py # 전략 데이터 어댑터
│   └── strategy/           # 전략 모듈
│       ├── __init__.py
│       ├── base.py         # 전략 베이스 클래스
│       ├── momentum.py     # 모멘텀 전략
│       ├── mean_reversion.py # 평균회귀 전략
│       ├── volume_breakout.py # 거래량 돌파 전략
│       ├── pattern.py      # 패턴 인식 전략
│       └── ensemble.py     # 앙상블 매니저
├── telegram/
│   ├── __init__.py
│   └── bot.py             # 텔레그램 봇
├── database/
│   ├── __init__.py
│   ├── db_models.py          # DB 모델
│   └── db_manager.py         # DB 매니저
├── utils/
│   ├── __init__.py
│   ├── logger.py          # 로깅 시스템
│   ├── market_timer.py    # 장시간 체크
│   └── indicators.py      # 기술적 지표
└── logs/                  # 로그 디렉토리
```

## 🎯 시간대별 전략 시스템

### 전략 조합 (설정 파일 관리)
- **09:00-09:30 (장 시작 골든타임)**: Gap Trading (100%) + Volume Breakout (확인용)
- **09:30-11:30 (오전 주도주 시간)**: Volume Breakout (70%) + Momentum (30%) + Gap Trading (지속 확인)
- **11:30-14:00 (점심 시간대)**: Volume Breakout (80%) + Momentum (20%)
- **14:00-15:20 (마감 추세 시간)**: Momentum (60%) + Volume Breakout (40%)

### 핵심 전략

#### 1. 갭 트레이딩 (Gap Trading)
- **목표**: 갭 상승 종목의 초기 모멘텀 포착
- **조건**: 갭 3% 이상, 거래량 2배 이상, 상향 갭만
- **KIS API 데이터**:
  - 현재가 시세 (FHKST01010100)
  - 기간별 시세 (FHKST03010100)
  - 갭 크기, 거래량 비율, 시가 후 10분간 거래량

#### 2. 거래량 돌파 (Volume Breakout)
- **목표**: 거래량 급증 + 가격 돌파 동반 종목
- **조건**: 거래량 3배 이상, 20일 고/저가 2% 돌파
- **KIS API 데이터**:
  - 현재가 시세, 호가 정보 (FHKST01010200)
  - 20일 평균 거래량, 매수/매도 잔량 비율
  - 저항선/지지선, 돌파 지점, 매수세 강도

#### 3. 모멘텀 (Momentum)
- **목표**: 기술적 지표 기반 추세 추종
- **조건**: 이동평균 정배열, RSI 적정 구간, MACD 상향
- **KIS API 데이터**:
  - 분봉 조회 (FHKST03010200)
  - 5/20/60일 이동평균, RSI(9일), MACD(5,13,5)
  - 1일/5일 수익률, 추세 강도

## 🔧 KIS API 연동 시스템

### 아키텍처
- **broker.py**: 기본 KIS API 호출 (인증, 주문, 조회)
- **kis_api_manager.py**: broker.py 확장하여 전략용 데이터 조회 기능 추가
- **kis_data_models.py**: KIS API 응답을 전략에 최적화된 데이터 모델로 변환
- **strategy_data_adapter.py**: 기존 전략 인터페이스와 KIS 데이터 연결

### 데이터 모델
```python
# 현재가 정보
KISCurrentPrice: 주식현재가, 시가, 고저가, 거래량, 체결정보

# 기간별 시세
KISHistoricalData: 일봉 데이터, 이동평균 계산용

# 호가 정보
KISOrderBook: 1~10호가, 매수/매도 잔량, 체결강도

# 전략별 데이터
GapTradingData: 갭 크기, 방향, 거래량 비율
VolumeBreakoutData: 돌파 지점, 저항/지지선, 거래량 강도
MomentumData: 기술적 지표, 추세 강도, 모멘텀 점수
```

## 🎯 시간대별 전략 시스템

### 📖 개요
실시간 체결통보와 동적 종목 발굴을 통한 시간대별 자동 거래 시스템

### 🚀 주요 기능

### 📊 시간대별 전략 자동 전환
- **골든타임 (09:00-09:30)**: 갭 트레이딩 중심
- **주도주 시간 (09:30-11:30)**: 거래량 돌파 전략
- **점심시간 (11:30-14:00)**: 안정적 모멘텀 전략
- **마감 추세 (14:00-15:20)**: 모멘텀 강화 전략

### 🔄 동적 종목 발굴
- REST API를 통한 실시간 종목 탐색
- 하드코딩된 60개 → 수백개 동적 후보
- 백그라운드 시장 스크리닝

### 💾 완전한 데이터베이스 기록
- 시간대별 선택 종목 기록 (4개 테이블)
- 실시간 거래 기록 및 포지션 관리
- 전략별 성과 분석 및 통계

### 🤖 텔레그램 원격 제어 **(NEW!)**
- 별도 스레드에서 안전한 실행
- 실시간 체결 알림
- 원격 시스템 제어

## 🔧 설정

### 환경변수 (.env)
```env
# KIS API 설정
KIS_APP_KEY="your_app_key"
KIS_APP_SECRET="your_app_secret"
KIS_ACCOUNT_NO="your_account_number"

# 텔레그램 봇 설정 (선택사항)
TELEGRAM_BOT_TOKEN="your_bot_token"
TELEGRAM_CHAT_ID="your_chat_id"
```

### 텔레그램 봇 설정 방법

1. **BotFather에서 봇 생성**
   - 텔레그램에서 @BotFather 검색
   - `/newbot` 명령어로 새 봇 생성
   - 봇 토큰을 `.env`의 `TELEGRAM_BOT_TOKEN`에 설정

2. **Chat ID 확인**
   - 봇과 대화 시작 후 메시지 전송
   - `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates` 접속
   - `chat.id` 값을 `.env`의 `TELEGRAM_CHAT_ID`에 설정

## 📱 텔레그램 명령어

### 📊 상태 조회
- `/status` - 시스템 전체 상태
- `/scheduler` - 전략 스케줄러 상태
- `/stocks` - 현재 활성 종목
- `/today` - 오늘 거래 요약

### 💰 계좌 정보
- `/balance` - 계좌 잔고
- `/profit` - 오늘 수익률
- `/positions` - 현재 포지션
- `/trades` - 최근 거래 내역

### 🎮 제어 명령
- `/pause` - 거래 일시정지
- `/resume` - 거래 재개
- `/stop` - 시스템 종료
- `/help` - 명령어 도움말

### 🔔 자동 알림
- 실시간 체결 통보
- 중요 시스템 이벤트
- 수익/손실 알림

## 🏃‍♂️ 실행

### 🚀 간편 실행 (배치 파일 - 권장)

#### 초기 설치
```cmd
install.bat
```
- 가상환경 생성
- 패키지 자동 설치
- 환경설정 파일 템플릿 생성
- 데이터베이스 초기화

#### 일반 실행
```cmd
run_stockbot.bat
```
- 환경 자동 확인
- 의존성 설치 (필요시)
- StockBot 실행 (장시간 전용)

#### 텔레그램 없이 실행
```cmd
run_stockbot_no_telegram.bat
```
- 텔레그램 봇 비활성화
- 안전한 테스트 모드

#### 🧪 테스트 모드 (장외시간 대응) - **NEW!**
```cmd
run_stockbot_testing_mode.bat
```
- **장외시간에도 실행 가능**
- API 에러를 경고로 처리
- 웹소켓 연결 실패 시 계속 진행
- 제한적 데이터 수집 모드
- 교육/개발 목적

#### 업데이트
```cmd
update.bat
```
- 패키지 업데이트
- 데이터베이스 마이그레이션
- 로그 파일 정리

### 🔧 수동 실행

#### 1. 의존성 설치
```bash
pip install -r requirements.txt
```

#### 2. 데이터베이스 초기화
```bash
python database/init_db.py
```

#### 3. 봇 실행
```bash
python main.py
```

## 🐛 디버깅 환경

### Cursor/VS Code에서 F5 디버깅
1. **F5 키를 누르면** 디버깅 메뉴가 나타납니다
2. **실행 모드 선택**:
   - 🚀 **StockBot 실행**: 일반 실행 (장시간 전용)
   - 🐛 **StockBot 디버그 모드**: 중단점에서 시작
   - 🤖 **텔레그램 봇 없이 실행**: 텔레그램 없이 테스트
   - 🧪 **테스트 모드 (장외시간 대응)**: 장외시간에도 실행 가능 - **NEW!**
   - 🧪 **디버그 러너 (체크만)**: 환경 설정만 확인
   - 📊 **데이터베이스 초기화**: DB 초기화

### 디버깅 스크립트 실행
```bash
# 환경 설정 확인만
python debug_runner.py --check-only

# 텔레그램 봇 없이 실행
python debug_runner.py --no-telegram

# 일반 디버깅 실행
python debug_runner.py
```

### Tasks (Ctrl+Shift+P → Tasks: Run Task)
- 🧹 **코드 포맷팅 (Black)**: 코드 자동 정리
- 🔍 **코드 린팅 (Flake8)**: 코드 품질 검사
- 📦 **의존성 설치**: requirements.txt 설치
- 🧹 **로그 파일 정리**: 7일 이상된 로그 삭제
- 🔧 **문법 검사**: 현재 파일 문법 확인

### 설정된 개발 환경
- **자동 포맷팅**: 파일 저장 시 Black으로 자동 정리
- **자동 import 정리**: isort로 import 구문 정리
- **린팅**: Flake8으로 실시간 코드 품질 검사
- **IntelliSense**: 자동 완성 및 타입 힌트
- **가상환경 자동 인식**: .venv 자동 활성화

## 📁 프로젝트 구조

```
StockBot/
├── main.py                    # 메인 실행 파일
├── core/                      # 핵심 시스템
│   ├── strategy_scheduler.py  # 시간대별 전략 스케줄러
│   ├── hybrid_data_manager.py # 하이브리드 데이터 관리
│   ├── rest_api_manager.py    # REST API 관리
│   └── websocket_manager.py   # WebSocket 관리
├── database/                  # 데이터베이스
│   ├── db_models.py          # 테이블 모델 (10개 테이블)
│   ├── db_manager.py         # 데이터베이스 매니저
│   └── init_db.py            # 초기화 스크립트
├── telegram_bot/             # 텔레그램 봇
│   └── bot.py                # 봇 구현체
├── utils/                     # 유틸리티
│   └── logger.py             # 로깅 시스템
└── logs/                      # 로그 파일들
    └── 2025-05-24/           # 날짜별 로그
```

## 🎯 특징

### ✅ 안전성
- 스레드 안전한 설계
- 실시간 체결통보 기반 정확한 기록
- 예외 처리 및 로깅

### ✅ 확장성
- 새로운 전략 쉽게 추가 가능
- 동적 종목 발굴로 무한 확장
- 모듈화된 구조

### ✅ 모니터링
- 텔레그램을 통한 실시간 원격 제어
- 완전한 데이터베이스 기록
- 시간대별/전략별 성과 분석

## ⚠️ 주의사항

1. **모의투자 환경에서 충분한 테스트** 후 실전 적용
2. **리스크 관리**: 손절가, 투자 한도 설정 필수
3. **API 호출 제한**: KIS API 호출 제한 준수
4. **시장 상황**: 급변하는 시장에서는 수동 개입 필요

## 📞 지원

- 로그 파일: `logs/날짜/` 디렉토리 확인
- 텔레그램 봇을 통한 실시간 상태 모니터링
- 데이터베이스 기록을 통한 상세 분석

---

**주의**: 이 시스템은 교육 및 연구 목적으로 제작되었습니다. 실제 투자에 사용시 발생하는 손실에 대해 책임지지 않습니다.
