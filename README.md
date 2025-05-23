# StockBot - 자동 주식 거래 시스템

한국 주식시장을 위한 자동 거래 봇입니다.

## 주요 기능

- 실시간 웹소켓 데이터 수신
- 앙상블 전략 기반 매매 신호 생성
- 텔레그램을 통한 원격 제어
- 리스크 관리 시스템
- 거래 내역 데이터베이스 저장

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
│   ├── broker.py           # 증권사 API 래퍼
│   ├── trader.py           # 거래 로직
│   ├── risk_manager.py     # 리스크 관리
│   ├── websocket_manager.py # 웹소켓 관리
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
