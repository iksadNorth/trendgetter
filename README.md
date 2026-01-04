# TrendGetter
TrendSetter들이 만드는 데이터를 수집하는 프로그램. TrendGetter

![TrendGetter 그림](documents/cartoon-trendgetter.png)

*그림 0: SNS들이 던지는 데이터 받는 거미팔 포수*

TrendGetter는 3가지 부분으로 나뉩니다.

- Extract: 각종 SNS 웹페이지, Youtube 댓글, 커뮤니티 댓글 등등의 대중이 만드는 텍스트 정보 수집

- Transform: 각 데이터들을 통일된 스키마로 라벨링 및 데이터 품질 검사, Time-Windowed Temporal TF-IDF 스코어 계산.

- Load: Kibana를 활용해 데이터 시각화 구현. 이를 위해 Elasticsearch에 데이터 적재.

![PipeLine 그림](documents/cartoon-pipeline.png)

*그림 1: 파이프라인 개요*

## 왜 이런 식으로 만들었나요?

- **토크나이저 로직 변경, 데이터 Html 태그 제거 등의 이유로 로직이 빈번하게 변경돼서 기존에 계산한 데이터가 쓸모없어지네..**
    - Task들을 실행일자 기준으로 **멱등성을 확보**하여 부담없이 Backfill 수행
- **크롤링을 위해 Selenium, 프로비저닝을 위해 Terraform 등을 Airflow 이미지에 포함시키니 이미지 크기가 과하게 커지네..**
    - 각각 Pan, Hermes라는 헬퍼 API 서버로 분리해 **이미지 크기를 대폭 감소**시킴
- **크롤링 Task 크기가 너무 넓게 정의되어 있다보니, 많은 시간을 들여 Retry를 수행해야 하네..**
    - 크롤링 Task를 역할마다 잘게 쪼개서 **Retry 범위를 좁힘**
- **전통적인 스코어들 중에 트렌드 키워드를 정의할 만한 기준이 존재하지 않네..**
    - 기존 TF-IDF의 정의를 응용해 시간 의존적인 **TF-IDF 스코어를 새롭게 정의**

## 이거 어떻게 사용하는 거에요?

### 과정 1: Airflow Webserver를 이용한 Dag 실행

![demo](documents/demo-airflow-dagrun.gif)

## 그래서 의도한 대로 성과가 나왔나요?

1. **파이프라인 재시도 시간**
    - **Without Airflow vs With Airflow**: 전자(약 ??분)을 후자(약 ??분)로 단축
    - 로그확인 및 재시작 등 모든 과정이 자동화됨

## 프로젝트 구조
```
trendgetter/
├── example.py
└── scripts/
```