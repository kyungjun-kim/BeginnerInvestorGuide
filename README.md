# 📊 주린이를 위한 시장 트렌드 분석

## `프로그래머스 데이터 엔지니어링 데브코스 4기 [Team6] Engine-in-us`
## 📌 `2024/12/18 - 2025/01/14`

---

## OVERVIEW

<aside>
💡

### What?

이 프로젝트는 주식 초보자라 불리는 주린이를 대상으로 국내 주식 시장의 트렌드를 한 눈에 확인할 수 있는 대시보드를 제공합니다. 

**`주요 기능`** 

✔️ *한국투자증권 API*를 이용한 인기 종목과 실시간 데이터 시각화

✔️ *국내 주식 커뮤니티 글*을 수집해 소비자 기준 언급이 많은 종목 및 주식 관련 키워드 제공

✔️ *네이버 증권* 크롤링 정보를 통해 인기 종목에 대한 최신 정보 제공

</aside>

<aside>
💡

### **Why?**

**`주식 초보자들의 어려움`**

많은 주린이들이 정보의 홍수 속에서 어떤 종목이 주목받고 있는지, 투자 결정을 내리기 위한 핵심 데이터를 어디서 찾을지 막막해합니다.

**`효율적인 정보 접근`**

신뢰성 있는 데이터와 커뮤니티 트렌드를 결합하여, 주식 시장의 흐름을 빠르고 직관적이게 파악할 수 있는 도구를 제공합니다.

**`의사결정 지원`**

트렌드와 종목 정보를 한 눈에 볼 수 있어 초보자들이 좀 더 자신있게 투자 결정을 내릴 수 있습니다.

</aside>

<aside>
💡

### **How?**

**`인프라 설계 및 운영`**

Amazon EC2 서버 두 대를 사용하여 각각 Airflow와 Superset을 실행합니다. 또한 S3와 Redshift를 활용해 대량의 데이터를 안정적으로 저장 및 관리하도록 합니다.

✔️ Airflow는 Docker 환경에서 동작하며, DAG를 통해 데이터 수집 및 처리를 자동화 시켜줍니다.

✔️ Superset은 대시보드 시각화를 담당합니다.

**`데이터 수집 및 처리`**

Airflow의 DAG를  통해 한국투자증권 API, 네이버 주식, 커뮤니티(클리앙, FM코리아) 글 데이터를 정기적으로 수집합니다. 수집된 데이터는 S3에 저장되며, 정제 및 가공된 데이터를 Redshift로 전송하여 데이터를 관리합니다.

✔️ API 호출, 웹 사이트 크롤링에서 더 나아가 자연어처리를 통한 정제 작업을 진행합니다.

**`대시보드 구현`**

Redshift에 저장된 데이터를 기반으로 Superset에서 주식 시장 트렌드를 시각화합니다. 사용자 친화적인 인터페이스를 통해 주요 지표를 직관적으로 확인 가능합니다.

</aside>

## ARCHITECTURE

### PROJECT ARCHITECTURE

![image](https://github.com/user-attachments/assets/b46c1978-d109-4df8-a96b-a88c97fc67f5)


| Data Pipeline | `Apache Airflow` |
| --- | --- |
| Containerization | `Docker` |
| Data Lake | `AWS S3` |
| Data warehouse | `AWS Redshift` |
| Visulization | `Apache Superset` |
| CI/CD | `Git`, `Github Actions` |
| Collaboration tool | `AWS`, `Github`, `Slack`, `Notion`, `Google Spreadsheet`, `Zep` |
| NLP | `KoBERT` |
| Language | `Python`, `Redshift SQL`  |

🧐 **`활용한 데이터는...`**

[한국투자증권 API] https://apiportal.koreainvestment.com/intro

[네이버 증권] https://m.stock.naver.com/investment/research/company

[클리앙 국내주식] https://www.clien.net/service/board/cm_stock

[FM코리아 국내주식] https://www.fmkorea.com/index.php?mid=stock&category=2997203870

### DATA PIPELINE

![image](https://github.com/user-attachments/assets/4eec8b7e-e8ae-4d1d-a2a2-5ad6bf725385)


### ERD

![image](https://github.com/user-attachments/assets/96abe23c-caab-4bab-a343-117aaec8f0ce)


프로젝트 DB ERD

## PROJECT DETAIL

### Infra

> **EC2 + Docker**
> 

`main node`

![image](https://github.com/user-attachments/assets/4365ecef-9316-4bf5-aa42-b8fdd89c58e4)


`worker node`

![image](https://github.com/user-attachments/assets/67c2a6fb-3bd0-4587-bd7c-fa3406bccb17)


### ETL

> 💬 **커뮤니티 데이터 ETL**
> 

총 2개의 커뮤니티, 클리앙과 FM코리아에 있는 게시글을 크롤링한 후 자연어처리 모델을 사용해 가장 자주 언급된 종목과 키워드를 도출해낸다. 

![image](https://github.com/user-attachments/assets/34f64758-ead4-42ee-84b0-654c61673143)


community_extract_dag.py의 task Graph

`save_crawling_data`

> 📈 **한국투자증권 API ETL**
> 
1. 커뮤니티 상위 언급량 10개 종목의 실시간 데이터를 수집하는 데이터 파이프라인

![image](https://github.com/user-attachments/assets/954e021b-5be6-4536-85bf-2e93585796ed)


1. 커뮤니티 상위 언급량 10개 종목의 일자별 데이터(최근 30일)를 수집하는 데이터 파이프라인

![image](https://github.com/user-attachments/assets/be64b6f1-90ed-4e4e-b704-fc388a9941fa)


1. 시가총액, 거래량, 상승율 상위 10개 종목 각각의 데이터를 수집하는 데이터 파이프라인

![image](https://github.com/user-attachments/assets/be64b6f1-90ed-4e4e-b704-fc388a9941fa)

- **TASK FLOW**
    
    ### 1. `read_stock_codes`
    
    - 커뮤니티에서 추출한 언급량 상위 10개 종목명을 주식시장의 종목코드와 매핑합니다.
    
    ### 2. `fetch_stock_data`
    
    - 매핑된 종목코드를 매개변수로 한국투자증권 API를 통해 실시간 일자별 데이터를 가져옵니다.
    
    ### 3. `upload_raw_to_s3`
    
    - API로 받아온 데이터의 원본을 AWS S3에 저장합니다.
    
    ### 4. `process_stock_data`
    
    - 원본 데이터를 사용 가능한 형태로 변환 및 value를 전처리합니다.
    
    ### 5. `upload_transformed_to_s3`
    
    - 전처리가 완료된 데이터를 AWS S3에 저장합니다.
    
    ### 6. `create_redshift_table`
    
    - Redshift에 적재할 테이블의 스키마를 정의하여 테이블을 생성합니다.
    
    ### 7. `upload_to_redshift`
    
    - 생성된 Redshift 테이블에 전처리가 완료된 데이터를 적재합니다.

> 💸 **네이버페이 증권 데이터 ETL**
> 
1. 네이버페이 증권 사이트의 뉴스 기사와, 코스피-코스닥 데이터를 수집하고 저장하는 파이프라인

![image](https://github.com/user-attachments/assets/be64b6f1-90ed-4e4e-b704-fc388a9941fa)

**crawl_and_upload_Naver_data_dag**의 task Graph

### **1. `crawl_stock_data`**

- **역할**: 네이버 증권 뉴스 데이터를 크롤링합니다.
- **설명**:
    - Selenium을 사용하여 네이버 증권 뉴스 페이지에서 데이터를 가져옵니다.
    - 수집된 데이터는 CSV 파일로 저장되며 XCom을 통해 파일 경로를 다음 Task로 전달합니다.

### **2. `crawl_kospi_kosdaq_data`**

- **역할**: 네이버 코스피/코스닥 지수 데이터를 크롤링합니다.
- **설명**:
    - Selenium을 사용하여 네이버 코스피와 코스닥 지수 페이지에서 데이터를 수집합니다.
    - 수집된 데이터는 CSV 파일로 저장되며, XCom을 통해 파일 경로를 다음 Task로 전달합니다.

### **3. `upload_to_s3`**

- **역할**: 크롤링한 데이터를 AWS S3에 업로드합니다.
- **설명**:
    - `crawl_stock_data`와 `crawl_kospi_kosdaq_data`에서 생성된 CSV 파일 경로를 가져옵니다.
    - AWS S3에 데이터를 업로드하며, 폴더 구조는 `raw_data/파일명`입니다.

### **4. `create_and_load_redshift_tables`**

- **역할**: Redshift에 테이블을 생성하고 S3 데이터를 로드합니다.
- **설명**:
    - Redshift에 `naverNews`와 `kospiKosdaqData` 테이블을 생성합니다.
    - S3에 저장된 데이터를 Redshift 테이블로 업로드합니다.
    - 업로드 과정에서 CSV 파일의 헤더는 무시되며, 테이블 데이터는 덮어쓰기됩니다.

## PROJECT RESULT

### Dashboard

![📰-주린이를-위한-뉴스레터-2025-01-15T08-53-58 378Z](https://github.com/user-attachments/assets/182d61b1-121a-4462-8f62-829245149fc4)
