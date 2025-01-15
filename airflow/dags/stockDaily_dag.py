from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
import io

# 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}

# S3에서 종목 데이터를 읽어오는 함수
def read_stock_codes_from_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_conn")

    # community_top10_data 읽기
    community_file_path = "transformed_data/community_top10_data_20250115.csv"
    community_content = s3_hook.read_key(community_file_path, bucket_name="team6-s3")
    community_df = pd.read_csv(io.StringIO(community_content))

    # 종목명 컬럼 이름 매핑
    if "stockName" in community_df.columns and "mentionCount" in community_df.columns:
        community_df.rename(columns={"stockName": "종목명", "mentionCount": "언급수"}, inplace=True)
    else:
        raise ValueError("community_top10_data CSV 파일에 필요한 열(stockName, mentionCount)이 없습니다.")

    # 언급순위 계산 (언급수가 높을수록 순위가 높음)
    community_df["언급순위"] = community_df["언급수"].rank(method="min", ascending=False).astype(int)

    # kospi_stocks 읽기
    kospi_file_path = "data/raw_data/kospi_stocks.csv"
    kospi_content = s3_hook.read_key(kospi_file_path, bucket_name="team6-s3")
    kospi_df = pd.read_csv(io.StringIO(kospi_content))

    # kosdaq_stocks 읽기
    kosdaq_file_path = "data/raw_data/kosdaq_stocks.csv"
    kosdaq_content = s3_hook.read_key(kosdaq_file_path, bucket_name="team6-s3")
    kosdaq_df = pd.read_csv(io.StringIO(kosdaq_content))

    # 단축코드 6자리로 맞추기
    kospi_df["단축코드"] = kospi_df["단축코드"].astype(str).str.zfill(6)
    kosdaq_df["단축코드"] = kosdaq_df["단축코드"].astype(str).str.zfill(6)

    # kospi, kosdaq 병합
    stock_codes_df = pd.concat([kospi_df, kosdaq_df], ignore_index=True)

    # community_top10_data와 매칭
    merged_df = pd.merge(community_df, stock_codes_df, on="종목명", how="inner")

    # "단축코드"를 "종목코드"로 변경
    merged_df.rename(columns={"단축코드": "종목코드"}, inplace=True)

    # 필요한 데이터 추출
    stock_list = merged_df[["종목명", "종목코드", "언급수", "언급순위"]].to_dict("records")
    kwargs["ti"].xcom_push(key="stock_list", value=stock_list)

# API에서 데이터를 가져오는 함수
def fetch_stock_data(**kwargs):
    stock_list = kwargs["ti"].xcom_pull(key="stock_list")
    api_conn = BaseHook.get_connection("koreainvestment_api")
    endpoint = f"{api_conn.host}/uapi/domestic-stock/v1/quotations/inquire-daily-price"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {api_conn.extra_dejson['access_token']}",
        "appkey": api_conn.extra_dejson["app_key"],
        "appsecret": api_conn.extra_dejson["app_secret"],
        "tr_id": "FHKST01010400",
    }

    data_list = []
    for stock in stock_list:
        stock_code = str(stock["종목코드"]).zfill(6)
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        }
        response = requests.get(endpoint, headers=headers, params=params)
        if response.status_code == 200:
            outputs = response.json().get("output", [])
            for data in outputs:
                data["종목코드"] = stock_code
                data["종목명"] = stock["종목명"]
                data_list.append(data)

    current_date = datetime.now().strftime('%y%m%d')
    raw_data_path = f"/tmp/raw_stock_daily_price_data_{current_date}.csv"
    pd.DataFrame(data_list).to_csv(raw_data_path, index=False, encoding="utf-8-sig")
    kwargs["ti"].xcom_push(key="raw_data_path", value=raw_data_path)

# 데이터를 S3에 업로드하는 함수
def upload_raw_to_s3(**kwargs):
    raw_data_path = kwargs["ti"].xcom_pull(key="raw_data_path")
    current_date = datetime.now().strftime('%y%m%d')
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_hook.load_file(
        filename=raw_data_path,
        bucket_name="team6-s3",
        key=f"raw_data/raw_stock_daily_price_data_{current_date}.csv",
        replace=True,
    )

# 데이터를 처리하는 함수
def process_stock_data(**kwargs):
    raw_data_path = kwargs["ti"].xcom_pull(key="raw_data_path")
    stock_list = kwargs["ti"].xcom_pull(key="stock_list")  # 언급수와 언급순위 포함
    df = pd.read_csv(raw_data_path)

    # 컬럼 매핑
    column_mapping = {
        "종목코드": "종목코드",
        "종목명": "종목명",
        "stck_bsop_date": "영업일자",
        "stck_oprc": "시가",
        "stck_hgpr": "최고가",
        "stck_lwpr": "최저가",
        "stck_clpr": "종가",
        "acml_vol": "누적거래량",
        "prdy_vrss_vol_rate": "전일대비거래량비율",
        "prdy_vrss": "전일대비",
        "prdy_ctrt": "전일대비율",
    }

    # 필요한 컬럼만 필터링
    filtered_df = df[list(column_mapping.keys())]
    filtered_df.rename(columns=column_mapping, inplace=True)

    # 종목코드 앞에 0을 채우고 문자열로 강제 변환
    filtered_df["종목코드"] = filtered_df["종목코드"].astype(str).str.zfill(6)

    # 언급수와 언급순위 병합
    mention_data = pd.DataFrame(stock_list)
    mention_data["종목코드"] = mention_data["종목코드"].astype(str).str.zfill(6)
    final_df = pd.merge(filtered_df, mention_data[["종목코드", "언급수", "언급순위"]], on="종목코드", how="left")

    # 데이터 타입 변환
    dtype_mapping = {
        "종목코드": "string",
        "종목명": "string",
        "영업일자": "datetime64[ms]",
        "시가": "int32",
        "최고가": "int32",
        "최저가": "int32",
        "종가": "int32",
        "누적거래량": "int64",
        "전일대비거래량비율": "float32",
        "전일대비": "int32",
        "전일대비율": "float32",
        "언급수": "int32",
        "언급순위": "int32",
    }

    for column, dtype in dtype_mapping.items():
        if column == "영업일자":
            final_df[column] = pd.to_datetime(final_df[column], format='%Y%m%d').dt.date
        elif column in final_df.columns:
            final_df[column] = final_df[column].astype(dtype)
    
    # 데이터 저장
    current_date = datetime.now().strftime('%y%m%d')
    processed_path = f"/tmp/transformed_stock_daily_price_data_{current_date}.parquet"
    final_df.to_parquet(processed_path, index=False)
    kwargs["ti"].xcom_push(key="processed_path", value=processed_path)

# 처리된 데이터를 S3에 업로드하는 함수
def upload_transformed_to_s3(**kwargs):
    processed_path = kwargs["ti"].xcom_pull(key="processed_path")
    current_date = datetime.now().strftime('%y%m%d')
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_hook.load_file(
        filename=processed_path,
        bucket_name="team6-s3",
        key=f"transformed_data/transformed_stock_daily_price_data_{current_date}.parquet",
        replace=True,
    )

# Redshift 테이블 생성 함수
def create_redshift_table(**kwargs):
    create_table_sql = """
    DROP TABLE IF EXISTS transformed_stock_daily_price;
    CREATE TABLE transformed_stock_daily_price (
        종목코드 VARCHAR(12),
        종목명 VARCHAR(100),
        영업일자 DATE,
        시가 INT,
        최고가 INT,
        최저가 INT,
        종가 INT,
        누적거래량 BIGINT,
        전일대비거래량비율 FLOAT,
        전일대비 INT,
        전일대비율 FLOAT,
        언급수 INT,
        언급순위 INT
    );
    """
    postgres_hook = PostgresHook(postgres_conn_id="redshift_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

# Redshift에 데이터를 적재하는 함수
def upload_to_redshift(**kwargs):
    processed_path = kwargs["ti"].xcom_pull(key="processed_path")
    aws_conn = BaseHook.get_connection("aws_conn")
    copy_sql = f"""
    COPY transformed_stock_daily_price
    FROM 's3://team6-s3/transformed_data/{os.path.basename(processed_path)}'
    ACCESS_KEY_ID '{aws_conn.login}'
    SECRET_ACCESS_KEY '{aws_conn.password}'
    FORMAT AS PARQUET;
    """
    postgres_hook = PostgresHook(postgres_conn_id="redshift_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(copy_sql)
    conn.commit()
    cursor.close()
    conn.close()

# DAG 정의
with DAG(
    dag_id="stock_daily_data_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    read_stock_codes_task = PythonOperator(
        task_id="read_stock_codes",
        python_callable=read_stock_codes_from_s3,
    )

    fetch_data_task = PythonOperator(
        task_id="fetch_stock_data",
        python_callable=fetch_stock_data,
    )

    upload_raw_task = PythonOperator(
        task_id="upload_raw_to_s3",
        python_callable=upload_raw_to_s3,
    )

    process_data_task = PythonOperator(
        task_id="process_stock_data",
        python_callable=process_stock_data,
    )

    upload_transformed_task = PythonOperator(
        task_id="upload_transformed_to_s3",
        python_callable=upload_transformed_to_s3,
    )

    create_table_task = PythonOperator(
        task_id="create_redshift_table",
        python_callable=create_redshift_table,
    )

    upload_redshift_task = PythonOperator(
        task_id="upload_to_redshift",
        python_callable=upload_to_redshift,
    )


    read_stock_codes_task >> fetch_data_task >> upload_raw_task >> process_data_task >> upload_transformed_task >> create_table_task >> upload_redshift_task
