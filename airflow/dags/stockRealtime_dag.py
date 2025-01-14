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
    file_path = "data/stock_code.csv"  # S3의 종목코드 파일 경로
    file_content = s3_hook.read_key(file_path, bucket_name="team6-s3")
    stock_df = pd.read_csv(io.StringIO(file_content))
    stock_list = stock_df.to_dict("records")
    kwargs["ti"].xcom_push(key="stock_list", value=stock_list)

# API에서 데이터를 가져오는 함수
def fetch_stock_data(**kwargs):
    stock_list = kwargs["ti"].xcom_pull(key="stock_list")
    api_conn = BaseHook.get_connection("koreainvestment_api")
    endpoint = f"{api_conn.host}/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {api_conn.extra_dejson['access_token']}",
        "appkey": api_conn.extra_dejson["app_key"],
        "appsecret": api_conn.extra_dejson["app_secret"],
        "tr_id": "FHKST01010100",
    }

    data_list = []
    for stock in stock_list:
        stock_code = str(stock["종목코드"]).zfill(6)
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
        }
        response = requests.get(endpoint, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json().get("output", {})
            data["종목코드"] = stock_code
            data["종목명"] = stock["종목명"]
            data_list.append(data)

    current_date = datetime.now().strftime('%y%m%d')
    raw_data_path = f"/tmp/raw_stock_current_price_data_{current_date}.csv"
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
        key=f"raw_data/raw_stock_current_price_data_{current_date}.csv",
        replace=True,
    )

# 데이터를 처리하는 함수
def process_stock_data(**kwargs):
    raw_data_path = kwargs["ti"].xcom_pull(key="raw_data_path")
    df = pd.read_csv(raw_data_path)

    # Redshift 테이블 스키마에 맞게 컬럼 필터링 및 매핑
    column_mapping = {
        "종목코드": "종목코드",
        "종목명": "종목명",
        "iscd_stat_cls_code": "종목상태코드",
        "rprs_mrkt_kor_name": "대표시장한글명",
        "bstp_kor_isnm": "업종한글종목명",
        "stck_prpr": "주식현재가",
        "prdy_vrss": "전일대비",
        "prdy_vrss_sign": "전일대비부호",
        "prdy_ctrt": "전일대비율",
        "stck_oprc": "주식시가",
        "stck_hgpr": "주식최고가",
        "stck_lwpr": "주식최저가",
        "stck_mxpr": "주식상한가",
        "stck_llam": "주식하한가",
        "stck_sdpr": "주식기준가",
        "mrkt_warn_cls_code": "시장경고코드",
        "short_over_yn": "단기과열여부",
    }

    # 스키마에 맞는 컬럼만 선택
    filtered_df = df[list(column_mapping.keys())]
    filtered_df.rename(columns=column_mapping, inplace=True)

    # 종목코드 앞에 0을 채우고 문자열로 강제 변환
    filtered_df["종목코드"] = filtered_df["종목코드"].astype(str).str.zfill(6)

    # 데이터 타입 변환
    dtype_mapping = {
        "종목코드": "string",             # VARCHAR
        "종목명": "string",              # VARCHAR
        "종목상태코드": "string",         # VARCHAR
        "대표시장한글명": "string",       # VARCHAR
        "업종한글종목명": "string",       # VARCHAR
        "주식현재가": "int32",           # INTEGER
        "전일대비": "int32",            # INTEGER
        "전일대비부호": "string",         # VARCHAR
        "전일대비율": "float32",         # FLOAT4
        "주식시가": "int32",            # INTEGER
        "주식최고가": "int32",          # INTEGER
        "주식최저가": "int32",          # INTEGER
        "주식상한가": "int32",          # INTEGER
        "주식하한가": "int32",          # INTEGER
        "주식기준가": "int32",          # INTEGER
        "시장경고코드": "string",         # VARCHAR
        "단기과열여부": "string",         # VARCHAR
    }

    for column, dtype in dtype_mapping.items():
        if column in filtered_df.columns:
            filtered_df[column] = filtered_df[column].astype(dtype)
    
    # 데이터 저장
    current_date = datetime.now().strftime('%y%m%d')
    processed_path = f"/tmp/transformed_stock_current_price_data_{current_date}.parquet"
    filtered_df.to_parquet(processed_path, index=False)
    kwargs["ti"].xcom_push(key="processed_path", value=processed_path)

# 처리된 데이터를 S3에 업로드하는 함수
def upload_transformed_to_s3(**kwargs):
    processed_path = kwargs["ti"].xcom_pull(key="processed_path")
    current_date = datetime.now().strftime('%y%m%d')
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_hook.load_file(
        filename=processed_path,
        bucket_name="team6-s3",
        key=f"transformed_data/transformed_stock_current_price_data_{current_date}.parquet",
        replace=True,
    )

# Redshift 테이블 생성 함수
def create_redshift_table(**kwargs):
    create_table_sql = """
    DROP TABLE IF EXISTS transformed_stock_realtime_price;
    CREATE TABLE transformed_stock_realtime_price (
        종목코드 VARCHAR(12),
        종목명 VARCHAR(100),
        종목상태코드 VARCHAR(3),
        대표시장한글명 VARCHAR(40),
        업종한글종목명 VARCHAR(40),
        주식현재가 INT,
        전일대비 INT,
        전일대비부호 VARCHAR(5),
        전일대비율 FLOAT,
        주식시가 INT,
        주식최고가 INT,
        주식최저가 INT,
        주식상한가 INT,
        주식하한가 INT,
        주식기준가 INT,
        시장경고코드 VARCHAR(3),
        단기과열여부 VARCHAR(1)
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
    COPY transformed_stock_realtime_price
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
    dag_id="stock_realtime_data_dag",
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
