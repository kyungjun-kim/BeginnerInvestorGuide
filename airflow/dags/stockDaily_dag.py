from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

# 공통 연결 정보 가져오기
def get_connections(conn_id):
    conn = BaseHook.get_connection(conn_id)
    return {
        "host": conn.host,
        "schema": conn.schema,
        "login": conn.login,
        "password": conn.password,
        "port": conn.port,
        "extra": conn.extra_dejson,
    }

# 데이터 가져오기
def fetch_data(**kwargs):
    task_type = kwargs["task_type"]
    api_conn = get_connections("koreainvestment_api")

    endpoint = f"{api_conn['host']}/{kwargs['endpoint']}"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {api_conn['extra']['access_token']}",
        "appkey": api_conn["extra"]["app_key"],
        "appsecret": api_conn["extra"]["app_secret"],
        "tr_id": kwargs["tr_id"],
    }
    params = kwargs["params"]

    response = requests.get(endpoint, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(f"API 호출 실패: {response.status_code}, {response.text}")

    # 응답 데이터 가져오기
    data = response.json().get("output", [])
    columns = kwargs["columns"]
    filtered_data = [{col: item.get(col, None) for col in columns} for item in data]
    df = pd.DataFrame(filtered_data)

    # CSV 저장
    file_path = f"/tmp/raw_{task_type}_data_{datetime.now().strftime('%y%m%d')}.csv"
    df.to_csv(file_path, index=False, encoding="utf-8-sig")
    kwargs['ti'].xcom_push(key=f"{task_type}_csv_path", value=file_path)

# S3 업로드
def upload_raw_to_s3(**kwargs):
    task_type = kwargs["task_type"]
    bucket_path = kwargs["bucket_path"]
    csv_path = kwargs['ti'].xcom_pull(key=f"{task_type}_csv_path", task_ids="fetch_data_task")

    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV 파일 경로를 찾을 수 없습니다: {csv_path}")

    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_hook.load_file(
        filename=csv_path,
        bucket_name="team6-s3",
        key=f"{bucket_path}/{os.path.basename(csv_path)}",
        replace=True,
    )

# 데이터 처리
def process_data(**kwargs):
    task_type = kwargs["task_type"]
    csv_path = kwargs['ti'].xcom_pull(key=f"{task_type}_csv_path", task_ids="fetch_data_task")

    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV 파일 경로를 찾을 수 없습니다: {csv_path}")

    df = pd.read_csv(csv_path)
    column_mapping = kwargs["column_mapping"]
    dtypes = kwargs["dtypes"]

    df.rename(columns=column_mapping, inplace=True)
    for col, dtype in dtypes.items():
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(dtype)

    processed_path = f"/tmp/processed_{task_type}_data_{datetime.now().strftime('%y%m%d')}.parquet"
    df.to_parquet(processed_path, index=False)
    kwargs['ti'].xcom_push(key=f"{task_type}_processed_path", value=processed_path)

# S3 업로드 (처리된 데이터)
def upload_transformed_to_s3(**kwargs):
    task_type = kwargs["task_type"]
    bucket_path = kwargs["bucket_path"]
    processed_path = kwargs['ti'].xcom_pull(key=f"{task_type}_processed_path", task_ids="process_data_task")

    if not processed_path or not os.path.exists(processed_path):
        raise FileNotFoundError(f"처리된 파일 경로를 찾을 수 없습니다: {processed_path}")

    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_hook.load_file(
        filename=processed_path,
        bucket_name="team6-s3",
        key=f"{bucket_path}/{os.path.basename(processed_path)}",
        replace=True,
    )

# Redshift 테이블 생성
def create_redshift_table(**kwargs):
    table_name = kwargs["table_name"]
    columns_sql = kwargs["columns_sql"]
    postgres_hook = PostgresHook(postgres_conn_id="redshift_conn")

    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    create_table_sql = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} ({columns_sql});
    """
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

# Redshift 데이터 적재
def upload_to_redshift(**kwargs):
    task_type = kwargs["task_type"]
    table_name = kwargs["table_name"]
    processed_path = kwargs['ti'].xcom_pull(key=f"{task_type}_processed_path", task_ids="process_data_task")

    if not processed_path:
        raise Exception(f"{task_type} 데이터 Redshift 업로드 실패: 처리된 데이터 경로가 없습니다.")

    aws_conn = BaseHook.get_connection("aws_conn")
    access_key = aws_conn.login
    secret_key = aws_conn.password

    postgres_hook = PostgresHook(postgres_conn_id="redshift_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    copy_sql = f"""
    COPY {table_name}
    FROM 's3://team6-s3/transformed_data/{os.path.basename(processed_path)}'
    ACCESS_KEY_ID '{access_key}'
    SECRET_ACCESS_KEY '{secret_key}'
    FORMAT AS PARQUET;
    """
    cursor.execute(copy_sql)
    conn.commit()
    cursor.close()
    conn.close()

# DAG 정의
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="fetch_stock_daily_data_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_type = "daily_stock_data"
    endpoint = "uapi/domestic-stock/v1/quotations/inquire-daily-price"
    tr_id = "FHKST01010400"
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_PERIOD_DIV_CODE": "D",
        "FID_ORG_ADJ_PRC": "0",
    }
    columns = [
        "stck_bsop_date",
        "stck_oprc",
        "stck_hgpr",
        "stck_lwpr",
        "stck_clpr",
        "acml_vol",
        "prdy_vrss_vol_rate",
        "prdy_vrss",
        "prdy_ctrt",
    ]
    column_mapping = {
        "stck_bsop_date": "영업일자",
        "stck_oprc": "시가",
        "stck_hgpr": "고가",
        "stck_lwpr": "저가",
        "stck_clpr": "종가",
        "acml_vol": "누적거래량",
        "prdy_vrss_vol_rate": "전일대비거래량비율",
        "prdy_vrss": "전일대비",
        "prdy_ctrt": "전일대비율",
    }
    dtypes = {
        "영업일자": "string",
        "시가": "int32",
        "고가": "int32",
        "저가": "int32",
        "종가": "int32",
        "누적거래량": "int64",
        "전일대비거래량비율": "float64",
        "전일대비": "int32",
        "전일대비율": "float64",
    }
    columns_sql = """
        "영업일자" VARCHAR(10),
        "시가" INT,
        "고가" INT,
        "저가" INT,
        "종가" INT,
        "누적거래량" BIGINT,
        "전일대비거래량비율" FLOAT,
        "전일대비" INT,
        "전일대비율" FLOAT
    """

    fetch_data_task = PythonOperator(
        task_id="fetch_data_task",
        python_callable=fetch_data,
        op_kwargs={"task_type": task_type, "endpoint": endpoint, "tr_id": tr_id, "params": params, "columns": columns},
    )

    upload_raw_data_to_s3_task = PythonOperator(
        task_id="upload_raw_data_to_s3_task",
        python_callable=upload_raw_to_s3,
        op_kwargs={"task_type": task_type, "bucket_path": "raw_data"},
    )

    process_data_task = PythonOperator(
        task_id="process_data_task",
        python_callable=process_data,
        op_kwargs={"task_type": task_type, "column_mapping": column_mapping, "dtypes": dtypes},
    )

    upload_transformed_data_to_s3_task = PythonOperator(
        task_id="upload_transformed_data_to_s3_task",
        python_callable=upload_transformed_to_s3,
        op_kwargs={"task_type": task_type, "bucket_path": "transformed_data"},
    )

    create_table_task = PythonOperator(
        task_id="create_table_task",
        python_callable=create_redshift_table,
        op_kwargs={"table_name": "daily_stock_data", "columns_sql": columns_sql},
    )

    upload_to_redshift_task = PythonOperator(
        task_id="upload_to_redshift_task",
        python_callable=upload_to_redshift,
        op_kwargs={"task_type": task_type, "table_name": "daily_stock_data"},
    )

    fetch_data_task >> upload_raw_data_to_s3_task >> process_data_task >> upload_transformed_data_to_s3_task >> create_table_task >> upload_to_redshift_task
