from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

# API 및 S3, Redshift 설정 가져오기
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

# 데이터를 가져오는 함수
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
    print(f"API 응답 상태 코드: {response.status_code}")
    print(f"API 응답 데이터: {response.text}")

    # 응답 데이터 구조 확인
    try:
        data = response.json().get("output", [])
        print(f"가져온 데이터: {data}")
    except Exception as e:
        raise Exception(f"API 응답 파싱 실패: {e}")

    # 상위 10개 데이터만 가져오기
    top_10_data = data[:10]
    
    # CSV 파일로 저장
    file_path = f"/tmp/raw_{task_type}_data_{datetime.now().strftime('%y%m%d')}.csv"
    pd.DataFrame(top_10_data).to_csv(file_path, index=False, encoding='utf-8-sig')

    # XCom에 파일 경로 저장
    kwargs['ti'].xcom_push(key=f"{task_type}_csv_path", value=file_path)
    print(f"{task_type} 데이터가 CSV 파일로 저장되었습니다: {file_path}")

# 데이터를 S3에 저장 (CSV 형식)
def upload_raw_to_s3(**kwargs):
    task_type = kwargs["task_type"]
    bucket_path = kwargs["bucket_path"]

    # XCom에서 CSV 파일 경로 가져오기
    csv_path = kwargs['ti'].xcom_pull(key=f"{task_type}_csv_path", task_ids=f"fetch_{task_type}_data")
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV 파일 경로를 찾을 수 없습니다: {csv_path}")

    # S3로 업로드
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_hook.load_file(
        filename=csv_path,
        bucket_name="team6-s3",
        key=f"{bucket_path}/{os.path.basename(csv_path)}",
        replace=True
    )
    print(f"{task_type} 데이터를 S3의 {bucket_path}/{os.path.basename(csv_path)}에 저장 완료.")


# 데이터를 처리하는 함수
def process_data(**kwargs):
    task_type = kwargs["task_type"]
    columns = kwargs["columns"]

    # XCom에서 CSV 파일 경로 가져오기
    csv_path = kwargs['ti'].xcom_pull(key=f"{task_type}_csv_path", task_ids=f"fetch_{task_type}_data")
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV 파일 경로를 찾을 수 없습니다: {csv_path}")
    
    # 데이터 로드 및 처리
    df = pd.read_csv(csv_path)
    df = df[columns]

    # 데이터 변환 작업 수행
    for col in ["순위", "현재가", "거래량"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce').fillna(0).astype(int)

    if "등락률" in df.columns:
        df["등락률"] = df["등락률"].apply(lambda x: float(x.strip('%')) / 100 if isinstance(x, str) else x)

    # 처리된 데이터 저장 (Parquet 포맷)
    processed_path = f"/tmp/transformed_{task_type}_data_{datetime.now().strftime('%y%m%d')}.parquet"
    df.to_parquet(processed_path, index=False)
    kwargs['ti'].xcom_push(key=f"{task_type}_processed_path", value=processed_path)
    print(f"{task_type} 처리된 데이터가 저장되었습니다: {processed_path}")


# S3에 데이터를 저장 (Parquet 형식)
def upload_transformed_to_s3(**kwargs):
    task_type = kwargs["task_type"]
    bucket_path = kwargs["bucket_path"]

    # XCom에서 처리된 Parquet 파일 경로 가져오기
    processed_path = kwargs['ti'].xcom_pull(key=f"{task_type}_processed_path", task_ids=f"process_{task_type}_data")
    if not processed_path or not os.path.exists(processed_path):
        raise FileNotFoundError(f"처리된 Parquet 파일 경로를 찾을 수 없습니다: {processed_path}")

    # S3로 업로드
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_hook.load_file(
        filename=processed_path,
        bucket_name="team6-s3",
        key=f"{bucket_path}/{os.path.basename(processed_path)}",
        replace=True
    )
    print(f"{task_type} 처리된 데이터를 S3의 {bucket_path}/{os.path.basename(processed_path)}에 저장 완료.")


# Redshift 테이블 생성
def create_redshift_table(**kwargs):
    table_name = kwargs["table_name"]
    redshift_conn = get_connections("redshift_conn")

    # Redshift 연결
    postgres_hook = PostgresHook(postgres_conn_id="redshift_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # 테이블 생성 SQL
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        data_rank INT,
        mksc_shrn_iscd VARCHAR(10),
        hts_kor_isnm VARCHAR(40),
        stck_prpr INT,
        acml_vol INT,
        prdy_ctrt FLOAT
    );
    """

    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Redshift 테이블 {table_name}이 성공적으로 생성되었습니다.")



# Redshift에 데이터 적재 (COPY 명령)
def upload_to_redshift(**kwargs):
    task_type = kwargs["task_type"]
    table_name = kwargs["table_name"]
    processed_path = kwargs['ti'].xcom_pull(key=f"{task_type}_processed_path", task_ids=f"process_{task_type}_data")

    if not processed_path:
        raise Exception(f"{task_type} 데이터 Redshift 업로드 실패: 처리된 데이터 경로가 없습니다.")

    # AWS Connection 사용
    aws_conn = get_connections("aws_conn")
    access_key = aws_conn.login  # AWS Access Key ID
    secret_key = aws_conn.password

    postgres_hook = PostgresHook(postgres_conn_id="redshift_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # COPY 명령 실행
    copy_sql = f"""
    COPY {table_name}
    FROM '{s3_path}'
    ACCESS_KEY_ID '{access_key}'
    SECRET_ACCESS_KEY '{secret_key}'
    FORMAT AS PARQUET;
    """
    cursor.execute(copy_sql)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"{task_type} 데이터를 Redshift 테이블 {table_name}에 적재 완료.")

# DAG 정의
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="stock_ranking_data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    fetch_volume_data = PythonOperator(
        task_id="fetch_volume_data",
        python_callable=fetch_data,
        op_kwargs={
            "task_type": "stock_volume_top10",
            "endpoint": "uapi/domestic-stock/v1/quotations/volume-rank",
            "tr_id": "FHPST01710000",
            "params": {"FID_COND_MRKT_DIV_CODE": "J"},
        },
    )

    upload_raw_data_to_s3 = PythonOperator(
        task_id="upload_raw_data_to_s3",
        python_callable=upload_raw_to_s3,
        op_kwargs={
            "task_type": "stock_volume_top10",
            "bucket_path": "raw_data",
        },
    ) 

    process_volume_data = PythonOperator(
        task_id="process_volume_data",
        python_callable=process_data,
        op_kwargs={
            "task_type": "stock_volume_top10",
            "columns": ["data_rank", "mksc_shrn_iscd", "hts_kor_isnm", "stck_prpr", "acml_vol", "prdy_ctrt"],
        },
    )

    upload_transformed_data_to_s3 = PythonOperator(
        task_id="upload_transformed_data_to_s3",
        python_callable=upload_transformed_to_s3,
        op_kwargs={
            "task_type": "stock_volume_top10",
            "bucket_path": "transformed_data",
        },
    )

    create_table = PythonOperator(
    task_id="create_table",
    python_callable=create_redshift_table,
    op_kwargs={"table_name": "transformed_stock_volume"},
)

    upload_to_redshift = PythonOperator(
        task_id="upload_to_redshift",
        python_callable=upload_to_redshift,
        op_kwargs={
            "task_type": "stock_volume_top10",
            "table_name": "transformed_stock_volume",
        },
    )

    # 태스크 의존성 설정
    fetch_volume_data >> upload_raw_data_to_s3 >> process_volume_data >> upload_transformed_data_to_s3 >> create_table >> upload_to_redshift