from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
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

# 데이터를 API에서 가져오는 함수
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

    # 필요한 컬럼만 필터링
    columns = kwargs["columns"]
    filtered_data = [{col: item.get(col, None) for col in columns} for item in data]

    # DataFrame으로 변환
    df = pd.DataFrame(filtered_data)

    # CSV 파일로 저장
    file_path = f"/tmp/raw_{task_type}_data_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(file_path, index=False, encoding="utf-8-sig")

    # XCom에 파일 경로 저장
    kwargs["ti"].xcom_push(key=f"{task_type}_csv_path", value=file_path)
    print(f"{task_type} 데이터를 CSV로 저장했습니다: {file_path}")

# 데이터를 S3에 저장 (CSV 파일 업로드)
def upload_raw_to_s3(**kwargs):
    task_type = kwargs["task_type"]
    bucket_path = kwargs["bucket_path"]

    # XCom에서 CSV 파일 경로 가져오기
    csv_path = kwargs["ti"].xcom_pull(key=f"{task_type}_csv_path", task_ids=f"fetch_{task_type}_data")
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV 파일 경로를 찾을 수 없습니다: {csv_path}")

    # S3로 업로드
    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_hook.load_file(
        filename=csv_path,
        bucket_name="team6-s3",
        key=f"{bucket_path}/{os.path.basename(csv_path)}",
        replace=True,
    )
    print(f"{task_type} 데이터를 S3의 {bucket_path}/{os.path.basename(csv_path)}에 저장 완료.")

# 데이터를 처리하는 함수
def process_data(**kwargs):
    task_type = kwargs["task_type"]

    # XCom에서 CSV 파일 경로 가져오기
    csv_path = kwargs["ti"].xcom_pull(key=f"{task_type}_csv_path", task_ids=f"fetch_{task_type}_data")
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV 파일 경로를 찾을 수 없습니다: {csv_path}")

    # 데이터 로드 및 컬럼 확인
    df = pd.read_csv(csv_path)

    # 컬럼 이름 매핑 및 스키마 정의
    schema_mapping = kwargs["schema_mapping"]
    column_mapping = schema_mapping["columns"]  # 컬럼 이름
    dtypes = schema_mapping["dtypes"]   # 컬럼 데이터 타입

    # 컬럼 이름 변경
    df.rename(columns=column_mapping, inplace=True)

    # 데이터 타입 변환
    for col, dtype in dtypes.items():
        if col in df.columns:
            if dtype == "string":
                df[col] = df[col].astype(str)
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(dtype)

    # 처리된 데이터 저장 (Parquet 포맷)
    processed_path = f"/tmp/transformed_{task_type}_data_{datetime.now().strftime('%Y%m%d')}.parquet"
    df.to_parquet(processed_path, index=False)

    kwargs["ti"].xcom_push(key=f"{task_type}_processed_path", value=processed_path)
    print(f"{task_type} 처리된 데이터를 저장했습니다: {processed_path}")

# 처리된 데이터를 S3에 업로드 (Parquet 파일 업로드)
def upload_transformed_to_s3(**kwargs):
    task_type = kwargs["task_type"]
    bucket_path = kwargs["bucket_path"]

    # XCom에서 처리된 Parquet 파일 경로 가져오기
    processed_path = kwargs['ti'].xcom_pull(key=f"{task_type}_processed_path", task_ids=f"process_{task_type}_data")
    if not processed_path or not os.path.exists(processed_path):
        raise FileNotFoundError(f"처리된 Parquet 파일 경로를 찾을 수 없습니다: {processed_path}")

    s3_hook = S3Hook(aws_conn_id="aws_conn")
    s3_hook.load_file(
        filename=processed_path,
        bucket_name="team6-s3",
        key=f"{bucket_path}/{os.path.basename(processed_path)}",
        replace=True,
    )
    print(f"{task_type} 처리된 데이터를 S3의 {bucket_path}/{os.path.basename(processed_path)}에 저장 완료.")

# Redshift 테이블 생성 함수
def create_redshift_table(**kwargs):
    table_name = kwargs["table_name"]
    columns_sql = kwargs["columns_sql"]
    redshift_conn = get_connections("redshift_conn")

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
    print(f"Redshift 테이블 {table_name}이 생성되었습니다.")

# Redshift에 데이터 적재 (COPY 명령)
def upload_to_redshift(**kwargs):
    task_type = kwargs["task_type"]
    table_name = kwargs["table_name"]
    processed_path = kwargs["ti"].xcom_pull(key=f"{task_type}_processed_path", task_ids=f"process_{task_type}_data")
    
    if not processed_path:
        raise Exception(f"{task_type} 데이터 업로드 실패: 처리된 데이터 경로를 찾을 수 없습니다.")

    # AWS Connection 사용
    aws_conn = BaseHook.get_connection("aws_conn")
    access_key = aws_conn.login
    secret_key = aws_conn.password

    postgres_hook = PostgresHook(postgres_conn_id="redshift_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # COPY 명령 실행
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
    dag_id="stock_realtime_current_price_data_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # 1. API 데이터를 가져오는 태스크
    fetch_data_task = PythonOperator(
        task_id="fetch_stock_current_price_data",
        python_callable=fetch_data,  # fetch_data 함수 호출
        op_kwargs={
            "task_type": "stock_current_price",  # 태스크 유형
            "endpoint": "uapi/domestic-stock/v1/quotations/inquire-price",  # API 엔드포인트
            "tr_id": "FHKST01010100",  # 거래 ID
            "params": {
                "FID_COND_MRKT_DIV_CODE": "J",  # 시장 구분 코드
            },
            "columns": [
                "iscd_stat_cls_code", "rprs_mrkt_kor_name", "new_hgpr_lwpr_cls_code",
                "bstp_kor_isnm", "stck_prpr", "prdy_vrss", "prdy_vrss_sign",
                "prdy_ctrt", "stck_oprc", "stck_hgpr", "stck_lwpr", "stck_mxpr",
                "stck_llam", "stck_sdpr", "hts avls", "mrkt_warn_cls_code", "short_over_yn"
            ],
        },
    )

    # 2. 가져온 데이터를 S3에 업로드하는 태스크
    upload_raw_data_to_s3_task = PythonOperator(
        task_id="upload_raw_stock_current_price_data_to_s3",
        python_callable=upload_raw_to_s3,  # S3 업로드 함수 호출
        op_kwargs={
            "task_type": "stock_current_price",
            "bucket_path": "raw_data",
        },
    )

    # 3. 데이터를 처리하는 태스크
    process_data_task = PythonOperator(
        task_id="process_stock_current_price_data",
        python_callable=process_data,  # 데이터 처리 함수 호출
        op_kwargs={
            "task_type": "stock_current_price",
            "schema_mapping": {
                "columns": {
                    "iscd_stat_cls_code": "종목상태코드",
                    "rprs_mrkt_kor_name": "대표시장한글명명",
                    "new_hgpr_lwpr_cls_code": "신고가/신저가구분코드",
                    "bstp_kor_isnm": "업종한글종목명",
                    "stck_prpr": "주식현재가",
                    "prdy_vrss": "전일대비",
                    "prdy_vrss_sign": "전일대비부호",
                    "prdy_ctrt": "전일대비율",
                    "stck_oprc": "시가",
                    "stck_hgpr": "최고가",
                    "stck_lwpr": "최저가",
                    "stck_mxpr": "상한가",
                    "stck_llam": "하한가",
                    "stck_sdpr": "기준가",
                    "hts avls": "시가총액",
                    "mrkt_warn_cls_code": "시장경고코드",
                    "short_over_yn": "단기과열여부",
                },
                "dtypes": {
                    "종목상태코드": "string",
                    "대표시장한글명": "string",
                    "신고가/신저가구분코드": "string",
                    "업종한글종목명": "string",
                    "주식현재가": "int32",
                    "전일대비": "int32",
                    "전일대비부호": "string",
                    "전일대비율": "float64",
                    "시가": "int32",
                    "최고가": "int32",
                    "최저가": "int32",
                    "상한가": "int32",
                    "하한가": "int32",
                    "기준가": "int32",
                    "시가총액": "int64",
                    "시장경고코드": "string",
                    "단기과열여부": "string",
                },
            },
        },
    )

    # 4. 처리된 데이터를 S3에 업로드하는 태스크
    upload_transformed_data_to_s3_task = PythonOperator(
        task_id="upload_transformed_stock_current_price_data_to_s3",
        python_callable=upload_transformed_to_s3,  # S3 업로드 함수 호출
        op_kwargs={
            "task_type": "stock_current_price",
            "bucket_path": "transformed_data",
        },
    )

    # 5. Redshift 테이블 생성 태스크
    create_table_task = PythonOperator(
        task_id="create_stock_current_price_table",
        python_callable=create_redshift_table,  # 테이블 생성 함수 호출
        op_kwargs={
            "table_name": "stock_current_price",  # 테이블 이름
            "columns_sql": """
                "종목 상태 코드" VARCHAR(3),
                "시장 이름" VARCHAR(40),
                "신고가/신저가 구분" VARCHAR(10),
                "업종 이름" VARCHAR(40),
                "현재가" INT,
                "전일 대비" INT,
                "전일 대비 부호" INT,
                "전일 대비율" FLOAT,
                "시가" INT,
                "고가" INT,
                "저가" INT,
                "상한가" INT,
                "하한가" INT,
                "기준가" INT,
                "시가총액" BIGINT,
                "시장경고코드" VARCHAR(2),
                "단기과열여부" VARCHAR(1)
            """,
        },
    )

    # 6. 데이터를 Redshift로 적재하는 태스크
    upload_to_redshift_task = PythonOperator(
        task_id="upload_stock_current_price_data_to_redshift",
        python_callable=upload_to_redshift,  # 데이터 적재 함수 호출
        op_kwargs={
            "task_type": "stock_current_price",
            "table_name": "stock_current_price",
        },
    )

    # DAG 태스크 의존성 정의
    fetch_data_task >> upload_raw_data_to_s3_task >> process_data_task >> upload_transformed_data_to_s3_task >> create_table_task >> upload_to_redshift_task
