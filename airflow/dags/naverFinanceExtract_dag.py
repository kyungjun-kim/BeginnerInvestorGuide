from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO
import time, boto3, os, logging
import pandas as pd

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def init_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    # service = Service(ChromeDriverManager().install())
    # driver = webdriver.Chrome(service=service, options=options)
    driver = webdriver.Remote('remote_chromedriver:4444/wd/hub', options=options)
    driver.implicitly_wait(5)
    
    return driver

def crawl_stock_data(**kwargs):
    # Selenium 드라이버 설정
    driver = init_driver()
    print("드라이버 생성 완료")

    today = datetime.now().strftime('%Y.%m.%d')
    url = 'https://m.stock.naver.com/investment/research/company'
    
    text_list = []
    cols = ["date", "stockCode", "stockName", "investmentOpinion", "targetPrice", "currentPrice", "title", "text", "url"]
    
    try : 
        driver.get(url)
        time.sleep(2)  # 페이지 로딩 대기
        i = 0
        while i < 2:
            i += 1
            time.sleep(2)
            search_page = driver.find_elements(By.CLASS_NAME, "ResearchList_item__I6Z7_")[i]

            news_date = driver.find_elements(By.CLASS_NAME, 'ResearchList_description___nHPv')[1].text.strip(".")
            stock_name = search_page.text.split("\n")[0]

            if news_date not in search_page.text:
                break
            else:
                try:
                    search_page.click()
                except:
                    driver.execute_script(f"window.scrollBy(0, 110);")
                    search_page.click()

            time.sleep(2)

            news_url = driver.current_url

            stock_code = int(driver.find_element(By.CLASS_NAME, 'HeaderResearch_tag__qwHzD').text.replace(stock_name, ""))

            investment_opinion = driver.find_element(By.CLASS_NAME, 'ResearchConsensus_text__BFWiw').text

            try:
                target_price_text = driver.find_element(By.CLASS_NAME, 'ResearchConsensus_price___VI3M').text.split("\n")[-1]
                target_price = int(target_price_text.replace(",", "").replace("원", ""))
            except:
                target_price = None

            try:
                current_price_text = driver.find_element(By.CLASS_NAME, 'ResearchConsensus_price_today__zpk_T').text.split("\n")[-1]
                current_price = int(current_price_text.replace(",", "").replace("원", ""))
            except:
                current_price = None

            title = driver.find_element(By.CLASS_NAME, 'ResearchContent_text_area__BsfMF').text.split('\n')[0]
            
            text = driver.find_element(By.CLASS_NAME, 'ResearchContent_text_area__BsfMF').text.split('\n')[-1]

            tmp = [news_date, stock_code, stock_name, investment_opinion, target_price, current_price, title, text, news_url]
            if len(text_list) > 0 and text_list[-1] == tmp :
                break
            else :
                text_list.append(tmp)

            driver.back()
            time.sleep(2)
            driver.execute_script(f"window.scrollBy(0, 110);")

    except Exception as e:
        print("에러 발생:", e)

    finally:
        driver.quit()
        if text_list :
            # 데이터프레임 생성 및 저장
            df = pd.DataFrame(text_list, columns=cols)
            ti = kwargs['ti']
            path_news = f"naverNews_{datetime.now().strftime('%Y%m%d')}.csv"
            df.to_csv(path_news, index=False, encoding="utf-8-sig")
            ti.xcom_push(key = "path_news" ,value = path_news)
            return "뉴스 데이터 생성 완료"
        else :
            return "데이터가 없어 파일을 생성하지 않았습니다."
    return df

def crawl_kospi_kosdaq_data(**kwargs):
    # Selenium 드라이버 설정
    # Selenium 드라이버 설정
    driver = init_driver()
    print("드라이버 생성 완료")

    url_kospi = 'https://m.stock.naver.com/domestic/index/KOSPI/total'
    url_kosdaq = 'https://m.stock.naver.com/domestic/index/KOSDAQ/total'

    data = []

    try:
        # 코스피 데이터 크롤링
        driver.get(url_kospi)
        time.sleep(3)

        kospi_index = driver.find_element(By.CLASS_NAME, 'GraphMain_price__GT8dV').text
        kospi_rate = driver.find_elements(By.CLASS_NAME, 'VGap_gap__LQYpL')[1].text

        # 코스닥 데이터 크롤링
        driver.get(url_kosdaq)
        time.sleep(3)

        kosdaq_index = driver.find_element(By.CLASS_NAME, 'GraphMain_price__GT8dV').text
        kosdaq_rate = driver.find_elements(By.CLASS_NAME, 'VGap_gap__LQYpL')[1].text

        # 데이터 저장
        data.append({
            'indexName': "kospi_index",
            'value': kospi_index
        })
        data.append({
            'indexName': 'kospi_rate',
            'value': kospi_rate
        })
        data.append({
            'indexName': 'kosdaq_index',
            'value': kosdaq_index
        })
        data.append({
            'indexName': 'kosdaq_rate',
            'value': kosdaq_rate
        })

        # DataFrame 생성
        df = pd.DataFrame(data)

    except Exception as e:
        print("에러 발생:", e)

    finally:
        driver.quit()
        if data :
            # 데이터프레임 생성 및 저장
            df = pd.DataFrame(data)
            #print("뉴스 데이터 생성 완료")
            ti = kwargs['ti']
            path_kos = f"kospi_kosdaq_data_{datetime.now().strftime('%Y%m%d')}.csv"
            df.to_csv(path_kos, index=False, encoding="utf-8-sig")
            ti.xcom_push(key = "path_kos" ,value = path_kos)
            return "뉴스 데이터 생성 완료"
        else :
            return "데이터가 없어 파일을 생성하지 않았습니다."

def save_load_data(**kwargs) :
    ti = kwargs['ti']
    path_news = ti.xcom_pull(task_ids='crawl_stock_data', key ='path_news')
    path_kos = ti.xcom_pull(task_ids='crawl_kospi_kosdaq_data',  key ='path_news')

    # S3 업로드
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    s3_hook.load_file(
        filename = path_news,
        bucket_name='team6-s3',
        replace=True,
        key=f"raw_data/{os.path.basename(path_news)}"
    )
    s3_hook.load_file(
        filename = path_kos,
        bucket_name='team6-s3',
        replace=True,
        key=f"raw_data/{os.path.basename(path_kos)}"
    )

# Redshift 테이블 생성 함수
def create_and_load_redshift_tables(**kwargs):
    # Redshift 연결 설정
    postgres_hook = PostgresHook(postgres_conn_id="redshift_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # 테이블 생성 SQL 정의
    create_table_sqls = [
        """
        CREATE TABLE IF NOT EXISTS naverNews (
            date DATE,
            stockCode INT,
            stockName VARCHAR(40),
            investmentOpinion VARCHAR(20),
            targetPrice INT,
            currentPrice INT,
            title VARCHAR(255),
            text VARCHAR(65535),
            url VARCHAR(255)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS kospiKosdaqData (
            indexName VARCHAR(20),
            value VARCHAR(40)
        );
        """
    ]

    # 테이블 생성 실행
    for sql in create_table_sqls:
        cursor.execute(sql)

    conn.commit()
    print("Redshift 테이블 생성 완료.")

    # 데이터 적재
    ti = kwargs['ti']
    s3_paths = {
        #'naverNews': ti.xcom_pull(task_ids='upload_to_s3', key='path_news'),
        #'kospiKosdaqData': ti.xcom_pull(task_ids='upload_to_s3', key='path_kos')
        'naverNews': f"s3://team6-s3/raw_data/{ti.xcom_pull(task_ids='upload_to_s3', key='path_news')}",
        'kospiKosdaqData': f"s3://team6-s3/raw_data/{ti.xcom_pull(task_ids='upload_to_s3', key='path_kos')}"
    }
    aws_conn = BaseHook.get_connection("aws_conn")
    access_key = aws_conn.login
    secret_key = aws_conn.password

    for table_name, s3_path in s3_paths.items():
        if not s3_path:
            raise Exception(f"{table_name} 데이터 Redshift 업로드 실패: S3 경로가 없습니다.")

        # COPY 명령 SQL
        copy_sql = f"""
        COPY {table_name}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        CSV DELIMITER ',' IGNOREHEADER 1;
        """
        
        try:
            cursor.execute(copy_sql)
        except Exception as e:
            print(f"{table_name} 데이터 Redshift 업로드 중 에러 발생: {e}")
            conn.rollback()
            raise

    conn.commit()
    cursor.close()
    conn.close()

    print("Redshift 데이터 적재 완료.")

# DAG 정의
dag = DAG(
    'crawl_and_upload_Naver_data_dag',
    default_args=default_args,
    description='DAG for crawling stock data and uploading to Redshift',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Task 정의
crawl_stock_task = PythonOperator(
    task_id='crawl_stock_data',
    python_callable=crawl_stock_data,
    provide_context=True,
    dag=dag
)

crawl_kospi_kosdaq_task = PythonOperator(
    task_id='crawl_kospi_kosdaq_data',
    python_callable=crawl_kospi_kosdaq_data,
    provide_context=True,
    dag=dag
)

upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=save_load_data,  # 동일 함수 재사용
    provide_context=True,
    dag=dag
)

create_and_load_redshift_task = PythonOperator(
    task_id='create_and_load_redshift_tables',
    python_callable=create_and_load_redshift_tables,
    provide_context=True,
    dag=dag
)

# Task 순서 정의
crawl_stock_task >> crawl_kospi_kosdaq_task >> upload_to_s3_task >> create_and_load_redshift_task
