from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time, boto3, os
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

def crawl_stock_data(**kwargs):
    # Selenium 드라이버 설정
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("headless")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    today = datetime.now().strftime('%Y.%m.%d')
    url = 'https://m.stock.naver.com/investment/research/company'

    text_list = []
    cols = ["date", "stock_code", "stock_name", "investment_opinion", "target_price", "current_price", "title", "text", "url"]

    try : 
        driver.get(url)
        time.sleep(2)  # 페이지 로딩 대기

        for i in range(30):
            time.sleep(2)
            search_page = driver.find_elements(By.CLASS_NAME, "ResearchList_item__I6Z7_")[i]

            news_date = today
            stock_name = search_page.text.split("\n")[0]

            if today not in search_page.text:
                break
            else:
                try:
                    search_page.click()
                except:
                    driver.execute_script(f"window.scrollBy(0, 110);")
                    search_page.click()

            time.sleep(2)

            news_url = driver.current_url

            stock_code = driver.find_element(By.CLASS_NAME, 'HeaderResearch_tag__qwHzD').text.replace(stock_name, "")

            investment_opinion = driver.find_element(By.CLASS_NAME, 'ResearchConsensus_text__BFWiw').text

            try:
                target_price = driver.find_element(By.CLASS_NAME, 'ResearchConsensus_price___VI3M').text.split("\n")[-1]
            except:
                target_price = "주가정보없음"

            try:
                current_price = driver.find_element(By.CLASS_NAME, 'ResearchConsensus_price_today__zpk_T').text.split("\n")[-1]
            except:
                current_price = "주가정보없음"

            title = driver.find_element(By.CLASS_NAME, 'ResearchContent_text_area__BsfMF').text.split('\n')[0]
            
            text = driver.find_element(By.CLASS_NAME, 'ResearchContent_text_area__BsfMF').text.split('\n')[-1]

            text_list.append([news_date, stock_code, stock_name, investment_opinion, target_price, current_price, title, text, news_url])

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
            file_path_news = 'naverNews.csv'
            df.to_csv(file_path_news, index=False)
            print("데이터가  저장되었습니다.")
        else :
            print("데이터가 없어 파일을 생성하지 않았습니다.")

    return file_path_news

def crawl_kospi_kosdaq_data(**kwargs):
    # Selenium 드라이버 설정
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

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
            '칼럼명': '코스피 지수',
            '내용': '코스피 지수',
            'type': 'float',
            '예시': kospi_index
        })
        data.append({
            '칼럼명': '코스피 변화량',
            '내용': '코스피 변화량',
            'type': 'float',
            '예시': kospi_rate
        })
        data.append({
            '칼럼명': '코스닥 지수',
            '내용': '코스닥 지수',
            'type': 'float',
            '예시': kosdaq_index
        })
        data.append({
            '칼럼명': '코스닥 변화량',
            '내용': '코스닥 변화량',
            'type': 'float',
            '예시': kosdaq_rate
        })

        # DataFrame 생성
        df = pd.DataFrame(data)
        file_path_kospi_kosdaq = 'kospi_kosdaq_data.csv'
        df.to_csv(file_path_kospi_kosdaq, index=False, encoding='utf-8-sig')
        print(f"CSV 파일 {file_path_kospi_kosdaq} 생성 완료.")

    except Exception as e:
        print("에러 발생:", e)

    finally:
        driver.quit()

    return file_path_kospi_kosdaq


def upload_to_s3(file_path, bucket_name, object_key):
    # GitHub Secrets에서 AWS 자격 증명을 환경 변수로 설정
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    # S3 클라이언트 생성
    s3_client = boto3.client(
        's3',
        aws_access_key_id= "${{ secrets.AWS_ACCESS_KEY_ID}}",
        aws_secret_access_key= "${{ secrets.AWS_SECRET_ACCESS_KEY}}"
    )

    try:
        s3_client.upload_file(file_path, bucket_name, object_key)
        print(f"파일이 S3에 업로드되었습니다: s3://{bucket_name}/{object_key}")
    except Exception as e:
        print(f"S3 업로드 실패: {e}")

# DAG 정의
dag = DAG(
    'crawl_and_upload_stock_data_dag',
    default_args=default_args,
    description='DAG for crawling stock data and uploading to S3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# PythonOperator로 크롤링 작업 실행
crawl_stock_task = PythonOperator(
    task_id='crawl_stock_data_task',
    python_callable=crawl_stock_data,
    provide_context=True,
    dag=dag,
)

crawl_kospi_kosdaq_task = PythonOperator(
    task_id='crawl_kospi_kosdaq_data_task',
    python_callable=crawl_kospi_kosdaq_data,
    provide_context=True,
    dag=dag,
)

def s3_upload_task_func(**kwargs):
    # 크롤링 작업의 출력 파일 경로 가져오기
    ti = kwargs['ti']
    stock_file_path = ti.xcom_pull(task_ids='crawl_stock_data_task')
    kospi_kosdaq_file_path = ti.xcom_pull(task_ids='crawl_kospi_kosdaq_data_task')

    # S3 업로드 정보
    bucket_name = 'team6-s3'
    stock_object_key = 'raw/naverFinance/naverFinanceNews.csv'
    kospi_kosdaq_object_key = 'raw/naverFinance/kospiKosdaqData.csv'

    upload_to_s3(stock_file_path, bucket_name, stock_object_key)
    upload_to_s3(kospi_kosdaq_file_path, bucket_name, kospi_kosdaq_object_key)

s3_upload_task = PythonOperator(
    task_id='upload_to_s3_task',
    python_callable=s3_upload_task_func,
    provide_context=True,
    dag=dag,
)

# Task 순서 설정
crawl_stock_task >> crawl_kospi_kosdaq_task >> s3_upload_task
