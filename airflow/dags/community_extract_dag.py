from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import boto3
import json
import time
import os
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import re
import torch
from transformers import BertTokenizer, BertModel
from collections import Counter


# Setting DAG 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'community_crawling_dag',
    start_date=datetime(2025, 1, 1),
    default_args=default_args,
    description='주식 커뮤니티(Clien, FMKorea) 게시글 크롤링 DAG',
    schedule_interval='@daily',
    catchup=False
)        

# Initialize WebDriver
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

# Crawling FMkorea community posts
def get_fmkorea_posts(base_url):
    driver = init_driver()
    posts = []
    
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    date_start = (today - timedelta(days=1)).replace(hour=0, minute=0, second=0) 
    date_end = (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    
    page = 1
    stop_crawling = False
    
    while not stop_crawling:
        url = f"{base_url}&page={page}"
        driver.get(url)
        driver.implicitly_wait(60) 
        
        articles = driver.find_elements(By.CSS_SELECTOR, '.bd_lst.bd_tb_lst.bd_tb .title a')
        
        if not articles:
            print("[FMKorea] No more articles found.")
            break
        
        for article in articles:
            if article.get_attribute('title') == "댓글":
                continue
                
            parent_tr = article.find_element(By.XPATH, "./../..")  # article의 부모 tr 태그를 찾기
            post_element = parent_tr.find_element(By.CSS_SELECTOR, ".time")
            post_str = post_element.text.strip()
                
            try:
                if ":" in post_str: # HH:MM 형태 시간 계산
                    post_time = datetime.strptime(post_str, "%H:%M").time()
                    post_date = datetime.combine(today.date(), post_time)
                    if post_date > today:  # HH:MM이 오늘 이후라면 어제로 조정
                        post_date = datetime.combine(yesterday.date(), post_time)
                else:
                    post_date = datetime.strptime(post_str, "%Y.%m.%d %H:%M")
            except Exception as e:
                print(f"[FMKorea] Date parsing failed for {post_str}: {e}")
                continue
                
            # 첫번째 어제자 게시물을 찾으면, 본문 크롤링만 진행
            if date_start <= post_date <= date_end:
                print("[FMKorea] Found first yesterday post, starting detailed crawls...")
                article.click()
                time.sleep(2)
                
                while True:
                    title = driver.find_element(By.CSS_SELECTOR, 'h1.np_18px').text.strip()
                    url = driver.current_url
                    time_element = driver.find_element(By.CSS_SELECTOR, '.date')
                    time_str = time_element.text.strip()
                    date = datetime.strptime(time_str, "%Y.%m.%d %H:%M")
                    
                    if date < date_start:
                        stop_crawling = True
                        break
                    
                    content_element = driver.find_element(By.CSS_SELECTOR, '.rd_body article')
                    content = content_element.text.strip() if content_element else "No content"
                    
                    posts.append({
                        'title': title,
                        'date': date.strftime("%Y-%m-%d %H:%M"),
                        'text': content,
                        'link': url
                    })
                    
                    # 다음 게시물로 계속 넘어가기 (버튼 클릭 event)
                    next_button = driver.find_element(By.CSS_SELECTOR, 'div.prev_next_btns span.btn_pack.next.blockfmcopy')
                    driver.execute_script("arguments[0].scrollIntoView();", next_button)
                    next_button.click()
                    time.sleep(0.5)
                
                if stop_crawling:
                    print("[FMKorea] Crawling stopped. All posts from yesterday have been fetched.")
                    break
        page += 1
    
    driver.quit()
    return posts

# Crawling Clien community posts
def get_clien_posts(base_url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    today = datetime.now()
    date_start = (today - timedelta(days=1)).replace(hour=0, minute=0, second=0)
    date_end = (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    
    posts = []
    page = 0
    
    while True:
        url = f"{base_url}?&od=T31&category=0&po={page}"
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"Failed to fetch page {page}: {response.status_code}")
            break
        
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = soup.select('div.list_item.symph_row')
        
        if not articles:
            print("[Clien] No more articles found.")
            break
        
        for article in articles:
            title_element = article.select_one('span.subject_fixed')
            date_element = article.select_one('span.timestamp')
            link_element = article.select_one('a.list_subject')
            
            if not title_element or not date_element or not link_element:
                continue
            
            title = title_element.get_text(strip=True)
            link = f"https://www.clien.net{link_element['href']}"
            date_str = date_element.get_text(strip=True)
            post_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

            if post_date < date_start:
                print("[Clien] No more posts from yesterday.")
                return posts

            if date_start <= post_date <= date_end:
                # Crawling post content
                post_response = requests.get(link, headers=headers)
                if post_response.status_code != 200:
                    print(f"[Clien] Failed to fetch post: {link}")
                    continue
                
                post_soup = BeautifulSoup(post_response.text, 'html.parser')
                content_element = post_soup.select_one('div.post_article')
                content = content_element.get_text(strip=True) if content_element else "No content"
                
                posts.append({
                    'title': title,
                    'date': post_date.strftime("%Y-%m-%d %H:%M"),
                    'text': content,
                    'link' : link
                })
        page += 1
    return posts

# Load files from S3
def load_s3_file(bucket_name, file_key, download_path):
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    s3_hook.get_conn().download_file(bucket_name, file_key, download_path)

def load_stopwords(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        stopwords = set(line.strip() for line in f if line.strip())
    return stopwords

def load_stock_aliases(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        alias_dict = json.load(f)
    return alias_dict

def match_stock_names_with_aliases(text, stock_aliases):
    matched_stocks = []
    alias_to_main = {}

    for main_stock, aliases in stock_aliases.items():
        alias_to_main[main_stock] = main_stock
        for alias in aliases:
            alias_to_main[alias] = main_stock

    extended_stock_names = set(alias_to_main.keys())

    for stock in sorted(extended_stock_names, key=len, reverse=True):
        if stock in text:
            matched_stocks.append(alias_to_main[stock])
            text = text.replace(stock, '')

    return matched_stocks, text

# Extract keywords using KoBERT
def extract_keywords_with_kobert(text, tokenizer, model, stopwords):
    keywords = []
    split_texts = [text[i:i+512] for i in range(0, len(text), 512)]

    for chunk in split_texts:
        inputs = tokenizer(chunk, return_tensors='pt', truncation=True, padding='max_length', max_length=512)
        with torch.no_grad():
            outputs = model(**inputs)

        input_ids = inputs['input_ids'][0]
        tokens = tokenizer.convert_ids_to_tokens(input_ids)

        combined_tokens = []
        temp_token = ''
        for token in tokens:
            if token.startswith('##'):
                temp_token += token[2:]
            else:
                if temp_token:
                    combined_tokens.append(temp_token)
                    temp_token = ''
                if len(token) > 1 and re.match(r'[가-힣a-zA-Z]+', token):
                    combined_tokens.append(token)
        if temp_token:
            combined_tokens.append(temp_token)

        keywords.extend([token for token in combined_tokens if token not in stopwords])

    return keywords

# 1. Process of crawling and Save data
def crawling_data(**kwargs):
    ti = kwargs['ti']
    
    clien_url = "https://www.clien.net/service/board/cm_stock"
    fm_url = "https://www.fmkorea.com/index.php?mid=stock&category=2997203870"
    
    clien_posts = get_clien_posts(clien_url)
    fm_posts = get_fmkorea_posts(fm_url)
    all_posts = clien_posts + fm_posts
    
    crawling_df = pd.DataFrame(all_posts)
    crawling_path = f"/tmp/community_crawling_{datetime.now().strftime('%y%m%d')}.csv"
    crawling_df.to_csv(crawling_path, index=False, encoding='utf-8-sig')
    
    ti.xcom_push(key='crawling_path', value=crawling_path)
    print("[Done] Create community crawling csv file.")
    
    ti.xcom_push(key='all_posts', value=all_posts)
    print("[Done] Crawling data pushed to XCom.")

# 2. Extract keywords to use KoBERT
def extract_stock_keywords(posts, alias_path, stopwords_path):
    stock_aliases = load_stock_aliases(alias_path)
    stopwords = load_stopwords(stopwords_path)

    tokenizer = BertTokenizer.from_pretrained('monologg/kobert')
    model = BertModel.from_pretrained('monologg/kobert')

    results = []

    for post in posts:
        text = str(post['title']) + " " + str(post['text'])
        text = re.sub(r'[^가-힣a-zA-Z0-9\s]', '', text)

        matched_stocks, text = match_stock_names_with_aliases(text, stock_aliases)
        kobert_keywords = extract_keywords_with_kobert(text, tokenizer, model, stopwords)

        keyword_counts = Counter(matched_stocks + kobert_keywords)
        keyword_counts_dict = dict(sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True))

        results.append({
            'title': post['title'],
            'date': post['date'],
            'text': post['text'],
            'link': post['link'],
            'keywords': keyword_counts_dict
        })

    return pd.DataFrame(results)

def extract_raw_data(**kwargs):
    ti = kwargs['ti']
    
    # Load data from S3
    bucket_name = 'team6-s3'
    base_path = '/tmp/'
    files = {
        'alias': 'data/raw_data/stock_alias.json',
        'stopwords': 'data/stopwords.txt'
    }
    
    for key, file_key in files.items():
        load_s3_file(bucket_name, file_key, os.path.join(base_path, key))
    
    alias_path = os.path.join(base_path, 'alias')
    stopwords_path = os.path.join(base_path, 'stopwords')
    all_posts = ti.xcom_pull(task_ids='save_crawling_data', key='all_posts')
    
    results_df = extract_stock_keywords(all_posts, alias_path, stopwords_path)
    print("[Done] Extract community raw data.")
    
    raw_data_path = f"/tmp/community_raw_data_{datetime.now().strftime('%y%m%d')}.csv"
    results_df.to_csv(raw_data_path, index=False, encoding='utf-8-sig')    
    ti.xcom_push(key='raw_data_path', value=raw_data_path)
    print("[Done] Create community raw data csv file.")
    
# 3. Upload to S3
def upload_to_s3(**kwargs):
    ti = kwargs['ti']
    file_paths = [
        ti.xcom_pull(task_ids='save_crawling_data', key='crawling_path'),
        ti.xcom_pull(task_ids='extract_raw_data', key='raw_data_path')
    ]

    s3_hook = S3Hook(aws_conn_id='aws_conn')

    for file_path in file_paths:
        s3_hook.load_file(
            filename=file_path,
            bucket_name='team6-s3',
            replace=True,
            key=f"raw_data/{os.path.basename(file_path)}"
        )

# 4. Trasform data 
def extract_transformed_data(**kwargs):
    ti = kwargs['ti']
    
    # Read community raw data
    s3_hook = S3Hook(aws_conn_id='aws_conn')

    # Load raw data (+ JSON format)
    raw_data_obj = s3_hook.get_key(key=f"raw_data/community_raw_data_{datetime.now().strftime('%y%m%d')}.csv", bucket_name="team6-s3")
    raw_data_df = pd.read_csv(raw_data_obj.get()['Body'], encoding='utf-8-sig')
    # keywords column -> JSON transform
    def safe_json_loads(x):
        try:
            x = x.replace("'", '"') if isinstance(x, str) else x
            return json.loads(x) if pd.notnull(x) else {}
        except (json.JSONDecodeError, TypeError):
            print(f"[Warning] 잘못된 JSON: {x}")
            return {}
    
    raw_data_df['keywords'] = raw_data_df['keywords'].apply(safe_json_loads)

    # Read stock name list
    kospi_obj = s3_hook.get_key(key="data/raw_data/kospi_stocks.csv", bucket_name="team6-s3")
    kospi_stocks_df = pd.read_csv(kospi_obj.get()['Body'], encoding='utf-8-sig')
    kospi_stock_names = kospi_stocks_df['종목명'].tolist()
    
    # Extract top 10 mentioned stock 
    stock_mentions = Counter()
    for index, row in raw_data_df.iterrows():
        keywords = row['keywords']
        for keyword in keywords:
            # Count only stock names
            if keyword in kospi_stock_names:
                stock_mentions[keyword] += keywords[keyword]

    top_stocks = stock_mentions.most_common(10)
    top_stocks_df = pd.DataFrame(top_stocks, columns=['stock_name', 'mention_count'])

    # Extract top 15 keywords
    all_keywords = {}
    for index, row in raw_data_df.iterrows():
        keywords = row['keywords']
        for keyword in keywords:
            if keyword not in kospi_stock_names:
                all_keywords[keyword] = all_keywords.get(keyword, 0) + keywords[keyword]
    
    top_keywords = sorted(all_keywords.items(), key=lambda x: x[1], reverse=True)[:15]
    top_keywords_df = pd.DataFrame(top_keywords, columns=['keyword', 'mention_count'])

    top_stocks_path = f"/tmp/community_top10_data_{datetime.now().strftime('%y%m%d')}.csv"
    top_stocks_df.to_csv(top_stocks_path, index=False, encoding='utf-8-sig')    
    ti.xcom_push(key='top_stocks_path', value=top_stocks_path)
    
    top_keywords_path = f"/tmp/community_top_keyword_data_{datetime.now().strftime('%y%m%d')}.csv"
    top_keywords_df.to_csv(top_keywords_path, index=False, encoding='utf-8-sig')    
    ti.xcom_push(key='top_keywords_path', value=top_keywords_path)
    
    print("[Done] Create transformed data.")

# 5. Upload to S3
def upload_to_s3_trasformed_data(**kwargs):
    ti = kwargs['ti']
    file_paths = [
        ti.xcom_pull(task_ids='extract_transformed_data', key='top_stocks_path'),
        ti.xcom_pull(task_ids='extract_transformed_data', key='top_keywords_path')
    ]

    s3_hook = S3Hook(aws_conn_id='aws_conn')

    for file_path in file_paths:
        s3_hook.load_file(
            filename=file_path,
            bucket_name='team6-s3',
            replace=True,
            key=f"transformed_data/{os.path.basename(file_path)}"
        )
        
    print("[Done] Upload transformed data to S3")

# 6. Create Redshift table
def create_redshift_tables(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="redshift_conn")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    queries = [
        """
        CREATE TABLE IF NOT EXISTS community_top10 (
            stockName VARCHAR(255) NOT NULL,
            mentionCount INTEGER NOT NULL
        );
        """,
        """
        DELETE FROM community_top10;
        """,
        """
        CREATE TABLE IF NOT EXISTS community_top_keyword (
            keyword VARCHAR(255) NOT NULL,
            mentionCount INTEGER NOT NULL
        );
        """,
        """
        DELETE FROM community_top_keyword;
        """
    ]
    
    for query in queries:
        cursor.execute(query)
    
    conn.commit()
    print("[Done] Success to create Redshift table.")
    
    s3_paths = {
        'community_top10': f"s3://team6-s3/transformed_data/community_top10_data_{datetime.now().strftime('%Y%m%d')}.csv",
        'community_top_keyword': f"s3://team6-s3/transformed_data/community_top_keyword_data_{datetime.now().strftime('%Y%m%d')}.csv"
    }
    aws_conn = BaseHook.get_connection("aws_conn")
    access_key = aws_conn.login
    secret_key = aws_conn.password
    
    for table_name, s3_path in s3_paths.items():

        copy_query = f"""
        COPY {table_name}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        CSV DELIMITER ',' IGNOREHEADER 1;
        """
        cursor.execute(copy_query)

    conn.commit()
    cursor.close()
    conn.close()
    print("[Done] Success to save data on Redshift")

# Task 
crawling_data_task = PythonOperator(
    task_id='save_crawling_data',
    python_callable=crawling_data,
    dag=dag
)

extract_raw_data_task = PythonOperator(
    task_id='extract_raw_data',
    python_callable=extract_raw_data,
    dag=dag
)

upload_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    dag=dag
)

extract_transformed_data_task = PythonOperator(
    task_id='extract_transformed_data',
    python_callable=extract_transformed_data,
    dag=dag
)

upload_s3_transformed_data_task = PythonOperator(
    task_id='upload_to_s3_trasformed_data',
    python_callable=upload_to_s3,
    dag=dag
)

create_redshift_tables_task = PythonOperator(
    task_id='create_redshift_tables',
    python_callable=create_redshift_tables,
    dag=dag
)

crawling_data_task >> extract_raw_data_task >> upload_s3_task >> extract_transformed_data_task >> upload_s3_transformed_data_task >> create_redshift_tables_task