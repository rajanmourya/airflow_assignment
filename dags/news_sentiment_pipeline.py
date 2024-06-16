from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import brotli


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'news_sentiment_pipeline',
    default_args=default_args,
    description='A pipeline to fetch and analyze news sentiment for HDFC and Tata Motors',
    schedule_interval='0 19 * * 1-5',  #Daily 7PM workind day
    catchup=False,
)


def fetch_articles(**kwargs):
    try:
        logging.info("Starting fetch_articles task")
        sources = {
            'yourstory': 'https://yourstory.com',
            'finshots': 'https://finshots.in'
        }
        keywords = ['HDFC', 'Tata Motors']
        articles = []
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.google.com/',
        'DNT': '1',  
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1'
    }
    
        session = requests.Session()
        
        for source_name, base_url in sources.items():
            for keyword in keywords:
                url = f'{base_url}/search?q={keyword}'
                logging.info(f"Fetching articles for keyword header: {keyword} from source: {source_name}, {url}")
                response = session.get(url, headers=headers)
                logging.info(f"Received response status code: {response.status_code}")
                if response.status_code == 200:
                    print('response ',response.headers)
                    content = brotli.decompress(response.content).decode('utf-8')
                    soup = BeautifulSoup(content, 'html.parser')
                    for article in soup.find_all('title')[:5]:  
                        title = article.find('h2').get_text() if article.find('h2') else 'No Title'
                        text = article.find('p').get_text() if article.find('p') else 'No Text'
                        articles.append({'source': source_name, 'keyword': keyword, 'title': title, 'text': text})
                else:
                    logging.error(f"Failed to fetch articles from {base_url} with status code {response.status_code}")
        logging.info(f"Fetched articles: {articles}")
        kwargs['ti'].xcom_push(key='articles', value=articles)
    except Exception as e:
        logging.error(f"Error in fetch_articles: {str(e)}")
        raise

fetch_articles_task = PythonOperator(
    task_id='fetch_articles',
    python_callable=fetch_articles,
    dag=dag,
)


def clean_preprocess_data(**kwargs):
    try:
        articles = kwargs['ti'].xcom_pull(key='articles', task_ids='fetch_articles')
        logging.info(f"Fetched articles from XCom: {articles}")
        df = pd.DataFrame(articles)
        df.drop_duplicates(subset=['title', 'text'], inplace=True)
        kwargs['ti'].xcom_push(key='cleaned_articles', value=df.to_dict('records'))
    except Exception as e:
        logging.error(f"Error in clean_preprocess_data: {str(e)}")
        raise

clean_preprocess_data_task = PythonOperator(
    task_id='clean_preprocess_data',
    python_callable=clean_preprocess_data,
    dag=dag,
)


def call_sentiment_api(**kwargs):
    try:
        articles = kwargs['ti'].xcom_pull(key='cleaned_articles', task_ids='clean_preprocess_data')
        sentiment_scores = []
        
        for article in articles:
            text = article['text']
            # As of now hardcoded the sentiment score later on we can use the API to get the score
            # response = requests.post('https://mock-sentiment-api.com', json={'text': text})
            # sentiment_score = response.json().get('sentiment_score', 0.5)
            sentiment_score = 0.5
            article['sentiment_score'] = sentiment_score
            sentiment_scores.append(article)
        
        kwargs['ti'].xcom_push(key='sentiment_scores', value=sentiment_scores)
    except Exception as e:
        logging.error(f"Error in call_sentiment_api: {str(e)}")
        raise

call_sentiment_api_task = PythonOperator(
    task_id='call_sentiment_api',
    python_callable=call_sentiment_api,
    dag=dag,
)


def persist_sentiment_scores(**kwargs):
    try:
        sentiment_scores = kwargs['ti'].xcom_pull(key='sentiment_scores', task_ids='call_sentiment_api')
        df = pd.DataFrame(sentiment_scores)
        # Storing the data in CSV right now but we can store this in a DB (MongoDB NoSQL) that will help in faster query mechanish as this works on hashing
        df.to_csv('/opt/airflow/data/sentiment_scores.csv', index=False)
        logging.info(f"Sentiment scores persisted to CSV: {sentiment_scores}")
    except Exception as e:
        logging.error(f"Error in persist_sentiment_scores: {str(e)}")
        raise

persist_sentiment_scores_task = PythonOperator(
    task_id='persist_sentiment_scores',
    python_callable=persist_sentiment_scores,
    dag=dag,
)


# Created the dependency based on the flow
fetch_articles_task >> clean_preprocess_data_task >> call_sentiment_api_task >> persist_sentiment_scores_task
