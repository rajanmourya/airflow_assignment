from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
import pandas as pd
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1), 
    'email': ['rajanmourya2015@gmail.com'],  
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'movie_lens_analysis_pipeline',
    default_args=default_args,
    description='Pipeline to analyze MovieLens dataset',
    schedule_interval='0 20 * * 1-5',  
    catchup=False,
)

# Function to send_alert message
def send_alert(message):
    url = 'https://your-alerts-api.example.com/send'
    headers = {'Content-Type': 'application/json'}
    payload = {'message': message}
    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            print("Alert sent successfully!")
        else:
            print(f"Failed to send alert. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error sending alert: {str(e)}")


# this will be called whenever any failure in task
def send_alert_on_failure(context):
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id
    dag_id = dag_run.dag_id
    execution_date = context.get("execution_date")

    message = f"Airflow DAG '{dag_id}' failed on task '{task_id}' at {execution_date}."
    send_alert(message)


def load_data():
    """
    Function to load MovieLens dataset into Pandas DataFrames.
    Adjust file paths as per your environment.
    """     
    ratings_cols = ['user_id', 'movie_id', 'rating', 'timestamp']
    ratings = pd.read_csv('./data/ml-100k/u.data', sep='\t', names=ratings_cols)
    
    
    genres = ['unknown', 'Action', 'Adventure', 'Animation', 'Children\'s', 'Comedy', 'Crime', 
              'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 
              'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
    movies_cols = ['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url'] + genres
    movies = pd.read_csv('./data/ml-100k/u.item', sep='|', names=movies_cols, encoding='latin-1')
    
    
    movies = movies.drop(['release_date', 'video_release_date', 'imdb_url'], axis=1)
    

    movies['genres'] = movies[genres].apply(lambda x: [genres[i] for i, val in enumerate(x) if val == 1], axis=1)
    movies = movies.drop(genres, axis=1)
    
    
    users_cols = ['user_id', 'age', 'gender', 'occupation', 'zip_code']
    users = pd.read_csv('./data/ml-100k/u.user', sep='|', names=users_cols, encoding='latin-1')
    
    return users, ratings, movies


def compute_mean_age_per_occupation():
    users, _, _ = load_data()
    mean_age_per_occupation = users.groupby('occupation')['age'].mean().reset_index()
    print("Mean Age of Users per Occupation:")
    print(mean_age_per_occupation)


def find_top_20_highest_rated_movies():
    _, ratings, movies = load_data()
    
    
    movie_ratings_count = ratings.groupby('movie_id').size()
    popular_movies = movie_ratings_count[movie_ratings_count >= 35].index
    
    
    movie_avg_ratings = ratings[ratings['movie_id'].isin(popular_movies)].groupby('movie_id')['rating'].mean()
    
    
    top_20_movies = pd.merge(movies, movie_avg_ratings, left_on='movie_id', right_index=True)
    top_20_movies = top_20_movies.sort_values(by='rating', ascending=False).head(20)
    
    print("Top 20 Highest Rated Movies:")
    print(top_20_movies[['title', 'rating']])


def find_top_genres_per_occupation_age_group():
    users, ratings, movies = load_data()
    
    
    age_bins = [0, 20, 25, 35, 45, 150]
    age_labels = ['Under 20', '20-25', '25-35', '35-45', '45 and older']
    users['age_group'] = pd.cut(users['age'], bins=age_bins, labels=age_labels)
    
    
    user_ratings = pd.merge(users, ratings, on='user_id')
    user_ratings = pd.merge(user_ratings, movies[['movie_id', 'genres']], on='movie_id')
    
    
    top_genres_per_occupation_age_group = user_ratings.explode('genres').groupby(['occupation', 'age_group', 'genres']).size().reset_index(name='count')
    top_genres_per_occupation_age_group = top_genres_per_occupation_age_group.sort_values(by=['occupation', 'age_group', 'count'], ascending=[True, True, False]).groupby(['occupation', 'age_group']).head(1)
    
    print("Top Genres Rated by Users of Each Occupation in Every Age Group:")
    print(top_genres_per_occupation_age_group)


def find_top_10_similar_movies(movie_title):
    users, ratings, movies = load_data()
    
    
    movie_ratings = pd.merge(ratings, movies[['movie_id', 'title']], on='movie_id')
    
    
    ratings_matrix = movie_ratings.pivot_table(index='user_id', columns='title', values='rating')
    
    
    if movie_title not in ratings_matrix.columns:
        print(f"Movie '{movie_title}' not found in the ratings matrix.")
        return
    
    
    movie_ratings = ratings_matrix[movie_title]
    
    
    similarity_scores = ratings_matrix.corrwith(movie_ratings)
    
    
    similar_movies_df = pd.DataFrame({'title': similarity_scores.index, 'score': similarity_scores.values})
    co_occurrence_counts = ratings_matrix.apply(lambda x: x[movie_ratings.notnull()].notnull().sum())
    similar_movies_df['strength'] = co_occurrence_counts.values
    
    
    print("Similarity DataFrame:")
    print(similar_movies_df.head(10))
    
    
    similarity_threshold = 0.40
    co_occurrence_threshold = 30
    similar_movies_df = similar_movies_df[(similar_movies_df['score'] >= similarity_threshold) &
                                          (similar_movies_df['strength'] >= co_occurrence_threshold)]
    
    
    similar_movies_df = similar_movies_df[similar_movies_df['title'] != movie_title]
    
    
    similar_movies_df = similar_movies_df.sort_values(by=['score', 'strength'], ascending=False).head(10)
    
    
    top_similar_movies = similar_movies_df[['title', 'score', 'strength']].to_dict('records')
    
    print(f"Top 10 similar movies for '{movie_title}':")
    for idx, movie in enumerate(top_similar_movies, 1):
        print(f"{idx}. {movie['title']}")
        print(f"   Score: {movie['score']:.4f}")
        print(f"   Strength: {movie['strength']}")


# Used ExternalTaskSensor to check for the pipeline status
wait_for_news_sentiment_pipeline = ExternalTaskSensor(
    task_id='wait_for_news_sentiment_pipeline',
    external_dag_id='news_sentiment_pipeline',
    external_task_id='persist_sentiment_scores', 
    mode='poke',
    timeout=600,  # 10 minutes
    poke_interval=60,  # 1 minute
    dag=dag,
)


task1 = PythonOperator(
    task_id='mean_age_per_occupation',
    python_callable=compute_mean_age_per_occupation,
    dag=dag,
)

task2 = PythonOperator(
    task_id='top_20_highest_rated_movies',
    python_callable=find_top_20_highest_rated_movies,
    dag=dag,
)

task3 = PythonOperator(
    task_id='top_genres_per_occupation_age_group',
    python_callable=find_top_genres_per_occupation_age_group,
    dag=dag,
)

task4 = PythonOperator(
    task_id='find_top_10_similar_movies',
    python_callable=find_top_10_similar_movies,
    op_kwargs={'movie_title': "Usual Suspects, The (1995)"},  
    dag=dag,
)

# To check if any task is geetting failed
wait_for_news_sentiment_pipeline.on_failure_callback = send_alert_on_failure
task1.on_failure_callback = send_alert_on_failure
task2.on_failure_callback = send_alert_on_failure
task3.on_failure_callback = send_alert_on_failure
task4.on_failure_callback = send_alert_on_failure

# Made all task dependent on wait task and then run all other in parallel
wait_for_news_sentiment_pipeline >> [task1, task2, task3, task4]