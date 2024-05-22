from datetime import datetime, timedelta
import sys
import subprocess

sys.path.append('../plugins/operators/')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

# from airflow.contrib.hooks import SSHHook
# from airflow.contrib.operators.ssh_operator import SSHOperator

# from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator

# from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# from airflow.operators.goodreads_plugin import DataQualityOperator

# from airflow.operators.goodreads_plugin import LoadAnalyticsOperator

from operators.data_quality import DataQualityOperator

from operators.goodreads_analytics import LoadAnalyticsOperator

sys.path.append('../plugins/helpers/')

from helpers.analytics_queries import AnalyticsQueries

sys.path.append('../src')

# from goodreads_driver import main

#config = configparser.ConfigParser()
#config.read_file(open(f"{Path(__file__).parents[0]}/emr_config.cfg"))

def execute_command():
    python_file = r'c:\Users\thang\Desktop\Data_Engineer_Project\src\goodreads_driver.py'  # Đường dẫn đến file Python của bạn
    
    # Gọi lệnh python để chạy file Python
    subprocess.call(['python', '-u', python_file])


default_args = {
    'owner': 'goodreads',
    'depends_on_past': True,
    'start_date' : datetime(2024, 5, 15, 0, 0, 0, 0),
    'end_date' : datetime(2024, 5, 17, 0, 0, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=15),
    'catchup': True
}



dag_name = 'goodreads_pipeline'
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and Transform data from landing zone to processed zone. Populate data from Processed zone to goodreads Warehouse.',
          #schedule_interval=None,
          schedule_interval='@daily',
          max_active_runs = 10
        )

start_operator = EmptyOperator(task_id='Begin_execution',  dag=dag)

# emrsshHook= SSHHook(ssh_conn_id='emr_ssh_connection')

# jobOperator = SSHOperator(
#     task_id="GoodReadsETLJob",
#     command='cd /mnt/c/Users/thang/Desktop/Data_Engineer_Project/src; export PYSPARK_DRIVER_PYTHON=python3; export PYSPARK_PYTHON=python3; spark-submit --master yarn goodreads_driver.py;',
#     ssh_hook=emrsshHook,
#     dag=dag)


jobOperator = PythonOperator(
    task_id='GoodReadsETLJob',
    python_callable=execute_command,
    dag=dag
)

warehouse_data_quality_checks = DataQualityOperator(
    task_id='Warehouse_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = ["goodreads_warehouse.authors", "goodreads_warehouse.reviews", "goodreads_warehouse.books", "goodreads_warehouse.users"]

)

create_analytics_schema = LoadAnalyticsOperator(
    task_id='Create_analytics_schema',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticsQueries.create_schema],
    dag=dag
)

create_author_analytics_table = LoadAnalyticsOperator(
    task_id='Create_author_analytics_table',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticsQueries.create_author_reviews,AnalyticsQueries.create_author_rating, AnalyticsQueries.create_best_authors],
    dag=dag
)

create_book_analytics_table = LoadAnalyticsOperator(
    task_id='Create_book_analytics_table',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticsQueries.create_book_reviews,AnalyticsQueries.create_book_rating, AnalyticsQueries.create_best_books],
    dag=dag
)

# Authors Analytics Tasks

load_author_table_reviews = LoadAnalyticsOperator(
    task_id='Load_author_table_reviews',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticsQueries.populate_authors_reviews.format('2024-05-01 00:00:00.000000', '2024-05-31 00:00:00.000000')],
    dag=dag
)


load_author_table_ratings = LoadAnalyticsOperator(
    task_id='Load_author_table_ratings',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticsQueries.populate_authors_ratings.format('2024-05-01 00:00:00.000000', '2024-05-31 00:00:00.000000')],
    dag=dag
)

load_best_author = LoadAnalyticsOperator(
    task_id='Load_best_author',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticsQueries.populate_best_authors],
    dag=dag
)


# Book Analytics Tasks
load_book_table_reviews = LoadAnalyticsOperator(
    task_id='Load_book_table_reviews',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticsQueries.populate_books_reviews.format('2024-05-01 00:00:00.000000', '2024-05-31 00:00:00.000000')],
    dag=dag
)


load_book_table_ratings = LoadAnalyticsOperator(
    task_id='Load_book_table_ratings',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticsQueries.populate_books_ratings.format('2024-05-01 00:00:00.000000', '2024-05-31 00:00:00.000000')],
    dag=dag
)

load_best_book = LoadAnalyticsOperator(
    task_id='Load_best_books',
    redshift_conn_id = 'redshift',
    sql_query = [AnalyticsQueries.populate_best_books],
    dag=dag
)



authors_data_quality_checks = DataQualityOperator(
    task_id='Authors_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = ["goodreads_analytics.popular_authors_average_rating", "goodreads_analytics.popular_authors_average_rating"]

)

books_data_quality_checks = DataQualityOperator(
    task_id='Books_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = ["goodreads_analytics.popular_books_average_rating", "goodreads_analytics.popular_books_review_count"]

)


end_operator = EmptyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> jobOperator >> warehouse_data_quality_checks >> create_analytics_schema
create_analytics_schema >> [create_author_analytics_table, create_book_analytics_table]
create_author_analytics_table >> [load_author_table_reviews, load_author_table_ratings, load_best_author] >> authors_data_quality_checks
create_book_analytics_table >> [load_book_table_reviews, load_book_table_ratings, load_best_book] >> books_data_quality_checks
[authors_data_quality_checks, books_data_quality_checks] >> end_operator