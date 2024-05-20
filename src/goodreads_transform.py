from pyspark.sql.types import StringType
from pyspark.sql import functions as fn
import goodreads_udf
import logging
import configparser
from pathlib import Path
import os
import sys
import boto3
import pandas as pd

logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

class GoodreadsTransform:
    """
    This class performs transformation operations on the dataset.
    1. Transform timestamp format, clean text part, remove extra spaces etc.
    2. Create a lookup dataframe which contains the id and the timestamp for the latest record.
    3. Join this lookup data frame with original dataframe to get only the latest records from the dataset.
    4. Save the dataset by repartitioning. Using gzip compression
    """

    def __init__(self, spark):
        self._spark = spark
        self._load_path = config.get('BUCKET', 'WORKING_ZONE')
        self._save_path = config.get('BUCKET', 'PROCESSED_ZONE')
        self._session = boto3.Session(
                            aws_access_key_id='AWS-KEY',
                            aws_secret_access_key='AWS-SECRET-KEY'
                        )

    def transform_author_dataset(self):

        logging.debug("Inside transform author dataset module")

        s3 = self._session.resource('s3')
        obj = s3.Object(self._load_path, 'author.csv')

        df_pandas = pd.read_csv(obj.get()['Body'])

        author_df = self._spark.createDataFrame(df_pandas)

        author_lookup_df = author_df.groupby('author_id')\
                            .agg(fn.max('record_create_timestamp').alias('record_create_timestamp'))
        author_lookup_df.persist()
        fn.broadcast(author_lookup_df)

        deduped_author_df = author_df\
                            .join(author_lookup_df, ['author_id', 'record_create_timestamp'], how='inner')\
                            .select(author_df.columns) \
                            .withColumn('name', goodreads_udf.remove_extra_spaces('name'))
        
        logging.debug(f"Attempting to write data to {self._save_path + '/author.csv'}")

        filename = 'processed_author.csv'
        author_df = deduped_author_df.toPandas()
        author_df.to_csv(filename, index=False)

        # Upload file to S3
        s3.Object(self._save_path, filename).put(Body=open(filename, 'rb'))
    

    def transform_reviews_dataset(self):
        logging.debug("Inside transform reviews dataset module")

        s3 = self._session.resource('s3')
        obj = s3.Object(self._load_path, 'reviews.csv')

        df_pandas = pd.read_csv(obj.get()['Body'])

        reviews_df = self._spark.createDataFrame(df_pandas)

        reviews_lookup_df = reviews_df\
                            .groupby('review_id')\
                            .agg(fn.max('record_create_timestamp').alias('record_create_timestamp'))

        reviews_lookup_df.persist()
        fn.broadcast(reviews_lookup_df)

        deduped_reviews_df = reviews_df \
                             .join(reviews_lookup_df, ['review_id', 'record_create_timestamp'], how='inner')\
                             .select(reviews_df.columns)

        deduped_reviews_df = deduped_reviews_df \
            .withColumn('review_added_date', goodreads_udf.stringtodatetime('review_added_date')) \
            .withColumn('review_updated_date', goodreads_udf.stringtodatetime('review_updated_date'))


        logging.debug(f"Attempting to write data to {self._save_path + '/reviews.csv'}")

        filename = 'processed_reviews.csv'
        reviews_df = deduped_reviews_df.toPandas()
        reviews_df.to_csv(filename, index=False)

        # Upload file to S3
        s3.Object(self._save_path, filename).put(Body=open(filename, 'rb'))

    def transform_books_dataset(self):
        logging.debug("Inside transform books dataset module")
        
        s3 = self._session.resource('s3')
        obj = s3.Object(self._load_path, 'book.csv')

        df_pandas = pd.read_csv(obj.get()['Body'])
        
        books_df = self._spark.createDataFrame(df_pandas)

        books_lookup_df = books_df\
                            .groupby('book_id')\
                            .agg(fn.max('record_create_timestamp').alias('record_create_timestamp'))
        books_lookup_df.persist()
        fn.broadcast(books_lookup_df)

        deduped_books_df = books_df\
                           .join(books_lookup_df, ['book_id', 'record_create_timestamp'], how='inner')\
                           .select(books_df.columns)

        logging.debug(f"Attempting to write data to {self._save_path + '/book.csv'}")

        filename = 'processed_book.csv'
        book_df = deduped_books_df.toPandas()
        book_df.to_csv(filename, index=False)

        # Upload file to S3
        s3.Object(self._save_path, filename).put(Body=open(filename, 'rb'))


    def tranform_users_dataset(self):
        logging.debug("Inside transform users dataset module")
        
        s3 = self._session.resource('s3')
        obj = s3.Object(self._load_path, 'user.csv')

        df_pandas = pd.read_csv(obj.get()['Body'])
        
        users_df = self._spark.createDataFrame(df_pandas)

        users_lookup_df = users_df\
                          .groupby('user_id')\
                           .agg(fn.max('record_create_timestamp').alias('record_create_timestamp'))

        users_lookup_df.persist()
        fn.broadcast(users_lookup_df)

        deduped_users_df = users_df\
                           .join(users_lookup_df, ['user_id', 'record_create_timestamp'], how='inner')\
                           .select(users_df.columns)

        logging.debug(f"Attempting to write data to {self._save_path + '/user.csv'}")

        filename = 'processed_user.csv'
        user_df = deduped_users_df.toPandas()
        user_df.to_csv(filename, index=False)

        # Upload file to S3
        s3.Object(self._save_path, filename).put(Body=open(filename, 'rb'))