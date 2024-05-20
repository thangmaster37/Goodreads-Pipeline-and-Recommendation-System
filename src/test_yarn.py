import boto3
from pyspark.sql import SparkSession
import pandas as pd
import os
import goodreads_udf
import sys

from pyspark.sql import functions as fn

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

session = boto3.Session(
    aws_access_key_id='AKIA47CRXYUC3K2IZINA',
    aws_secret_access_key='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'
)

spark = SparkSession.builder \
           .config('spark.master', 'local') \
           .config('spark.app.name', 'goodreads') \
           .getOrCreate()

s3 = session.resource('s3')
obj = s3.Object('working-zone-goodreads', 'reviews.csv')

df_pandas = pd.read_csv(obj.get()['Body'])

reviews_df = spark.createDataFrame(df_pandas)

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



filename = 'processed_reviews.csv'
reviews_df = deduped_reviews_df.toPandas()
reviews_df.to_csv(filename, index=False)

# Upload file to S3
s3.Object('processed-zone-goodreads', filename).put(Body=open(filename, 'rb'))