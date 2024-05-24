import psycopg2
import configparser
from pathlib import Path
from pyspark.sql import SparkSession
import os
import sys

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/warehouse.cfg"))

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("Read Data from Array") \
    .getOrCreate()

host = config.get('CLUSTER', 'HOST')
port = config.get('CLUSTER', 'DB_PORT')
database = config.get('CLUSTER', 'DB_NAME')
user = config.get('CLUSTER', 'DB_USER')
password = config.get('CLUSTER', 'DB_PASSWORD')

# Kết nối đến Redshift cluster
conn = psycopg2.connect(
    host=host,
    port=port,
    dbname=database,
    user=user,
    password=password
)

# Tạo một cursor để thực thi truy vấn
cur = conn.cursor()

# Thực hiện truy vấn SQL
cur.execute("""
            SELECT book.title, author.name, author.average_rating, book.average_rating, book.ratings_count
            FROM goodreads_warehouse.books as book, goodreads_warehouse.authors as author
            WHERE book.authors = author.author_id
                AND author.average_rating > 3.5
                AND book.average_rating > 3.5
            """)

# Lấy kết quả truy vấn
rows = cur.fetchall()

# Tạo DataFrame từ mảng
df = spark.createDataFrame(rows, ['title', 'author', 'average_rating_author', 'average_rating_book', 'ratings_count_book'])

# Hiển thị dữ liệu
df.show()

# Đóng kết nối
cur.close()
conn.close()