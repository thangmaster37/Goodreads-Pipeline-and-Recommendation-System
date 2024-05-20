import subprocess

python_file = r'c:\Users\thang\Desktop\Data_Engineer_Project\src\goodreads_driver.py'  # Đường dẫn đến file Python của bạn
    
# Gọi lệnh python để chạy file Python
subprocess.call(['python', '-u', python_file])



# import configparser
# import logging
# from pathlib import Path

# logger = logging.getLogger(__name__)

# config = configparser.ConfigParser()
# config.read_file(open('src/warehouse/warehouse_config.cfg'))
# print("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

#Connect to the cluster
# from sqlalchemy import create_engine
# from sqlalchemy import text
# import psycopg2



# conn = psycopg2.connect(
#     host="goodreads.891377140997.us-east-1.redshift-serverless.amazonaws.com",
#     port="5439",
#     database="sample_data_dev",
#     user="admin",
#     password="Thang2003"
# )

# # Create a cursor to execute SQL statements
# cursor = conn.cursor()

# # Query a table using the Cursor
# cursor.execute("select * from tickit.category")
                
# #Retrieve the query result set
# result: tuple = cursor.fetchall()
# print(result)
                

