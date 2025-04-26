# from pyspark.sql import SparkSession
#
# # Path to PostgreSQL JDBC driver
# jar = r'C:\Users\suket\PycharmProjects\taf_dec\jars\postgresql-42.2.5.jar'
#
# # Create a Spark session
# spark = SparkSession.builder.master("local[2]") \
#     .appName("test") \
#     .config("spark.jars", jar) \
#     .config("spark.driver.extraClassPath", jar) \
#     .config("spark.executor.extraClassPath", jar) \
#     .getOrCreate()
#
# # Load data from PostgreSQL into DataFrame
# df2 = spark.read.format("jdbc"). \
#     option("url", "jdbc:postgresql://localhost:5432/postgres"). \
#     option("user", "postgres"). \
#     option("password", "1234"). \
#     option("dbtable", "employees"). \
#     option("driver", "org.postgresql.Driver").load()
#
# # Show the first 20 rows (or adjust as needed)
# df2.show(50)  # You can change 50 to another number if you want more rows


import pyspark
print(pyspark.__version__)

import yaml
print(yaml.__version__)