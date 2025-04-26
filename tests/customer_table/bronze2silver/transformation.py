from pyspark.sql import SparkSession

from gitdb.fun import delta_types

from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.master("local[1]") \
        .appName("pytest_framework") \
        .config("spark.jars", r"C:\Users\suket\PycharmProjects\pytest_project_suketh\jars\mssql-jdbc-12.2.0.jre8.jar") \
        .config("spark.driver.extraClassPath", r"C:\Users\suket\PycharmProjects\pytest_project_suketh\jars\mssql-jdbc-12.2.0.jre8.jar") \
        .config("spark.executor.extraClassPath", r"C:\Users\suket\PycharmProjects\pytest_project_suketh\jars\mssql-jdbc-12.2.0.jre8.jar") \
        .getOrCreate()

adls_account_name = "decautoadls"
adls_container_name = "test"
key = "vnH8MP/h4VB5vbfsP1x9rAZ5PiyMkIk5RBPnxCbrAjupr7GXMiCv0fHDuySVA036WYaKQDVXcMzz+AStHfeBKQ=="

# ADLS file path and credentials
adls_path = f"abfss://{adls_container_name}@{adls_account_name}.dfs.core.windows.net/raw/customer/"
spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)

# Azure SQL Server JDBC configuration
jdbc_url = "jdbc:sqlserver://decautoserver.database.windows.net:1433;database=decauto"
jdbc_properties = {
    "user": "decadmin",
    "password": "Dharmavaram1@",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

bronze_df = spark.read.jdbc(url=jdbc_url, table='customer_bronze', properties=jdbc_properties).drop('hash_key')

silver_df = spark.read.jdbc(url=jdbc_url, table='customers_silver_backup', properties=jdbc_properties)

columns = ['customer_id','name','email','phone','batchid','created_date','updated_date']
updates = bronze_df.join(silver_df.select("customer_id", "created_date","batchid"), on="customer_id", how="inner").drop(bronze_df.created_date,bronze_df.batchid)

silver_not_in_bronze = silver_df.join(bronze_df, on="customer_id", how="left_anti")

new_records = bronze_df.join(silver_df, on="customer_id", how="left_anti")

final_df = updates.select(*columns).union(new_records.select(*columns)).union(silver_not_in_bronze.select(*columns))

final_df.cache()
print("final df")
final_df.show()
final_df.write.jdbc(url=jdbc_url, table='customer_silver_expected', mode="overwrite", properties=jdbc_properties)