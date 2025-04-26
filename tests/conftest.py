from pyspark.sql import SparkSession
import pytest
import yaml
import os
import json
from pyspark.sql.types import StructType
from src.utility.general_utility import flatten
import subprocess


@pytest.fixture(scope='session')
# def spark_session(request):
#     # Ensure the correct path to the PostgreSQL JDBC driver for Windows
#     postgres_jar = r'C:\Users\suket\PycharmProjects\taf_dec\jars\postgresql-42.2.5.jar'  # Update to actual path
#     jar_path = postgres_jar
#     spark = SparkSession.builder.master("local[1]") \
#         .appName("pytest_framework") \
#         .config("spark.jars", jar_path) \
#         .config("spark.driver.extraClassPath", jar_path) \
#         .config("spark.executor.extraClassPath", jar_path) \
#         .getOrCreate()
#     return spark

def spark_session(request):
    taf_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    postgres_jar = taf_path+'\\jars\\postgresql-42.2.5.jar'
    azure_storage = taf_path+'\\jars\\azure-storage-8.6.6.jar'
    hadoop_azure = taf_path+'\\jars\\hadoop-azure-3.3.1.jar'
    sql_server = taf_path+'\\jars\\mssql-jdbc-12.2.0.jre8.jar'

    jar_path = postgres_jar + ',' + azure_storage + ',' + hadoop_azure + ',' + sql_server

    spark = SparkSession.builder.master("local[1]") \
        .appName("pytest_framework") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()
    cred = load_credentials('qa')['adls']
    adls_account_name = cred['adls_account_name']
    key = cred['key']
    spark.conf.set(f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net", "SharedKey")
    spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)
    return spark


@pytest.fixture(scope='module')
def read_config(request):
    # Use os.path.join for better cross-platform path handling
    config_path = os.path.join(request.node.fspath.dirname, 'config.yml')
    print("Config path:", config_path)
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    return config_data


def read_schema(dir_path):
    # Use os.path.join for better cross-platform path handling
    schema_path = os.path.join(dir_path, 'schema.json')
    with open(schema_path, 'r') as schema_file:
        schema = StructType.fromJson(json.load(schema_file))
    return schema


def read_query(dir_path):
    # Use os.path.join for better cross-platform path handling
    sql_query_path = os.path.join(dir_path, 'transformation.sql')
    with open(sql_query_path, "r") as file:
        sql_query = file.read()
    return sql_query


def read_file(config_data, spark, dir_path):
    df = None
    if config_data['type'] == 'csv':
        if config_data['schema'] == 'Y':
            schema = read_schema(dir_path)
            df = spark.read.schema(schema).csv(config_data['path'], header=config_data['options']['header'])
        else:
            df = spark.read.csv(config_data['path'], header=config_data['options']['header'], inferSchema=True)
    elif config_data['type'] == 'json':
        df = spark.read.json(config_data['path'], multiLine=config_data['options']['multiline'])
        df = flatten(df)
    elif config_data['type'] == 'parquet':
        df = spark.read.parquet(config_data['path'])
    elif config_data['type'] == 'avro':
        df = spark.read.format('avro').load(config_data['path'])
    elif config_data['type'] == 'txt':
        pass
    return df


def read_db(config_data, spark, dir_path):
    creds = load_credentials()
    cred_lookup = config_data['cred_lookup']
    creds = creds[cred_lookup]
    print("Credentials:", creds)

    if config_data['transformation'][0].lower() == 'y' and config_data['transformation'][1].lower() == 'sql':
        sql_query = read_query(dir_path)
        print("SQL Query:", sql_query)
        df = spark.read.format("jdbc"). \
            option("url", creds['url']). \
            option("user", creds['user']). \
            option("password", creds['password']). \
            option("query", sql_query). \
            option("driver", creds['driver']).load()
    else:
        df = spark.read.format("jdbc"). \
            option("url", creds['url']). \
            option("user", creds['user']). \
            option("password", creds['password']). \
            option("dbtable", config_data['table']). \
            option("driver", creds['driver']).load()

    return df


@pytest.fixture(scope='module')
def read_data(read_config, spark_session, request):
    spark = spark_session
    config_data = read_config
    source_config = config_data['source']
    target_config = config_data['target']
    dir_path = request.node.fspath.dirname

    # Handle reading the source data
    if source_config['type'] == 'database':
        if source_config['transformation'][1].lower() == 'python' and source_config['transformation'][0].lower() == 'y':
            python_file_path = dir_path + '\\transformation.py'
            print("python_file_path",python_file_path)
            subprocess.run(["python", python_file_path])
        source = read_db(config_data=source_config, spark=spark, dir_path=dir_path)
    else:
        source = read_file(config_data=source_config, spark=spark, dir_path=dir_path)

    # Handle reading the target data
    if target_config['type'] == 'database':
        target = read_db(config_data=target_config, spark=spark, dir_path=dir_path)
    else:
        target = read_file(config_data=target_config, spark=spark, dir_path=dir_path)

    print("Target Exclude Columns:", target_config['exclude_cols'])

    return source.drop(*source_config['exclude_cols']), target.drop(*target_config['exclude_cols'])


def load_credentials(env="qa"):
    """Load credentials from the centralized YAML file."""
    taf_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    credentials_path = os.path.join(taf_path, 'project_config', 'cred_config.yml')

    with open(credentials_path, "r") as file:
        credentials = yaml.safe_load(file)
        print("Credentials for environment:", credentials[env])
    return credentials[env]
