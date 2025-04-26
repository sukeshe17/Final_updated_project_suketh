from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
import yaml



def load_credentials(env="qa"):
    """Load credentials from the centralized YAML file."""

    credentials_path = '\\Users\\suket\\PycharmProjects\\taf_dec\\project_config\\cred_config.yml'

    with open(credentials_path, "r") as file:
        credentials = yaml.safe_load(file)
        print(credentials[env])
    return credentials[env]


postgres_jar = r'C:\Users\suket\PycharmProjects\taf_dec\jars\postgresql-42.2.5.jar'

jar_path = postgres_jar
spark = SparkSession.builder.master("local[1]") \
        .appName("pytest_framework") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()


creds = load_credentials()
creds = creds['postgres']

source1 = spark.read.format("jdbc"). \
    option("url", creds['url']). \
    option("user", creds['user']). \
    option("password", creds['password']). \
    option("query", "select id, first_name from employees"). \
    option("driver", creds['driver']).load()


source1 = source1.withColumn('source_id', lit('postgres'))

source1.show()
source2 = spark.read.csv("\\Users\\suket\\PycharmProjects\\taf_dec\\input_files\\customers.csv", header=True, inferSchema=True)

source2 = source2.select("id", 'first_name').withColumn('source_id', lit('file'))
source2.show()
source = source1.unionAll(source2)
source.show()

(source.write.mode("overwrite")
    .format("jdbc")
    .option("url", creds['url'])
    .option("user", creds['user'])
    .option("password", creds['password'])
    .option("dbtable", "employees_expected")
    .option("driver", creds['driver'])
    .save())

