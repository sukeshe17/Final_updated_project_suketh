from src.utility.report_lib import write_output
from pyspark.sql import SparkSession
from src.data_validations.duplicate_validation import duplicate_check

# spark = SparkSession.builder.master("local[1]").appName('test').getOrCreate()

def uniqueness_check(df, unique_cols):
    """Validate that specified columns have unique values."""
    duplicate_counts = {}  #  {'identifier':0 , 'surname':0,'last_name':3}
    for column in unique_cols:
        count_duplicates = df.groupBy(column).count().filter("count > 1").count()
        print("count_duplicates", column, count_duplicates)
        duplicate_counts[column] = count_duplicates

    print("duplicate_counts", duplicate_counts)

    status = "PASS" if all(count == 0 for count in duplicate_counts.values()) else "FAIL"
    write_output(
        "Uniqueness Check",
        status,
        f"Duplicate counts per column: {duplicate_counts}"
    )
    return status

# df = spark.read.csv('/Users/admin/PycharmProjects/taf_dec/input_files/Contact_info_t.csv', header=True, inferSchema=True)
# print(uniqueness_check(df=df, unique_cols=['Identifier','Surname','given_name']))
# print(duplicate_check(df=df, key_col=['Identifier','Surname','given_name']))