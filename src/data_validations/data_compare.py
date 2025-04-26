from pyspark.sql.functions import lit, col, when
from src.utility.report_lib import write_output

def data_compare(source, target, key_column, num_records=5):
    columnList = source.columns
    smt = source.exceptAll(target).withColumn("datafrom", lit("source"))
    tms = target.exceptAll(source).withColumn("datafrom", lit("target"))
    failed = smt.union(tms)


    failed_count = failed.count()
    if failed_count > 0:
        failed_records = failed.limit(num_records).collect()  # Get the first 5 failing rows
        failed_preview = [row.asDict() for row in failed_records]
        write_output(
                "data compare Check",
                "FAIL",
                f"Data mismatch data: {failed_preview}"
            )
    else:
        write_output(
            "data compare Check",
            "PASS",
            f"No mismatches found"
        )


    if failed_count > 0:

        print("columnList", columnList)
        print("keycolumns", key_column)
        for column in columnList:
            print(column.lower())
            if column not in key_column:
                key_column.append(column)
                temp_source = source.select(key_column).withColumnRenamed(column, "source_" + column)

                temp_target = target.select(key_column).withColumnRenamed(column, "target_" + column)
                key_column.remove(column)
                temp_join = temp_source.join(temp_target, key_column, how='full_outer')
                temp_join.withColumn("comparison", when(col('source_' + column) == col("target_" + column),
                                                        "True").otherwise("False")).filter(
                    f"comparison == False ").show()

        status ='FAIL'

        return status
    else:
        status = 'PASS'
        return status



# def data_compare(source_df, target_df, key_columns, compare_columns, validation_name, metadata):
#     """
#     Compare source and target DataFrames row by row and identify mismatches.
#
#     Args:
#         source_df (DataFrame): Source DataFrame.
#         target_df (DataFrame): Target DataFrame.
#         key_columns (list): Columns used to match rows.
#         compare_columns (list): Columns to compare for mismatches.
#         validation_name (str): Name of the validation for reporting.
#         metadata (dict): Metadata for reporting (e.g., source, target details).
#
#     Returns:
#         bool: True if data matches, False otherwise.
#     """
#     # Outer join to compare rows by key_columns
#     joined_df = source_df.alias("source").join(
#         target_df.alias("target"),
#         on=key_columns,
#         how="outer"
#     )
#
#     # Compare specified columns and create mismatch indicators
#     mismatch_conditions = []
#     for column in compare_columns:
#         mismatch_column = f"mismatch_{column}"
#         joined_df = joined_df.withColumn(
#             mismatch_column,
#             when(
#                 col(f"source.{column}") != col(f"target.{column}"),
#                 lit("Mismatch")
#             ).otherwise(lit(None))
#         )
#         mismatch_conditions.append(col(mismatch_column).isNotNull())
#
#     # Filter rows with any mismatches
#     mismatched_rows = joined_df.filter(
#         mismatch_conditions[0] if len(mismatch_conditions) == 1 else (
#             mismatch_conditions[0] | mismatch_conditions[1]  # Combine conditions for multiple columns
#         )
#     )
#
#     mismatch_count = mismatched_rows.count()
#
#     # Prepare metadata for reporting
#     source_path = metadata.get("source", "Unknown Source")
#     target_path = metadata.get("target", "Unknown Target")
#     source_type = metadata.get("source_type", "Unknown Type")
#     target_type = metadata.get("target_type", "Unknown Type")
#
#     source_count = source_df.count()
#     target_count = target_df.count()
#
#     if mismatch_count > 0:
#         failed_records = mismatched_rows.limit(5).collect()
#         failed_preview = [row.asDict() for row in failed_records]
#         message = (
#             f"Source Type: {source_type}, Source: {source_path}, "
#             f"Target Type: {target_type}, Target: {target_path}, "
#             f"Source Count: {source_count}, Target Count: {target_count}, "
#             f"Mismatch Count: {mismatch_count}, Sample Mismatches: {failed_preview}"
#         )
#         write_output(validation_name, "FAIL", message)
#         return False
#     else:
#         message = (
#             f"Source Type: {source_type}, Source: {source_path}, "
#             f"Target Type: {target_type}, Target: {target_path}, "
#             f"Source Count: {source_count}, Target Count: {target_count}, "
#             "No mismatches found."
#         )
#         write_output(validation_name, "PASS", message)
#         return True