"""
You are working as a Data Engineer in a company that processes customer data across multiple Indian regions.
The organization uses Apache Spark to handle millions of records daily.
Your task is to clean and prepare customer data by dynamically filling missing values based on each column’s data type.
"""
from pyspark.sql.functions import col, when, desc, lit
from pyspark.sql.types import NumericType, StringType
from spark.spark_session import init_spark_session


def fill_missing_vals_dynamically(customer_df):
    # Identify the columns based on type
    numeric_columns = [field.name for field in customer_df.schema.fields if isinstance(field.dataType, NumericType)]
    string_columns = [field.name for field in customer_df.schema.fields if isinstance(field.dataType, StringType)]

    # Fill numeric columns with median
    for column_name in numeric_columns:
        median_val = customer_df.approxQuantile(column_name, [0.5], 0.0)
        median_val = median_val[0] if median_val else 0
        customer_df = customer_df.withColumn(column_name,
                                             when(col(column_name).isNull(), median_val).otherwise(col(column_name)))

    # Fill other columns(categorical) columns
    categorical_columns = ["Region", "Gender"]
    for column_name in categorical_columns:
        mode_val = customer_df.groupBy(column_name).count().orderBy(desc("count")).first()[0]
        customer_df = customer_df.withColumn(column_name,
                                             when(col(column_name).isNull(), mode_val).otherwise(col(column_name)))

    # Fill string/date columns with "Unknown"
    for column_name in ["Last_Visit"]:
        customer_df = customer_df.withColumn(column_name, when(col(column_name).isNull(), lit("Unknown")).otherwise(
            col(column_name)))

    return customer_df


class FillMissingValuesDynamically:
    def __init__(self):
        self.spark, self.sc = init_spark_session()

    def create_dataframe(self):
        customer_data = [
            (1, 25, 'North', 'M', '2025-01-01', 150),
            (2, None, 'East', None, '2025-01-02', None),
            (3, 30, 'South', 'F', None, 200),
            (4, 22, None, 'M', '2025-01-03', 180),
            (5, 28, 'West', 'F', None, None)
        ]

        customer_schema = ['Customer_ID', 'Age', 'Region', 'Gender', 'Last_Visit', 'Purchase_Amount']

        return self.spark.createDataFrame(customer_data, customer_schema)

    def process(self):
        customer_df = self.create_dataframe()
        customer_df.show(truncate=False)

        result_df = fill_missing_vals_dynamically(customer_df)
        result_df.show(truncate=False)

        self.spark.stop()
