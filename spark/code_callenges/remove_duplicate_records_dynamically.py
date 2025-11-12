"""
Your task is to identify and remove duplicate records dynamically using Window functions in PySpark
"""
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col
from spark.spark_session import init_spark_session


def remove_duplicate_records(sales_df):
    # Define window function
    windowSpec = Window.partitionBy("Customer_ID", "Transaction_Date", "Product", "Amount").orderBy("Updated_Timestamp")

    # Assign row-number
    ranked_df = sales_df.withColumn("row_number", row_number().over(windowSpec))

    # Filter only the latest records
    cleaned_df = ranked_df.filter(col("row_number") == 1).drop("row_number")

    return cleaned_df


class RemoveDuplicateRecordsDynamically:
    def __init__(self):
        self.spark, self.sc = init_spark_session()

    def create_dataframe(self):
        # Sample data
        sales_data = [
            (101, '2025-01-01', 'Mobile', 15000, '2025-01-02 10:00:00'),
            (101, '2025-01-01', 'Mobile', 15000, '2025-01-02 12:00:00'),
            (102, '2025-01-03', 'Laptop', 60000, '2025-01-03 09:00:00'),
            (103, '2025-01-04', 'Headphones', 2000, '2025-01-04 11:00:00'),
            (103, '2025-01-04', 'Headphones', 2000, '2025-01-04 15:00:00'),
            (104, '2025-01-05', 'TV', 40000, '2025-01-05 13:00:00')
        ]

        # Sales data's schema
        sales_data_schema = ['Customer_ID', 'Transaction_Date', 'Product', 'Amount', 'Updated_Timestamp']

        # Create dataframe using sample data and schema
        return self.spark.createDataFrame(sales_data, sales_data_schema)

    def process(self):
        # sales dataframe
        sales_df = self.create_dataframe()
        sales_df.show(truncate=False)

        result_df = remove_duplicate_records(sales_df)
        result_df.show(truncate=False)

        self.spark.stop()