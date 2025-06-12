from pyspark.sql.functions import col, sum
from spark.spark_session import init_spark_session


def filter_high_sales_products(df, threshold=1000):
    return df.filter(col("total_sales") > threshold)


def compute_total_sales_per_product(df):
    return (
        df.withColumn("total_sales", col("quantity") * col("price_per_unit"))
        .groupBy("product_id", "product_name")
        .agg(sum("total_sales").alias("total_sales"))
    )


class SalesDataProcessor:
    def __init__(self):
        self.spark, self.sc = init_spark_session()

    def load_sales_data(self):
        # Sample data
        sales_data = [
            (1, "P001", "Laptop", 2, 600),
            (2, "P002", "Phone", 3, 300),
            (3, "P001", "Laptop", 1, 600),
            (4, "P003", "Tablet", 5, 150),
            (5, "P002", "Phone", 1, 300)
        ]

        # Schema
        schema = ["transaction_id", "product_id", "product_name", "quantity", "price_per_unit"]

        # Create DataFrame
        return self.spark.createDataFrame(data=sales_data, schema=schema)

    def process(self):
        sales_df = self.load_sales_data()
        sales_summary_df = compute_total_sales_per_product(sales_df)
        filtered_df = filter_high_sales_products(sales_summary_df)

        filtered_df.show(truncate=False)

        self.spark.stop()
        self.sc.stop()
