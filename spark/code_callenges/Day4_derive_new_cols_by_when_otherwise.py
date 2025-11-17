"""
categorize each restaurant’s performance based on average delivery ratings and order volume.

The logic is as follows:

* If average_rating ≥ 4.5 and orders > 1000, label as "Excellent"
* If average_rating between 3.5 and 4.5, label as "Good"
* If average_rating < 3.5, label as "Needs Improvement"
* For any missing ratings, label as "Not Rated"
our task is to create a new derived column `Performance_Category` using the `when()` and `otherwise()` functions in PySpark.
"""
from pyspark.sql.functions import col, when
from spark.spark_session import init_spark_session


def performance_category(restaurant_data_df):

    return restaurant_data_df.withColumn(
        'performance_category',
        when((col("Avg_Rating") >= 4.5) & (col("Orders") > 1000), "Excellent",)
        .when((col("Avg_Rating") >= 3.5) & (col("Avg_Rating") < 4.5), "Good",)
        .when((col("Avg_Rating") < 3.5), "Need Improvement", )
        .otherwise("Not Rated")
    )


class PerformanceCategoryByRatings:
    def __init__(self):
        self.spark, self.sc = init_spark_session()

    def create_dataframe(self):
        # Sample data
        return self.spark.read.option("header", "true").option('inferSchema', 'true').csv(
            "C:\\Users\\bnare\\Documents\\professional\\Learnings\\Projects\\pyspark\\spark\\code_callenges\\data_files\\restaurant_data.csv")

    def process(self):
        restaurant_data_df = self.create_dataframe()
        restaurant_data_df.show(truncate=False)

        # Performance_Category function
        result_df = performance_category(restaurant_data_df)
        result_df.show(truncate=False)
