"""
The analytics team wants to analyze customer orders, where each order can contain multiple food items.
"""
from pyspark.sql.functions import explode, col, posexplode
from spark.spark_session import init_spark_session


def explode_data(food_orders_df):
    return (
        food_orders_df
        .withColumn("Food_Item", explode(col("Food_Items")))
        .drop("Food_Items")
    )


def pos_explode_data(food_orders_df):
    return (
        food_orders_df
        .select(
            "Order_ID",
            "Customer_Name",
            posexplode(col("Food_Items")).alias("Item_Position", "Food_Item")
        )
    )


class ExplodeAndPosExplode:
    def __init__(self):
        self.spark, self.sc = init_spark_session()

    def create_dataframe(self):
        # sample data
        food_order_data = [
            (101, "Rahul", ["Pizza", "Burger", "Coke"]),
            (102, "Priya", ["Biryani", "Gulab Jamun"]),
            (103, "Aman", ["Sandwich"]),
            (104, "Neha", ["Pasta", "Coffee", "Cake"])
        ]

        # sample schema
        food_order_data_schema = ["Order_ID", "Customer_Name", "Food_Items"]

        return self.spark.createDataFrame(food_order_data, food_order_data_schema)

    def process(self):
        # Sample data dataframe
        food_orders_df = self.create_dataframe()
        food_orders_df.show(truncate=False)

        exploded_data_df = explode_data(food_orders_df)
        exploded_data_df.show(truncate=False)

        pos_explode_df = pos_explode_data(food_orders_df)
        pos_explode_df.show(truncate=False)
