"""
𝐒𝐜𝐞𝐧𝐚𝐫𝐢𝐨:
You’re working as a Data Engineer at Flipkart India, responsible for building data pipelines that generate daily customer insights.
The marketing team wants to identify high-value active customers** to target with personalized offers.
You receive a large customer transactions dataset stored in a Delta Lake table. Your task is to filter customers based on multiple business conditions — a very common real-time PySpark interview and production use case.

𝐏𝐫𝐨𝐛𝐥𝐞𝐦 𝐒𝐭𝐚𝐭𝐞𝐦𝐞𝐧𝐭 :
Filter all customers who meet the following criteria:
1. Belong to North or West Indian regions.
2. Have a purchase amount between ₹10,000 and ₹50,000 .
3. Have an order status other than ‘Cancelled’ .
4. Have a valid Gender (M or F).
"""
from pyspark.sql.functions import col
from spark.spark_session import init_spark_session


def filter_customers_on_business_conditions(customer_df):
    # Apply all the conditions mentioned
    return customer_df.filter(
        (col("Region").isin("North", "West")) &
        (col("Purchase_Amount").between(10000, 50000)) &
        (col("Order_Status") != "Cancelled") &
        (col("Gender").isin("M", "F"))
    )


class FilterCustomersBaseOnBusinessConditions:
    def __init__(self):
        self.spark, self.sc = init_spark_session()

    def create_dataframe(self):
        # Sample data
        customers_data = [
            (101, 'Amit Sharma', 'North', 'M', 25000, 'Completed'),
            (102, 'Priya Nair', 'West', 'F', 12000, 'Completed'),
            (103, 'Rohit Verma', 'East', 'M', 18000, 'Cancelled'),
            (104, 'Anjali Gupta', 'South', 'F', 52000, 'Completed'),
            (105, 'Ravi Kumar', 'North', None, 40000, 'Completed'),
            (106, 'Neha Singh', 'West', 'F', 15000, 'Cancelled'),
            (107, 'Kiran Patel', 'North', 'M', 47000, 'Completed'),
            (108, 'Meena Das', 'West', 'F', 30000, 'Completed'),
            (109, 'Suresh Reddy', 'South', 'M', 20000, 'Completed')
        ]

        # Schema for customer data
        customers_data_schema = ['Customer_ID', 'Customer_Name', 'Region', 'Gender', 'Purchase_Amount', 'Order_Status']

        # Create customer dataframe
        return self.spark.createDataFrame(customers_data, customers_data_schema)

    def process(self):
        # customer dataframe
        customer_df = self.create_dataframe()
        customer_df.show(truncate=False)

        # Result dataframe
        result_df = filter_customers_on_business_conditions(customer_df)
        result_df.show(truncate=False)
