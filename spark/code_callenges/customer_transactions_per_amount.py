from pyspark.sql.functions import sum, avg, count
from spark.spark_session import init_spark_session


# Calculate 'total transaction amount', 'average transaction amount' and 'number of transactions'
# for each customer by customer_id column
def agg_transactions(customer_data_df):
    return (
        customer_data_df
        .groupBy("customer_id").agg(
            sum(customer_data_df.transaction_amount).alias("total_transaction_amount"),
            avg(customer_data_df.transaction_amount).alias("average_transaction_amount"),
            count(customer_data_df.transaction_amount).alias("transaction_amount_count")
        ).orderBy("customer_id", ascending=True)
    )


# Filter customers with more than 5 transactions
def customers_with_more_than_5_trn(agg_transactions_df):
    return agg_transactions_df.filter(agg_transactions_df.transaction_amount_count > 5)


class CustomerTransactionsPerAmount:
    def __init__(self):
        self.spark, self.sc = init_spark_session()

    def create_dataframe(self):
        # Sample data
        customer_data = [
            ("CUST001", "2023-01-01", 1500),
            ("CUST001", "2023-01-15", 2000),
            ("CUST001", "2023-02-01", 2500),
            ("CUST001", "2023-03-10", 1800),
            ("CUST001", "2023-04-05", 1700),
            ("CUST001", "2023-04-25", 1900),
            ("CUST002", "2023-01-02", 2200),
            ("CUST002", "2023-03-18", 1800),
            ("CUST003", "2023-02-10", 3000),
            ("CUST003", "2023-03-20", 1200),
            ("CUST004", "2023-01-01", 500)
        ]

        # Define schema for customer data
        customer_data_schema = ["customer_id", "transaction_date", "transaction_amount"]

        # Create dataframe
        return self.spark.createDataFrame(data=customer_data, schema=customer_data_schema)

    def process(self):
        customer_data_df = self.create_dataframe()
        # customer_data_df.show(truncate=False)

        agg_transactions_df = agg_transactions(customer_data_df)
        # agg_transactions_df.show(truncate=False)

        customers_with_more_than_5_trn_df = customers_with_more_than_5_trn(agg_transactions_df)
        customers_with_more_than_5_trn_df.show(truncate=False)

        self.spark.stop()
        self.sc.stop()
