from spark.code_callenges.customer_transactions_per_amount import CustomerTransactionsPerAmount
from spark.code_callenges.sales_per_product import SalesDataProcessor

if __name__ == "__main__":
    sales_data_processor = SalesDataProcessor()
    sales_data_processor.process()

    customer_Transactions_processor = CustomerTransactionsPerAmount()
    customer_Transactions_processor.process()
