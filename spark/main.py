from spark.code_callenges.customer_transactions_per_amount import CustomerTransactionsPerAmount
from spark.code_callenges.employees_latest_sales import EmployeesLatestSales
from spark.code_callenges.update_nulls_of_agg_sales_for_employees import UpdatedNullsByAggSalesForEmployee
from spark.code_callenges.sales_per_product import SalesDataProcessor


if __name__ == "__main__":
    sales_data_processor = SalesDataProcessor()
    sales_data_processor.process()

    customer_Transactions_processor = CustomerTransactionsPerAmount()
    customer_Transactions_processor.process()

    employees_sales_data_analysis = UpdatedNullsByAggSalesForEmployee()
    employees_sales_data_analysis.process()

    employees_latest_sales_data = EmployeesLatestSales()
    employees_latest_sales_data.process()
