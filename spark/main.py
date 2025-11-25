from spark.code_callenges.Day3_filter_customers_based_on_multple_business_conditions import \
    FilterCustomersBaseOnBusinessConditions
from spark.code_callenges.Day4_derive_new_cols_by_when_otherwise import PerformanceCategoryByRatings
from spark.code_callenges.Day5_𝐞𝐱𝐩𝐥𝐨𝐝𝐞_𝐩𝐨𝐬𝐞𝐱𝐩𝐥𝐨𝐝𝐞 import ExplodeAndPosExplode
from spark.code_callenges.customer_transactions_per_amount import CustomerTransactionsPerAmount
from spark.code_callenges.Day1_dynamically_filling_missing_values import FillMissingValuesDynamically
from spark.code_callenges.employees_latest_sales import EmployeesLatestSales
from spark.code_callenges.Day2_remove_duplicate_records_dynamically import RemoveDuplicateRecordsDynamically
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

    # Fill missing values dynamically
    fill_missing_vals_dynamically = FillMissingValuesDynamically()
    fill_missing_vals_dynamically.process()

    # Removing duplicates dynamically
    removed_duplicates_dynamically = RemoveDuplicateRecordsDynamically()
    removed_duplicates_dynamically.process()

    # Filter customers based on business conditions
    filter_customers_on_business_conditions = FilterCustomersBaseOnBusinessConditions()
    filter_customers_on_business_conditions.process()

    # Rating the restaurants
    performance_category_by_ratings = PerformanceCategoryByRatings()
    performance_category_by_ratings.process()

    # Explode and position explode data
    explode_and_pos_explode = ExplodeAndPosExplode()
    explode_and_pos_explode.process()

