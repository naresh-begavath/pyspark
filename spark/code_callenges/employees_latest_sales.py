from pyspark.sql import Window
from pyspark.sql.functions import row_number, col
from spark.spark_session import init_spark_session


def window_condition(df):
    return (
        Window.partitionBy('employee_id').orderBy(df['sales_date'].desc())
    )


def get_latest_sales(df, row_number_window_condition):
    employee_by_ids_df = df.withColumn(
        'rn',
        row_number().over(row_number_window_condition)
    )

    filter_df = employee_by_ids_df.filter(col('rn') == 1)

    result_df = (
        filter_df
        .select('employee_id', 'sales_id', 'sales_date', 'sales_amount')
        .orderBy(col('sales_amount').desc())
    )

    return result_df


class EmployeesLatestSales:
    def __init__(self):
        self.spark, self.sc = init_spark_session()

    def load_sales_data(self):
        # Employees sales data
        employees_sales_data = [
            (101, 1, '2024-07-01', 500.00),
            (101, 2, '2024-07-05', 750.00),
            (102, 3, '2024-07-02', 300.00),
            (102, 4, '2024-07-06', 200.00),
            (103, 5, '2024-07-01', 1000.00),
            (103, 6, '2024-07-10', 500.00),
            (104, 7, '2024-07-03', 400.00),
            (105, 8, '2024-07-07', 600.00),
            (105, 9, '2024-07-09', 300.00),
            (106, 10, '2024-07-04', 700.00)
        ]

        # Employees sales data schema
        employees_sales_data_schema = ['employee_id', 'sales_id', 'sales_date', 'sales_amount']

        # Create dataframe
        employees_df = self.spark.createDataFrame(employees_sales_data, employees_sales_data_schema)

        return employees_df

    def process(self):
        df = self.load_sales_data()
        row_number_window_condition = window_condition(df)
        result_df = get_latest_sales(df, row_number_window_condition)

        # Display results
        # print("Latest sales data for employees!")
        result_df.show(truncate=False)
