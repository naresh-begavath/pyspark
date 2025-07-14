from spark.spark_session import init_spark_session
from pyspark.sql.functions import sum, col, when


def get_agg_sales(df):
    return (
        df.groupBy("employee_id").agg(sum(df.sales_amount).alias("agg_total_sales"))
    )


def get_updated_sales(employee_df, agg_sales_df):
    return (
        employee_df
        .alias('e')
        .join(agg_sales_df.alias('s'), on='employee_id', how='left')
        .withColumn(
            'total_sales',
            when(col('e.total_sales') == 0, col('s.agg_total_sales')).otherwise(col('e.total_sales'))
        )
        .select('employee_id', 'total_sales')
    )


class UpdatedNullsByAggSalesForEmployee:
    def __init__(self):
        self.spark, self.sc = init_spark_session()

    def load_sales_data(self):
        # Employees data
        employees = [
            (101, "Jack Smith", 0),
            (102, "Jams Smith", 1500),
            (103, "Jack Boone", 0)
        ]

        # Employees schema
        employees_schema = ['employee_id', 'employees_name', 'total_sales']

        # Create employees dataframe
        employees_df = self.spark.createDataFrame(employees, employees_schema)

        # Sales data
        sales = [
            (101, 500),
            (101, 1000),
            (103, 200)
        ]

        # Sales schema
        sales_schema = ['employee_id', 'sales_amount']

        # Create sales dataframe
        sales_df = self.spark.createDataFrame(sales, sales_schema)

        return employees_df, sales_df

    def process(self):
        employees_df, sales_df = self.load_sales_data()
        agg_sales_amount_df = get_agg_sales(sales_df)
        updated_sales_df = get_updated_sales(employees_df, agg_sales_amount_df)

        # Display results
        updated_sales_df.show(truncate=False)

        # Stop the spark and sc
        self.spark.stop()
        self.sc.stop()
