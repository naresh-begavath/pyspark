from spark.spark_session import init_spark_session


class SampleDataProcessor:
    def __init__(self):
        # Initialize Spark session and context
        self.spark, self.sc = init_spark_session()

    def create_sample_dataframe(self):
        # Sample data
        sample_data = [
            (1, None, 3),
            (None, 2, None),
            (4, None, 6)
        ]

        # Define schema
        schema = ["a", "b", "c"]

        # Create DataFrame
        df = self.spark.createDataFrame(data=sample_data, schema=schema)
        return df

    def process(self):
        df = self.create_sample_dataframe()
        df.show(truncate=False)


if __name__ == '__main__':
    processor = SampleDataProcessor()
    processor.process()
