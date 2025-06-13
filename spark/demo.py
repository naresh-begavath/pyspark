from spark.spark_session import init_spark_session


def main():
    # import spark, sc
    spark, sc = init_spark_session()

    # Sample data
    sample_data = [
        (1, None, 3),
        (None, 2, None),
        (4, None, 6)
    ]

    # Sample data's schema
    sample_data_schema = ["a", "b", "c"]

    df = spark.createDataFrame(data=sample_data, schema=sample_data_schema)
    df.show(truncate=False)


if __name__ == '__main__':
    main()
