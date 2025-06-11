import os
import sys

from pyspark.sql import SparkSession


def init_spark_session():

    # Enabling the Python environment
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # Creating the Spark Context and Spark Session variable
    spark = SparkSession.builder.appName("PySpark Session").getOrCreate()
    sc = spark.sparkContext
    return spark, sc
