import os
import signal
import subprocess
import boto3
from src.sample import transform
from awsglue.context import GlueContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


SOURCE_NAME = "data.csv"
TABLE_NAME = "dummy"
S3_BUCKET_NAME = "data-s3"
ENDPOINT_URL = "http://127.0.0.1:5000"


def initialize_test(spark: SparkSession):
    """
    Function to setup and initialize test case execution

    Args:
        spark (SparkSession): PySpark session object

    Returns:
        process: Process object for the moto server that was started
    """
    process = subprocess.Popen(
        "moto_server s3 -p5000",
        stdout=subprocess.PIPE,
        shell=True,
        preexec_fn=os.setsid,
    )
    s3 = boto3.resource("s3", endpoint_url=ENDPOINT_URL)
    s3.create_bucket(Bucket=S3_BUCKET_NAME)

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", "dummy-value")
    hadoop_conf.set("fs.s3a.secret.key", "dummy-value")
    hadoop_conf.set("fs.s3a.endpoint", ENDPOINT_URL)
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    values = [
        ("sam", "1962-05-25"),
        ("let", "1999-05-21"),
        ("nick", "1996-04-03"),
    ]
    columns = ["name", "dt"]
    df = spark.createDataFrame(values, columns)
    df.write.parquet(f"s3://{S3_BUCKET_NAME}/{SOURCE_NAME}")
    return process


def compare_schema(schema_a: StructType, schema_b: StructType) -> bool:
    """
    Utility menthod to comapre two schema and return the results of comparison

    Args:
        schema_a (StructType): Schema for comparison
        schema_b (StructType): Schema for comparison

    Returns:
        bool: Result of schema comparison
    """
    return len(schema_a) == len(schema_b) and all(
        (a.name, a.dataType) == (b.name, b.dataType)
        for a, b in zip(schema_a, schema_b)
    )


# Test to verify data transformation
def test_transform(glueContext: GlueContext):
    """
    Test case to test the transform function

    Args:
        glueContext (GlueContext): Test Glue context object
    """
    spark = glueContext.spark_session
    input_data = spark.createDataFrame(
        [("sam", "1962-05-25"), ("let", "1999-05-21"), ("nick", "1996-04-03")],
        ["name", "dt"],
    )
    output_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("dt", StringType(), False),
            StructField("test", IntegerType(), False),
            StructField("data", IntegerType(), False),
            StructField("msg", IntegerType(), False),
        ]
    )
    real_output = transform(spark, input_data)
    assert compare_schema(real_output.schema, output_schema)


# Test to verify data present in valid partioned format
def test_process_data_record(glueContext: GlueContext):
    """
    Test case to test the process_data function for
    valid partitioned data output

    Args:
        glueContext (GlueContext): Test Glue context object
    """
    spark = glueContext.spark_session
    process = initialize_test(spark)

    from src.sample import process_data

    process_data(spark, SOURCE_NAME, TABLE_NAME)
    df = spark.read.parquet(
        f"s3a://{S3_BUCKET_NAME}/{TABLE_NAME}/test=1962/data=5/msg=25"
    )
    assert isinstance(df, DataFrame)

    os.killpg(os.getpgid(process.pid), signal.SIGTERM)


# Test to verify number of records
def test_process_data_record_count(glueContext: GlueContext):
    """
    Test case to test the process_data function for
    number of records in input and output

    Args:
        glueContext (GlueContext): Test Glue context object
    """
    spark = glueContext.spark_session
    process = initialize_test(spark)

    from src.sample import process_data

    process_data(spark, SOURCE_NAME, TABLE_NAME)

    df = spark.read.parquet(f"s3a://{S3_BUCKET_NAME}/{TABLE_NAME}")
    assert df.count() == 3

    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
