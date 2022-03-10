import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions


def transform(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Function to extract and transform dataframe columns with date
    to get day, month and year.

    Args:
        spark (SparkSession): PySpark session object
        df (DataFrame): Dataframe object containing the data before transform

    Returns:
        DataFrame: Dataframe object containing the data after transform
    """
    return (
        df.withColumn("test", year(col("dt")))
        .withColumn("data", month(col("dt")))
        .withColumn("msg", dayofmonth(col("dt")))
    )


def process_data(spark: SparkSession, source_path, table_name):
    """
    Function to read and process data from parquet file

    Args:
        spark (SparkSession): PySpark session object
        source_path (String): Data file path
        table_name (String): Output Table name
    """
    df = spark.read.parquet(f"s3://data-s3/{source_path}")

    df_transform = transform(spark, df)
    df_transform.write.mode("append").partitionBy(
        "test", "data", "msg"
    ).parquet(f"s3://data-s3/{table_name}")


if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "table_name", "source_path"]
    )
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    spark._jsc.haddopConfiguration().set(
        "fs.s3.useRequesterPaysHeader", "true"
    )

    process_data(spark, args["source_path"], args["table_name"])

    job.commit()
