import argparse
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as pyf

from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

def clean(spark: SparkSession, environment: str, tag: str):
    
    questions_df = spark.read.json("data/questions.json" )
    minimal_questions = questions_df\
        .selectExpr("explode(items) as item")\
        .select("item.*")\
        .select(pyf.col("body").alias("question"), "question_id")


    answers_df = spark.read.json("data/answers.json" )
    minimal_answers = answers_df \
        .selectExpr("explode(items) as item")\
        .select("item.*") \
        .filter(pyf.col("is_accepted") == True) \
        .select(pyf.col("body").alias("answer"), "question_id")


    joined_df = minimal_questions \
        .join(minimal_answers, on="question_id", how="inner")

    count = joined_df.count()
    joined_df.repartition(count).write.mode("overwrite").json(
        "s3a://dataminded-academy-capstone-llm-data-us/cleaned/{tag}/"
    )

def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=True
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .getOrCreate()
        )
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
