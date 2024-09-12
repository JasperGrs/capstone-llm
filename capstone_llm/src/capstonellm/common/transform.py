from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, LongType, StructType, StringType, IntegerType, BooleanType

spark = SparkSession.builder.getOrCreate()

questions_df = spark.read.option().json("data/questions.json" )
exploded_questions_df = questions_df.selectExpr("explode(items) as item").select("item.*")
exploded_questions_df.show(truncate=False)



answers_df = spark.read.option.json("data/answers.json" )
exploded_answers_df = answers_df.selectExpr("explode(items) as item").select("item.*")
exploded_answers_df.show(truncate=False)