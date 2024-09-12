from pyspark.sql import SparkSession
import pyspark.sql.functions as pyf
import json

spark = SparkSession.builder.getOrCreate()

questions_df = spark.read.json("../../data/questions.json" )
minimal_questions = questions_df\
    .selectExpr("explode(items) as item")\
    .select("item.*")\
    .select(pyf.col("body").alias("question"), "question_id")


answers_df = spark.read.json("../../data/answers.json" )
minimal_answers = answers_df \
    .selectExpr("explode(items) as item")\
    .select("item.*") \
    .filter(pyf.col("is_accepted") == True) \
    .select(pyf.col("body").alias("answer"), "question_id")


joined_df = minimal_questions \
    .join(minimal_answers, on="question_id", how="inner")

joined_list = joined_df.collect()
for x in joined_list:
    json_object = json.dumps({"question": x.question, "answer": x.answer})    
    with open(f"../../jsons/{x.question_id}.json", "w") as outfile:
        outfile.write(json_object)
