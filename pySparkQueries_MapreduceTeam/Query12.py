from pyspark.sql.functions import col, count
from dataLoad import cleanData

spark, athletes, noc = cleanData()

winners = athletes.filter(col("Medal").isNotNull())
top_athletes = winners.groupby("Name") \
    .agg(count("Medal").alias("Total Medals")) \
    .orderBy(col("Total Medals").desc()) \
    .limit(5)
(top_athletes.coalesce(1).write.mode("overwrite").option("header", True)
 .csv(r"C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query12"))