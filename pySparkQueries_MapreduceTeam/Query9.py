
from dataLoad import cleanData
from pyspark.sql.functions import col, max as spark_max
spark, athletes, nocs = cleanData()

gold_df = (athletes.filter(col("Medal") == "Gold")
           .select("ID", "Name", "Age", "Medal", "Sport", "Event", "Team", "Year", "NOC"))
max_age = gold_df.agg(spark_max("Age").alias("max_age")).collect()[0]["max_age"]
oldest_gold_athletes = gold_df.filter(col("Age") == max_age)
oldest_gold_athletes.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv(r"C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query5")
