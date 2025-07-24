from dataLoad import cleanData
from pyspark.sql.functions import col, count
spark, athletes, nocs = cleanData()

gold_medalists = athletes.filter(col("Medal") == "Gold").select("ID", "Name")
gold_counts = gold_medalists.groupBy("ID", "Name").agg(count("*").alias("No_of_Golds"))
top_5 = gold_counts.orderBy(col("No_of_Golds").desc()).limit(5)
top_5.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv(r"C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query5")
