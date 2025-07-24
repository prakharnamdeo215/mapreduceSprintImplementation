from pyspark.sql.functions import col, when
from dataLoad import cleanData

spark, athletes, nocs = cleanData()

silver_bronze_df = athletes.filter((col("Medal") == "Silver") | (col("Medal") == "Bronze")) \
    .select("NOC").distinct()

gold_df = athletes.filter(col("Medal") == "Gold") \
    .select("NOC").distinct()

only_silver_bronze = silver_bronze_df.subtract(gold_df)

result = only_silver_bronze.join(nocs, on="NOC", how="left") \
    .select("NOC", "region").orderBy("region")

(result.coalesce(1).write
 .csv(r"C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query18", header=True, mode="overwrite"))