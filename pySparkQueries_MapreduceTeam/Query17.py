from pyspark.sql import Window
from pyspark.sql.functions import col, count, desc, row_number

from dataLoad import cleanData

spark, athletes, noc = cleanData()

medalsDf = athletes.filter(col("Medal").isNotNull())

medalsCountDf = medalsDf.groupby("Games","Team","Medal").agg(count("*").alias("Medal_Count")).persist()

# print(medalsCountDf.show(10))

goldDf = medalsCountDf.filter(col("Medal") == "Gold")

silverDf = medalsCountDf.filter(col("Medal") == "Silver")

bronzeDf = medalsCountDf.filter(col("Medal") == "Bronze")


window = Window.partitionBy("Games").orderBy(desc("Medal_Count"))

top_gold = goldDf.withColumn("Rank",row_number().over(window)) \
    .filter(col("Rank") == 1) \
    .select("Games",col("Team").alias("Gold_Team"),col("Medal_Count").alias("Gold_Medals"))

top_silver = silverDf.withColumn("Rank",row_number().over(window)) \
    .filter(col("Rank") == 1) \
    .select("Games",col("Team").alias("Silver_Team"),col("Medal_Count").alias("Silver_Medals"))

top_bronze = bronzeDf.withColumn("Rank",row_number().over(window)) \
    .filter(col("Rank") == 1) \
    .select("Games",col("Team").alias("Bronze_Team"),col("Medal_Count").alias("Bronze_Medals"))

# print(top_gold.show(10))


totalCountDf = medalsDf.groupby("Games","Team").agg(count("*").alias("Total_Medals")).persist()

Total_window = Window.partitionBy("Games").orderBy(desc("Total_Medals"))

top_total = totalCountDf.withColumn("Rank",row_number().over(Total_window)) \
    .filter(col("Rank") == 1) \
    .select("Games",col("Team").alias("Most_Total_Team"),col("Total_Medals"))

finalResultDf = top_gold.join(top_silver, on="Games").join(top_bronze,on="Games").join(top_total,on="Games").orderBy("Games")
finalResultDf.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv(r"C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query17")