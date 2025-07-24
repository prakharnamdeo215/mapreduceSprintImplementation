from pyspark.sql.functions import col, count
from dataLoad import cleanData

spark, athletes, noc = cleanData()

medalNotNullDf = athletes.filter(col("Medal").isNotNull())
TeamIndiaDf = medalNotNullDf.filter(col("Team") == "India")
eventGroupedDf = TeamIndiaDf.groupBy(col("Event")).agg(count("*").alias("Total_Medals"))
resultDf = eventGroupedDf.orderBy(col("Total_Medals").desc()).limit(1)

(resultDf.coalesce(1).write
.csv(r"C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query18", header=True, mode="overwrite"))