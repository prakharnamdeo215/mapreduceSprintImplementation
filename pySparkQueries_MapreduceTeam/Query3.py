from dataLoad import cleanData
from pyspark.sql.functions import countDistinct

spark, athletes, noc = cleanData()

total_no_of_nations_df = (athletes.select("Games", "NOC").distinct().groupBy("Games")
                          .agg(countDistinct("NOC").alias("Total_Nations")).orderBy("Games"))
(total_no_of_nations_df.coalesce(1).write
 .csv(r"C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query3", header=True, mode="overwrite"))