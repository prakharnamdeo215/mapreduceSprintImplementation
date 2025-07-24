from dataLoad import cleanData
from pyspark.sql.functions import countDistinct
spark, athletes, nocs = cleanData()

joined_df = athletes.select("NOC", "Games") \
    .join(nocs.select("NOC", "region"), on="NOC", how="inner") \
    .dropDuplicates(["region", "Games"])

region_game_counts = joined_df.groupBy("region") \
    .agg(countDistinct("Games").alias("No_of_Games"))

total_games = athletes.select("Games").distinct().count()

result = region_game_counts.filter(f"No_of_Games = {total_games}") \
    .selectExpr("region as Countries")

result.coalesce(1).write.csv(r"C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query5", header=True, mode="overwrite")
