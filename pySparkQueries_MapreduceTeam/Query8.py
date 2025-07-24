from pyspark.sql.functions import col,countDistinct
from dataLoad import cleanData
spark, athletes, noc = cleanData()

sports_per_game1 = athletes.groupBy("Games").agg(countDistinct("Sport").alias("No_of_sport_in_game"))
total_no_of_sports_per_games = sports_per_game1.orderBy("Games")
(total_no_of_sports_per_games.coalesce(1).write
 .csv(r"C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query8", header=True, mode="overwrite"))
