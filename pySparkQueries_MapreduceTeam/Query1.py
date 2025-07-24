
from dataLoad import cleanData

spark, athletes, noc = cleanData()

athlete_clean_df = athletes.dropna(subset=["Name", "Games","Sport","Event","NOC"])
games_df = athlete_clean_df.select("Games").distinct()
no_of_games = games_df.count()
data = [("total_Number_of_Sports", no_of_games)]
df = spark.createDataFrame(data, ["Category", "Count"])
df.coalesce(1).write.csv(r"C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query1", header=True, mode="overwrite")
