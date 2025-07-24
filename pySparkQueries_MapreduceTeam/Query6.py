from dataLoad import cleanData
spark, athletes, nocs = cleanData()

df = athletes

rdd = df.rdd

summer_rdd = rdd.filter(lambda row: row["Season"] == "Summer")
sport_games_rdd = summer_rdd.map(lambda row: (row["Sport"], row["Games"])).distinct()
total_summer_games = sport_games_rdd.map(lambda x: x[1]).distinct().count()
sport_appearance_rdd = sport_games_rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
result_rdd = sport_appearance_rdd.filter(lambda x: x[1] == total_summer_games)
result_rdd.coalesce(1).saveAsTextFile(r"C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query6")