from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


def cleanData():
    spark = SparkSession.builder \
        .appName("OlympicDataAnalysis") \
        .getOrCreate()


    athletes = spark.read.option("header", True).csv(r"C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\projectMapreduce\datasets\athlete_events.csv", inferSchema=True)
    nocs = spark.read.option("header", True).csv(r"C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\projectMapreduce\datasets\noc_regions.csv", inferSchema=True)


    athletes_clean = athletes \
        .withColumn("Age", col("Age").cast("int")) \
        .withColumn("Height", col("Height").cast("int")) \
        .withColumn("Weight", col("Weight").cast("int")) \
        .withColumn("Year", col("Year").cast("int")) \
        .withColumn("Medal", when(col("Medal") == "NA", None).otherwise(col("Medal"))) \
        .dropna(subset=["Year", "NOC", "Name"]) \
        .dropDuplicates()\
    .cache()

    # Clean noc_regions
    nocs_clean = nocs.dropDuplicates(["NOC"]).cache()

    return spark, athletes_clean, nocs_clean
