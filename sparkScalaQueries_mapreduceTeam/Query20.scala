import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Query20 extends App {
  val spark = SparkSession.builder
    .appName("OlympicDataAnalysis").master("local[*]")
    .getOrCreate()

  val athletes = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\projectMapreduce\datasets\athlete_events.csv""")

  val athletes_clean = athletes
    .withColumn("Age", col("Age").cast("int"))
    .withColumn("Height", col("Height").cast("int"))
    .withColumn("Weight", col("Weight").cast("int"))
    .withColumn("Year", col("Year").cast("int"))
    .withColumn("Medal", when(col("Medal") === "NA", lit(null)).otherwise(col("Medal")))
    .na.drop(Seq("Year", "NOC", "Name"))
    .dropDuplicates()

  val notNullMedal = athletes_clean.filter(col("Medal").isNotNull)
  val indiaWonMedal = notNullMedal.filter(col("Team") === "India")
  val firstResult = indiaWonMedal.filter(col("Sport") === "Hockey")

  val groupedByOlympic = firstResult.groupBy(col("Games")).agg(count("Medal").alias("Total_Medals_by_india")).orderBy("Games")
  groupedByOlympic.coalesce(1).write.option("header","true").mode("overwrite")
    .csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query20""")
}
