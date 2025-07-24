import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object Query16 extends App {
  val spark = SparkSession.builder.appName("sprintQuery16").master("local[*]").getOrCreate()
  val eventsDf = spark.read.option("header", "true").option("inferSchema", "true").option("multiLine", "true")
    .option("mode", "DROPMALFORMED")
    .csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\projectMapreduce\datasets\athlete_events.csv""")
  val removedDuplicates = eventsDf.dropDuplicates().filter(col("Medal")=!="NA")
  val medalsCountDf = removedDuplicates.groupBy("Games", "Team", "Medal")
    .agg(count("*").alias("Medal_Count"))

  val goldDf   = medalsCountDf.filter(col("Medal") === "Gold")
  val silverDf = medalsCountDf.filter(col("Medal") === "Silver")
  val bronzeDf = medalsCountDf.filter(col("Medal") === "Bronze")

  val medalWindow = Window.partitionBy("Games").orderBy(desc("Medal_Count"))

  val topGold = goldDf.withColumn("Rank", row_number().over(medalWindow))
    .filter(col("Rank") === 1)
    .select(col("Games"), col("Team").alias("Gold_team"), col("Medal_Count").alias("Gold_Medals"))

  val topSilver = silverDf.withColumn("Rank", row_number().over(medalWindow))
    .filter(col("Rank") === 1)
    .select(col("Games"), col("Team").alias("Silver_Team"), col("Medal_Count").alias("Silver_Medals"))

  val topBronze = bronzeDf.withColumn("Rank", row_number().over(medalWindow))
    .filter(col("Rank") === 1)
    .select(col("Games"), col("Team").alias("Bronze_Team"), col("Medal_Count").alias("Bronze_Medals"))

  // Step 5: Final Result
  val finalResultDf = topGold.join(topSilver, Seq("Games"), "inner")
    .join(topBronze, Seq("Games"), "inner").orderBy(desc("Games"))
  finalResultDf.coalesce(1).write.option("header","true").mode("overwrite")
    .csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query16""")


}
