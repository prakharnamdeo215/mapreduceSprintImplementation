import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Query10 extends App {
  val spark = SparkSession.builder.appName("sprintQuery10").master("local[*]").getOrCreate()
  val eventsDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("mode", "DROPMALFORMED")
    .csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\projectMapreduce\datasets\athlete_events.csv""")

  val eventsCleanedDf = eventsDf.groupBy("ID","Name","Games","City","Sport","Sex").agg(count("ID")).orderBy("ID","Name")

  eventsCleanedDf.withColumn("Sex",trim(col("Sex"))).filter(col("Sex").isNotNull || col("Sex")=!="")
  val counts = eventsCleanedDf.groupBy("Sex").agg(count("Sex").alias("Count"))
  val totalCount = counts.agg(sum("Count")).first().getLong(0)
  val resultRatio = counts.withColumn("Ratio",round(col("Count")/lit(totalCount),2)).select("Sex","Ratio")
  resultRatio.coalesce(1).write.option("header","true").mode("overwrite").csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query10""")


}
