import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Query14 extends App {
  val spark = SparkSession.builder.appName("sprintQuery10").master("local[*]").getOrCreate()
  val eventsDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("mode", "DROPMALFORMED")
    .csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\projectMapreduce\datasets\athlete_events.csv""")
  val removedDuplicates = eventsDf.dropDuplicates().filter(col("Medal")=!="NA")
  val regionsDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("mode", "DROPMALFORMED")
    .csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\projectMapreduce\datasets\noc_regions.csv""")
  val regionsCleaned = regionsDf.dropDuplicates(Seq("NOC","region"))
  val medalCount = removedDuplicates.groupBy("NOC").agg(sum(when(col("Medal") ==="Gold", 1)
    .otherwise(0)).alias("Gold"),sum(when(col("Medal") === "Silver",1)
    .otherwise(0)).alias("Silver"),sum(when(col("Medal") === "Bronze",1)
    .otherwise(0)).alias("Bronze")).orderBy(desc("Gold"))
  val result = medalCount.join(regionsCleaned,Seq("NOC"),"inner")
    .select(col("region").alias("Country"),col("Gold"),col("Silver"),col("Bronze"))
    .orderBy(desc("Gold"),desc("Silver"),desc("Bronze"))
  result.coalesce(1).write.option("header","true").mode("overwrite").csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query14""")

}
