import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Query13 extends App{
  val spark = SparkSession.builder.appName("sprintQuery10").master("local[*]").getOrCreate()
  val eventsDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("mode", "DROPMALFORMED")
    .csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\projectMapreduce\datasets\athlete_events.csv""")
  val removedDuplicates = eventsDf.dropDuplicates().filter(col("Medal")=!="NA").orderBy("ID").select("Medal","NOC")
  val topCountries = removedDuplicates.groupBy("NOC").agg(count("Medal").alias("CountOfMedals")).orderBy(desc("CountOfMedals")).select("NOC","CountOfMedals").limit(5)
  val regionsDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("mode", "DROPMALFORMED")
    .csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\projectMapreduce\datasets\noc_regions.csv""")
  val regionsCleaned = regionsDf.dropDuplicates(Seq("NOC","region"))
  val joinedDf = topCountries.join(regionsCleaned,Seq("NOC"),"inner").select(col("region").alias("Country"),col("CountOfMedals"))
  joinedDf.coalesce(1).write.option("header","true").mode("overwrite").csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query13""")
}
