import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Query15 extends App {
  val spark = SparkSession.builder
    .appName("OlympicDataAnalysis").master("local[*]")
    .getOrCreate()

  val athletes = spark.read.option("header", "true").option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("mode", "DROPMALFORMED")
    .csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\projectMapreduce\datasets\athlete_events.csv""")

  val athletesClean = athletes.dropDuplicates().where(col("Medal")=!="NA")

  val regionsDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("mode", "DROPMALFORMED")
    .csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\projectMapreduce\datasets\noc_regions.csv""")
  val regionsCleaned = regionsDf.dropDuplicates(Seq("NOC","region"))

  val medalCounts = athletesClean
    .groupBy("Games", "NOC")
    .pivot("Medal", Seq("Gold", "Silver", "Bronze"))
    .count()
    .na.fill(0).orderBy("Games","Gold","Silver","Bronze")

  val mcount = medalCounts.join(regionsCleaned,Seq("NOC"),"inner").select(col("Games"),col("region").alias("Country"),col("Gold"),col("Silver"),col("Bronze"))

  mcount.coalesce(1).write.option("header","true").mode("overwrite")
    .csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query15""")


}
