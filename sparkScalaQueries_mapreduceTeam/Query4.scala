import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Query4 extends App {
  val spark = SparkSession.builder.appName("sprintQuery4").master("local[*]").getOrCreate()

  val eventsDf = spark.read.option("header","True").option("inferSchema","True")
    .option("sep",",")
    .csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\projectMapreduce\datasets\athlete_events.csv""")
  val colToDrop = Seq("Name","Age","Sex","Height","Weight","Games","Season","City","Sport","Event","Medal")

//  dropping unwanted columns
  val inter1Df = eventsDf.drop(colToDrop: _*)
  val cleanedYear = inter1Df.withColumn("Year",regexp_extract(col("Year"),"\\d{4}",0))
    .withColumn("Year",trim(col("Year")))
    .filter(col("Year")=!="" && col("Year").isNotNull)

  val inter2Df = cleanedYear.groupBy("Year").agg(countDistinct("NOC").alias("countryCount"))
    .orderBy("countryCount")
  inter2Df.cache()
  inter2Df.count()
  val extremes = inter2Df.agg(
  min("countryCount").alias("minCount"),
  max("countryCount").alias("maxCount")
  ).first()

  val maxCount = extremes.getAs[Long]("maxCount").toInt
  val minCount = extremes.getAs[Long]("minCount").toInt
  val resultMin = inter2Df.where(col("countryCount")===minCount)
  val resultMax = inter2Df.where(col("countryCount")===maxCount)
  inter2Df.unpersist()
  resultMin.coalesce(1).write.option("header","true").mode("overwrite").csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query4\min""")
  resultMax.coalesce(1).write.option("header","true").mode("overwrite").csv("""C:\Users\pnamdeo\OneDrive - Capgemini\Desktop\training\outputs\Query4\max""")

}