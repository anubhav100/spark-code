package stream

import java.nio.file.Paths

import org.apache.spark.sql.SparkSession

//TODO take paths from command line parmaters

/**
 * this example consumes csv as streaming data each cs will be taken as one new batch
 */
object StreamingCsvReaderExample extends App {
  val csvPath = Paths.get(".").toAbsolutePath + "/src/main/resources"
  val spark = SparkSession
    .builder()
    .appName("StreamingCsvReaderExample")
    .master("local")
    .getOrCreate()

  spark.sql("set spark.sql.streaming.schemaInference=true")

  val insuranceDataDf = spark.
    readStream.
    option("header", "true").
    format("csv")
    .load(csvPath)

  insuranceDataDf.select("*")
    .where("Price > 1200")
    .writeStream.format("orc")
    .option("path","D:\\")
    .option("checkpointLocation", "E:\\")
    .outputMode("append")
    .start()
    .awaitTermination()
}
