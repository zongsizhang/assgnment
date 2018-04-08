package grab

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType


object AggOnDayTime {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(s"""
                            |Usage: BatchHandler <inputPath> <timeStart> <timeEnd> <threshold>
                            |  <inputPath> input file path
                            |  <timeStart> time lower bound of query
                            |  <timeEnd> time upper bound of query
                            |  <time-window> window of time that is aggregated on and compared.
                            |  example: average congestion at January: 1m
                            |           average congestion at 1st day of every month: 1md
                            |           average congestion at monday every week: 1wd
                            |           average congestion at 7pm every day: 1h
                            |  <threshold> time in seconds, times pass which will be seen as in congestions
        """.stripMargin)
      System.exit(1)
    }

    val filePath = args(0)

    //    val timeStart = new SimpleDateFormat("MM-DD-yyyy").parse(args(1)).getTime / 1000
    //
    //    val timeEnd = new SimpleDateFormat("MM-DD-yyyy").parse(args(2)).getTime / 1000

    val time_tail = " 00:00:00"

    val timeStart = args(1) + time_tail

    val timeEnd = args(2)  + time_tail

    val threshold = args(3).toInt

    val outputPath = args(4)

    val spark = SparkSession
      .builder()
      //      .master("local[2]")
      .appName("BatchHandler")
      .getOrCreate()

    val rawData = spark.read.option("header", "true")csv(filePath)

    rawData.createOrReplaceTempView("tripdata")

    val time = rawData.select(
      unix_timestamp(col("tpep_pickup_datetime")).as("pickup_time"),
      unix_timestamp(col("tpep_dropoff_datetime")).as("dropoff_time"),
      col("pickup_longitude").cast(DoubleType).as("pu_lon"),
      col("pickup_latitude").cast(DoubleType).as("pu_lat"),
      col("dropoff_longitude").cast(DoubleType).as("do_lon"),
      col("dropoff_latitude").cast(DoubleType).as("do_lat"),
      col("payment_type"),
      col("trip_distance")
    )
      .filter(
        (col("dropoff_time") - col("pickup_time")) >= 5
          and col("pickup_time") > unix_timestamp(lit(timeStart))
          and col("dropoff_time") < unix_timestamp(lit(timeEnd))
          and col("pu_lon") > -74.262611
          and col("pu_lon") < -73.694086
          and col("pu_lat") > 40.492765
          and col("pu_lat") < 40.920628
          and col("do_lon") > -74.262611
          and col("do_lon") < -73.694086
          and col("do_lat") > 40.492765
          and col("do_lat") < 40.920628
      )
      .select(
        (col("dropoff_time") - col("pickup_time")).as("duration"),
        SqlFunctions.GeoHashBlocksIntersects(col("pu_lon"), col("pu_lat"), col("do_lon"), col("do_lat")).as("overlaps")
      )
      .select(
        (col("duration") / size(col("overlaps"))).as("block_time"),
        explode(col("overlaps")).as("geohash_code")
      )
      .groupBy(col("geohash_code"))
      .mean("block_time")

    import spark.implicits._

    val blockCodes = time.select(SqlFunctions.tranlsateToHtml(col("geohash_code"), col("avg(block_time)"), lit(threshold)))
      .as[String]

    val file_name = outputPath + "/" + "congestion-" + args(1) + "to" + args(2)

    blockCodes.write.text(file_name)
  }
}
