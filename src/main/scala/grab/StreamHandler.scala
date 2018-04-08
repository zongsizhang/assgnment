package grab

import java.util.Date

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * Created by zongsizhang on 3/29/18.
  */
object StreamHandler {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <output_path> is a directory path that we will use store the outputs
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics, output_path) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamIntegration")
    val ssc = new StreamingContext(sparkConf, Minutes(10))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafka_params = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "metadata.broker.list" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val drivers_raw_data = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafka_params))

    val orders_count = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array("order").toSet, kafka_params))
      .map( record => {
        val words = record.value.split(",")
        val lon = words(1).toDouble
        val lat = words(2).toDouble
        (grab_functions.toGeoHash(lon, lat), 1L)
      }).reduceByKey(_ + _)
      .map(f => (f._1, (f._2.toDouble, 1)))

    val drivers_count = drivers_raw_data
      .map( record => {
      val words = record.value.split(",")
      val lon = words(1).toDouble
      val lat = words(2).toDouble
      (grab_functions.toGeoHash(lon, lat), 1L)
    }).reduceByKey(_ + _)
      .map(f => (f._1, (f._2.toDouble, 0)))


    val ratio_union = drivers_count
      .union(orders_count)
      .reduceByKey( (f1, f2) => {
        (f1._1 / f2._1, 2)
      }).mapValues(f => {
      f._2 match{
        case 1 => 0
        case _ => f._1
      }
    })

    val congestion = drivers_raw_data
      .map( record => {
        val words = record.value.split(",")
        val id = words(0).toInt
        val lon = words(1).toDouble
        val lat = words(2).toDouble
        var timeStamp = words(3).toLong
        // interval 1 minutes
        timeStamp = timeStamp / 60000
        ((grab_functions.toGeoHash(lon, lat), id, timeStamp), 1L)
      })
      .reduceByKey(_ + _)
      .map(x => ((x._1._1, x._1._2), (1, x._2)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(f => {
        (f._1._1, (1, f._2._2.toDouble / f._2._1.toDouble))
      }).reduceByKey((f1, f2) => {
      (f1._1 + f2._1, f1._2 + f2._2)
    }).mapValues(f => f._1 / f._2.toDouble)


    val congestionOutputPath = output_path.last match {
      case '/' => output_path + "congestion/"
      case _ => output_path + "/congestion/"
    }

    val ratio_output_path = output_path.last match {
      case '/' => output_path + "ratio/"
      case _ => output_path + "/ratio/"
    }

    congestion.saveAsTextFiles(congestionOutputPath + System.currentTimeMillis())
    ratio_union.saveAsTextFiles(ratio_output_path + System.currentTimeMillis())

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
