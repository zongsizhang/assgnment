package grab

import java.io.{File, PrintWriter}
import java.net.URI
import java.util.concurrent.Executors

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.client.HdfsAdmin
import org.apache.hadoop.hdfs.inotify.Event.{CloseEvent, EventType}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source
import sys.process._

/**
  * Created by zongsizhang on 3/31/18.
  */
object GeoHashVisualizer {

  def main(args: Array[String]): Unit = {
    // check change in file

    val hdfsNameNodePath = args(0)

    val listenPath = args(1)

    val hadoopBinPath = args(2)

    val collectPath = args(3)

    val drawPath = args(4)

    val pool = Executors.newFixedThreadPool(4)

    val admin = new HdfsAdmin(URI.create(hdfsNameNodePath + "/" + listenPath), new Configuration())
    val eventStream = admin.getInotifyEventStream
    while(true){
      val events = eventStream.take()
      for (event <- events.getEvents){
        event.getEventType match {
          case EventType.CLOSE => {
            val closeEvent = event.asInstanceOf[CloseEvent]
            val info = closeEvent.getPath
            if(info.endsWith("_SUCCESS") && (info.contains("congestion") || info.contains("ratio"))){
              val sourcePath = hdfsNameNodePath + info.dropRight(8)
              val subCollectPath = collectPath + (if(info.contains("congestion")) "/congestion" else "ratio")
              val outputPath = drawPath +  (if(info.contains("congestion")) "/congestion.html" else "/ratio.html")
              pool.execute(new CollectAndDraw(hadoopBinPath, sourcePath, subCollectPath, outputPath))
            }
          }
          case _ => {

          }
        }
      }
    }
  }

  def recordBlocks(inputPath : String, drawPath : String) : Unit= {

    val writer = new PrintWriter(new File(drawPath))

    val line = new File(inputPath)
      .listFiles
      .filter(!_.getName.contains("SUCCESS"))
      .toList.flatMap( f => {
      Source.fromFile(f).getLines()
    }).map( line => {
      val Array(geohashCode, statRes) = line.filterNot(c => c == '(' || c == ')').split(",")
      val coordinate = grab_functions.fromGeoHash(geohashCode)
      (coordinate, statRes.toDouble)
    }).map( f => {
      // start drawing
      val alphaVal =  1 - ( (if (f._2 > 1) 0.99d else if(f._2 == 0) 0.01 else f._2 ))
      val (lon, lat) = f._1
      val side = 0.00275
      val code = JSHardCode.block_head +
      "fillOpacity: " + alphaVal + ",\n" +
      "bounds: {\n" +
      "north: " + (lat + side) + ",\n" +
        "south: " + (lat - side) + ",\n" +
        "east: " + (lon + side * 2) + ",\n" +
        "west: " + (lon - side * 2) + "\n}\n});\n"
      code
    }).reduce(_ + _)

    writer.print(JSHardCode.html_head + line + JSHardCode.html_end)
    writer.close()
  }

  class CollectAndDraw(hadoopBinPath :String, sourcePath : String, collectPath : String, outputPath : String) extends Runnable {
    override def run(): Unit = {

      // ${hadoopBin}/hadoop fs -get hdfs://master:9000:sourcePath collectPath
      val collectCommand = hadoopBinPath + "/hadoop fs -get " + sourcePath + " " + collectPath

      // do collect operation
      collectCommand!

      val f: Future[Int] = Future(0)
      val f2: Future[Unit] = f.map { x => {
        recordBlocks(collectPath, outputPath)
      }
      }

      Await.ready(f2, Duration.Inf)

      val removeSourceCommand = hadoopBinPath + "/hadoop fs -rm -r " + sourcePath

      removeSourceCommand!

      val removeCollectCommand = "rm -rf " + collectPath

      removeCollectCommand!

    }
  }
}
