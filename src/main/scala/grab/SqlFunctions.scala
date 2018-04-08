package grab

import org.apache.spark.sql.functions._

/**
  * Created by zongsizhang on 4/4/18.
  */
object SqlFunctions {
  val GeoHashBlocksIntersects = udf((lon1 : Double, lat1 : Double, lon2 : Double, lat2 : Double) => {
    grab_functions.geoHashBlocksThatIntersect((lon1, lat1), (lon2, lat2))
  })

  val tranlsateToHtml = udf((geohash_code : String, time : Double, time_threshold : Double) => {
    val (lon, lat) = grab_functions.fromGeoHash(geohash_code)
    val alphaVal = if(time> time_threshold) 1 else (time / time_threshold)
    val side = 0.00275
    val code = JSHardCode.block_head +
      "fillOpacity: " + alphaVal + ",\n" +
      "bounds: {\n" +
      "north: " + (lat + side) + ",\n" +
      "south: " + (lat - side) + ",\n" +
      "east: " + (lon + side * 2) + ",\n" +
      "west: " + (lon - side * 2) + "\n}\n});\n"
    code
  })

  val fromGeoHash = udf((f : String ) => {
    grab_functions.fromGeoHash(f)
  })
}
