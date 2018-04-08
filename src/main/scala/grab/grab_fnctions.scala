package grab

import java.util.BitSet

import scala.collection.immutable.NumericRange

object grab_functions {

  val digits = Array('0', '1', '2', '3', '4', '5', '6', '7', '8',
    '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p',
    'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')

  val lookup = digits.zip(List.range(0, digits.length, 1)).toMap

  def fromGeoHash(code : String) : (Double, Double) = {
    val buffer = new StringBuilder();
    for(c <- code.toCharArray){
      val i = lookup.get(c) match {
        case Some(num) => num + 32
        case _ => 0
      }
      buffer.append(Integer.toString(i, 2).substring(1))
    }
    val lonset = new BitSet();
    val latset = new BitSet();
    var j = 0
    for(i <- List.range(0, 30, 2)){
      var isSet = false
      if ( i < buffer.length() )
      isSet = buffer.charAt(i) == '1';
      lonset.set(j, isSet);
      j += 1
    }

    j=0;
    for (i <- List.range(1, 30, 2)) {
      var isSet = false;
      if ( i < buffer.length() ) isSet = buffer.charAt(i) == '1';
      latset.set(j, isSet);
      j += 1
    }

    (decode(lonset, -180, 180), decode(latset, -90, 90))
  }

  def decode(bs: BitSet, floor: Double, ceiling: Double) : Double = {
    var mid = 0d;
    var f = floor
    var c = ceiling
    for( i <- Range(0, bs.length())){
      mid = (f + c) / 2
      if(bs.get(i)){
        f = mid
      }else{
        c = mid
      }
    }
    return mid
  }

  def toGeoHash(lon : Double, lat : Double) : String = {
    val latBits = getBits(lat, -90, 90)
    val lonBits = getBits(lon, -180, 180);
    val buffer = new StringBuilder
    for(i <- Range(0, 15)){
      buffer.append(lonBits.get(i) match {
        case true => '1'
        case _ => '0'
      })
      buffer.append(latBits.get(i) match {
        case true => '1'
        case _ => '0'
      })
    }
    // generate base32 string
    val codeVal = java.lang.Long.parseLong(buffer.toString(),2)
    base32(codeVal)
  }

  def base32(value : Long) : String = {
    val buf = Array.ofDim[Char](65)
    var charPos = 64
    var i = value
    if(value >= 0){
      i = -i
    }
    while(i <= -32){
      buf(charPos) = digits((-(i % 32)).toInt)
      charPos = charPos - 1
      i /= 32
    }

    buf(charPos) = digits((-i).toInt)

    if(value < 0){
      charPos -= 1
      buf(charPos) = '-'
    }

    return new String(buf, charPos, (65 - charPos))
  }

  def getBits(value : Double, floor : Double, ceiling : Double) : BitSet = {
    val buffer = new BitSet();
    var f = floor
    var c = ceiling
    for(i <- Range(0, 15)){
      val mid = (f + c) / 2;
      if(value >= mid){
        buffer.set(i)
        f = mid
      }else{
        c = mid;
      }
    }
    return buffer;
  }

  /**
    * funtions that find out the geohash blocks that intersect with a line
    * @param p1 longitude, latitude
    * @param p2 longitude, latitude
    * @return
    */
  def geoHashBlocksThatIntersect(p1 : (Double, Double), p2 : (Double, Double)) : Array[String] = {
    var it : List[(Double, Double)] = null
    // special case when p1.lat = p2.lat, unable to calculate (p1.lon - p2.lon) / (p1.lat - p2.lat)
    if(p1._1 == p2._1 && p1._2 == p2._2) return Array()
    if(p1._2 == p2._2){
      val lat = p1._2
      val (start, end) = if(p1._1 < p2._1) (p1._1, p2._1) else (p2._1, p1._1)
      val lonset = (start to end+0.005 by 0.005).toList
//      val lonset = NumericRange[Double](start, end + 0.005, 0.005).toList
      val latset = List.fill(lonset.length)(lat)
      it = lonset.zip(latset)
    }else {
      val latmove = if(p1._2 < p2._2) 0.005 else -0.005
      val latset = (p1._2 to p2._2+latmove by latmove).toList
      var lonset : List[Double] = null
      if(p1._1 == p2._1){ // if p1.lon == p2.lon
        lonset = List.fill(latset.length)(p1._1)
      }else{
        val lonMove = latmove * (p2._1 - p1._1) / (p2._2 - p1._2)
        lonset = (p1._1 to p2._1 + latmove by lonMove).toList
      }
      it = lonset.zip(latset)
    }
    it.map( f => toGeoHash(f._1, f._2)).toSet.toList.toArray
  }

  def main(args: Array[String]): Unit = {
    geoHashBlocksThatIntersect((-1, 1), (0, 1))
  }
}
