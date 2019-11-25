package spatiotemporal

import java.text.SimpleDateFormat
import org.joda.time.{Days, MutableDateTime}

/**
 * class that represents the loaded satellite images
 * @param id image id
 * @param catalogueid catalogue id
 * @param filename name of the image
 * @param datetime the timestamp of the image as days since java epoch
 * @param datetimeSTR the date as String as yyyy-MM-dd HH:mm:ss
 * @param size image size
 * @param coverage image coverage geometry as hex WKB
 */
class SentinelImageRecord(val id : String, val catalogueid : String, val filename : String, val datetime : Long ,
                          val datetimeSTR : String , val size : Long, val coverage : String )  extends Serializable

object SentinelImageRecord {

  val epoch = new MutableDateTime
  epoch.setDate(0)
  epoch.setTime(0)

  def apply(line : Array[String]): SentinelImageRecord = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //converting WKB into WKT
    val coverageWKB : String =  line(25)
    val coverage = GeometryUtils.creteGeometryFromWKB(coverageWKB)

    // converting date as days since epoch
    val datetimeSTR : String = line(3)
    val datetimeE = sdf.parse(datetimeSTR)
    val datetime = Days.daysBetween(epoch, new MutableDateTime(datetimeE.getTime)).getDays

    val size = if (line(5) != "" ) line(5).toLong  else -1
    new SentinelImageRecord(line(0), line(1), line(2), datetime, datetimeSTR, size, coverage)

  }
}