package spatiotemporal.structures

import java.text.SimpleDateFormat

import org.joda.time.{Days, MutableDateTime}
import spatiotemporal.utils.GeometryUtils

/**
 * class that represents the loaded satellite images
 * @param id image id
 * @param catalogueid catalogue id
 * @param filename name of the image
 * @param timestamp the timestamp of the image as days since java epoch
 * @param timestampSTR the date as String as yyyy-MM-dd HH:mm:ss
 * @param size image size
 * @param coverage image coverage geometry as hex WKB
 */
class SentinelImageRecord(val id : String, val catalogueid : String, val filename : String, val timestamp : Long ,
                          val timestampSTR : String , val size : Long, val coverage : String )  extends Serializable

object SentinelImageRecord {

  val epoch = new MutableDateTime
  epoch.setDate(0)
  epoch.setTime(0)

  /**
   *
   * @param id satellite image id
   * @param catalogueid catalogue id
   * @param filename image filename
   * @param coverageWKB geometry of coverage as hex WKB
   * @param timestampSTR datetime of the image following the "yyyy-MM-dd HH:mm:ss" pattern
   * @param sizeSTR size of the image as string
   * @return new SentinelImageRecord
   */
  def apply(id : String, catalogueid : String, filename : String, coverageWKB : String, timestampSTR : String, sizeSTR : String)
  : SentinelImageRecord = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //converting WKB into WKT
    val coverage = GeometryUtils.creteGeometryFromWKB(coverageWKB)

    // converting date as days since epoch
    val timestampE = sdf.parse(timestampSTR)
    val timestamp = Days.daysBetween(epoch, new MutableDateTime(timestampE.getTime)).getDays

    val size = if (sizeSTR != "" ) sizeSTR.toLong  else -1
    new SentinelImageRecord(id, catalogueid, filename, timestamp, timestampSTR, size, coverage)

  }

  /**
   *
   * @param id satellite image id
   * @param coverageWKB geometry of coverage as hex WKB
   * @param datetimeSTR datetime of the image following the "yyyy-MM-dd HH:mm:ss" pattern
   * @return new SentinelImageRecord
   */
  def apply(id : String, coverageWKB : String, datetimeSTR : String)
  : SentinelImageRecord = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //converting WKB into WKT
    val coverage = GeometryUtils.creteGeometryFromWKB(coverageWKB)

    // converting date as days since epoch
    val datetimeE = sdf.parse(datetimeSTR)
    val datetime = Days.daysBetween(epoch, new MutableDateTime(datetimeE.getTime)).getDays

    new SentinelImageRecord(id, "0'", "", datetime, datetimeSTR, -1, coverage)

  }
}