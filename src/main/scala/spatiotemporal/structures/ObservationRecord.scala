package spatiotemporal.structures

import java.text.SimpleDateFormat

import org.joda.time.{Days, MutableDateTime}
import spatiotemporal.utils.GeometryUtils

/**
 * Class to represent the observations
 * @param id id  of the observation
 * @param secondary_id secondary id
 * @param timestamp timestamp of its observation as days since the epoch
 * @param timestampSTR timestamp as yyyy-MM-dd HH:mm:ss
 * @param point geometry in WKT
 */
class ObservationRecord(val id : String, val secondary_id : String, val timestamp : Long, val timestampSTR : String,
                        val point : String, var other : List[Any]) extends  Serializable

object ObservationRecord {

  val epoch = new MutableDateTime
  epoch.setDate(0) //Set to Epoch time
  epoch.setTime(0)

  /**
   *
   * @param id observation id
   * @param secondary_id secondary id
   * @param lat lat of observation point
   * @param long long of observation point
   * @param timestampSTR observation date following the "yyyy-MM-dd HH:mm:ss" pattern
   * @return an observationRecord
   */
  def apply(id : String, secondary_id : String, lat : Double, long : Double, timestampSTR : String, other : List[Any])
  : ObservationRecord = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timestampE = sdf.parse(timestampSTR)
    val timestamp = Days.daysBetween(epoch, new MutableDateTime(timestampE.getTime)).getDays

    val point : String = GeometryUtils.createPoints(lat, long)

    new ObservationRecord(id, secondary_id, timestamp, timestampSTR, point, other)
  }

}
