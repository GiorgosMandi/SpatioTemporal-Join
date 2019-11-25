package spatiotemporal

import java.text.SimpleDateFormat
import org.joda.time.{Days, MutableDateTime}

/**
 * Class to represent the observations
 * @param id id  of the observation
 * @param observed_at timestamp of its observation as days since the epoch
 * @param observed_at_STR timestamp as yyyy-MM-dd HH:mm:ss
 * @param point geometry in WKT
 */
class ObservationRecord(val id : String, val observed_at : Long, val observed_at_STR : String,
                        val point : String) extends  Serializable

object ObservationRecord {

  val epoch = new MutableDateTime
  epoch.setDate(0) //Set to Epoch time
  epoch.setTime(0)

  def apply(line : Array[String]): ObservationRecord = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val lat : Double =  line(3).toDouble
    val long : Double =  line(4).toDouble
    val point : String = GeometryUtils.createPoints(lat, long)

    val observedAtSTR : String = line(2).split("\\.")(0)
    val observedAtΕ = sdf.parse(observedAtSTR)
    val observedAt = Days.daysBetween(epoch, new MutableDateTime(observedAtΕ.getTime)).getDays

    new ObservationRecord(line(0), observedAt, observedAtSTR, point)
  }

}
