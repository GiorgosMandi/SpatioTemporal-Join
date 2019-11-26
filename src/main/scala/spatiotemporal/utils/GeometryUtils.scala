package spatiotemporal.utils

import com.google.protobuf.ByteString
import javax.xml.bind.DatatypeConverter
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point}
import org.locationtech.jts.io.WKBReader
import org.locationtech.spatial4j.context.jts.{JtsSpatialContextFactory, ValidationRule}
import org.locationtech.spatial4j.io.jts.JtsWKTReaderShapeParser
import org.locationtech.spatial4j.shape.jts.JtsGeometry

/**
 * Construct Geometries
 */
object GeometryUtils extends Serializable {

  val geometryFactory = new  GeometryFactory()
  val noCheckFactory = new JtsSpatialContextFactory()

  noCheckFactory.validationRule = ValidationRule.none
  noCheckFactory.geo = true
  val noCheckContext = noCheckFactory.newSpatialContext
  val noCheckParser = new JtsWKTReaderShapeParser(noCheckContext, noCheckFactory)

  /**
   * Construct Point from coordinates
   * @param lat coordinate
   * @param long coordinate
   * @return the produced Point as WKT
   */
  def createPoints(lat : Double, long : Double): String ={

      val point : Point = geometryFactory.createPoint(new Coordinate(long, lat))
      point.toText
    }

  /**
   * For given WKB as hex, construct the Geometry
   * @param wkbHex geometry in WKB as hex
   * @return the produced geometry as WKT
   */
  def creteGeometryFromWKB(wkbHex : String) : String = {

    val wkbBytes: Array[Byte] = ByteString.copyFrom(
      DatatypeConverter.parseHexBinary(wkbHex)
    ).toByteArray

    val wkbr: WKBReader = new WKBReader(geometryFactory)
    val polygon = wkbr.read(wkbBytes)

    val polygonS4J = noCheckParser.parse(polygon.toText).asInstanceOf[JtsGeometry]
    polygonS4J.toString
  }

}
