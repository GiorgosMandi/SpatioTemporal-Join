package spatiotemporal.experiments

import dbis.stark.spatial.JoinPredicate
import dbis.stark.{Instant, STObject}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.SpatialRDD._
import spatiotemporal.structures.{ObservationRecord, SentinelImageRecord}

object PolarExperiment {

  type OptionMap = Map[String, String]

  // Parsing the input arguments
  @scala.annotation.tailrec
  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case "-ob" :: value :: tail =>
        nextOption(map ++ Map("ob" -> value), tail)
      case "-s1" :: value :: tail =>
        nextOption(map ++ Map("s1" -> value), tail)
      case "-ice_obs" :: value :: tail =>
        nextOption(map ++ Map("ice_obs" -> value), tail)
      case "-out1" :: value :: tail =>
        nextOption(map ++ Map("out1" -> value), tail)
      case "-out2" :: value :: tail =>
        nextOption(map ++ Map("out2" -> value), tail)
      case option :: tail => println("Unknown option " + option)
        nextOption(map ++ Map("unknown" -> ""), tail)
    }
  }

  def main(args: Array[String]): Unit = {

    val options = nextOption(Map(), args.toList)

    //Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("akka").setLevel(Level.ERROR)
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val sparkConf = new SparkConf().setMaster("local").setAppName("PolarInterlinker")
    val spark = SparkSession.builder().getOrCreate()
    val sc = SparkContext.getOrCreate(sparkConf)

    //reading observation data and converting them to STObject
    val iceObservatiosRDD =
      if (options.contains("ob") && options.contains("ice_obs")) {
        val headerObs = sc.textFile(options("ob"))
          .map(l => l.split(","))
          .first()
        val observationRDD = sc.textFile(options("ob"))
          .map(l => l.split(","))
          .filter(line => !line.sameElements(headerObs))
          .filter(line => line(3) != "" && line(4) != "")
          .map(line => (line(0), (line(3).toDouble, line(4).toDouble, line(2).split("\\.")(0))))

        val icesRDD = sc.textFile(options("ice_obs"))
          .map(l => l.split(","))
        val headerIces = icesRDD.first()

        icesRDD
          .filter(line => !line.sameElements(headerIces))
          .filter(line  => line(14) == "primary")
          .map(line => (line(1), (line(0), line(11), { if (line(10) != "") line(10).toInt else -1})))
          .leftOuterJoin(observationRDD)
          .filter(_._2._2.isDefined)
          .map(p => (p._1 , p._2._1, p._2._2.get)) //RDD[ obs_id, ice_id, ice_tmstmp,  obs_info, other]
          .map(p => ObservationRecord(p._1, p._2._1, p._3._1, p._3._2, p._2._2.split("\\.")(0), List(p._2._3)))
          .map(rec => (STObject(rec.point, new Instant(rec.timestamp)), rec))
          .repartition(15)
          .cache()
      }
      else {
        log.error("No Observation file.")
        System.exit(1)
        null
      }

    //reading sentinel data and converting them to STObject
    var sentinelImagesRDD = if (options.contains("s1")) {
      val s1 = sc.textFile(options("s1"))
        .map(l => l.split(","))
      val header = s1.first()
      s1
        .filter(line => !line.sameElements(header))
        .map(line => SentinelImageRecord(line(0), line(1), line(2), line(25), line(3), line(5)))
        .map(rec => (STObject(rec.coverage, new Instant(rec.timestamp)), rec))
        .repartition(15)
        .cache()
    }
    else {
      log.error("No S1 file.")
      System.exit(1)
      null
    }

    val out =
      if (options.contains("out1"))
        options("out1")
      else {
        log.error("No output file.")
        System.exit(1)
        null
      }

    val joinedRDD = sentinelImagesRDD
      .join(iceObservatiosRDD, JoinPredicate.CONTAINS)
      .cache()

    val schema = new StructType()
      .add(StructField("sentinel_image_id", StringType, nullable = true))
      .add(StructField("ice_observation_id", StringType, nullable = true))
      .add(StructField("datetime", StringType, nullable = true))
      .add(StructField("image_coverage", StringType, nullable = true))
      .add(StructField("observation_datetime", StringType, nullable = true))
      .add(StructField("observation_point", StringType, nullable = true))
      .add(StructField("ice_thickness", IntegerType, nullable = true))


    val df = spark.createDataFrame(
      joinedRDD
        .map(rec => Row(rec._1.id, rec._2.secondary_id, rec._1.timestampSTR, rec._1.coverage, rec._2.timestampSTR, rec._2.point, rec._2.other(0)))
      ,schema
    )
    df.show()
    df
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(out)

  }
}
