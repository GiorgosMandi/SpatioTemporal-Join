package spatiotemporal.experiments

import dbis.stark.spatial.JoinPredicate
import dbis.stark.{Instant, STObject}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SpatialRDD._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import spatiotemporal.structures.{ObservationRecord, SentinelImageRecord}



object  Experiment {

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
      case "-s1_out" :: value :: tail =>
        nextOption(map ++ Map("s1_out" -> value), tail)
      case "-ob_out" :: value :: tail =>
        nextOption(map ++ Map("ob_out" -> value), tail)
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
      val sparkConf = new SparkConf().setMaster("local") .setAppName("PolarInterlinker")
      val spark = SparkSession.builder().getOrCreate()
      val sc = SparkContext.getOrCreate(sparkConf)

     // initialising input
     val ob_out =
       if (options.contains("ob_out"))
         options("ob_out")
       else {
         log.error("No Observation output file.")
         System.exit(1)
         null
       }

     val s1_out =
       if (options.contains("s1_out"))
         options("s1_out")
       else {
         log.error("No S1 output file.")
         System.exit(1)
         null
       }

      //reading observation data and converting them to STObject
      var observationRDD =
        if (options.contains("ob")) {
          val ob = sc.textFile(options("ob"))
            .map(l => l.split(","))
          val header = ob.first()
          ob
            .filter(line => ! line.sameElements(header))
            .filter(line => line(3) != "" && line(4) != "")
            .map(line => ObservationRecord(line(0), line(3).toDouble, line(4).toDouble, line(2).split("\\.")(0)))
            .map(rec => (STObject(rec.point,  new Instant(rec.timestamp)), rec))
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
            .filter(line => ! line.sameElements(header))
            .map(line => SentinelImageRecord(line(0), line(1), line(2), line(25), line(3), line(5)))
            .map(rec => (STObject(rec.coverage,  new Instant(rec.timestamp)), rec))
        }
        else {
          log.error("No S1 file.")
          System.exit(1)
          null
        }

       sentinelImagesRDD = sentinelImagesRDD.repartition(15).cache()
       observationRDD = observationRDD.repartition(10).cache()


       val joinedRDD = sentinelImagesRDD
         .join(observationRDD, JoinPredicate.CONTAINS)
         .cache()

       val schemaS1 = new StructType()
         .add(StructField("sentinel_image_id", StringType, nullable = true))
         .add(StructField("datetime", StringType, nullable = true))
         .add(StructField("coverage", StringType, nullable = true))

     val schemaObs = new StructType()
       .add(StructField("sentinel_image_id", StringType, nullable = true))
       .add(StructField("observation_id", StringType, nullable = true))
       .add(StructField("observed_at", StringType, nullable = true))
       .add(StructField("point", StringType, nullable = true))


     // Spatio-Temporal join
     val dfS1 = spark.createDataFrame(
       joinedRDD
         .map(rec => Row(rec._1.id, rec._1.timestampSTR, rec._1.coverage))
       ,schemaS1
     ).distinct()

     val dfObs = spark.createDataFrame(
       joinedRDD
         .map(rec => Row(rec._1.id, rec._2.id, rec._2.timestampSTR, rec._2.point))
       ,schemaObs
     )

     dfObs.show()
     dfS1.show()

     // storing results
     dfS1
       .coalesce(1)
       .write
       .mode(SaveMode.Overwrite)
       .option("header", "true")
       .csv(s1_out)

     dfObs
       .coalesce(1)
       .write
       .mode(SaveMode.Overwrite)
       .option("header", "true")
       .csv(ob_out)
  }
}
