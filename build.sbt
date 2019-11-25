
name := "sptemp_join"
version := "0.1"
scalaVersion := "2.11.12"
val sparkVersion = "2.4.1"

// https://mvnrepository.com/artifact/joda-time/joda-time
libraryDependencies += "joda-time" % "joda-time" % "2.3"


// file:///home/gmandi/Documents/projects/stark/target/scala-2.11/stark.jar
libraryDependencies += "dbis" % "stark" %"1.0" from "file:///home/gmandi/Documents/projects/stark/target/scala-2.11/stark.jar"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion % Provided

// https://mvnrepository.com/artifact/org.locationtech.spatial4j/spatial4j
libraryDependencies += "org.locationtech.spatial4j" % "spatial4j" % "0.7"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}