name := "flight_assignment"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.10"

// Add Spark dependency
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test // ScalaTest
)
