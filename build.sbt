name := "junior-data-exercise"

version := "0.1"

scalaVersion := "2.13.12"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
)

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:postfixOps"
)

run / fork := true
run / envVars := Map("HADOOP_HOME" -> "C:\\hadoop")