package com.hospital.config

import org.apache.spark.sql.SparkSession

object SparkProvider {
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("FHIR-Patient-Pipeline")
    .master("local[*]")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
}