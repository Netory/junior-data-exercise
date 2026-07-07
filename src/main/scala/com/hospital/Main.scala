package com.hospital

import com.hospital.config.SparkProvider
import com.hospital.pipeline.PatientPipeline

object Main {
def main(args: Array[String]): Unit = {
// init

  System.setProperty("hadoop.home.dir", "C:\\hadoop")
val spark = SparkProvider.spark
    spark.sparkContext.setLogLevel("WARN") // cancel les logs info de spark

println("Démarrage du pipeline FHIR Data Engineer...")


// en prod ça devient des arguments (args(0), args(1))
val inputPath = "data/input"
val outputPath = "data/output/fhir_patients"

// 3. Exécution de la logique métier
val pipeline = new PatientPipeline(spark)
val fhirData = pipeline.run(inputPath)


println("Échantillon des données consolidées :")
    fhirData.show(5, truncate = false)
    fhirData.printSchema()


// save en json pour que ce soit usuable par une API
//JSON Lines (NDJSON) generate by Spark
println(s"Sauvegarde des données dans : $outputPath")
    fhirData.write
            .mode("overwrite") // ça le rend indépendant et ça casse pas les résults
      .json(outputPath)

println("Traitement terminé avec succès !")

    spark.stop()
  }
          }