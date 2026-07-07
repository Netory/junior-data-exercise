package com.hospital.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class PatientPipeline(spark: SparkSession) {

  // on creer la fonction pour read
  private def readCsv(path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("encoding", "UTF-8")
      .option("escape", "\"")
      .csv(path)
  }

  def run(inputPath: String): DataFrame = {
    // getter des csv
    val dfPatients = readCsv(s"$inputPath/patients.csv")
    val dfIpp = readCsv(s"$inputPath/identifiants_ipp.csv")
    val dfAdresses = readCsv(s"$inputPath/adresses.csv")
    val dfOpposition = readCsv(s"$inputPath/opposition_recherche.csv")

    // normalisation dates
    val parseDateCol = (colName: String) => coalesce(
      to_date(col(colName), "yyyy-MM-dd"),
      to_date(col(colName), "dd/MM/yyyy"),
      to_date(col(colName), "dd-MM-yyyy"),
      to_date(col(colName), "yyyy/MM/dd")
    )

    val dfIppClean = dfIpp.select(
      col("ipp").as("ipp_source"),
      // regex pour "DEPRECIE", "DÉPRÉCIÉ", et autres
      when(upper(col("statut")).rlike("D[EÉ]PR[EÉ]CI[EÉ]"), col("ipp_principal"))
        .otherwise(col("ipp")).as("ipp_cible")
    )

    val dfPatientsCleanIpp = dfPatients
      .join(dfIppClean, dfPatients("ipp") === dfIppClean("ipp_source"), "left")
      .withColumn("ipp_actif", coalesce(col("ipp_cible"), col("ipp")))

    // coupure des doublons
    val windowSpec = Window.partitionBy("ipp_actif").orderBy(
      parseDateCol("date_fin_validite").desc_nulls_first
    )

    val dfUniquePatients = dfPatientsCleanIpp
      .withColumn("rank", row_number().over(windowSpec))
      .filter(col("rank") === 1)

    // normalisation adresses
    val dfAdressesGrouped = dfAdresses
      .withColumn("ville_propre", initcap(trim(col("ville"))))
      .groupBy("ipp")
      .agg(collect_list(
        struct(
          col("ligne_adresse").as("line"),
          col("code_postal").as("postalCode"),
          col("ville_propre").as("city"),
          col("pays").as("country"),
          col("type_adresse").as("use"),
          parseDateCol("date_debut").as("period_start")
        )
      ).as("address_list"))

    // Jointure
    val dfConsolidated = dfUniquePatients
      .join(dfAdressesGrouped, dfUniquePatients("ipp_actif") === dfAdressesGrouped("ipp"), "left")
      .join(dfOpposition, dfUniquePatients("ipp_actif") === dfOpposition("ipp"), "left")

    // FHIR
    val dfFhir = dfConsolidated.select(
      lit("Patient").as("resourceType"),
      col("ipp_actif").cast("string").as("id"),


      array(
        struct(
          lit("official").as("use"),
          upper(trim(col("nom_naissance"))).as("family"),
          transform(
            split(regexp_replace(col("prenoms"), "[\\[\\]\"]", ""), ","),
            x => trim(x)
          ).as("given")
        )
      ).as("name"),

      //normalisation

      when(upper(trim(col("sexe"))).isin("M", "1", "HOMME", "MALE"), "male")
        .when(upper(trim(col("sexe"))).isin("F", "2", "FEMME"), "female")
        .otherwise("unknown").as("gender"),

      parseDateCol("date_naissance").cast("string").as("birthDate"),
      col("address_list").as("address"),


      when(
        lower(trim(col("opposition"))).isin("o", "oui", "true", "opposé"),
        lit("opt-out")
      ).otherwise(lit("opt-in")).as("researchConsentStatus")
    )

    dfFhir
  }
}