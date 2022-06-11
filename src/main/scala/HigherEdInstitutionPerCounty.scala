package net.jgp.books.sparkInAction.ch12.lab300_join

import org.apache.spark.sql.functions.{ size, split, col, element_at }
import org.apache.spark.sql.{ SparkSession, DataFrame }

import net.jgp.books.spark.basic.Basic

object HigherEdInstitutionPerCounty extends Basic {
  def run(): Unit = {
    val spark = getSession("Join")

    val censusDF = loadCensusData(spark)
    showDF(censusDF)

    val campusDF = loadInstitutionCampusData(spark)
    showDF(campusDF)

    val countyDF = loadCountyZipData(spark)
    showDF(countyDF)

    val joinCamCountyDF = campusDF.join(countyDF, campusDF("zip").equalTo(countyDF("zip")), "inner").
      drop(campusDF("zip"))
    showDF(joinCamCountyDF)

    val joinCenCamDF = censusDF.join(joinCamCountyDF, censusDF("countyId").equalTo(joinCamCountyDF("county")), "left").
      drop(joinCamCountyDF("county")).
      distinct
    showDF(joinCenCamDF)
  }

  private def showDF(df: DataFrame): Unit = {
    df.printSchema
    df.sample(0.1).show(5, false)
  }

  private def loadCensusData(spark: SparkSession): DataFrame = {
    val df = spark.read.format("csv").
      option("header", true).
      option("inferSchema", true).
      option("encoding", "cp1252").
      load("data/census/PEP_2017_PEPANNRES.csv")

    // record old column's name
    val cols = df.columns.toSeq

    df.withColumnRenamed("respop72017", "pop2017").
      withColumnRenamed("GEO.id2", "countyId").
      withColumnRenamed("GEO.display-label", "county").
      drop(cols: _*)
  }

  private def loadInstitutionCampusData(spark: SparkSession): DataFrame = {
    val df = spark.read.format("csv").
      option("header", true).
      option("inferSchema", true).
      load("data/dapip/InstitutionCampus.csv").
      //process
      filter("LocationType = 'Institution'")
      
    // record old column's name
    val cols = df.columns.toSeq

      // transform
    df.withColumn("addressElements", split(col("Address"), " ")).
      withColumn("addressElementCount", size(col("addressElements"))).
      withColumn("zip9", element_at(col("addressElements"), col("addressElementCount"))).
      withColumn("splitZipCode", split(col("zip9"), "-")).
      withColumn("zip", col("splitZipCode").getItem(0)).
      withColumnRenamed("LocationName", "location").
      drop("splitZipCode").
      drop("zip9").
      drop("addressElementCount").
      drop("addressElements").
      drop(cols: _*)
  }

  private def loadCountyZipData(spark: SparkSession): DataFrame = {
    spark.read.format("csv").
      option("header", true).
      option("inferSchema", true).
      load("data/hud/COUNTY_ZIP_092018.csv").
      drop( Seq("res", "bus", "oth", "tot").map(s => s"${s}_ratio"): _* )
  }
}