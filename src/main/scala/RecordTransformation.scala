package net.jgp.books.spark.ch12.lab200_record_transformation

import org.apache.spark.sql.functions.{ expr, split, col }

import net.jgp.books.spark.basic.Basic

object RecordTransformation extends Basic {
  def run(): Unit = {
    val spark = getSession("Record transformations")

    val df = spark.read.format("csv").
      option("header", "true").
      option("inferSchema", "true").
      load("data/census/PEP_2017_PEPANNRES.csv")

    //df.printSchema
    //df.show(2, true)

    val interDF = df.drop("GEO.id").
      withColumnRenamed("GEO.id2", "id").
      withColumnRenamed("GEO.display-label", "label").
      withColumnRenamed("rescen42010", "real2010").
      drop("resbase42010")
    
    val cleanDF = (2010 to 2017).map(n => (s"respop7$n", s"est$n")).foldLeft(interDF)((tDF, co) => {
      tDF.withColumnRenamed(co._1, co._2)
    })

    //cleanDF.printSchema
    //cleanDF.show(2, true)

    val transDF = cleanDF.
      withColumn("countyState", split(col("label"), ", ")).
      withColumn("state", col("countyState").getItem(1)).
      withColumn("county", col("countyState").getItem(0)).
      drop("countyState").
      drop("label").
      withColumn("stateId", expr("int(id/1000)")).
      withColumn("countyId", expr("id%1000"))

    transDF.printSchema
    transDF.sample(.02).show(5, false)

    val statDF = transDF.
      withColumn("diff", expr("est2010 - real2010")).
      withColumn("growth", expr("est2017 - est2010")).
      drop("id").
      drop((2011 to 2016).map(n => s"est$n").toSeq: _*).
      sort(col("growth").desc)

    statDF.printSchema
    statDF.show(5, false)

    spark.close
  }
}