package org.learning

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object Chapter2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Chapter2")
      .getOrCreate()

    // Para ocultar los INFO y WARNING
    spark.sparkContext.setLogLevel("ERROR")

/*    if (args.length < 1) {
      print("Usage: MnMcount <mnm_file_dataset>")
      sys.exit(1)
    }

    val mnmFile = args(0)*/

    val mnmFile = "src/main/resources/learning/inputs/mnm_dataset.csv"

    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    val countMnMDF = mnmDF.select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(functions.count("Count").alias("Total"))
      .orderBy(desc("Total"))

    countMnMDF.show(10)
    println("Total Rows: ", countMnMDF.count())
    println()

    val caCountMnMDF = mnmDF.select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(functions.count("Count").alias("Total"))
      .orderBy(desc("Total"))

    caCountMnMDF.show(10)
    println("Total Rows: ", caCountMnMDF.count())
    println()

    spark.stop()

  }

}
