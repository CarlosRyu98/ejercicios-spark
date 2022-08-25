package org.learning

import org.apache.spark.sql.SparkSession

object Ejers4 {

  def main(args: Array[String]): Unit = {

    // Declaración de la sesión de Spark
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Ejers4")
      .getOrCreate()

    // Para ocultar los INFO y WARNING
    spark.sparkContext.setLogLevel("ERROR")

  }

}
