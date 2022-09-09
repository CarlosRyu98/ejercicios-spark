package org.sparksession

import org.apache.spark.sql.SparkSession

trait spark {
  // Declaración de la sesión de Spark
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("spark session")
    .getOrCreate()

  // Para ocultar los INFO y WARNING
  spark.sparkContext.setLogLevel("ERROR")

}
