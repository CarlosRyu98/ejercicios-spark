package org.ejercicios

import org.apache.spark.sql.SparkSession

object Module01 extends App {
  // Declaración de la Spark Session
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Module01")
    .getOrCreate()

  println("First SparkContext:")
  println("APP Name: "+spark.sparkContext.appName)
  println("Deploy Mode: "+spark.sparkContext.deployMode)
  println("Master: "+spark.sparkContext.master)
}
