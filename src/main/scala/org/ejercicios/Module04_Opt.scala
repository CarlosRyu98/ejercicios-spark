package org.ejercicios

import org.apache.spark.sql.SparkSession

object Module04_Opt extends App {

  // Declaración de la Spark Session
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Module04")
    .getOrCreate()

  // Para ocultar los INFO y WARNING
  spark.sparkContext.setLogLevel("ERROR")

  // Preparar el stopwords
  println("Preparando stop-words")
  var stopwords = spark.sparkContext.textFile("src/main/resources/inputs/stop-word-list.csv")
  stopwords = stopwords.flatMap(line => line.split(","))
  stopwords = stopwords.map(word => word.replace(" ", ""))

  // Leer shakespeare y limpiar rdd
  println("Leyendo y limpiando logs")
  var logs = spark.sparkContext.textFile("src/main/resources/inputs/shakespeare/*")
  logs = logs.map(line => line.replaceAll("[^a-zA-z]+", " "))
  logs = logs.flatMap(line => line.split(" "))
  logs = logs.map(word => word.toLowerCase)

  // Quitar las stop-words
  println("Aplicando stop-words")
  logs = logs.subtract(spark.sparkContext.parallelize(Seq(" ")))
  logs = logs.subtract(stopwords)

  // Agrupar y preparar datos para mostrar
  println("Preparando resultado")
  var logs2 = logs.map(word => (word, 1)).reduceByKey(_+_)
  var logs3 = logs2.map(word => word.swap).sortByKey(ascending = false).map(_.swap)
  var logs4 = logs3.filter(word => word._1.length > 1)

  println("Conteo:")
  println(logs4.take(10).foreach(println))

}
