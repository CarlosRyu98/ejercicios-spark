package org.ejercicios

import org.apache.spark.sql.SparkSession

object Module02 extends App {
  // Declaración de la Spark Session
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Module02")
    .getOrCreate()

  // Declaramos una variable para tener el contexto
  val sc = spark.sparkContext

  // Para ocultar los INFO y WARNING
  sc.setLogLevel("ERROR")

  // Parte 1
  println("--------------------------------------")
  println("Parte 1:")
  println

  // Guardamos el path donde están los datos
  val relato = sc.textFile("src/main/resources/relato.txt")

  println("Número de líneas de relato: " + relato.count())
  println("Relato con collect : " + relato.collect().mkString("Array(", ", ", ")"))
  println("Relato con foreach: ")
  relato.foreach(println)

  // Parte 2
  println("--------------------------------------")
  println("Parte 2:")
  println

  val logs = sc.textFile("src/main/resources/weblogs/0213-09-15.log")
  val jpglogs = logs.filter(x => x.contains(".jpg"))

}
