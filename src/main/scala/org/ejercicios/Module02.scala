package org.ejercicios

import org.apache.spark.sql.SparkSession

object Module02 extends App {
  // Declaración de la Spark Session
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Module02")
    .getOrCreate()

  // Declaramos una variable para tener el contexto
  //val sc = spark.sparkContext

  // Para ocultar los INFO y WARNING
  spark.sparkContext.setLogLevel("ERROR")

  // Parte 1
  println("--------------------------------------")
  println("Parte 1:")
  println

  // Guardamos el path donde están los datos
  val relato = spark.sparkContext.textFile("src/main/resources/inputs/relato.txt")

  println("Número de líneas de relato: " + relato.count())
  println("Relato con collect : " + relato.collect().mkString("Array(", ", ", ")"))
  println("Relato con foreach: ")
  relato.foreach(println)

  // Parte 2
  println("--------------------------------------")
  println("Parte 2:")
  println

  val logs = spark.sparkContext.textFile("src/main/resources/inputs/weblogs/2013-09-15.log")
  val jpglogs = logs.filter(x => x.contains(".jpg"))
  println(jpglogs.take(5).mkString("Array(", ", ", ")"))

  val jpglogs2 = jpglogs.count()
  println("Número de líneas con la cadena .jpg: ", jpglogs2)

  println(logs.map(x => x.length).take(5).mkString("Longitud de los 5 primeros: (", ", ", ")"))

  println("Número de logs con .jpg: ", logs.filter(line =>
      line.contains(".jpg"))
    .count())

  var ips = logs.map(line => line.split(' ')(0))
  println(ips.take(10).mkString("IPS: (", ", ", ")"))

  ips.saveAsTextFile("src/main/resources/outputs/iplist")

  // Parte 3
  println("--------------------------------------")
  println("Parte 3:")
  println

  var alllogs = spark.sparkContext.textFile("src/main/resources/inputs/weblogs/*")
  alllogs.map(line => line.split(' ')(0)).saveAsTextFile("src/main/resources/outputs/iplistw")

  var htmllogs  = alllogs.filter(_.contains(".html")).map(line => (line.split(' ')(0), line.split(' ')(2)))
  println("IPs e IDs: ")
  htmllogs.take(5).foreach(t => println(t._1 + " / " + t._2))
}
