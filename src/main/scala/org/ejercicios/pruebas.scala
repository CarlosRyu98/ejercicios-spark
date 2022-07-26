package org.ejercicios

import org.apache.spark.sql.SparkSession

object pruebas extends App {

  // Declaración de la Spark Session
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Module04")
    .getOrCreate()

  // Para ocultar los INFO y WARNING
  spark.sparkContext.setLogLevel("ERROR")

  val ruta = "src/main/resources/inputs/weblogs/2013-09-15.log"
  val logs = spark.sparkContext.textFile(ruta)
  val jpglogs = logs.filter(x=>x.contains(".jpg"))
  println(jpglogs.take(5).mkString("JPG Logs: (", ", ", ")"))
  //anidar varios metodos en una linea
  val jpglogscount = logs.filter(x=>x.contains(".jpg")).count()

  //calcula al longitud de las primeras 5 lineas tambien vale length()
  val longitud = logs.map(x=>x.size)
  println(longitud.take(5).mkString("Longitud(", ", ", ")"))

  //mostrar las palabras de las 5 primeras linas
  val palabras = logs.map(x=>x.split(' '))
  palabras.take(5).foreach{
    x => println(x.mkString("Array: (", ", ", ")"))
  }

}
