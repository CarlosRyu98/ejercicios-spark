package org.ejercicios

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Module06 extends App {

  // Declaración de la Spark Session
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Module06")
    .getOrCreate()

  // Para ocultar los INFO y WARNING
  spark.sparkContext.setLogLevel("ERROR")

  // Crear contexto de Streaming
  var ssc = new StreamingContext(spark.sparkContext, Seconds(5))

  // Crear un socket para leer un streaming de entrada
  var mystream = ssc.socketTextStream("localhost", 4444)

  // Separar las palabras, contarlas y devolver el resultado
  var words = mystream.flatMap(line => line.split("\\W"))
  var wordCounts = words.map(x => (x, 1)).reduceByKey((x,y) => (x+y))
  wordCounts.print()

  // Arrancar el streaming context y esperar a que termine
  ssc.start()
  ssc.awaitTermination()

}
