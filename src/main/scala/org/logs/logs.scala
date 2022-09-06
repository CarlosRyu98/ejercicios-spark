package org.logs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object logs {

  def main(args: Array[String]): Unit ={

    // Declaración de la sesión de Spark
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("logs")
      .getOrCreate()

    // Para ocultar los INFO y WARNING
    spark.sparkContext.setLogLevel("ERROR")

    // Lectura de los archivos
    val logs_csv = spark.read.csv("src/main/resources/logs/inputs/*")

    // Preparación del RegEx
    val regex = """(\S+)\s(-|\S+)\s(-|\S+)\s\[(\S+)\s-0400\]\s\"(\S+)\s(\S+)\s(\S+)\"\s(\d+)\s(\d+)"""
    // val regex = """(\S+)\s(-|\S+)\s(-|\S+)\s\[(\S+)\s-\d+\]\s\"(\S+)\s(\S+)\s(\S+)\"\s(\d+)\s(\d+)"""

    val logs_df_raw = logs_csv.select(
      regexp_extract(col("_c0"), regex, 1).alias("host"),
      regexp_extract(col("_c0"), regex, 2).alias("user-identifier"),
      regexp_extract(col("_c0"), regex, 3).alias("userid"),
      regexp_extract(col("_c0"), regex, 4).alias("date"),
      regexp_extract(col("_c0"), regex, 5).alias("request-method"),
      regexp_extract(col("_c0"), regex, 6).alias("resource"),
      regexp_extract(col("_c0"), regex, 7).alias("protocol"),
      regexp_extract(col("_c0"), regex, 8).cast("integer").alias("http-status-code"),
      regexp_extract(col("_c0"), regex, 9).cast("integer").alias("size")
    )

    // Pasar la columna date a formato timestamp
    val logs_df = logs_df_raw.withColumn("date", to_timestamp(logs_df_raw("date"), "dd/MMM/yyyy:HH:mm:ss"))
    logs_df.show(5)

    //¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.
    println("¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.")
    logs_df.select("protocol")
      .distinct()
      .show()

    //¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos para ver cuál es el más común.
    println("¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos para ver cuál es el más común.")
    logs_df.select("http-status-code")
      .groupBy("http-status-code")
      .count()
      .orderBy("http-status-code")
      .show()

    //¿Y los métodos de petición(verbos) más utilizados?
    println("¿Y los métodos de petición(verbos) más utilizados?")
    logs_df.select("request-method")
      .groupBy("request-method")
      .count()
      .orderBy("request-method")
      .show()

    //¿Qué recurso tuvo la mayor transferencia de bytes de la página web?
    println("¿Qué recurso tuvo la mayor transferencia de bytes de la página web?")
    logs_df.select("resource", "size")
      .orderBy(desc("size"))
      .show(1)

    // Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es decir el recurso con más registros en nuestro log
    println("Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es decir el recurso con más registros en nuestro log")
    logs_df.groupBy("resource")
      .count()
      .orderBy(desc("count"))
      .show(1)

    //¿Qué días la web recibió más tráfico?
    println("¿Qué días la web recibió más tráfico?")
    logs_df.select(dayofmonth(logs_df("date")).alias("day"), month(logs_df("date")).alias("month"))
      .groupBy("day", "month")
      .count()
      .orderBy(desc("count"))
      .show()

    //¿Cuáles son los hosts son los más frecuentes?
    println("¿Cuáles son los hosts son los más frecuentes?")
    logs_df.groupBy("host")
      .count()
      .orderBy(desc("count"))
      .show()

    //¿A qué horas se produce el mayor número de tráfico en la web?
    println("¿A qué horas se produce el mayor número de tráfico en la web?")
    logs_df.select(hour(logs_df("date")).alias("hour"))
      .groupBy("hour")
      .count()
      .orderBy(desc("count"))
      .show()

    //¿Cuál es el número de errores 404 que ha habido cada día?
    println("¿Cuál es el número de errores 404 que ha habido cada día?")
    logs_df.select(col("http-status-code"), dayofmonth(logs_df("date")).alias("day"), month(logs_df("date")).alias("month"))
      .filter(col("http-status-code") === "404")
      .groupBy("day", "month")
      .count()
      .orderBy(desc("count"))
      .show()

  }

}
