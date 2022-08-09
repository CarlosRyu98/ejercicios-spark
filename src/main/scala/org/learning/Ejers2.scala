package org.learning

// Imports necesarios para el funcionamiento de los ejercicios
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._

object Ejers2 {

  def main(args: Array[String]): Unit = {

    // Declaración de la sesión de Spark
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Ejers2")
      .getOrCreate()

    // Para ocultar los INFO y WARNING
    spark.sparkContext.setLogLevel("ERROR")

    println("---EJERCICIO QUIJOTE---")
    println

    // Preparación de datos
    val quixDF = spark.read.text("src/main/resources/learning/inputs/el_quijote.txt")
    println("El número de líneas es: " + quixDF.count())

    // Diferentes formas de visualizar los datos
    println
    println("* Visualización con show()")
    quixDF.show(5, truncate = false)
    println
    println("* Visualización con take()")
    quixDF.take(5).foreach(println)
    println
    println("* Visualización con head()")
    quixDF.head(5).foreach(println)
    println
    println("* Visualización con first")
    println(quixDF.first)

    println
    println("---EJERCICIO M&M---")

    // Lectura del archivo
    val mnmFile = "src/main/resources/learning/inputs/mnm_dataset.csv"
    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    // Uso de funciones de agregación
    println
    println("Uso de funciones de agregación:")
    val countMnMDF = mnmDF.select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(functions.min("Count").alias("Mínimo"),
        functions.max("Count").alias("Máximo"),
        functions.avg("Count").alias("Average"),
        functions.count("Count").alias("Total"))
      .orderBy(asc("Average"))

    // Mostrar resultados
    countMnMDF.show(10)
    println("Total Rows: " + countMnMDF.count())
    println()

    // Selección del total por estado y color en california
    println
    println("Uso del where con varias opciones y varias funciones:")
    val caCountMnMDF = mnmDF.select("State", "Color", "Count")
      .where(col("State") === "NV" or
        col("State") === "TX" or
        col("State") === "CA" or
        col("State") === "CO")
      .groupBy("State", "Color")
      .agg(functions.min("Count").alias("Mínimo"),
        functions.max("Count").alias("Máximo"),
        functions.avg("Count").alias("Average"),
        functions.count("Count").alias("Total"))
      .orderBy(asc("Average"))

    // Mostrar resultados
    caCountMnMDF.show(10)
    println("Total Rows: " + caCountMnMDF.count())
    println()

    spark.stop()

  }

}
