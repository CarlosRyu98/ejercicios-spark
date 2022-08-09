package org.learning

// Imports necesarios para el funcionamiento de los ejercicios
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Ejers3 {

  def main(args: Array[String]): Unit = {

    // Declaración de la sesión de Spark
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Ejers3")
      .getOrCreate()

    // Para ocultar los INFO y WARNING
    spark.sparkContext.setLogLevel("ERROR")

    println("--- EJERCICIOS BOMBEROS ---")
    println

    // Lectura del fichero de llamadas a los bomberos
    val fireDF = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load("src/main/resources/learning/inputs/sf-fire-calls.csv")

    // Todos los diferentes tipos de llamadas en 2018
    println
    println("Todos los diferentes tipos de llamadas en 2018")
    fireDF.select("CallType")
      .filter(year(to_date(col("CallDate"), "dd/MM/yyyy")) === 2018)
      .distinct()
      .show(false)

    // Meses con mayor número de llamadas en 2018
    println
    println("Meses con mayor número de llamadas en 2018")
    fireDF.select(month(to_date(col("CallDate"), "dd/MM/yyyy")).alias("Month"))
      .filter(year(to_date(col("CallDate"), "dd/MM/yyyy")) === 2018)
      .groupBy("Month")
      .count()
      .orderBy(desc("count"))
      .show(false)

    // Barrios de San Francisco con mayor número de llamadas en 2018
    println
    println("Barrios de San Francisco con mayor número de llamadas en 2018")
    fireDF.select("Neighborhood")
      .filter(year(to_date(col("CallDate"), "dd/MM/yyyy")) === 2018)
      .filter(col("City") === "San Francisco")
      .groupBy("Neighborhood")
      .count()
      .orderBy(desc("count"))
      .show(false)

    // Barrios con peor tiempo de respuesta en 2018
    println
    println("Barrios con peor tiempo de respuesta en 2018")
    fireDF.select("Neighborhood", "Delay")
      .filter(year(to_date(col("CallDate"), "dd/MM/yyyy")) === 2018)
      .groupBy("Neighborhood")
      .agg(functions.avg("Delay").alias("Delay"))
      .orderBy(desc("Delay"))
      .show(false)

    // Correlación entre Barrio ZipCode y número de llamadas
    println
    println("Semana del año con más llamadas en 2018")
    fireDF.select(weekofyear(to_date(col("CallDate"), "dd/MM/yyyy")).alias("Week"))
      .filter(year(to_date(col("CallDate"), "dd/MM/yyyy")) === 2018)
      .groupBy("Week")
      .count()
      .orderBy(desc("count"))
      .show(false)

    // Correlación entre Barrio ZipCode y número de llamadas
    println
    println("Correlación entre Barrio ZipCode y número de llamadas")
    fireDF.select("Neighborhood", "Zipcode")
      .groupBy("Neighborhood", "Zipcode")
      .count()
      .orderBy(desc("count"))
      .show(false)

    println
    println("Guardando datos...")
    println("Guardando en parquet... ")
    fireDF.write.format("parquet").mode("overwrite").save("src/main/resources/learning/outputs/fireDF/fireDF_parquet")
    println("Guardadno en json... ")
    fireDF.write.format("json").mode("overwrite").save("src/main/resources/learning/outputs/fireDF/fireDF_json")
    println("Guardando en csv...")
    fireDF.write.format("csv").mode("overwrite").save("src/main/resources/learning/outputs/fireDF/fireDF_csv")
    //println("Guadando en avro... ")
    // fireDF.write.format("avro").save("src/main/resources/learning/outputs/fireDF/fireDF_avro")

  }

}
