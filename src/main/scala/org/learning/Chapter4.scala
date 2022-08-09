// COMPROBAR POM O POR QUÉ NO FUNCIONA


package org.learning

// Imports necesarios para el funcionamiento de los ejercicios
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object Chapter4 {

  def main(args: Array[String]): Unit = {

    // Declaración de la sesión de Spark
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Chapter4")
      .config("spark.sql.catalogImplementation","hive")
      .getOrCreate()

    // Para ocultar los INFO y WARNING
    spark.sparkContext.setLogLevel("ERROR")

    // Path to data set
    val csvFile="src/main/resources/learning/inputs/departuredelays.csv"
    // Read and create a temporary view
    // Infer schema (note that for larger files you may want to specify the schema)
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)
    // Create a temporary view
    df.createOrReplaceTempView("us_delay_flights_tbl")

    spark.sql("""DROP DATABASE IF EXISTS learn_spark_db CASCADE""")

    spark.sql("""CREATE DATABASE learn_spark_db""")
    spark.sql("""USE learn_spark_db""")

    spark.sql("""CREATE TABLE managed_us_delay_flights_tbl
          (date STRING, delay INT, distance INT, origin STRING, destination STRING)""")

    spark.sql("SELECT * FROM managed_us_delay_flights_tbl").show(10)

    spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
             distance INT, origin STRING, destination STRING)
             USING csv OPTIONS (PATH
             'src/main/resources/learning/inputs/departuredelays.csv')""")

    spark.sql("SELECT * FROM us_delay_flights_tbl").show(10)

    spark.catalog.listDatabases().show(false)
    spark.catalog.listTables().show(false)
    spark.catalog.listColumns("us_delay_flights_tbl").show(false)

     // In Scala
     // Use Parquet
     val file1 = "src/main/resources/learning/inputs/flights/summary-data/parquet/2010-summary.parquet"
     val df1 = spark.read.format("parquet").load(file1)
     // Use Parquet; you can omit format("parquet") if you wish as it's the default
     val df2 = spark.read.load(file1)
     // Use CSV
     val df3 = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .load("src/main/resources/learning/inputs/flights/summary-data/csv/*")
     // Use JSON
     val df4 = spark.read.format("json")
       .load("src/main/resources/learning/inputs/flights/summary-data/json/*")

    // Leyendo un fichero Parquet a DataFrame
    val file2 = "src/main/resources/learning/inputs/flights/summary-data/parquet/2010-summary.parquet/"
    val df5 = spark.read.format("parquet").load(file2)
    df5.show()

    // Leyendo un fichero a tabla SQL
    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
             USING parquet
             OPTIONS (
             path 'src/main/resources/learning/inputs/flights/summary-data/parquet/2010-summary.parquet/' )""")

    spark.sql("""SELECT * FROM us_delay_flights_tbl""").show()

    // Escribiendo DataFrames en archivos Parquet
    df5.write.format("parquet")
      .mode("overwrite")
      .save("src/main/resources/learning/outputs/flights/parquet/df_parquet")

    // Escribiendo DataFrames a tabla SQL
    df5.write
      .mode("overwrite")
      .saveAsTable("us_delay_flights_tbl")


    spark.sql("""SELECT * FROM us_delay_flights_tbl""").show()

    // Leyendo un fichero Json a DataFrame
    val file3 = "src/main/resources/learning/inputs/flights/summary-data/json/*"
    val df6 = spark.read.format("json").load(file3)
    df6.show()

    // Leyendo un fichero a tabla SQL
    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
             USING json
             OPTIONS (
             path 'src/main/resources/learning/inputs/flights/summary-data/json/*' )""")

    spark.sql("""SELECT * FROM us_delay_flights_tbl""").show()

    // Escribiendo DataFrames en archivos Json
    df6.write.format("json")
      .mode("overwrite")
      .save("src/main/resources/learning/outputs/flights/json/df_json")

    // Escribiendo DataFrames a tabla SQL
    df6.write
      .mode("overwrite")
      .saveAsTable("us_delay_flights_tbl")


    spark.sql("""SELECT * FROM us_delay_flights_tbl""").show()

    // Leyendo un fichero csv a DataFrame
    val file4 = "src/main/resources/learning/inputs/flights/summary-data/csv/*"
    val df7 = spark.read.format("csv")
      .option("header", "true")
      .option("nullValue", "")
      .load(file4)
    df7.show()

    // Leyendo un fichero a tabla SQL
    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
             USING csv
             OPTIONS (
             path 'src/main/resources/learning/inputs/flights/summary-data/csv/*' )""")

    spark.sql("""SELECT * FROM us_delay_flights_tbl""").show()

    // Escribiendo DataFrames en archivos csv
    df7.write.format("csv")
      .mode("overwrite")
      .save("src/main/resources/learning/outputs/flights/csv/df_csv")

    // Escribiendo DataFrames a tabla SQL
    df7.write
      .mode("overwrite")
      .saveAsTable("us_delay_flights_tbl")


    spark.sql("""SELECT * FROM us_delay_flights_tbl""").show()

//    // Leyendo un fichero csv a DataFrame
//    val file5 = "src/main/resources/learning/inputs/flights/summary-data/avro/*"
//    val df8 = spark.read.format("avro").load(file5)
//    df8.show()
//
//    // Leyendo un fichero a tabla SQL
//    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
//             USING avro
//             OPTIONS (
//             path 'src/main/resources/learning/inputs/flights/summary-data/avro/*' )""")
//
//    spark.sql("""SELECT * FROM us_delay_flights_tbl""").show()
//
//    // Escribiendo DataFrames en archivos csv
//    df8.write.format("avro")
//      .mode("overwrite")
//      .save("src/main/resources/learning/outputs/flights/avro/df_avro")
//
//    // Escribiendo DataFrames a tabla SQL
//    df8.write
//      .mode("overwrite")
//      .saveAsTable("us_delay_flights_tbl")
//
//
//    spark.sql("""SELECT * FROM us_delay_flights_tbl""").show()

    spark.stop()

  }

}
