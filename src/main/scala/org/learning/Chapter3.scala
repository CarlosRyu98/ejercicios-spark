package org.learning

// Imports necesarios para el funcionamiento de los ejercicios
import org.apache.spark.sql.{Row, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.annotation.unused

object Chapter3 {

  def main(args: Array[String]): Unit = {

    // Declaración de la sesión de Spark
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Chapter3")
      .getOrCreate()

    // Para ocultar los INFO y WARNING
    spark.sparkContext.setLogLevel("ERROR")

    val dataDF = spark.createDataFrame(Seq(("Brooke", 20),
      ("Brooke", 25),
      ("Denny", 25),
      ("Jules", 30),
      ("TD", 35))).toDF("name", "age")

    val avgDF = dataDF.groupBy("name").agg(avg("age"))

    avgDF.show()

    @unused
    val schema1 = StructType(Array(StructField("author", StringType, nullable = false),
      StructField("title", StringType, nullable = false),
      StructField("pages", IntegerType, nullable = false)))

    @unused
    val schema2 = "author STRING, title STRING, pages INT"

    val jsonFile = "src/main/resources/learning/inputs/blogs.json"

    val schema3 = StructType(Array(StructField("Id", IntegerType, nullable = false),
      StructField("First", StringType, nullable = false),
      StructField("Last", StringType, nullable = false),
      StructField("Url", StringType, nullable = false),
      StructField("Published", StringType, nullable = false),
      StructField("Hits", IntegerType, nullable = false),
      StructField("Campaigns", ArrayType(StringType), nullable = false)))

    val blogsDF = spark.read.schema(schema3).json(jsonFile)

    blogsDF.show(false)
    println(blogsDF.printSchema)
    println(blogsDF.schema)

    println(blogsDF.columns.mkString("Columnas(", ", ", ")"))

    println(blogsDF.col("Id"))

    // Usar una expresión para computar un valor
    blogsDF.select(col("Hits") * 2).show(2)

    // Añadir una nueva columna con una expresión condicional
    blogsDF.withColumn("Big Hitters", expr("Hits > 10000")).show()

    // Concatenar 3 columnas en una nueva
    blogsDF
      .withColumn("AuthorsId", concat(expr("First"), expr("Last"), expr("Id")))
      .select(col("AuthorsId"))
      .show(4)

    // Ordenar la columna Id
    blogsDF.sort(col("Id").desc).show()
    // blogsDF.sort($"Id".desc).show()

    val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015", Array("twitter", "LinkedIn"))
    println(blogRow(1))

//    val rows = Seq(("Matei Zaharia", "CA"),
//      ("Reynold Xin", "CA"))
//
//    val authorsDF = rows.toDF("Author", "State")
//
//    authorsDF.show()

    val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, nullable = true),
      StructField("UnitID", StringType, nullable = true),
      StructField("IncidentNumber", IntegerType, nullable = true),
      StructField("CallType", StringType, nullable = true),
      StructField("CallDate", StringType, nullable = true),
      StructField("WatchDate", StringType, nullable = true),
      StructField("CallFinalDisposition", StringType, nullable = true),
      StructField("AvailableDtTm", StringType, nullable = true),
      StructField("Address", StringType, nullable = true),
      StructField("City", StringType, nullable = true),
      StructField("Zipcode", IntegerType, nullable = true),
      StructField("Battalion", StringType, nullable = true),
      StructField("StationArea", StringType, nullable = true),
      StructField("Box", StringType, nullable = true),
      StructField("OriginalPriority", StringType, nullable = true),
      StructField("Priority", StringType, nullable = true),
      StructField("FinalPriority", IntegerType, nullable = true),
      StructField("ALSUnit", BooleanType, nullable = true),
      StructField("CallTypeGroup", StringType, nullable = true),
      StructField("NumAlarms", IntegerType, nullable = true),
      StructField("UnitType", StringType, nullable = true),
      StructField("UnitSequenceInCallDispatch", IntegerType, nullable = true),
      StructField("FirePreventionDistrict", StringType, nullable = true),
      StructField("SupervisorDistrict", StringType, nullable = true),
      StructField("Neighborhood", StringType, nullable = true),
      StructField("Location", StringType, nullable = true),
      StructField("RowID", StringType, nullable = true),
      StructField("Delay", FloatType, nullable = true)))


    // Read the file using the CSV DataFrameReader
    val sfFireFile="src/main/resources/learning/inputs/sf-fire-calls.csv"
    val fireDF = spark.read.schema(fireSchema)
      .option("header", "true")
      .csv(sfFireFile)

    println(fireDF.printSchema)

    val fewFireDF = fireDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where(col("CallType") =!= "Medical Incident")

    fewFireDF.show(5, truncate = false)

    fireDF.select("CallType")
      .where(col("CallType").isNotNull)
      .agg(countDistinct("CallType") as "DistinctCallTypes")
      .show()

    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .distinct()
      .show(10, truncate = false)

    val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
    newFireDF
      .select("ResponseDelayedinMins")
      .where(col("ResponseDelayedinMins") > 5)
      .show(5, truncate = false)

    val fireTsDF = newFireDF
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
        "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")
    // Select the converted columns
    fireTsDF
      .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
      .show(5, truncate = false)

    fireTsDF
      .select(year(col("IncidentDate")))
      .distinct()
      .orderBy(year(col("IncidentDate")))
      .show()

    fireTsDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .show(10, truncate = false)

    fireTsDF
      .select(functions.sum("NumAlarms"),
        functions.avg("ResponseDelayedinMins"),
        functions.min("ResponseDelayedinMins"),
        functions.max("ResponseDelayedinMins"))
      .show()

  }

}
