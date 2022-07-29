package org.ejercicios

import org.sameersingh.scalaplot.Implicits._
import org.sameersingh.scalaplot.gnuplot.GnuplotPlotter
import org.sameersingh.scalaplot.{MemXYSeries, XYData, XYChart}
import org.apache.spark.rdd
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.hive.HiveContext

object Module05 extends App {

  // Declaración de la Spark Session
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Module05")
    .getOrCreate()

  // Para ocultar los INFO y WARNING
  spark.sparkContext.setLogLevel("ERROR")

  // Parte 1
  println("--------------------------------------")
  println("Parte 1: JSON")
  println

  // Crear un contexto SQLContext
  println("Creando contexto SQLContext")
  var ssc = new SQLContext(spark.sparkContext)

  // Cargar el dataset zips
  println("Cargando dataset zips")
  var zips = ssc.load("src/main/resources/inputs/zips.json", "json")

  // Mostrar la carga
  zips.show()

  // Filtrar los datos donde la población sea mayor de 10.000
  zips.filter(zips("pop") > 10000).show()

  // Guardar una tabla zips en sql
  zips.registerTempTable("zips")

  // Mostrar los datos desde sql
  ssc.sql("select * " +
    "from zips where " +
    "pop > 10000 ")
    .show()

  // Mostrar la población de Wisconsin
  ssc.sql("select sum(pop) as population " +
    "from zips " +
    "where state='WI' ")
    .show()

  // Los 5 estados más poblados
  ssc.sql("select state, sum(pop) as population " +
    "from zips " +
    "group by state " +
    "order by sum(pop) desc ")
    .show(5)
/*
  // Parte 2
  println("--------------------------------------")
  println("Parte 2: Hive")
  println

  // Crear el contexto para hive
  val ssch = new HiveContext(spark.sparkContext)

  // Crear la base de datos
  ssch.sql("create database if not exists hivespark")

  // Crear la tabla
  ssch.sql("create table if hivespark.empleados(" +
    "id int, name string, age int) " +
    "row format delimited " +
    "fields terminated by ',' " +
    "lines terminated by '\n'")

  // Cargar los datos en la tabla
  ssch.sql("" +
    "load data local inpath 'src/main/resources/inputs/empleado.txt " +
    "into table hivespark.empleados")

  // Mostrar la query
  var query1 = ssch.sql("select * from hivespark.empleados")
  query1.show()*/


  // Parte 3
  println("--------------------------------------")
  println("Parte 3: DataFrames")
  println

  // Carga de datos
  println("Cargando datos")
  var datos = spark.sparkContext.textFile("src/main/resources/inputs/DataSetPartidos.txt")

  // Creación del schema
  println("Creando schema")
  val schemaString = "idPartido::temporada::jornada::EquipoLocal::EquipoVisitante::golesLocal::golesVisitante::fecha::timestamp"
  val schema = StructType(schemaString.split("::").map(fieldName =>
    StructField(fieldName, StringType, true)))

  // Pasar el RDD a Row
  println("Pasando rdd a row")
  val rowRDD = datos.map(_.split("::"))
    .map(p => Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8).trim))

  // Crear DataFrame
  println("Creando DataFrame")
  val partidosDataFrame = ssc.createDataFrame(rowRDD, schema)

  // Registrar tabla "partidos"
  println("Creando tabla partidos")
  partidosDataFrame.registerTempTable("partidos")

  // Obtener resultados
  val results = ssc.sql("select temporada, jornada, EquipoLocal, golesLocal, golesVisitante, EquipoVisitante " +
    "from partidos")
  results.show()

  // Record de goles como visitante en una temporada por el Oviedo
  val recordOviedo = ssc.sql("" +
    "select sum(golesVisitante) as goles, temporada " +
    "from partidos " +
    "where equipoVisitante='Real Oviedo' " +
    "group by temporada " +
    "order by goles desc")
  recordOviedo.show(1)

  // Quién ha estado más temporadas en 1ª entre Sporting y Oviedo
  val tempsOviSpo = ssc.sql("" +
    "select 'Real Oviedo' as Equipo, count(distinct(temporada)) Temps  " +
    "from partidos " +
    "where equipoLocal = 'Real Oviedo' or equipoVisitante = 'Real Oviedo' " +
    "union " +
    "select 'Sporting de Gijon' as Equipo, count(distinct(temporada)) as Temps " +
    "from partidos " +
    "where equipoLocal = 'Sporting de Gijon' or equipoVisitante = 'Sporting de Gijon'")
  tempsOviSpo.show()


  // Parte 4
  println("--------------------------------------")
  println("Parte 4: SparkSQL")
  println

  // Crear dataframe
  println("Creando DataFrame")
  val df = spark.sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema","true")
    .load("src/main/resources/inputs/simpsons.csv")
  df.show()

  // Registrar tabla
  println("Registrando tabla")
  df.registerTempTable("NombreVirtual")

  // Seleccionar datos
  println("Seleccionando datos")
  val datos2 = spark.sqlContext.sql("" +
    "select cast(season as int), mean(imdb_rating) " +
    "from NombreVirtual " +
    "group by season " +
    "order by cast(season as int) asc")

  datos2.show()

  // val rdd = datos2.rdd.map(line => (line.getDouble(0), line.getDouble(1)))
  val rdd = datos2.rdd.map(line => (line.getInt(0), line.getDouble(1)))

  val x = rdd.map({
    case (key, value) =>
      key.toDouble
  })

  val y = rdd.map({
    case (key, value) =>
      value
  })

  // Preparar el Chart
  println("Preparando Series")
  val series = new MemXYSeries(x.collect(), y.collect(), "puntuacion")
  println("Preparando Data")
  val data = new XYData(series)
  println("Preparando Chart")
  val chart = new XYChart("Media de puntuación de episodios de los Simpsons por temporada", data)
  // output(PNG("src/main/resources/outputs", "test"), chart)

}
