package org.padron

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object padron {

  def main(args: Array[String]): Unit = {

    // Declaración de la sesión de Spark
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("padron")
      .getOrCreate()

    // Para ocultar los INFO y WARNING
    spark.sparkContext.setLogLevel("ERROR")


    // 6.1) Comenzamos realizando la misma práctica que hicimos en Hive en Spark,
    // importando el csv.Sería recomendable intentarlo con opciones que quiten las "" de los campos,
    // que ignoren los espacios innecesarios en los campos, que sustituyan los valores vacíos por 0 y que infiera el esquema.
    println("* 6.1) Comenzamos realizando la misma práctica que hicimos en Hive en Spark, importando el csv." +
      "\nSería recomendable intentarlo con opciones que quiten las \"\" de los campos," +
      "\nque ignoren los espacios innecesarios en los campos, que sustituyan los valores vacíos por 0 y que infiera el esquema.")
    val padron_df_raw = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .option("quotes", "\"")
      .option("ignoreTrailingWhiteSpace", "true") // No funciona
      .option("ignoreLeadingWhiteSpace", "true") // No funciona
      .option("emptyValue", 0)
      .load("src/main/resources/padron/inputs/Rango_Edades_Seccion_202208.csv")

    val padron_df = padron_df_raw.select(
      col("COD_DISTRITO"), trim(col("DESC_DISTRITO")).alias("DESC_DISTRITO"),
      col("COD_DIST_BARRIO"), trim(col("DESC_BARRIO")).alias("DESC_BARRIO"),
      col("COD_DIST_SECCION"), col("COD_SECCION"), col("COD_EDAD_INT"),
      col("EspanolesHombres"), col("EspanolesMujeres"), col("ExtranjerosHombres"), col("ExtranjerosMujeres"))

    // 6.3) Enumera todos los barrios diferentes.
    println("6.3) Enumera todos los barrios diferentes.")
    val barrios = padron_df.select("DESC_BARRIO").distinct()
    barrios.show()

    // 6.4) Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barrios diferentes que hay.
    println("6.4) Crea una vista temporal de nombre \"padron\" y a través de ella cuenta el número de barrios diferentes que hay.")
    padron_df.createOrReplaceTempView("padron")

    spark.sqlContext.sql("select count(distinct DESC_BARRIO) from padron").show()

    // 6.5) Crea una nueva columna que muestre la longitud de los campos de la columna DESC_DISTRITO y que se llame "longitud".
    println("6.5) Crea una nueva columna que muestre la longitud de los campos de la columna DESC_DISTRITO y que se llame \"longitud\".")
    val padron_long = padron_df.withColumn("longitud", length(col("DESC_DISTRITO")))
    padron_long.show()

    // 6.6) Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla.
    println("6.6) Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla.")
    val padron_5 = padron_df.withColumn("Extra", lit(5))
    padron_5.show()

    // 6.7) Borra esta columna.
    println("6.7) Borra esta columna.")
    val padron_drop = padron_5.drop("Extra")
    padron_drop.show()

    // 6.8) Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO.
    println("6.8) Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO.")
    val padron_part = padron_df.repartition(padron_df("DESC_DISTRITO"), padron_df("DESC_BARRIO"))
    padron_part.show()

    // 6.9) Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estado de los rdds almacenados.
    println("6.9) Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estado de los rdds almacenados.")
    padron_part.cache()
    padron_part.count()
    padron_part.count()

    // 6.10) Lanza una consulta contra el DF resultante en la que muestre el número total de
    // "espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres" para cada barrio de cada distrito.
    // Las columnas distrito y barrio deben ser las primeras en aparecer en el show.
    // Los resultados deben estar ordenados en orden de más a menos según la columna "extranjerosmujeres"
    // y desempatarán por la columna "extranjeroshombres".
    println("6.10) Lanza una consulta contra el DF resultante en la que muestre el número total de" +
      "\n\"espanoleshombres\", \"espanolesmujeres\", extranjeroshombres\" y \"extranjerosmujeres\" para cada barrio de cada distrito." +
      "\nLas columnas distrito y barrio deben ser las primeras en aparecer en el show." +
      "\nLos resultados deben estar ordenados en orden de más a menos según la columna \"extranjerosmujeres\"" +
      "\ny desempatarán por la columna \"extranjeroshombres\".")
    padron_part.select("DESC_DISTRITO", "DESC_BARRIO", "EspanolesHombres", "EspanolesMujeres", "ExtranjerosHombres", "ExtranjerosMujeres")
      .groupBy("DESC_DISTRITO", "DESC_BARRIO")
      .sum("EspanolesHombres", "EspanolesMujeres", "ExtranjerosHombres", "ExtranjerosMujeres")
      .orderBy(sum("ExtranjerosMujeres"), sum("ExtranjerosHombres"))
      .show()

    // 6.11) Elimina el registro en caché.
    println("6.11) Elimina el registro en caché.")
    padron_part.unpersist()

    // 6.12) Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con DESC_BARRIO,
    // otra con DESC_DISTRITO y otra con el número total de "espanoleshombres" residentes en cada distrito de cada barrio.
    // Únelo (con un join) con el DataFrame original a través de las columnas en común.
    println("6.12) Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con DESC_BARRIO," +
      "\notra con DESC_DISTRITO y otra con el número total de" +
      "\n\"espanoleshombres\" residentes en cada distrito de cada barrio." +
      "\nÚnelo(con un join) con el DataFrame original a través de las columnas en común.")
    val padron_esphom = padron_df.select("DESC_BARRIO", "DESC_DISTRITO", "EspanolesHombres")
      .groupBy("DESC_BARRIO", "DESC_DISTRITO")
      .sum("EspanolesHombres")

    padron_esphom.join(padron_df, padron_df("DESC_BARRIO") === padron_esphom("DESC_BARRIO") && padron_df("DESC_DISTRITO") === padron_esphom("DESC_DISTRITO"), "inner")
      .select("EspanolesHombres", "sum(EspanolesHombres)")
      .show()

    // 6.13) Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....)).
    println("6.13) Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....)).")
    padron_df.withColumn("Suma", sum("EspanolesHombres") over Window.partitionBy("DESC_BARRIO", "DESC_DISTRITO"))
      .select("DESC_BARRIO", "DESC_DISTRITO", "Suma")
      .distinct
      .show()

    // 6.14) Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que contenga los valores totales (la suma de valores)
    // de espanolesmujeres para cada distrito y en cada rango de edad COD_EDAD_INT).
    // Los distritos incluidos deben ser únicamente CENTRO, BARAJAS y RETIRO y deben figurar como columnas.
    // El aspecto debe ser similar a este:
    println("6.14) Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que contenga los valores totales (la suma de valores)" +
      "\nde espanolesmujeres para cada distrito y en cada rango de edad COD_EDAD_INT)." +
      "\nLos distritos incluidos deben ser únicamente CENTRO, BARAJAS y RETIRO y deben figurar como columnas." +
      "\nEl aspecto debe ser similar a este:")
    val padron_pivot = padron_df.groupBy("COD_EDAD_INT")
      .pivot("DESC_DISTRITO", Seq("BARAJAS", "CENTRO", "RETIRO"))
      .avg("EspanolesMujeres")
      .orderBy("COD_EDAD_INT")
    padron_pivot.show()

    // 6.15) Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje de la suma de "espanolesmujeres"
    // en los tres distritos para cada rango de edad representa cada uno de los tres distritos. Debe estar redondeada a 2 decimales.
    // Puedes imponerte la condición extra de no apoyarte en ninguna columna auxiliar creada para el caso.
    println("6.15) Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje de la suma de \"espanolesmujeres\"" +
      "\nen los tres distritos para cada rango de edad representa cada uno de los tres distritos. Debe estar redondeada a 2 decimales." +
      "\nPuedes imponerte la condición extra de no apoyarte en ninguna columna auxiliar creada para el caso.")
    val padron_percent = padron_pivot
      .withColumn("BARAJAS_PERCENT", round(col("BARAJAS") / (col("BARAJAS") + col("CENTRO") + col("RETIRO")) * 100, 2))
      .withColumn("CENTRO_PERCENT", round(col("CENTRO") / (col("BARAJAS") + col("CENTRO") + col("RETIRO")) * 100, 2))
      .withColumn("RETIRO_PERCENT", round(col("RETIRO") / (col("BARAJAS") + col("CENTRO") + col("RETIRO")) * 100, 2))
    padron_percent.show()

    // 6.16) Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un directorio local.
    // Consulta el directorio para ver la estructura de los ficheros y comprueba que es la esperada.
    println("6.16) Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un directorio local." +
      "\nConsulta el directorio para ver la estructura de los ficheros y comprueba que es la esperada.")
    padron_part.write
      .format("csv")
      .mode("overwrite")
      .save("src/main/resources/padron/outputs/PadronCSV")

    // 6.17) Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el resultado anterior.
    println("6.17) Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el resultado anterior.")
    padron_part.write
      .format("parquet")
      .mode("overwrite")
      .save("src/main/resources/padron/outputs/PadronParquet")

  }

}
