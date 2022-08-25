package org.learning

import org.apache.spark.sql.SparkSession

object Ejers5 {

  def main(args: Array[String]): Unit = {

    // Declaración de la sesión de Spark
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Ejers5")
      .getOrCreate()

    // Para ocultar los INFO y WARNING
    spark.sparkContext.setLogLevel("ERROR")






    // In Scala
    // Loading data from a JDBC source using load
    val jdbcDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .load()

  }

}
