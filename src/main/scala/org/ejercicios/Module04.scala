package org.ejercicios

import org.apache.spark.sql.SparkSession

object Module04 extends App {

  // Declaración de la Spark Session
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Module02")
    .getOrCreate()

  // Para ocultar los INFO y WARNING
  spark.sparkContext.setLogLevel("ERROR")

  // Parte 1
  println("--------------------------------------")
  println("Parte 1:")
  println

  // Número de peticiones por usuario
  var logs = spark.sparkContext.textFile("src/main/resources/inputs/weblogs/*")

  var userid  = logs.filter(_.contains(".html")).map(line => (line.split(' ')(2), 1))

  var usercount = userid.reduceByKey((v1, v2) => v1 + v2)

  // usercount.take(10).foreach(t => println(t._1 + " / " + t._2))
  val usercountorder = usercount.map(f => f.swap).sortByKey(ascending = false).map(f => f.swap)
  // usercountorder.take(10).foreach(println)

  var userips = logs.map(line => line.split(' ')).map(words => (words(2), words(0))).groupByKey()
  println(userips.take(10).foreach(println))

  // Parte 2
  println("--------------------------------------")
  println("Parte 2:")
  println

  var accounts = spark.sparkContext.textFile("src/main/resources/inputs/accounts.csv")
  accounts = accounts.map(line => line.split(',')).map(account => (account(0), account))

  var accounthits = accounts.join(usercountorder)


}
