package org.ejercicios

import org.apache.spark.sql.SparkSession

object Module04 extends App {

  // Declaración de la Spark Session
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Module04")
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

  var data = spark.sparkContext.textFile("src/main/resources/inputs/accounts.csv")
  var accounts = data.map(line => line.split(',')).map(account => (account(0), account))

  accounts.take(5).foreach(println)

  var accounthits = accounts.join(usercountorder)

  // accounthits.take(5).foreach(println)

  var accdetails = data.map(line => line.split(',')).map(account => (account(0), account(4), account(5)))

  accdetails.take(5).foreach(println)

  // Parte 3
  println("--------------------------------------")
  println("Parte 3:")
  println

  var accountsByPCode = data.map(_.split(',')).keyBy(_(8))
  accountsByPCode.take(5).foreach{
    case(x, y) => println("\n---" + x)
      y.foreach(println)
  }

  var namesByPCode = accountsByPCode.mapValues(values => values(4) + ',' + values(3)).groupByKey()
  namesByPCode.take(5).foreach{
    case(x, y) => println("---" + x)
    y.foreach(println)
  }

}
