package org.lotds

import org.apache.spark.sql.functions._

object lotds extends App with org.sparksession.spark {

  // Lectura de los archivos
  val products = spark.read.parquet("src/main/resources/lotds/inputs/products_parquet/*")
  val sales = spark.read.parquet("src/main/resources/lotds/inputs/sales_parquet/*")
  val sellers = spark.read.parquet("src/main/resources/lotds/inputs/sellers_parquet/*")


  //Find out how many orders, how many products and how many sellers are in the data.
  //How many products have been sold at least once? Which is the product contained in more orders?
  WarmUp1()

  // How many distinct products have been sold in each day?
  WarmUp2()

  // What is the average revenue of the orders?
  Exercise1()

  // For each seller, what is the average % contribution of an order to the seller's daily quota?
  Exercise2()

  // Who are the second most selling and the least selling persons (sellers) for each product? Who are those for product with `product_id = 0`
  Exercise3()

  // Create a new column called "hashed_bill" defined as follows:
  //- if the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field, once for each 'A' (capital 'A') present in the text. E.g. if the bill text is 'nbAAnllA', you would apply hashing three times iteratively (only if the order number is even)
  //- if the order_id is odd: apply SHA256 hashing to the bill text
  //Finally, check if there are any duplicate on the new column
  Exercise4()

  private def WarmUp1() {

    println("\nWarm Up 1:\n")

    println("Number of Products: " + products.count())
    println("Number of Orders: " + sales.count())
    println("Number of sellers: " + sellers.count())

    println("Number of Products sold at least once: " + products.distinct().count())
//    println("Number of Products sold at least once: " + products.select(countDistinct("*")).collect()(0)(0))
    val prodInMoreOrders = sales
      .groupBy("product_id")
      .agg(count("*"))
      .orderBy(desc("count(1)"))
      .first()

    println("Product present in more Orders: " + prodInMoreOrders(0) + ", " + prodInMoreOrders(1) + " times.")

  }

  private def WarmUp2(){

    println("\nWarm Up 2\n")

    sales.groupBy("date")
      .agg(countDistinct("product_id").alias("sales"))
      .orderBy(desc("sales"))
      .show()

  }

  private def Exercise1(){

    println("\nExercise 1\n")

    val avgPrice = sales.join(products)
      .agg(avg(col("price") * col("num_pieces_sold")))

    println("Average Price: " + avgPrice.collect()(0)(0))

  }

  private def Exercise2(){


  }

  private def Exercise3(){


  }

  private def Exercise4(){


  }

}
