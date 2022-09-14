package org.lotds

import org.apache.spark.sql.expressions.Window
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
  //- if the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field, once for each 'A' (capital 'A') present in the text.
  // E.g. if the bill text is 'nbAAnllA', you would apply hashing three times iteratively (only if the order number is even)
  //- if the order_id is odd: apply SHA256 hashing to the bill text
  //Finally, check if there are any duplicate on the new column
  Exercise4()

  private def WarmUp1(): Unit = {

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

  private def WarmUp2(): Unit = {

    println("\nWarm Up 2\n")

    sales.groupBy("date")
      .agg(countDistinct("product_id").alias("sales"))
      .orderBy(desc("sales"))
      .show()

  }

  private def Exercise1(): Unit = {

    println("\nExercise 1\n")

    val avgPrice = sales.join(products, "product_id")
      .agg(avg(products("price") * sales("num_pieces_sold")))

    println("Average Price: " + avgPrice.collect()(0)(0))

  }

  private def Exercise2(): Unit = {

    println("\nExercise 2\n")

    val sellersQuota = sales.join(sellers, "seller_id")
      .groupBy(sellers("seller_id"))
      .agg(avg(sales("num_pieces_sold") / col("daily_target")))

    println("Sellers Quota: " + sellersQuota.collect()(0)(0))

  }

  private def Exercise3(): Unit = {

    println("\nExercise 3\n")

    // ventas por vendedor y producto
    val salesByProdSell = sales.groupBy("product_id", "seller_id")
      .agg(sum("num_pieces_sold").alias("num_pieces_sold"))
      .withColumn("rank_desc", dense_rank().over(Window.partitionBy("product_id").orderBy(desc("num_pieces_sold"))))
      .withColumn("rank_asc", dense_rank().over(Window.partitionBy("product_id").orderBy(asc("num_pieces_sold"))))

    // persona con menos ventas por producto
    val least = salesByProdSell.select(col("product_id"), col("seller_id").alias("least_seller_id"))
      .filter("rank_asc == 1")
      .orderBy("product_id")

    // segunda persona con más ventas por producto
    val most2 = salesByProdSell.select(col("product_id"), col("seller_id").alias("sec_most_seller_id"))
      .filter("rank_desc == 2")
      .orderBy("product_id")

    // tabla unificada
    val unified = most2.join(least, "product_id")
      .orderBy("product_id")

    println("Second most selling and least selling person by product: ")
    unified.filter("sec_most_seller_id <> least_seller_id")
      .show()

    println("Second most selling and least selling person for product 0: ")
    unified.filter("product_id == 0")
      .show()

  }

  private def Exercise4(): Unit = {

    //    def funct(order_id: Column, bill_text: String): Unit = {
    //
    //      var ret = bill_text.encode("utf-8")
    //      if (order_id.cast("int") % 2 == 0) {
    //        var NumA = bill_text.count("A")
    //        for (_c in range(0, NumA)) {
    //          ret = hashlib.md5(ret).hexdigest().encode("utf-8")
    //        }
    //        ret = ret.decode("utf-8")
    //      } else {
    //        ret = hashlib.sha256(ret).hexdigest()
    //      }
    //      return ret
    //    }
    //
    //    var funct = spark.udf.register("funct", funct)
    //
    //    sales.withColumn("hashed_bill", funct(col("order_id"), col("bill_raw_text")))
    //      .groupBy("hashed_bill")
    //      .agg(count("*").alias("count"))
    //      .filter("cnt > 1")
    //      .show()

  }

}
