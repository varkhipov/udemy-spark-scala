import java.util.concurrent.TimeUnit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object PurchaseByCustomer extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "PurchaseByCustomer")

  val rdd = sc.textFile("../extras/customer-orders.csv") // customer_id, order_id, price

  // -> (customer_id, price)
  val customersAndPrices = rdd.map(line => {
    val tokens = line.split(",")
    (tokens(0).toInt, tokens(2).toFloat)
  })

  // count expenses -> (customer_id, total_expenses)
  val customersAndExpenses = customersAndPrices.reduceByKey((x, y) => x + y)

  // Start timer
  val start = System.nanoTime()

  // to set precision: BigDecimal(floatNum).setScale(2).toFloat
  val result = customersAndExpenses.sortBy(_._2, ascending = false).collect()

  // End timer
  println(s"Sorting took: ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)} ms\n\n")

  result.foreach(x => {
    println(f"ID: ${x._1}%2s, Total expenses: ${x._2}%.2f")
  })

}
