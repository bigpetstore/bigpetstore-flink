package org.apache.bigtop.bigpetstore.flink.etl

import java.util.Date

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.utils._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.examples.java.bigpetstore.FlinkTransaction

import scala.collection.JavaConversions._

object ETL {
  def main(args: Array[String]) {

    // parse input parameters
    val parameters = ParameterTool.fromArgs(args)
    val numStores = parameters.getInt("numStores", 10)
    val numCustomers = parameters.getInt("numCustomers", 100)
    val simLength = parameters.getDouble("simLength", 100.0)
    val input = parameters.get("input", "/tmp/flink-bps-out")
    val output = parameters.get("output", "/tmp/flink-etl-out")

    // Initialize context
    val env = ExecutionEnvironment.getExecutionEnvironment

    val transactions = env.readTextFile(input)
      .map(new FlinkTransaction(_))

    // Generate unique product IDs
    val productsWithIndex = transactions.flatMap(t => t.getProducts)
      .distinct
      .zipWithUniqueId

    // Generate the customer-product pairs
    val customerAndProductIDS = transactions.flatMap(t => t.getProducts.map(p => (t.getCustomer.getId, p)))
      .join(productsWithIndex)
      .where(_._2)
      .equalTo(_._2)
      .map(pair => (pair._1._1, pair._2._1))
      .distinct

    customerAndProductIDS.writeAsCsv(output, "\n", ",", WriteMode.OVERWRITE)

    //Print stats on the generated dataset
    val numProducts = productsWithIndex.count
    val filledFields = customerAndProductIDS.count
    val sparseness = 1 - (filledFields.toDouble / (numProducts * numCustomers))

    // Statistics with the Table API
    val table = transactions.map(toCaseClass(_)).toTable

    // Transaction count of stores
    val storeTransactionCount = table.groupBy('storeId).select('storeId, 'storeName, 'storeId.count as 'count)

    // Store(s) with the most transactions
    val bestStores = storeTransactionCount.select('count.max as 'max)
      .join(storeTransactionCount)
      .where("count = max")
      .select('storeId, 'storeName, 'count)
      .toDataSet[StoreCount].collect

    // Transaction count of months
    val monthTransactionCount = table.groupBy('month).select('month, 'month.count as 'count)
      .toDataSet[MonthCount].collect

    println("Generated bigpetstore stats")
    println("---------------------------")
    println("Customers:\t" + numCustomers)
    println("Stores:\t\t" + numStores)
    println("simLength:\t" + simLength)
    println("Products:\t" + numProducts)
    println("sparse:\t\t" + sparseness)
    println()
    println("Store(s) with the most transactions")
    println("---------------------------")
    bestStores.foreach(println(_))
    println()
    println("Monthly transaction count")
    println("---------------------------")
    monthTransactionCount.foreach(println(_))
  }

  case class Transaction(month : Int,
                         customerId : Int, customerName : (String, String),
                         transactionId : Int,
                         storeId : Int, storeName : String, storeCity : String,
                         products : Traversable[String])

  case class StoreCount(storeId : Int, storeName : String, count : Int)
  case class MonthCount(month : Int, count : Int)

  def toCaseClass(t : FlinkTransaction) = {
    val millis = (t.dateTime * 24 * 3600 * 1000).toLong
    val month = new Date(millis).getMonth
    Transaction(month, t.customer.getId, (t.customer.getName.getFirst, t.customer.getName.getFirst),
      t.id, t.store.getId, t.store.getName, t.store.getLocation.getCity, t.products)
  }
}
