package org.apache.bigtop.bigpetstore.flink.generator

import java.util

import com.github.rnowling.bps.datagenerator._
import com.github.rnowling.bps.datagenerator.datamodels._
import com.github.rnowling.bps.datagenerator.datamodels.inputs.ProductCategory
import com.github.rnowling.bps.datagenerator.framework.SeedFactory
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.examples.java.bigpetstore.FlinkTransaction
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

object DataGenerator {
  def main(args: Array[String]) {

    // parse input parameters
    val parameters = ParameterTool.fromArgs(args)
    val seed = parameters.getInt("seed", 42)
    val numStores = parameters.getInt("numStores", 10)
    val numCustomers = parameters.getInt("numCustomers", 100)
    val burningTime = parameters.getDouble("burningTime", 1.0)
    val simLength = parameters.getDouble("simLength", 100.0)
    val output = parameters.get("output", "/tmp/flink-bps-out")

    val startTime = java.lang.System.currentTimeMillis().toDouble / (24 * 3600 * 1000)

    // Initialize context
    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputData = new DataLoader().loadData()
    val seedFactory = new SeedFactory(seed)

    val storeGenerator = new StoreGenerator(inputData, seedFactory)
    val stores = for (i <- 1 to numStores) yield storeGenerator.generate
    val storesDataSet = env.fromCollection(stores)

    val customerGenerator = new CustomerGenerator(inputData, stores, seedFactory)
    val customers = for (i <- 1 to numCustomers) yield customerGenerator.generate

    // Generate transactions
    val transactions = env.fromCollection(customers)
      .flatMap(new TransactionGeneratorFlatMap(inputData.getProductCategories, simLength, burningTime))
      .withBroadcastSet(storesDataSet, "stores")
      .map{t => t.setDateTime(t.getDateTime + startTime); t}

    transactions.writeAsText(output, WriteMode.OVERWRITE)

    env.execute("DataGenerator")
  }

  class TransactionGeneratorFlatMap(val products : util.Collection[ProductCategory],
                                    val simLength : Double,
                                    val burningTime : Double)
    extends RichFlatMapFunction[Customer, FlinkTransaction] {

    var profile : PurchasingProfile = null
    var seedFactory : SeedFactory = null
    var stores : util.List[Store] = null

    override def open(parameters: Configuration): Unit = {
      val seed = getRuntimeContext.getIndexOfThisSubtask
      seedFactory = new SeedFactory(seed)
      profile = new PurchasingProfileGenerator(products, seedFactory).generate
      stores = getRuntimeContext.getBroadcastVariable[Store]("stores")
    }

    override def flatMap(customer: Customer, collector: Collector[FlinkTransaction]): Unit =  {
      val transGen = new TransactionGenerator(customer, profile, stores, products, seedFactory)
      var transaction = transGen.generate

      while (transaction.getDateTime < simLength) {
        if (transaction.getDateTime > burningTime) collector.collect(transaction)
        transaction = transGen.generate
      }
    }
  }
}
