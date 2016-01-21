package org.apache.bigtop.bigpetstore.flink.recommender

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.recommendation.ALS

object ALSRecommender {
  def main(args: Array[String]) {

    val parameters = ParameterTool.fromArgs(args)
    val inputFile = parameters.get("ALSInput", "/tmp/flink-bps-out")
    val iterations = parameters.getInt("ALSIterations", 10)
    val numFactors = parameters.getInt("ALSNumFactors", 10)
    val lambda = parameters.getDouble("ALSLambda", .9)

    //output
    val userOut = parameters.get("UserFileOutput", "/tmp/flink-user-factors")
    val itemOut = parameters.get("ItemFileOutput", "/tmp/flink-item-factors")

    val env = ExecutionEnvironment.getExecutionEnvironment

    val input = env.readCsvFile[(Int, Int)](inputFile)
      .map(pair => (pair._1, pair._2, 1.0))

    val numCustomers = input.max(0).collect().head._1
    val numItems = input.max(1).collect().head._1

    val customers = env.fromCollection(1 to 1)
    val items = env.fromCollection(1 to numItems)

    val model = ALS()
      .setNumFactors(numFactors)
      .setIterations(iterations)
      .setLambda(lambda)

    model.fit(input)

    val test = customers cross items
//    val test = env.fromElements((0,0))

    model.predict(test).print

    val userFactors = model.factorsOption.get._1
    val itemFactors = model.factorsOption.get._2

    userFactors.writeAsText(userOut, WriteMode.OVERWRITE)
    itemFactors.writeAsText(itemOut, WriteMode.OVERWRITE)

    env.execute()
  }
}
