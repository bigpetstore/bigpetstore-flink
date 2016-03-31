package org.apache.bigtop.bigpetstore.flink.scala.recommender

import org.apache.bigtop.bigpetstore.flink.java.util.Utils
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.recommendation.ALS

object FactorModel {
  def main(args: Array[String]) {

    val parameters = Utils.parseArgs(args)
    val inputFile = parameters.getRequired("ETLOutput")
    val iterations = parameters.getRequired("iterations").toInt
    val numFactors = parameters.getRequired("numFactors").toInt
    val lambda = parameters.getRequired("lambda").toDouble
    val customerOut = parameters.getRequired("customerOut")
    val productOut = parameters.getRequired("productOut")

    val env = ExecutionEnvironment.getExecutionEnvironment

    val input = env.readCsvFile[(Int, Int)](inputFile)
      .map(pair => (pair._1, pair._2, 1.0))

    val model = ALS()
      .setNumFactors(numFactors)
      .setIterations(iterations)
      .setLambda(lambda)

    model.fit(input)

    //Factor model is persisted to file, prediction is done in streaming
    val (userFactors, itemFactors) = model.factorsOption.get
    userFactors.writeAsText(customerOut, WriteMode.OVERWRITE).setParallelism(1)
    itemFactors.writeAsText(productOut, WriteMode.OVERWRITE).setParallelism(1)

    env.execute("Factor model using FlinkML")
  }
}
