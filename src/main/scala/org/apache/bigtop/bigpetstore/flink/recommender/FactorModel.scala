package org.apache.bigtop.bigpetstore.flink.recommender

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.recommendation.ALS

object FactorModel {
  def main(args: Array[String]) {

    val parameters = ParameterTool.fromArgs(args)
    val inputFile = parameters.get("ALSInput", "/tmp/flink-etl-out")
    val iterations = parameters.getInt("ALSIterations", 10)
    val numFactors = parameters.getInt("ALSNumFactors", 10)
    val lambda = parameters.getDouble("ALSLambda", .9)
    val userOut = parameters.get("UserFileOutput", "/tmp/flink-user-factors")
    val itemOut = parameters.get("ItemFileOutput", "/tmp/flink-item-factors")

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
    userFactors.writeAsText(userOut, WriteMode.OVERWRITE).setParallelism(1)
    itemFactors.writeAsText(itemOut, WriteMode.OVERWRITE).setParallelism(1)

    env.execute("Factor model using FlinkML")
  }
}
