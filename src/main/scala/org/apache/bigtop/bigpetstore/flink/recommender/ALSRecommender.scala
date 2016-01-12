package org.apache.bigtop.bigpetstore.flink.recommender

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.ml.recommendation.ALS

object ALSRecommender {
  def main(args: Array[String]) {

    val parameters = ParameterTool.fromArgs(args)
    val inputFile = parameters.get("ALSInput", "/tmp/flink-bps-out")
    val iterations = parameters.getInt("ALSIterations", 10)
    val numFactors = parameters.getInt("ALSNumFactors", 10)
    val lambda = parameters.getDouble("ALSLambda", .9)

    val env = ExecutionEnvironment.getExecutionEnvironment

    val input = env.readCsvFile[(Int, Int)](inputFile)
      .map(pair => (pair._1, pair._2, 1.0))

    val model = ALS()
      .setNumFactors(numFactors)
      .setIterations(iterations)
      .setLambda(lambda)

    model.fit(input)

    // Testing for an existing dummy value
    val test = env.fromElements((0,0))

    model.predict(test).print
  }
}
