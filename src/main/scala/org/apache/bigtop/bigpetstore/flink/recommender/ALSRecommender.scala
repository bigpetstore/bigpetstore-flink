/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
