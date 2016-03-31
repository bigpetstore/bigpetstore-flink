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

package org.apache.bigtop.bigpetstore.flink.java;

import org.apache.bigtop.bigpetstore.flink.java.util.Utils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Predictor for a matrix based recommender. User ids are read from a socket,
 * then augmented with their characteristic vectors. These vectors are broadcasted
 * to partial top item computers distributing the scalar product computation of the
 * item matrix and the user vector. To compute the global top k these are grouped by
 * user.
 *
 * Usage: run a 'nc -lk 9999' and type integers in [0-99].
 */
@SuppressWarnings("unchecked")
public class FlinkStreamingRecommender {

  private final static int NUM_TOP_K = 10;

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream recommendations = env.socketTextStream("localhost", 9999)
                                    .map(new GetUserVector())
                                    .broadcast()
                                    .map(new PartialTopItem(NUM_TOP_K))
                                    .keyBy(0)
                                    .flatMap(new GlobalTopK());

    recommendations.print();

    env.execute("Flink Streaming Recommendations");
  }

  // ***********************************
  // User defined functions
  // ***********************************

  /**
   * Parses the user ids received from the socket input and augments them with the corresponding
   * user vector.
   */
  public static class GetUserVector extends RichMapFunction<String, Tuple2<Integer, Double[]>>{

    // This array contains the user feature vectors
    private Double[][] userVectorMatrix;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      userVectorMatrix = Utils.getUserMatrix();
    }

    @Override
    public Tuple2<Integer, Double[]> map(String s) throws Exception {
      Integer id = Integer.parseInt(s);
      return new Tuple2(id, userVectorMatrix[id]);
    }
  }

  /**
   * Is responsible to compute the scalar product of the user vector and a partition of the matrix.
   * Forwards the topk item candidates and their values for each user.
   */
  public static class PartialTopItem extends RichMapFunction<Tuple2<Integer, Double[]>, Tuple3<Integer, Integer[], Double[]>> {

    // desired number of top items
    private final int topItemCount;
    // array containing item feature vectors
    double[][] partialItemFeature;
    // global IDs of the item partition
    Integer[] itemIDs;
    private int partitionSize;

    Integer[] partialTopItemIDs;
    Double[] partialTopItemScores;

    public PartialTopItem(int topItemCount) {
      this.topItemCount = topItemCount;

      partialTopItemIDs = new Integer[topItemCount];
      partialTopItemScores = new Double[topItemCount];
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      partialItemFeature = Utils.getItemMatrix(getRuntimeContext().getNumberOfParallelSubtasks());
      itemIDs = Utils.getItemIDs();
      partitionSize = itemIDs.length;
    }

    @Override
    public Tuple3<Integer, Integer[], Double[]> map(Tuple2<Integer, Double[]> value) throws Exception {
      Double[] userVector = value.f1;
      Double[] scores = new Double[partitionSize];

      for (int item = 0; item < partitionSize; item++) {
        double sum = 0;
        for (int i = 0; i < userVector.length; i++) {
          sum += userVector[i] * partialItemFeature[item][i];
        }
        scores[item] = sum;
      }

      Utils.getTopK(topItemCount, partialTopItemIDs, partialTopItemScores, itemIDs, scores);

      return new Tuple3<>(value.f0, partialTopItemIDs, partialTopItemScores);
    }
  }

  /**
   * Gathers the partial topk information and computes the global recommendation.
   *
   * <p>
   * Note that I assume that global and partial topk have the same parallelism and that partial
   * topk information is not overlayed between transactions. The first assumption is fair for a
   * local setting, the second is not.
   */
  public static class GlobalTopK extends RichFlatMapFunction<Tuple3<Integer, Integer[], Double[]>,
          Tuple3<Integer, Integer[], Double[]>>{

    private int numberOfPartitions;

    // mapping the user ID to the global top items of the user
    // partitionCount counts down till the all the partitions are processed
    Map<Integer, Integer> partitionCount = new HashMap<>();
    Map<Integer, Integer[]> topIDs = new HashMap<>();
    Map<Integer, Double[]> topScores = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      numberOfPartitions = getRuntimeContext().getNumberOfParallelSubtasks();
    }

    @Override
    public void flatMap(Tuple3<Integer, Integer[], Double[]> value,
                        Collector<Tuple3<Integer, Integer[], Double[]>> collector) throws Exception {
      Integer id = value.f0;
      Integer[] pTopIds = value.f1;
      Double[] pTopScores = value.f2;

      if (partitionCount.containsKey(id)) {
        // we already have the user in the maps

        updateTopItems(id, pTopIds, pTopScores);

        Integer newCount = partitionCount.get(id) - 1;

        if (newCount > 0) {
          // update partition count
          partitionCount.put(id, newCount);
        } else {
          // all the partitions are processed, we've got the global
          // top now
          collector.collect(new Tuple3<>(id, topIDs.get(id), topScores.get(id)));

          partitionCount.remove(id);
          topIDs.remove(id);
          topScores.remove(id);
        }
      } else {
        // the user is not in the maps

        if (numberOfPartitions == 1) {
          // if there's only one partition that has the global top
          // scores
          collector.collect(new Tuple3<>(id, pTopIds, pTopScores));
        } else {
          // if there are more partitions the first one is the initial
          // top scores
          partitionCount.put(id, numberOfPartitions - 1);
          topIDs.put(id, pTopIds);
          topScores.put(id, pTopScores);
        }
      }
    }

    private void updateTopItems(Integer uid, Integer[] pTopIDs, Double[] pTopScores) {
      Double[] currentTopScores = topScores.get(uid);
      Integer[] currentTopIDs = topIDs.get(uid);

      Utils.merge(currentTopIDs, currentTopScores, pTopIDs, pTopScores);
    }
  }
}
