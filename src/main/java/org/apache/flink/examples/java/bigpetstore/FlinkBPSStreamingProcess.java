package org.apache.flink.examples.java.bigpetstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rnowling.bps.datagenerator.datamodels.Transaction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;

import java.util.Map;

/**
 * Created by jayvyas on 10/3/15.
 */
public class FlinkBPSStreamingProcess {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   *
   */
  public static void main(String inputStreamDir, String interval) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> dataStream = env.readFileStream(inputStreamDir, Integer.parseInt(interval), FileMonitoringFunction.WatchType.ONLY_NEW_FILES);

    /**
     * Keep it simple : Read in maps rather than the transacted objects.
     */
    DataStream<Tuple2<Map, Integer>> counts =
        dataStream.map(
            new MapFunction<String, Tuple2<Map, Integer>>() {
              @Override
              public Tuple2<Map, Integer> map(String s) throws Exception {
                Map transaction = MAPPER.readValue(s, Map.class);
                return new Tuple2<>(transaction, 1);
              }
            });
    counts.print();
    env.execute("Streaming WordCount");
  }

}
