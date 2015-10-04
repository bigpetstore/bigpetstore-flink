package org.apache.flink.examples.java.bigpetstore;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;

/**
 * Created by jayvyas on 10/3/15.
 */
public class FlinkBPSStreamingProcess {


  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> dataStream = env.readFileStream("/tmp/a", 10, FileMonitoringFunction.WatchType.ONLY_NEW_FILES);

    DataStream<Tuple2<String, Integer>> counts =
        dataStream.map(
            new MapFunction<String, Tuple2<String, Integer>>() {
              @Override
              public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
              }
            });

    counts.print();
    env.execute("Streaming WordCount");
  }

}
