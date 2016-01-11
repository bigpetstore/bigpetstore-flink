package org.apache.flink.examples.java.bigpetstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class FlinkBPSStreamingProcess {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   *
   */
  public static void main(String[] args) throws Exception {

    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String inputStreamDir = parameterTool.get("inputStreamDir");
    String interval = parameterTool.get("interval");

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> dataStream = env.readFileStream(inputStreamDir, Integer.parseInt(interval),
        FileMonitoringFunction.WatchType.ONLY_NEW_FILES);

    DataStream<Tuple2<String, Integer>> counts =
        dataStream.flatMap(new Splitter())
        .keyBy(0).sum(1);

    counts.print();

    env.execute("Streaming WordCount");
  }


  public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
      Map transaction = MAPPER.readValue(s, Map.class);
      System.out.println(transaction);
      Map customer = (Map) transaction.get("customer");
      String state = (String)((Map) customer.get("location")).get("state");
      collector.collect(new Tuple2<>(state,1));
    }
  }

}
