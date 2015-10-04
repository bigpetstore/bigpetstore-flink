package org.apache.flink.examples.java.bigpetstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rnowling.bps.datagenerator.datamodels.Transaction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;

/**
 * Created by jayvyas on 10/3/15.
 */
public class FlinkBPSStreamingProcess {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> dataStream = env.readFileStream("/tmp/a", 10, FileMonitoringFunction.WatchType.ONLY_NEW_FILES);

    DataStream<Tuple2<Transaction, Integer>> counts =
        dataStream.map(
            new MapFunction<String, Tuple2<Transaction, Integer>>() {
              @Override
              public Tuple2<Transaction, Integer> map(String s) throws Exception {
                Transaction transaction = MAPPER.readValue(s, Transaction.class);
                return new Tuple2<>(transaction, 1);
              }
            });

    counts.print();
    env.execute("Streaming WordCount");
  }

}
