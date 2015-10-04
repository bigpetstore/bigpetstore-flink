package org.apache.flink.examples.java.bigpetstore.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.examples.java.bigpetstore.FlinkBPSGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.stream.Collector;

/**
 * Created by jayvyas on 10/3/15.
 */
public class FlinkBPSStreamingProcess {


    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.readFileStream("/tmp/a", 10000, FileMonitoringFunction.WatchType.ONLY_NEW_FILES);


        DataStream<Tuple2<String, Integer>> counts =
                // normalize and split each line
                dataStream.map(
                        new MapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> map(String s) throws Exception {

                                return new Tuple2<String, Integer>(s, 1);
                            }
                        });

        counts.print();
        try {

            env.execute("Streaming WordCount");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
