package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.util.Collector;
import org.apache.flink.core.execution.JobClient;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class PortToHDFSFlinkApp {

    public static void main(String[] args) throws Exception {
        
	// Create the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing
        env.enableCheckpointing(5000);  // Checkpoint every 5 seconds

        // Obtain the input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("localhost", 10010, "\n")
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis()));

        // Parse the data, group by word, and perform the window and aggregation operations.
        DataStream<Tuple2<String, Integer>> windowCounts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        for (String word : value.split("\\s")) {
                            System.out.println("Processing word: " + word); // Add logging
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(value -> value.f0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // Define the HDFS sink
        final StreamingFileSink<Tuple2<String, Integer>> sink = StreamingFileSink
                .forRowFormat(new org.apache.flink.core.fs.Path("hdfs:///tmp/flink_output"), new SimpleStringEncoder<Tuple2<String, Integer>>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.ofMinutes(2).toMillis())  // 2 minutes
                        .withInactivityInterval(Duration.ofMinutes(1).toMillis())  // 1 minute
                        .withMaxPartSize(1024 * 1024 * 1024)  // 1 GB
                        .build())
                .build();

        // Add the sink to the pipeline
        windowCounts.addSink(sink);

        // Execute the Flink job
        JobClient jobClient = env.executeAsync("Socket Window WordCount");

        // Trigger savepoint
        CompletableFuture<String> savepointPathFuture = jobClient.triggerSavepoint("hdfs:///tmp/savepoints");
        savepointPathFuture.thenAccept(savepointPath -> {
            System.out.println("Savepoint stored at: " + savepointPath);
            try {
                jobClient.cancel().get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        // Wait for the savepoint to complete
        savepointPathFuture.get();
    }
}
