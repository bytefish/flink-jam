package de.bytefish.flinkjam;

import de.bytefish.flinkjam.models.RawTrafficEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class TrafficCongestionWarningSystem {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // We define a WatermarkStrategy for all event streams. forBoundedOutOfOrderness is a common choice for
        // real-time applications, allowing for some events to arrive out of order. We allow 5 seconds
        // out-of-orderness.
        WatermarkStrategy<RawTrafficEvent> rawEventWmStrategy = WatermarkStrategy
                .<RawTrafficEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp());

        // TODO Implement it maybe?

        env.execute("flink-jam: Traffic Congestion Warning System");
    }
}
