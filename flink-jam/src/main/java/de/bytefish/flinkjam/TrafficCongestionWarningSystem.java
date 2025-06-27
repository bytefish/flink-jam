package de.bytefish.flinkjam;

import de.bytefish.flinkjam.lookup.AsyncRoadSegmentLookupFunction;
import de.bytefish.flinkjam.models.RawTrafficEvent;
import de.bytefish.flinkjam.models.RoadEnrichedTrafficEvent;
import de.bytefish.flinkjam.sources.RawTrafficEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class TrafficCongestionWarningSystem {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/flinkjam"; // Database name reverted
    private static final String DB_USER = "postgis"; // User reverted
    private static final String DB_PASSWORD = "postgis"; // Password reverted

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // We define a WatermarkStrategy for all event streams. forBoundedOutOfOrderness is a common choice for
        // real-time applications, allowing for some events to arrive out of order. We allow 5 seconds
        // out-of-orderness.
        WatermarkStrategy<RawTrafficEvent> rawEventWmStrategy = WatermarkStrategy
                .<RawTrafficEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, recordTimestamp) -> event.timestamp);

        // The simulation should include the following types: Normal Traffic, Light Slowdown, Sustained Slowdown,
        // Traffic Jam, Red Light Phase.  We don't have access to a real data source, so we generate our own
        // traffic events and feed them to Apache Flink using a SourceFunction<>.
        DataStream<RawTrafficEvent> rawTrafficEvents = env.addSource(new RawTrafficEventSource()) // Reverted to addSource
                .assignTimestampsAndWatermarks(rawEventWmStrategy); // Apply watermark strategy

        // Now we need to enrich the raw events with the road segment data, so we get the speed limit and road_segment,
        // the possible congestion is on. This is done using the RichAsyncFunction, with 10m as the Lookup Function
        // for Road Segments.
        DataStream<RoadEnrichedTrafficEvent> roadEnrichedEvents = AsyncDataStream.unorderedWait(
                rawTrafficEvents,
                new AsyncRoadSegmentLookupFunction(DB_URL, DB_USER, DB_PASSWORD, 10),
                5000, TimeUnit.MILLISECONDS, 100
        ).filter(event -> event.roadSegmentId != null);

        roadEnrichedEvents.print("Road Enriched Event");

        // Next is enriching the events with the TrafficLightInformation. This needs to be done, because we don't
        // want to generate dozens of false positives, if cars are in a red light phase. If we don't take traffic
        // lights into consideration, each red light phase would generate an event.

        env.execute("flink-jam: Traffic Congestion Warning System");
    }
}
