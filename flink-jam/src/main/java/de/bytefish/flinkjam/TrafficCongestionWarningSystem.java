package de.bytefish.flinkjam;

import de.bytefish.flinkjam.cep.CongestionPatternProcessFunction;
import de.bytefish.flinkjam.lookup.AsyncRoadSegmentLookupFunction;
import de.bytefish.flinkjam.lookup.AsyncTrafficLightLookupFunction;
import de.bytefish.flinkjam.models.CongestionWarning;
import de.bytefish.flinkjam.models.FullyEnrichedTrafficEvent;
import de.bytefish.flinkjam.models.RawTrafficEvent;
import de.bytefish.flinkjam.models.RoadEnrichedTrafficEvent;
import de.bytefish.flinkjam.sources.RawTrafficEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class TrafficCongestionWarningSystem {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/flinkjam"; // Database name reverted
    private static final String DB_USER = "postgis"; // User reverted
    private static final String DB_PASSWORD = "postgis"; // Password reverted

    /**
     * A Configuration used in Traffic Congestions Patterns.
     */
    public static class TrafficCongestionConfig implements Serializable { // Implement Serializable
        public int minEvents;
        public  double speedThresholdFactor; // Speed < factor of speed limit
        public  long windowMillis; // Changed to long for serializability
        public  int minUniqueVehicles;
        public int trafficLightNearbyRadiusInMeters;
        public double absoluteSpeedThresholdKmh; // Absolute speed < this value

        public TrafficCongestionConfig(int minEvents, double speedThresholdFactor, double absoluteSpeedThresholdKmh, Duration window, int minUniqueVehicles, int trafficLightNearbyRadiusInMeters) {
            this.minEvents = minEvents;
            this.speedThresholdFactor = speedThresholdFactor;
            this.absoluteSpeedThresholdKmh = absoluteSpeedThresholdKmh;
            this.windowMillis = window.toMillis(); // Store as milliseconds
            this.minUniqueVehicles = minUniqueVehicles;
            this.trafficLightNearbyRadiusInMeters = trafficLightNearbyRadiusInMeters;

        }

        public int getMinEvents() {
            return minEvents;
        }

        public double getSpeedThresholdFactor() {
            return speedThresholdFactor;
        }

        public long getWindowMillis() {
            return windowMillis;
        }

        public int getMinUniqueVehicles() {
            return minUniqueVehicles;
        }

        public int getTrafficLightNearbyRadiusInMeters() {
            return trafficLightNearbyRadiusInMeters;
        }

        public double getAbsoluteSpeedThresholdKmh() {
            return absoluteSpeedThresholdKmh;
        }
    }

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
        DataStream<FullyEnrichedTrafficEvent> fullyEnrichedEvents = AsyncDataStream.unorderedWait(
                roadEnrichedEvents,
                new AsyncTrafficLightLookupFunction(DB_URL, DB_USER, DB_PASSWORD, 20),
                500, TimeUnit.MILLISECONDS, 100 // Increased timeout to 15 seconds
        );

        fullyEnrichedEvents.print("Fully Enriched Event");

        // Watermark Strategy for FullyEnrichedTrafficEvents
        WatermarkStrategy<FullyEnrichedTrafficEvent> fullyEnrichedEventWmStrategy = WatermarkStrategy
                .<FullyEnrichedTrafficEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // Allow 5 seconds out-of-orderness
                .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp());

        DataStream<FullyEnrichedTrafficEvent> trafficEventsForCEP = fullyEnrichedEvents
                .assignTimestampsAndWatermarks(fullyEnrichedEventWmStrategy) // Apply watermark strategy after enrichment
                .filter(event -> event.roadSegmentId != null); // Ensure only valid road segments go to CEP

        // Key the stream by road segment ID to detect patterns per road segment
        DataStream<FullyEnrichedTrafficEvent> keyedTrafficEvents = trafficEventsForCEP
                .keyBy(FullyEnrichedTrafficEvent::getRoadSegmentId);

        // Pattern 1: Light Slowdown
        TrafficCongestionConfig lightSlowdownConfig = new TrafficCongestionConfig(3, 0.7, 0, Duration.ofSeconds(60), 2, 50);

        Pattern<FullyEnrichedTrafficEvent, ?> lightSlowdownPattern = Pattern.<FullyEnrichedTrafficEvent>begin("lightSlowEvent")
                .where(new IterativeCondition<FullyEnrichedTrafficEvent>() {
                    @Override
                    public boolean filter(FullyEnrichedTrafficEvent event, Context<FullyEnrichedTrafficEvent> ctx) throws Exception {
                        return event.getSpeed() < (event.getSpeedLimitKmh() * lightSlowdownConfig.speedThresholdFactor);
                    }
                })
                .timesOrMore(lightSlowdownConfig.minEvents)
                .greedy()
                .within(Duration.ofMillis(lightSlowdownConfig.windowMillis));

        DataStream<CongestionWarning> lightSlowdownWarnings = CEP.pattern(keyedTrafficEvents, lightSlowdownPattern).process(
                new CongestionPatternProcessFunction("Light Slowdown", lightSlowdownConfig.speedThresholdFactor, 0.0, lightSlowdownConfig.minUniqueVehicles, lightSlowdownConfig.getTrafficLightNearbyRadiusInMeters()));
        lightSlowdownWarnings.print("Light Slowdown Warning");


        // Pattern 2: Sustained Slowdown (more severe than Light Slowdown)
        TrafficCongestionConfig sustainedSlowdownConfig = new TrafficCongestionConfig(5, 0.5, 0, Duration.ofSeconds(60), 3, 30);

        Pattern<FullyEnrichedTrafficEvent, ?> sustainedSlowdownPattern = Pattern.<FullyEnrichedTrafficEvent>begin("sustainedSlowEvent")
                .where(new IterativeCondition<>() {
                    @Override
                    public boolean filter(FullyEnrichedTrafficEvent event, Context<FullyEnrichedTrafficEvent> ctx) throws Exception {
                        return event.getSpeed() < (event.getSpeedLimitKmh() * sustainedSlowdownConfig.speedThresholdFactor);
                    }
                })
                .timesOrMore(sustainedSlowdownConfig.minEvents)
                .greedy()
                .within(Duration.ofMillis(sustainedSlowdownConfig.windowMillis));

        DataStream<CongestionWarning> sustainedSlowdownWarnings = CEP
                .pattern(keyedTrafficEvents, sustainedSlowdownPattern)
                .process(new CongestionPatternProcessFunction("Sustained Slowdown", sustainedSlowdownConfig.speedThresholdFactor, 0.0, sustainedSlowdownConfig.minUniqueVehicles, sustainedSlowdownConfig.getTrafficLightNearbyRadiusInMeters()));

        sustainedSlowdownWarnings.print("Sustained Slowdown Warning");

        // Pattern 3: Traffic Jam (most severe, near-stop)
        TrafficCongestionConfig trafficJamConfig = new TrafficCongestionConfig(7, 0, 10, Duration.ofSeconds(60), 5, 30);

        Pattern<FullyEnrichedTrafficEvent, ?> trafficJamPattern = Pattern.<FullyEnrichedTrafficEvent>begin("trafficJamEvent")
                .where(new IterativeCondition<FullyEnrichedTrafficEvent>() {
                    @Override
                    public boolean filter(FullyEnrichedTrafficEvent event, Context<FullyEnrichedTrafficEvent> ctx) throws Exception {
                        return event.getSpeed() < trafficJamConfig.absoluteSpeedThresholdKmh; // Absolute low speed
                    }
                })
                .timesOrMore(trafficJamConfig.minEvents)
                .greedy()
                .within(Duration.ofMillis(trafficJamConfig.windowMillis));

        DataStream<CongestionWarning> trafficJamWarnings = CEP.pattern(keyedTrafficEvents, trafficJamPattern).process(
                new CongestionPatternProcessFunction("Traffic Jam", 0.0, trafficJamConfig.absoluteSpeedThresholdKmh, trafficJamConfig.minUniqueVehicles, trafficJamConfig.trafficLightNearbyRadiusInMeters));

        trafficJamWarnings.print("Traffic Jam Warning");

        env.execute("flink-jam: Traffic Congestion Warning System");
    }
}
