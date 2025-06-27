package de.bytefish.flinkjam.cep;

import de.bytefish.flinkjam.models.CongestionWarning;
import de.bytefish.flinkjam.models.FullyEnrichedTrafficEvent;
import de.bytefish.flinkjam.models.TrafficLightInfo;
import org.apache.flink.util.Collector;

import java.util.*;

public class CongestionPatternProcessFunction extends org.apache.flink.cep.functions.PatternProcessFunction<FullyEnrichedTrafficEvent, CongestionWarning> {

    private final String warningBaseType;
    private final double speedThresholdFactor; // Used for "Slowdown" types relative to speed limit
    private final double absoluteSpeedThresholdKmh; // Used for "Traffic Jam" types
    private final int minUniqueVehicles;
    private final double trafficLightRadius; // Radius used to consider lights "nearby"

    // Constructor for Light and Sustained Slowdown (using speedThresholdFactor)
    public CongestionPatternProcessFunction(String warningBaseType, double speedThresholdFactor, double absoluteSpeedThresholdKmh, int minUniqueVehicles, double trafficLightRadius) {
        this.warningBaseType = warningBaseType;
        this.speedThresholdFactor = speedThresholdFactor;
        this.absoluteSpeedThresholdKmh = absoluteSpeedThresholdKmh; // Will be 0.0 for slowdowns
        this.minUniqueVehicles = minUniqueVehicles;
        this.trafficLightRadius = trafficLightRadius;
    }

    @Override
    public void processMatch(Map<String, List<FullyEnrichedTrafficEvent>> match, Context ctx, Collector<CongestionWarning> out) throws Exception {

        // The matched events will be under the first key defined in the pattern (e.g., "firstSlowEvent")
        List<FullyEnrichedTrafficEvent> allMatchedEvents = null;

        // Iterate through the map to find the list of matched events.
        // The key used in `begin("patternName")` is what needs to be retrieved.
        // For simple patterns with a single `begin` event, iterating `values()` is sufficient.
        if (!match.isEmpty()) {
            allMatchedEvents = match.values().iterator().next(); // Get the list from the first (and likely only) key
        }

        if (allMatchedEvents == null || allMatchedEvents.isEmpty()) {
            return; // Should not happen if pattern is correctly defined, but good check
        }

        FullyEnrichedTrafficEvent firstEvent = allMatchedEvents.get(0);

        double totalSpeed = 0;

        Set<String> uniqueVehicles = new HashSet<>();

        Set<TrafficLightInfo> uniqueRelatedLights = new HashSet<>(); // Use Set for uniqueness

        for (FullyEnrichedTrafficEvent event : allMatchedEvents) {
            totalSpeed += event.getSpeed();
            uniqueVehicles.add(event.getVehicleId());
            if (event.getNearbyTrafficLights() != null) {
                uniqueRelatedLights.addAll(event.getNearbyTrafficLights());
            }
        }

        // Apply unique vehicle count filter
        if (uniqueVehicles.size() < minUniqueVehicles) {
            System.out.println(String.format("Skipping %s warning: Not enough unique vehicles (%d < %d) on road segment %s.",
                    warningBaseType, uniqueVehicles.size(), minUniqueVehicles, firstEvent.roadSegmentId));
            return;
        }

        double averageSpeed = totalSpeed / allMatchedEvents.size();

        String actualWarningType = warningBaseType;
        String details;

        // Use speedThresholdFactor (for relative) or absoluteSpeedThresholdKmh (for absolute)
        if (warningBaseType.equals("Traffic Jam")) {
            details = String.format("Severe congestion (Avg Speed: %.2f km/h, below %.1f km/h) detected on road segment %s (Type: %s). Involved vehicles: %d.",
                    averageSpeed, absoluteSpeedThresholdKmh, firstEvent.roadSegmentId, firstEvent.roadType, uniqueVehicles.size());
        } else {
            double thresholdSpeed = firstEvent.getSpeedLimitKmh() * speedThresholdFactor;
            details = String.format("Average speed (%.2f km/h) is below %.1f%% of speed limit (%.1f km/h vs %d km/h) on road segment %s (Type: %s). Involved vehicles: %d.",
                    averageSpeed, speedThresholdFactor * 100, thresholdSpeed, firstEvent.speedLimitKmh, firstEvent.roadSegmentId, firstEvent.roadType, uniqueVehicles.size());
        }

        // Traffic Light Consideration Logic
        if (!uniqueRelatedLights.isEmpty()) {
            boolean isNearTrafficLight = false;
            for (TrafficLightInfo light : uniqueRelatedLights) {
                if (light.distanceMeters <= trafficLightRadius) {
                    isNearTrafficLight = true;
                    break;
                }
            }

            if (isNearTrafficLight) {
                if (warningBaseType.equals("Traffic Jam")) {

                    // If it's a Traffic Jam and near a light, refine the type
                    actualWarningType = "Traffic Jam (Queue at Light)";
                    details = String.format("Severe congestion (Avg Speed: %.2f km/h) detected near traffic light(s) on segment %s. Possible queue at intersection. Involved vehicles: %d.",
                            averageSpeed, firstEvent.roadSegmentId, uniqueVehicles.size());
                } else if (warningBaseType.equals("Sustained Slowdown") || warningBaseType.equals("Light Slowdown")) {
                    // For slowdowns near a light, add a note
                    details += " (Near traffic light(s))";
                }
            } else {
                // If it's a Traffic Jam AND NOT near a light, it's a more serious general jam
                if (warningBaseType.equals("Traffic Jam")) {
                    actualWarningType = "Traffic Jam (General)";
                    details = String.format("Severe congestion (Avg Speed: %.2f km/h) detected on road segment %s. No immediate traffic light nearby. Potential incident/bottleneck. Involved vehicles: %d.",
                            averageSpeed, firstEvent.roadSegmentId, uniqueVehicles.size());
                }
            }
        }

        CongestionWarning warning = new CongestionWarning(
                ctx.timestamp(),
                firstEvent.roadSegmentId,
                actualWarningType,
                averageSpeed,
                firstEvent.speedLimitKmh,
                details,
                new ArrayList<>(uniqueRelatedLights), // Convert Set back to List for output
                uniqueVehicles.size()
        );
        out.collect(warning);
    }
}