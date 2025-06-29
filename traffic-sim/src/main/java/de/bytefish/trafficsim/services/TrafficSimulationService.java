package de.bytefish.trafficsim.services;

import de.bytefish.trafficsim.models.CongestionEvent;
import de.bytefish.trafficsim.models.SimulatedRouteSegment;
import de.bytefish.trafficsim.models.TrafficEventRecord;
import de.bytefish.trafficsim.models.Vehicle;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class TrafficSimulationService {

    private static final Random random = new Random();

    /**
     * Encapsulates the core traffic simulation logic as a static function.
     * It takes simulation parameters, road segments, and congestion events as input.
     *
     * @param simulatedRouteSegments The list of road segments (pgRouting ways) for the simulation.
     * @param congestionEvents The list of predefined congestion events.
     * @param numConcurrentVehicles Target number of vehicles active simultaneously.
     * @param simulationDurationMinutes Total simulation duration in minutes.
     * @param dataIntervalSeconds Data recording interval per vehicle (time step).
     * @param initialSpawnIntervalSeconds How often a new vehicle attempts to spawn initially.
     * @param baseAvgSpeedFactor Vehicles will try to maintain this factor of the segment's speed limit (uncongested).
     * @return A list of TrafficEventRecord objects representing traffic events.
     */
    public static List<TrafficEventRecord> runSimulationLogic( // Changed return type to List<TrafficEventRecord>
                                                                List<SimulatedRouteSegment> simulatedRouteSegments,
                                                                List<CongestionEvent> congestionEvents,
                                                                int numConcurrentVehicles, int simulationDurationMinutes,
                                                                int dataIntervalSeconds, int initialSpawnIntervalSeconds,
                                                                double baseAvgSpeedFactor) {

        List<TrafficEventRecord> allRecords = new ArrayList<>(); // Changed to List<TrafficEventRecord>
        // Removed header addition here, it's now handled by main method.

        LocalDateTime simulationStartTime = LocalDateTime.now();
        List<Vehicle> activeVehicles = new ArrayList<>();
        long vehicleCounter = 0;
        long nextSpawnTimeSeconds = 0;

        long totalTimeSteps = (long) (simulationDurationMinutes * 60 / dataIntervalSeconds);

        System.out.println("Starting route-based traffic simulation...");

        for (long step = 0; step < totalTimeSteps; step++) {
            long currentSimTimeSeconds = step * dataIntervalSeconds;
            LocalDateTime currentAbsoluteTime = simulationStartTime.plusSeconds(currentSimTimeSeconds);

            // --- Apply Congestion to Micro-Segments ---
            for (SimulatedRouteSegment segment : simulatedRouteSegments) {
                segment.currentCongestionFactor = 1.0; // Reset for this step
                for (CongestionEvent jam : congestionEvents) {
                    // Check if the current segment index is within the congestion event's range AND time window
                    if (segment.index >= jam.startSegmentIndex && segment.index <= jam.endSegmentIndex) {
                        long jamStartSec = jam.startTimeMinutes * 60;
                        long jamEndSec = jam.endTimeMinutes * 60;
                        if (currentSimTimeSeconds >= jamStartSec && currentSimTimeSeconds < jamEndSec) {
                            segment.currentCongestionFactor = Math.min(1.0, Math.max(0.1, jam.congestionFactor + (random.nextDouble() - 0.5) * 0.05));
                        }
                    }
                }
            }

            // --- Spawn New Vehicles ---
            // Only spawn if there are segments in the route
            if (!simulatedRouteSegments.isEmpty() && activeVehicles.size() < numConcurrentVehicles && currentSimTimeSeconds >= nextSpawnTimeSeconds) {
                vehicleCounter++;
                String newVehicleId = "V-" + UUID.randomUUID().toString().substring(0, 6) + "-" + vehicleCounter;
                // Initial speed based on the first segment's speed limit and a factor
                double firstSegmentSpeedLimit = simulatedRouteSegments.get(0).speedLimitKmh;
                double initialSpeed = firstSegmentSpeedLimit * baseAvgSpeedFactor * (0.8 + random.nextDouble() * 0.4); // 80-120% of factored limit
                initialSpeed = Math.max(10.0, initialSpeed); // Ensure a minimum sensible speed

                activeVehicles.add(new Vehicle(newVehicleId, 0, initialSpeed));

                nextSpawnTimeSeconds = currentSimTimeSeconds + initialSpawnIntervalSeconds + (long) (random.nextDouble() * initialSpawnIntervalSeconds - initialSpawnIntervalSeconds / 2);
                nextSpawnTimeSeconds = Math.max(currentSimTimeSeconds + dataIntervalSeconds, nextSpawnTimeSeconds);
            }

            // --- Update Active Vehicles' Positions and Speeds ---
            List<Vehicle> vehiclesToRemove = new ArrayList<>();
            for (Vehicle vehicle : activeVehicles) {
                if (vehicle.isFinished) {
                    continue;
                }

                SimulatedRouteSegment currentSegment = null;
                // Check if the current segment index is valid
                if (vehicle.currentSimulatedRouteSegmentIndex < simulatedRouteSegments.size()) {
                    currentSegment = simulatedRouteSegments.get(vehicle.currentSimulatedRouteSegmentIndex);
                } else {
                    // This vehicle has somehow gone past the last segment it should be on
                    vehicle.isFinished = true;
                    vehiclesToRemove.add(vehicle);
                    continue;
                }

                // Calculate effective speed limit for this vehicle on this micro-segment
                double effectiveSpeedLimit = currentSegment.speedLimitKmh * currentSegment.currentCongestionFactor;

                // Vehicle's target speed, influenced by effective limit with some randomness
                double targetSpeed = effectiveSpeedLimit * (0.7 + random.nextDouble() * 0.3); // 70-100% of effective limit

                // Simulate acceleration/deceleration
                double maxSpeedChange = 5.0; // km/h per interval
                if (targetSpeed > vehicle.currentSpeedKmh) {
                    vehicle.currentSpeedKmh = Math.min(targetSpeed, vehicle.currentSpeedKmh + maxSpeedChange);
                } else {
                    vehicle.currentSpeedKmh = Math.max(targetSpeed, vehicle.currentSpeedKmh - maxSpeedChange);
                }
                vehicle.currentSpeedKmh = Math.max(5.0, vehicle.currentSpeedKmh); // Minimum 5 km/h for movement

                // Calculate distance covered in this time interval
                double distanceCoveredKm = vehicle.currentSpeedKmh * (dataIntervalSeconds / 3600.0); // km/h * hours

                // Calculate progress on current micro-segment (ratio)
                double progressIncrement = distanceCoveredKm / currentSegment.lengthKm;
                vehicle.progressOnSegment += progressIncrement;

                // --- Handle Micro-Segment Transition ---
                if (vehicle.progressOnSegment >= 1.0) {
                    int nextSegmentIndex = vehicle.currentSimulatedRouteSegmentIndex + 1;

                    if (nextSegmentIndex < simulatedRouteSegments.size()) {
                        // Move to the next micro-segment
                        vehicle.currentSimulatedRouteSegmentIndex = nextSegmentIndex;
                        // Carry over any excess progress
                        vehicle.progressOnSegment = vehicle.progressOnSegment - 1.0;
                        if (vehicle.progressOnSegment >= 1.0) {
                            // If still >= 1.0 after moving to the next segment, cap it.
                            // This means the vehicle covered the next segment entirely in this time step too.
                            // This scenario is rare and typically implies a very large time step or very small segment.
                            vehicle.progressOnSegment = 0.999;
                        }
                    } else {
                        // Vehicle has reached the end of the simulated route
                        vehicle.isFinished = true;
                        vehiclesToRemove.add(vehicle);
                        vehicle.progressOnSegment = 1.0; // Ensure it's at the end for the final record
                    }
                }

                // --- Record Data ---
                // Get the segment for recording (could be the new segment if transition occurred)
                SimulatedRouteSegment segmentForRecord = simulatedRouteSegments.get(vehicle.currentSimulatedRouteSegmentIndex);
                double currentLat = segmentForRecord.start.lat + (segmentForRecord.end.lat - segmentForRecord.start.lat) * vehicle.progressOnSegment;
                double currentLon = segmentForRecord.start.lon + (segmentForRecord.end.lon - segmentForRecord.start.lon) * vehicle.progressOnSegment;

                // Add some minor GPS noise for realism
                currentLat += (random.nextDouble() - 0.5) * 0.00005; // +/- 0.000025 degrees
                currentLon += (random.nextDouble() - 0.5) * 0.00005; // +/- 0.000025 degrees

                // Create record and add to list
                TrafficEventRecord record = new TrafficEventRecord(
                        currentAbsoluteTime,
                        vehicle.id,
                        currentLat,
                        currentLon,
                        vehicle.currentSpeedKmh,
                        vehicle.currentSimulatedRouteSegmentIndex
                );
                allRecords.add(record); // Add TrafficEventRecord object to list
            }

            activeVehicles.removeAll(vehiclesToRemove);
        }
        return allRecords;
    }
}