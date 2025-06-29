package de.bytefish.trafficsim.models;

/**
 * Represents a single vehicle moving along the simulated route.
 */
public class Vehicle {
    public String id;
    public int currentSimulatedRouteSegmentIndex; // Index of the micro-segment it's currently on
    public double progressOnSegment;             // 0.0 (start) to 1.0 (end) of current micro-segment
    public double currentSpeedKmh;
    public boolean isFinished;                   // True if vehicle has completed its journey

    public Vehicle(String id, int initialSegmentIndex, double initialSpeedKmh) {
        this.id = id;
        this.currentSimulatedRouteSegmentIndex = initialSegmentIndex;
        this.progressOnSegment = 0.0;
        this.currentSpeedKmh = initialSpeedKmh;
        this.isFinished = false;
    }


}
