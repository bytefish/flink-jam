package de.bytefish.trafficsim.models;

/**
 * Defines a traffic congestion event on a specific range of simulated route segments.
 */
public class CongestionEvent {
    public int startSegmentIndex;
    public int endSegmentIndex;
    public long startTimeMinutes;
    public long endTimeMinutes;
    public double congestionFactor; // Multiplier for speed (e.g., 0.3 for 30%)

    public CongestionEvent(int startSegmentIndex, int endSegmentIndex, long startTimeMinutes, long endTimeMinutes, double congestionFactor) {
        this.startSegmentIndex = startSegmentIndex;
        this.endSegmentIndex = endSegmentIndex;
        this.startTimeMinutes = startTimeMinutes;
        this.endTimeMinutes = endTimeMinutes;
        this.congestionFactor = congestionFactor;
    }
}