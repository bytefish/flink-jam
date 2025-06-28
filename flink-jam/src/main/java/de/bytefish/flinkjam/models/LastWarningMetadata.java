package de.bytefish.flinkjam.models;


import java.io.Serializable;

/**
 * Stores metadata about the last warning issued for a road segment.
 */
public class LastWarningMetadata implements Serializable {
    public long timestamp;
    public String warningType;
    public int severity; // Numerical severity

    public LastWarningMetadata() {}

    public LastWarningMetadata(long timestamp, String warningType, int severity) {
        this.timestamp = timestamp;
        this.warningType = warningType;
        this.severity = severity;
    }

    @Override
    public String toString() {
        return "LastWarningMetadata{" +
                "timestamp=" + timestamp +
                ", warningType='" + warningType + '\'' +
                ", severity=" + severity +
                '}';
    }
}