package de.bytefish.flinkjam.cep;

import de.bytefish.flinkjam.models.CongestionWarning;
import de.bytefish.flinkjam.models.LastWarningMetadata;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class WarningDebouncerFunction extends KeyedProcessFunction<String, CongestionWarning, CongestionWarning> {

    private final long debouncePeriodMillis;
    private transient ValueState<LastWarningMetadata> lastWarningMetadataState;
    private transient Map<String, Integer> severityMap; // Maps warning type strings to integers

    public WarningDebouncerFunction(long debouncePeriodMillis) {
        this.debouncePeriodMillis = debouncePeriodMillis;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        lastWarningMetadataState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastWarningMetadata", TypeInformation.of(LastWarningMetadata.class))
        );

        // Initialize severity mapping
        severityMap = new HashMap<>();
        severityMap.put("Light Slowdown", 1);
        severityMap.put("Sustained Slowdown", 2);
        severityMap.put("Traffic Jam (Queue at Light)", 3);
        severityMap.put("Traffic Jam", 4); // Highest severity
    }

    // Helper to get severity from a warning type string
    private int getSeverity(String warningType) {
        return severityMap.getOrDefault(warningType, 0); // Default to 0 for unknown types (lowest)
    }

    @Override
    public void processElement(
            CongestionWarning warning,
            Context ctx,
            Collector<CongestionWarning> out) throws Exception {

        LastWarningMetadata lastMetadata = lastWarningMetadataState.value();
        long currentTime = ctx.timerService().currentProcessingTime();
        int incomingSeverity = getSeverity(warning.warningType);

        if (lastMetadata == null) {
            // No previous warning for this segment, emit immediately
            out.collect(warning);
            lastWarningMetadataState.update(new LastWarningMetadata(currentTime, warning.warningType, incomingSeverity));
        } else {
            int lastSeverity = lastMetadata.severity;
            long timeSinceLastWarning = currentTime - lastMetadata.timestamp;

            if (incomingSeverity > lastSeverity) {
                // Always emit if the new warning is more severe
                System.out.println(String.format("Emitting %s warning for segment %s (Severity escalated from %s to %s).",
                        warning.warningType, warning.roadSegmentId, lastMetadata.warningType, warning.warningType));
                out.collect(warning);
                lastWarningMetadataState.update(new LastWarningMetadata(currentTime, warning.warningType, incomingSeverity));
            } else if (timeSinceLastWarning >= debouncePeriodMillis) {
                // Emit if the debounce period has passed, even if severity is same or lower
                System.out.println(String.format("Emitting %s warning for segment %s (Debounce period passed).",
                        warning.warningType, warning.roadSegmentId));
                out.collect(warning);
                lastWarningMetadataState.update(new LastWarningMetadata(currentTime, warning.warningType, incomingSeverity));
            } else {
                // Suppress if less severe or same severity and within debounce period
                System.out.println(String.format("Suppressing %s warning for road segment %s (last warning %d ms ago, severity %d <= %d).",
                        warning.warningType, warning.roadSegmentId, timeSinceLastWarning, incomingSeverity, lastSeverity));
            }
        }
    }
}
