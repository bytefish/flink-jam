package de.bytefish.flinkjam;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TrafficCongestionWarningSystem {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(1);
        
        // TODO Implement it maybe?

		env.execute("flink-jam: Traffic Congestion Warning System");
	}
}
