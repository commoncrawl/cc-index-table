package org.commoncrawl.spark;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Accumulate and report Spark metrics. */
public class JobStatsListener extends SparkListener {

	private static final Logger LOG = LoggerFactory.getLogger(JobStatsListener.class);

	private AtomicLong bytesRead = new AtomicLong(0L);
	private AtomicLong recordsRead = new AtomicLong(0L);
	private AtomicLong bytesWritten = new AtomicLong(0L);
	private AtomicLong recordsWritten = new AtomicLong(0L);

	@Override
	public void onTaskEnd(SparkListenerTaskEnd sparkListenerTaskEnd) {
		if (sparkListenerTaskEnd.taskMetrics().inputMetrics() != null) {
			LOG.info("input metrics: {}", sparkListenerTaskEnd.taskMetrics().inputMetrics().recordsRead());
			bytesRead.addAndGet(sparkListenerTaskEnd.taskMetrics().inputMetrics().bytesRead());
			recordsRead.addAndGet(sparkListenerTaskEnd.taskMetrics().inputMetrics().recordsRead());
		}
		if (sparkListenerTaskEnd.taskMetrics().outputMetrics() != null) {
			LOG.info("output metrics: {}", sparkListenerTaskEnd.taskMetrics().outputMetrics().recordsWritten());
			bytesWritten.addAndGet(sparkListenerTaskEnd.taskMetrics().outputMetrics().bytesWritten());
			recordsWritten.addAndGet(sparkListenerTaskEnd.taskMetrics().outputMetrics().recordsWritten());
		}
	}

	private static String prettyPrintBytes(long value) {
		if (value >= 1099511627776L)
			return String.format("%.3f TB", value/1099511627776.0);
		if (value >= 1073741824)
			return String.format("%.3f GB", value/1073741824.0);
		if (value >= 1048576)
			return String.format("%.3f MB", value/1048576.0);
		if (value >= 1024)
			return String.format("%.3f kB", value/1024.0);
		return String.format("%d bytes", value);
	}

	public void report() {
		LOG.info("{}\trecords read", recordsRead.get());
		LOG.info("{}\tdata read", prettyPrintBytes(bytesRead.get()));
		if (recordsWritten.get() > 0) {
			LOG.info("{}\trecords written", recordsWritten.get());
			LOG.info("{}\tdata written", prettyPrintBytes(bytesWritten.get()));
		}
	}

}
