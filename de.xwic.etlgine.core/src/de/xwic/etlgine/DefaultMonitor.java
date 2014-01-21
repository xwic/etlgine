/*
 * de.xwic.etlgine.DefaultMonitor 
 */
package de.xwic.etlgine;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author lippisch
 */
public class DefaultMonitor implements IMonitor {

	public final static long STATUS_INTERVALL = 30 * 1000; // 30 sec.
	public final static int MAX_LOG_BUFFER = 4 * 1024 * 1024; // 4 MB log buffer
	
	protected long startTime = 0;
	protected long nextStatus = 0;
	protected int lastRecordsCount = 0;
	protected Log log = LogFactory.getLog(DefaultMonitor.class);
	protected StringBuilder logBuffer = new StringBuilder();
	protected DateFormat dateFormat = new SimpleDateFormat("dd-MMM:HH:mm:ss.SSS");
	
	/**
	 * Reset the monitor prior to a new job execution.
	 */
	public void reset() {
		startTime = 0;
		nextStatus = 0;
		logBuffer.setLength(0);
	}
	
	@Override
	public void initialize(IContext context) throws ETLException {
		// nothing to do
	}
	
	@Override
	public void onEvent(IProcessContext processContext, EventType eventType) {
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#onEvent(de.xwic.etlgine.IContext, de.xwic.etlgine.IMonitor.EventType, java.lang.Object)
	 */
	public void onEvent(IContext context, EventType eventType, Object eventSource) {
		
		if (context instanceof IProcessContext) {
			// Process event
			IProcessContext processContext = (IProcessContext)context;
			//call onEvent for backwards compatibility
			onEvent(processContext, eventType);
			if (eventType != EventType.RECORD_PROCESSED) {
				logDebug("[EVENT] " + eventType);
			} else {
				if (System.currentTimeMillis() > nextStatus) {
					nextStatus = System.currentTimeMillis() + STATUS_INTERVALL;
					String sourceName = processContext.getCurrentSource() != null ? processContext.getCurrentSource().getName() : "NO-SOURCE";
					Runtime rt = Runtime.getRuntime();
					logInfo("Processing: " 
							+ processContext.getRecordsCount() + "/"
							+ (processContext.getRecordsCount() - lastRecordsCount) + "/"
							+ processContext.getSkippedCount() + "/" 
							+ processContext.getInvalidCount() 
							+ " (total/now/skipped/invalid) records from " + sourceName + ", "
							+ "Java Heap: " + rt.totalMemory() / 1024 / 1024 + "MB/" + rt.freeMemory() / 1024 / 1024 + "MB (total/free)");
					lastRecordsCount = processContext.getRecordsCount();
				}
			}
			
			switch (eventType) {
			case PROCESS_START:
				lastRecordsCount = 0;
				startTime = System.currentTimeMillis();
				nextStatus = startTime + STATUS_INTERVALL;
				break;
			case PROCESS_FINISHED:
				long duration = System.currentTimeMillis() - startTime;
				logInfo("Total duration (in ms): " + duration);
				logInfo("Records processed:      " + processContext.getRecordsCount());
				logInfo("Records skipped:        " + processContext.getSkippedCount());
				logInfo("Records invalid:        " + processContext.getInvalidCount());
				break;
			}
			
		} else {
			logDebug("[EVENT] " + eventType);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#logError(java.lang.String)
	 */
	public void logError(String message) {
		log.error(message);
		logToBuffer("ERROR", message);
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#logError(java.lang.String, java.lang.Throwable)
	 */
	public void logError(String message, Throwable e) {
		log.error(message, e);
		logToBuffer("ERROR", message, e);
		
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#logInfo(java.lang.String)
	 */
	public void logInfo(String message) {
		log.info(message);
		logToBuffer("INFO", message);
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#logWarn(java.lang.String)
	 */
	public void logWarn(String message) {
		log.warn(message);
		logToBuffer("WARN", message);
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#logDebug(java.lang.String)
	 */
	public void logDebug(String message) {
		log.debug(message);
		logToBuffer("DEBUG", message);
	}
	
	protected void logToBuffer(String prefix, String message) {
		logToBuffer(prefix, message, null);
	}
	
	protected synchronized void logToBuffer(String prefix, String message, Throwable e) {
		if (logBuffer.length() > MAX_LOG_BUFFER) {
			// cut away first 1024 chars if buffer is to small.
			logBuffer = new StringBuilder(logBuffer.substring(1024, logBuffer.length()));
		}
		logBuffer.append(dateFormat.format(new Date()))
			.append(" [").append(prefix).append("] ")
			.append(message)
			.append("\n");
		
		if (e != null) {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(bos);
			e.printStackTrace(pw);
			pw.close();
			logBuffer.append(bos);
		}
	}
	
	/**
	 * Returns the content of the log buffer. The log buffer contains up to 256k of the
	 * last log entries.
	 * @return
	 */
	public String getLogBuffer() {
		return logBuffer.toString();
	}
	
}
