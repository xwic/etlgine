/*
 * de.xwic.etlgine.IMonitor 
 */
package de.xwic.etlgine;

/**
 * @author lippisch
 */
public interface IMonitor {

	public enum EventType {
		PROCESS_START,
		SOURCE_POST_OPEN,
		RECORD_PROCESSED,
		SOURCE_FINISHED,
		PROCESS_FINISHED
	}
	
	public void onEvent(IProcessContext processContext, EventType eventType);
	
	/**
	 * Log a warning.
	 * @param message
	 */
	public void logWarn(String message);

	/**
	 * Log a warning.
	 * @param message
	 */
	public void logInfo(String message);

	/**
	 * Log a warning.
	 * @param message
	 */
	public void logError(String message);

	/**
	 * @param string
	 * @param e
	 */
	public void logError(String string, Throwable t);

	/**
	 * Returns the content of the log buffer. The log buffer contains up to 256k of the
	 * last log entries.
	 * @return
	 */
	public String getLogBuffer();

	/**
	 * Reset the monitor prior to a new job execution.
	 */
	public void reset();

}
