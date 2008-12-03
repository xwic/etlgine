/*
 * de.xwic.etlgine.IMonitor 
 */
package de.xwic.etlgine;

/**
 * @author lippisch
 */
public interface IMonitor {

	public enum EventType {
		SOURCE_POST_OPEN,
		SOURCE_FINISHED,
		PROCESS_FINISHED
	}
	
	public void onEvent(IETLContext context, EventType eventType);
	
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
	public void logError(String string, Exception e);
	
}
