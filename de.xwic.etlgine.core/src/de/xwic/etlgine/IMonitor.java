/*
 * de.xwic.etlgine.IMonitor 
 */
package de.xwic.etlgine;


/**
 * @author lippisch
 */
public interface IMonitor {

	public abstract class Adapter implements IMonitor {
		@Override
		public void initialize(IContext context) throws ETLException {
		}
		@Override
		public void onEvent(IProcessContext processContext, EventType eventType) {
		}
		@Override
		public void onEvent(IContext processContext, EventType eventType, Object eventSource) {
		}
		@Override
		public void logWarn(String message) {
		}
		@Override
		public void logInfo(String message) {
		}
		@Override
		public void logError(String message) {
		}
		@Override
		public void logError(String string, Throwable t) {
		}
		@Override
		public void logDebug(String string) {
		}
		@Override
		public void logTrace(String string) {
		}
		@Override
		public String getLogBuffer() {
			return null;
		}
		@Override
		public void reset() {
		}
	}
	
	public static IMonitor Empty = new Adapter() {};
	
	public enum EventType {
		// Job execution
		JOB_LOAD_FROM_SCRIPT, // ServerContext
		JOB_EXECUTION_START, // ServerContext
		// ProcessChain events
		PROCESSCHAIN_LOAD_FROM_SCRIPT, // GlobalContext
		PROCESSCHAIN_CREATE_PROCESS_FROM_SCRIPT, // ProcessContext
		PROCESSCHAIN_CREATE_PROCESS, // ProcessContext
		PROCESSCHAIN_ADD_CUSTOM_PROCESS, // ProcessContext
		PROCESSCHAIN_START, // GlobalContext
		// Process events
		PROCESS_START, // ProcessContext
		SOURCE_POST_OPEN, // ProcessContext
		RECORD_PROCESSED, // ProcessContext
		CUBE_POST_LOAD, // ProcessContext
		DATAPOOL_POST_SAVE, // ProcessContext
		SOURCE_FINISHED, // ProcessContext
		PROCESS_FINISHED, // ProcessContext
		// Job finished
		PROCESSCHAIN_FINISHED, // GlobalContext
		JOB_EXECUTION_END, // ServerContext
	}
	
	/**
	 * Initialize.
	 * @param context
	 * @throws ETLException 
	 */
	public void initialize(IContext context) throws ETLException;
	
	/**
	 * ProcessContext events.
	 * @deprecated please use onEvent(IContext processContext, EventType eventType, Object eventSource)
	 * @param processContext
	 * @param eventType
	 */
	public void onEvent(IProcessContext processContext, EventType eventType);

	/**
	 * ConextServer, GlobalContext and ProcessContext events.
	 * @param processContext
	 * @param eventType
	 * @param eventSource
	 */
	public void onEvent(IContext processContext, EventType eventType, Object eventSource);

	/**
	 * Log a warning.
	 * @param message
	 */
	public void logWarn(String message);

	/**
	 * Log an info.
	 * @param message
	 */
	public void logInfo(String message);

	/**
	 * Log an error message.
	 * @param message
	 */
	public void logError(String message);

	/**
	 * Log an error message with throwable stacktrace.
	 * @param string
	 * @param e
	 */
	public void logError(String string, Throwable t);

	/**
	 * Log a debug message.
	 * @param string
	 */
	public void logDebug(String string);
	
	/**
	 * Log a trace message.
	 * @param string
	 */
	public void logTrace(String string);
	
	/**
	 * Returns the content of the log buffer. The log buffer contains up to 4M of the
	 * last log entries.
	 * @return
	 */
	public String getLogBuffer();

	/**
	 * Reset the monitor prior to a new job execution.
	 */
	public void reset();

}
