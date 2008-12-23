/*
 * de.xwic.etlgine.DefaultMonitor 
 */
package de.xwic.etlgine;

/**
 * @author lippisch
 */
public class DefaultMonitor implements IMonitor {

	private long startTime = 0;
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#onEvent(de.xwic.etlgine.IETLContext, de.xwic.etlgine.IMonitor.EventType)
	 */
	public void onEvent(IContext context, EventType eventType) {
		if (eventType != EventType.RECORD_PROCESSED) {
			System.out.println("[EVENT] " + eventType.name());
		}
		
		switch (eventType) {
		case PROCESS_START:
			startTime = System.currentTimeMillis();
			break;
		case PROCESS_FINISHED:
			long duration = System.currentTimeMillis() - startTime;
			logInfo("Total duration (in ms): " + duration);
			logInfo("Records processed:      " + context.getRecordsProcessed());
			break;
		}
		
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#logError(java.lang.String)
	 */
	public void logError(String message) {
		System.out.println("[ERROR] " + message);
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#logError(java.lang.String, java.lang.Exception)
	 */
	public void logError(String message, Exception e) {
		System.out.println("[ERROR] " + message);
		e.printStackTrace();
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#logInfo(java.lang.String)
	 */
	public void logInfo(String message) {
		System.out.println("[INFO ] " + message);
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#logWarn(java.lang.String)
	 */
	public void logWarn(String message) {
		System.out.println("[WARN ] " + message);
	}
	
}
