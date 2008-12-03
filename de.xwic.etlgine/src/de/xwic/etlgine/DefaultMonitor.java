/*
 * de.xwic.etlgine.DefaultMonitor 
 */
package de.xwic.etlgine;

/**
 * @author lippisch
 */
public class DefaultMonitor implements IMonitor {

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#onEvent(de.xwic.etlgine.IETLContext, de.xwic.etlgine.IMonitor.EventType)
	 */
	public void onEvent(IETLContext context, EventType eventType) {
		System.out.println("[EVENT] " + eventType.name());
		
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#logError(java.lang.String)
	 */
	public void logError(String message) {
		System.out.println("[ERROR] " + message);
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
