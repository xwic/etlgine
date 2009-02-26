/*
 * de.xwic.etlgine.DefaultMonitor 
 */
package de.xwic.etlgine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author lippisch
 */
public class DefaultMonitor implements IMonitor {

	public final static long STATUS_INTERVALL = 30 * 1000; // 30 sec.
	
	protected long startTime = 0;
	protected long nextStatus = 0;
	protected Log log = LogFactory.getLog(DefaultMonitor.class);
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#onEvent(de.xwic.etlgine.IETLContext, de.xwic.etlgine.IMonitor.EventType)
	 */
	public void onEvent(IProcessContext processContext, EventType eventType) {
		if (eventType != EventType.RECORD_PROCESSED) {
			log.debug("[EVENT] " + eventType.name());
		} else {
			if (System.currentTimeMillis() > nextStatus) {
				nextStatus = System.currentTimeMillis() + STATUS_INTERVALL;
				String sourceName = processContext.getCurrentSource() != null ? processContext.getCurrentSource().getName() : "NO-SOURCE";
				log.info("Processing: " 
						+ processContext.getRecordsCount() 
						+ " records from " + sourceName 
						+ " (" + processContext.getSkippedCount() + " skipped, " 
						+ processContext.getInvalidCount() + " invalid)");
			}
		}
		
		switch (eventType) {
		case PROCESS_START:
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
		
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#logError(java.lang.String)
	 */
	public void logError(String message) {
		log.error(message);
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#logError(java.lang.String, java.lang.Exception)
	 */
	public void logError(String message, Throwable e) {
		log.error(message, e);
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#logInfo(java.lang.String)
	 */
	public void logInfo(String message) {
		log.info(message);
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IMonitor#logWarn(java.lang.String)
	 */
	public void logWarn(String message) {
		log.warn(message);
	}
	
}
