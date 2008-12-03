/*
 * de.xwic.etlgine.IETLProcessParticipant 
 */
package de.xwic.etlgine;


/**
 * Common methods for transformers and loaders.
 * @author lippisch
 */
public interface IETLProcessParticipant {

	/**
	 * Initialize.
	 * @param context
	 * @throws ETLException 
	 */
	public void initialize(IETLContext context) throws ETLException;
	
	/**
	 * Invoked before a source is processed. This method is normally used
	 * to open the target store (i.e. file, connection, ...).
	 * @param context
	 */
	public void preSourceProcessing(IETLContext context) throws ETLException;
	
	/**
	 * Invoked after a source has been processed. This method is normally used
	 * to close the target store/connection.
	 * @param context
	 */
	public void postSourceProcessing(IETLContext context) throws ETLException;
	
	/**
	 * The process has been finished.
	 * @param context
	 */
	public void onProcessFinished(IETLContext context) throws ETLException;

	
}
