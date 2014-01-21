/*
 * de.xwic.etlgine.IETLProcessParticipant 
 */
package de.xwic.etlgine;


/**
 * Common methods for transformers and loaders.
 * @author lippisch
 */
public interface IProcessParticipant {

	/**
	 * Initialize.
	 * @param processContext
	 * @throws ETLException 
	 */
	public void initialize(IProcessContext processContext) throws ETLException;
	
	/**
	 * Invoked before the source is opened by the extractor.
	 * @param processContext
	 * @throws ETLException
	 */
	public void preSourceOpening(IProcessContext processContext) throws ETLException;
	
	/**
	 * Invoked before a source is processed. This method is normally used
	 * to open the target store (i.e. file, connection, ...).
	 * @param processContext
	 */
	public void preSourceProcessing(IProcessContext processContext) throws ETLException;
	
	/**
	 * Invoked after a source has been processed. This method is normally used
	 * to close the target store/connection.
	 * @param processContext
	 */
	public void postSourceProcessing(IProcessContext processContext) throws ETLException;
	
	/**
	 * The process has been finished.
	 * @param processContext
	 */
	public void onProcessFinished(IProcessContext processContext) throws ETLException;

	
}
