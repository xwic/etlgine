package de.xwic.etlgine;

import de.xwic.etlgine.impl.Context;


/**
 * Loads the data into its target destination. This might be something like a file, database
 * or cube. 
 * @author lippisch
 *
 */
public interface ILoader {

	/**
	 * Initialize the loader.
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
	 * Load the record.
	 * @param context
	 * @param record
	 * @throws ETLException 
	 */
	public void processRecord(IETLContext context, IRecord record) throws ETLException;

	/**
	 * The process has been finished.
	 * @param context
	 */
	public void onProcessFinished(Context context) throws ETLException;
	
}
