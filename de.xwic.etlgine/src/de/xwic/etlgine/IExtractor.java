/*
 * de.xwic.etlgine.ILoader 
 */
package de.xwic.etlgine;

/**
 * @author lippisch
 */
public interface IExtractor extends IProcessParticipant {

	/**
	 * Initialize the loader.
	 * @param processContext
	 */
	public void initialize(IProcessContext processContext) throws ETLException;
	
	/**
	 * Initialize the DataSet.
	 * @param dataSet 
	 * @return
	 */
	public void openSource(ISource source, IDataSet dataSet) throws ETLException;
	
	/**
	 * Get the next record.
	 * @param context
	 * @return
	 */
	public IRecord getNextRecord() throws ETLException;
	
	/**
	 * Close the loader and all sources.
	 * @throws ETLException 
	 */
	public void close() throws ETLException;
	
}
