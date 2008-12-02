/*
 * de.xwic.etlgine.IEtlContext 
 */
package de.xwic.etlgine;


/**
 * The context is used by the process participants to share the data.
 * 
 * @author lippisch
 */
public interface IETLContext {

	/**
	 * @return the dataSet
	 */
	public IDataSet getDataSet();

	/**
	 * @return the recordsProcessed
	 */
	public int getRecordsProcessed();

	/**
	 * Create a new record.
	 * @return
	 */
	public IRecord newRecord();

	/**
	 * @return the currentRecord
	 */
	public IRecord getCurrentRecord();

}
