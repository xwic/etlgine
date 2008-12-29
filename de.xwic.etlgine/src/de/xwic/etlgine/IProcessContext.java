/*
 * de.xwic.etlgine.IEtlContext 
 */
package de.xwic.etlgine;


/**
 * The context is used by the process participants to share the data.
 * 
 * @author lippisch
 */
public interface IProcessContext extends IContext {

	/**
	 * Returns the hosting process.
	 * @return
	 */
	public IProcess getProcess();
	
	/**
	 * Returns the source that is currently processed.
	 * @return
	 */
	public ISource getCurrentSource();
	
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

	/**
	 * @return the monitor
	 */
	public IMonitor getMonitor();

	/**
	 * @param monitor the monitor to set
	 */
	public void setMonitor(IMonitor monitor);

}
