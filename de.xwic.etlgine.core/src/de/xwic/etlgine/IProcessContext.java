/*
 * de.xwic.etlgine.IEtlContext 
 */
package de.xwic.etlgine;

import java.util.HashMap;

import javax.sql.DataSource;

import org.springframework.transaction.PlatformTransactionManager;

/**
 * The context is used by the process participants to share the data.
 * 
 * @author lippisch
 */
public interface IProcessContext extends IContext {

	/**
	 * Returns the hosting process.
	 * 
	 * @return
	 */
	public IProcess getProcess();

	/**
	 * Returns the source that is currently processed.
	 * 
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
	public int getRecordsCount();

	/**
	 * Returns the number of records skipped.
	 * 
	 * @return
	 */
	public int getSkippedCount();

	/**
	 * Returns the number of records that have become invalid.
	 * 
	 * @return
	 */
	public int getInvalidCount();

	/**
	 * Create a new record.
	 * 
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
	 * @param monitor
	 *            the monitor to set
	 */
	public void setMonitor(IMonitor monitor);

	/**
	 * @return the result
	 */
	public Result getResult();

	/**
	 * @param result
	 *            the result to set
	 */
	public void setResult(Result result);

	/**
	 * @return the stopFlag
	 */
	public boolean isStopFlag();

	/**
	 * @param stopFlag
	 *            the stopFlag to set
	 */
	public void setStopFlag(boolean stopFlag);

	/**
	 * Returns the Exception that has interrupted the Process. Can be used by finalizers to evaluate the cause of a failed process. Returns
	 * null if no exception was raised.
	 * 
	 * @return the lastException
	 */
	public Throwable getLastException();

	/**
	 * Caches the transactionManager for the current connectionName.
	 * 
	 * The connectionName string should be the identifier of the connection, from the properties files.
	 * 
	 * @param connectionName
	 *            the key of the cache
	 * @param transactionManager
	 *            the value of the cache
	 * @throws ETLException
	 *             if one of the parameters is null
	 */
	public void addTransactionManager(String connectionName, PlatformTransactionManager transactionManager) throws ETLException;
	
	/**
	 * Returns the transactionManager for the given connection, or null if none found.
	 * 
	 * @param connectionName
	 * @return
	 */
	public PlatformTransactionManager getTransactionManager(String connectionName);
	
	/**
	 * Caches the dataSource for the current connectionName.
	 * 
	 * The connectionName string should be the identifier of the connection, from the properties files.
	 * 
	 * @param connectionName
	 * @param dataSource
	 * @throws ETLException
	 */
	public void addDataSource(String connectionName, DataSource dataSource) throws ETLException;	
	
	/**
	 * Returns the dataSource for the given connection, or null if none found.
	 * 
	 * @param connectionName
	 * @return
	 */
	public DataSource getDataSource(String connectionName);

}
