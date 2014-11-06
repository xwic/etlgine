/*
 * de.xwic.etlgine.impl.Context 
 */
package de.xwic.etlgine.impl;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.transaction.PlatformTransactionManager;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IMonitor;
import de.xwic.etlgine.IProcess;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.ISource;
import de.xwic.etlgine.Result;

/**
 * @author lippisch
 */
public class ProcessContext extends Context implements IProcessContext {

	protected ISource currentSource = null;
	protected IMonitor monitor = null;
	protected IDataSet dataSet = null;
	protected IRecord currentRecord = null;
	protected int recordsCount = 0;
	protected int skippedCount = 0;
	protected int invalidCount = 0;
	protected IProcess process = null;
	protected Result result = null;
	protected Throwable lastException = null;
	
	/** A map containing all the transactionManagers for this process, with the connectionName as key. */
	protected Map<String, PlatformTransactionManager> transactionManagers = null;
	
	/** A map containing all the dataSources for this process, with the connectionName as key. */
	protected Map<String, DataSource> dataSources = null;

	/**
	 * 
	 */
	public ProcessContext(IProcess process) {
		super();
		this.process = process;
	}

	/**
	 * @param parentContext
	 */
	public ProcessContext(IProcess process, IContext parentContext) {
		super(parentContext);
		this.process = process;
	}

	/**
	 * @return the dataSet
	 */
	public IDataSet getDataSet() {
		return dataSet;
	}

	/**
	 * @param dataSet
	 *            the dataSet to set
	 */
	public void setDataSet(IDataSet dataSet) {
		this.dataSet = dataSet;
	}

	public IRecord newRecord() {
		currentRecord = new Record(dataSet);
		return currentRecord;
	}

	/**
	 * @return the currentRecord
	 */
	public IRecord getCurrentRecord() {
		return currentRecord;
	}

	/**
	 * A record has been processed. NOTE: this method is invoked by the process itself.
	 */
	public void recordProcessed(IRecord record) {
		recordsCount++;
		if (record.isInvalid())
			invalidCount++;
		if (record.isSkip())
			skippedCount++;
	}

	/**
	 * @return the recordsProcessed
	 */
	public int getRecordsCount() {
		return recordsCount;
	}

	/**
	 * Returns the number of records skipped.
	 * 
	 * @return
	 */
	public int getSkippedCount() {
		return skippedCount;
	}

	/**
	 * Returns the number of records that have become invalid.
	 * 
	 * @return
	 */
	public int getInvalidCount() {
		return invalidCount;
	}

	/**
	 * @return the monitor
	 */
	public IMonitor getMonitor() {
		return monitor;
	}

	/**
	 * @param monitor
	 *            the monitor to set
	 */
	public void setMonitor(IMonitor monitor) {
		this.monitor = monitor;
	}

	/**
	 * @return the currentSource
	 */
	public ISource getCurrentSource() {
		return currentSource;
	}

	/**
	 * @param currentSource
	 *            the currentSource to set
	 */
	public void setCurrentSource(ISource currentSource) {
		this.currentSource = currentSource;
	}

	/**
	 * @return the process
	 */
	public IProcess getProcess() {
		return process;
	}

	/**
	 * @return the result
	 */
	public Result getResult() {
		return result;
	}

	/**
	 * @param result
	 *            the result to set
	 */
	public void setResult(Result result) {
		this.result = result;
	}

	/**
	 * @return the stopFlag
	 */
	public boolean isStopFlag() {
		if (stopFlag) {
			return true;
		} else if (parentContext != null) {
			return parentContext.isStopFlag();
		}

		return false;
	}

	/**
	 * Returns the Exception that has interrupted the Process. Can be used by finalizers to evaluate the cause of a failed process. Returns
	 * null if no exception was raised.
	 * 
	 * @return the lastException
	 */
	public Throwable getLastException() {
		return lastException;
	}

	/**
	 * @param lastException
	 *            the lastException to set
	 */
	public void setLastException(Throwable lastException) {
		this.lastException = lastException;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.xwic.etlgine.IProcessContext#addTransactionManager(java.lang.String,
	 * org.springframework.transaction.PlatformTransactionManager)
	 */
	public void addTransactionManager(String connectionName, PlatformTransactionManager transactionManager) throws ETLException {
		if (connectionName == null) {
			throw new ETLException("Trying to cache a transactionManager with a null connectionName key.");
		}

		if (transactionManager == null) {
			throw new ETLException("Trying to cache a null transactionManager.");
		}

		if (transactionManagers == null) {
			// Lazy init
			transactionManagers = new HashMap<String, PlatformTransactionManager>();
		}

		transactionManagers.put(connectionName, transactionManager);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.xwic.etlgine.IProcessContext#getTransactionManager(java.lang.String)
	 */
	public PlatformTransactionManager getTransactionManager(String connectionName) {
		if (transactionManagers != null) {
			return transactionManagers.get(connectionName);
		}
		return null;
	}
	
	/*
	 * (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessContext#addDataSource(java.lang.String, javax.sql.DataSource)
	 */
	public void addDataSource(String connectionName, DataSource dataSource) throws ETLException {
		if (connectionName == null) {
			throw new ETLException("Trying to cache a dataSource with a null connectionName key.");
		}

		if (dataSource == null) {
			throw new ETLException("Trying to cache a null dataSource.");
		}

		if (dataSources == null) {
			// Lazy init
			dataSources = new HashMap<String, DataSource>();
		}

		dataSources.put(connectionName, dataSource);
	}
	
	/*
	 * (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessContext#getDataSource(java.lang.String)
	 */
	public DataSource getDataSource(String connectionName) {
		if (dataSources != null) {
			return dataSources.get(connectionName);
		}
		return null;
	}
}
