/*
 * de.xwic.etlgine.impl.Context 
 */
package de.xwic.etlgine.impl;

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
	
	protected boolean stopFlag = false;
	
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
	 * @param dataSet the dataSet to set
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
	 * A record has been processed. 
	 * NOTE: this method is invoked by the process itself.
	 */
	public void recordProcessed(IRecord record) {
		recordsCount++;
		if (record.isInvalid()) invalidCount ++;
		if (record.isSkip()) skippedCount++;
	}
	
	/**
	 * @return the recordsProcessed
	 */
	public int getRecordsCount() {
		return recordsCount;
	}
	
	/**
	 * Returns the number of records skipped.
	 * @return
	 */
	public int getSkippedCount() {
		return skippedCount;
	}
	
	/**
	 * Returns the number of records that have become invalid.
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
	 * @param monitor the monitor to set
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
	 * @param currentSource the currentSource to set
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
	 * @param result the result to set
	 */
	public void setResult(Result result) {
		this.result = result;
	}

	/**
	 * @return the stopFlag
	 */
	public boolean isStopFlag() {
		return stopFlag;
	}

	/**
	 * @param stopFlag the stopFlag to set
	 */
	public void setStopFlag(boolean stopFlag) {
		this.stopFlag = stopFlag;
	}

}
