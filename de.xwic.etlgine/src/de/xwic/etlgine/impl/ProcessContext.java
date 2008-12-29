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

/**
 * @author lippisch
 */
public class ProcessContext extends Context implements IProcessContext {

	protected ISource currentSource = null;
	protected IMonitor monitor = null;
	protected IDataSet dataSet = null;
	protected IRecord currentRecord = null;
	protected int recordsProcessed = 0;
	protected IProcess process = null;
	
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
	public void recordProcessed() {
		recordsProcessed++;
	}
	
	/**
	 * @return the recordsProcessed
	 */
	public int getRecordsProcessed() {
		return recordsProcessed;
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

}
