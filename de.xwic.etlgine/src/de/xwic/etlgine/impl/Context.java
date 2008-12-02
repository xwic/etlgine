/*
 * de.xwic.etlgine.impl.Context 
 */
package de.xwic.etlgine.impl;

import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IETLContext;
import de.xwic.etlgine.IRecord;

/**
 * @author lippisch
 */
public class Context implements IETLContext {

	protected IDataSet dataSet = null;
	protected IRecord currentRecord = null;
	protected int recordsProcessed = 0;
	
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
	
	
	
}
