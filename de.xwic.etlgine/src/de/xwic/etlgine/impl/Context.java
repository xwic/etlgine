/*
 * de.xwic.etlgine.impl.Context 
 */
package de.xwic.etlgine.impl;

import java.util.HashMap;
import java.util.Map;

import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IMonitor;
import de.xwic.etlgine.IRecord;

/**
 * @author lippisch
 */
public class Context implements IContext {

	protected IMonitor monitor = null;
	protected IDataSet dataSet = null;
	protected IRecord currentRecord = null;
	protected int recordsProcessed = 0;
	protected Map<String, String> properties = new HashMap<String, String>();
	protected Map<String, Object> globals = new HashMap<String, Object>();
	
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
	 * Set a global property.
	 * @param name
	 * @param value
	 */
	public void setProperty(String name, String value) {
		properties.put(name, value);
	}

	/**
	 * Returns a global property.
	 * @param name
	 * @return
	 */
	public String getProperty(String name) {
		return getProperty(name, null);
	}
	
	/**
	 * Get a global property.
	 * @param name
	 * @param value
	 */
	public String getProperty(String name, String defaultValue) {
		String value = properties.get(name);
		if (value == null) {
			return defaultValue;
		}
		return value;
	}

	/**
	 * Set a global object.
	 * @param name
	 * @param object
	 */
	public void setGlobal(String name, Object object) {
		globals.put(name, object);
	}

	/**
	 * Returns a global object.
	 * @param name
	 * @return
	 */
	public Object getGlobal(String name) {
		return getGlobal(name, null);
	}
	
	/**
	 * Returns a global object.
	 * @param name
	 * @param object
	 */
	public Object getGlobal(String name, Object defaultObject) {
		Object value = globals.get(name);
		if (value == null) {
			return defaultObject;
		}
		return value;
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

}
