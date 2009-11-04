/*
 * de.xwic.etlgine.impl.Record 
 */
package de.xwic.etlgine.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IRecord;

/**
 * @author lippisch
 */
public class Record implements IRecord, Cloneable {

	protected final IDataSet dataSet;
	protected boolean invalid = false;
	protected boolean skip = false;
	protected String invalidReason = null;
	protected Map<IColumn, Object> data = new HashMap<IColumn, Object>();
	protected Map<IColumn, Object> oldData = new HashMap<IColumn, Object>();
	protected List<IRecord> duplicates = new ArrayList<IRecord>();
	
	/**
	 * @param dataSet
	 */
	public Record(IDataSet dataSet) {
		this.dataSet = dataSet;
	}

	/**
	 * Set a value.
	 * @param column
	 * @param value
	 */
	public void setData(IColumn column, Object value) {
		data.put(column, value);
	}
	
	/**
	 * Change the data.
	 * @param columnName
	 * @param value
	 * @throws ETLException
	 */
	public void setData(String columnName, Object value) throws ETLException {
		IColumn column = dataSet.getColumn(columnName);
		setData(column, value);
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IRecord#isChanged(de.xwic.etlgine.IColumn)
	 */
	public boolean isChanged(IColumn column) throws ETLException {
		if (!data.containsKey(column) && !oldData.containsKey(column)) {
			return false;
		} else if (data.containsKey(column) && oldData.containsKey(column)) {
			Object o1 = data.get(column);
			Object o2 = oldData.get(column);
			if (o1 == null && o2 == null) {
				return false;
			} else if (o1 == null || o2 == null) {
				return true; 
			}
			return !o1.equals(o2);
		}
		return true;	// one element exists, the other does not
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IRecord#isChanged(java.lang.String)
	 */
	public boolean isChanged(String columnName) throws ETLException {
		return isChanged(dataSet.getColumn(columnName));
	}
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IRecord#resetChangeFlag()
	 */
	public void resetChangeFlag() {
		// copy data to oldData
		oldData.clear();
		oldData.putAll(data);
	}
	
	/**
	 * Returns the data with the specified column name.
	 * @param columnName
	 * @return
	 * @throws ETLException
	 */
	public Object getData(String columnName) throws ETLException {
		IColumn column = dataSet.getColumn(columnName);
		return getData(column);
	}
	
	/**
	 * Returns the data in the specified column.
	 * @param column
	 * @return
	 * @throws ETLException
	 */
	public Object getData(IColumn column) throws ETLException {
		return data.get(column);
	}

	/**
	 * Mark the record as invalid.
	 * @param reason
	 */
	public void markInvalid(String reason) {
		setInvalid(true);
		setInvalidReason(reason);
	}
	
	/**
	 * @return the invalid
	 */
	public boolean isInvalid() {
		return invalid;
	}

	/**
	 * @param invalid the invalid to set
	 */
	public void setInvalid(boolean invalid) {
		this.invalid = invalid;
	}

	/**
	 * @return the invalidReason
	 */
	public String getInvalidReason() {
		return invalidReason;
	}

	/**
	 * @param invalidReason the invalidReason to set
	 */
	public void setInvalidReason(String invalidReason) {
		this.invalidReason = invalidReason;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IRecord#getDataAsString(java.lang.String)
	 */
	public String getDataAsString(String columnName) throws ETLException {
		IColumn column = dataSet.getColumn(columnName);
		return getDataAsString(column);
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IRecord#getDataAsString(de.xwic.etlgine.IColumn)
	 */
	public String getDataAsString(IColumn column) throws ETLException {
		Object val = getData(column);
		return val != null ? val.toString() : null;
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IRecord#getDataAsDouble(java.lang.String)
	 */
	public Double getDataAsDouble(String columnName) throws ETLException {
		IColumn column = dataSet.getColumn(columnName);
		return getDataAsDouble(column);
	}
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IRecord#getDataAsDouble(de.xwic.etlgine.IColumn)
	 */
	public Double getDataAsDouble(IColumn column) throws ETLException {
		Object val = getData(column);
		if (val instanceof Double) {
			return (Double)val;
		} else if (val instanceof Number) {
			return ((Number)val).doubleValue();
		} else if (val instanceof String) {
			String s = (String)val;
			if (s.length() == 0) {
				return null;
			}
			return Double.parseDouble(s);
		}
		return null;
		
	}

	/**
	 * @return the skip
	 */
	public boolean isSkip() {
		return skip;
	}

	/**
	 * @param skip the skip to set
	 */
	public void setSkip(boolean skip) {
		this.skip = skip;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Record clone() {
		Record clone = new Record(dataSet);
		clone.data.putAll(data);
		return clone;
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IRecord#duplicate()
	 */
	public IRecord duplicate() {
		IRecord clone = clone();
		duplicates.add(clone);
		return clone;
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IRecord#getDuplicates()
	 */
	public List<IRecord> getDuplicates() {
		return duplicates;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return data.toString();
	}
}
