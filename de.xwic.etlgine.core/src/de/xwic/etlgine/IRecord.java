/*
 * de.xwic.etlgine.IRecord 
 */
package de.xwic.etlgine;

import java.util.List;

/**
 * A single data record.
 * @author lippisch
 */
public interface IRecord {

	/**
	 * Mark the record as invalid.
	 * @param reason
	 */
	public void markInvalid(String reason);

	/**
	 * @return the invalid
	 */
	public boolean isInvalid();

	/**
	 * @return the invalidReason
	 */
	public String getInvalidReason();

	/**
	 * Set a value.
	 * @param column
	 * @param value
	 */
	public void setData(IColumn column, Object value);

	/**
	 * Change the data.
	 * @param columnName
	 * @param value
	 * @throws ETLException
	 */
	public void setData(String columnName, Object value) throws ETLException;

	/**
	 * Returns true if the specified column has been changed since the last
	 * resetChangeFlag() method. The resetChangeFlag is usualy reset by the
	 * extractor after reading the record.
	 * @param columnName
	 * @return
	 * @throws ETLException
	 */
	public boolean isChanged(String columnName) throws ETLException;

	/**
	 * Returns true if the specified column has been changed since the last
	 * resetChangeFlag() method. The resetChangeFlag is usualy reset by the
	 * extractor after reading the record.
	 * @param column
	 * @return
	 * @throws ETLException
	 */
	public boolean isChanged(IColumn column) throws ETLException;
	
	/**
	 * Reset the change flag of all columns.
	 */
	public void resetChangeFlag();
	
	/**
	 * Returns the data with the specified column name.
	 * @param columnName
	 * @return
	 * @throws ETLException
	 */
	public Object getData(String columnName) throws ETLException;

	/**
	 * Returns the data in the specified column.
	 * @param column
	 * @return
	 * @throws ETLException
	 */
	public Object getData(IColumn column) throws ETLException;

	/**
	 * Returns the data with the specified column name.
	 * @param columnName
	 * @return
	 * @throws ETLException
	 */
	public String getDataAsString(String columnName) throws ETLException;

	/**
	 * Returns the data in the specified column.
	 * @param column
	 * @return
	 * @throws ETLException
	 */
	public String getDataAsString(IColumn column) throws ETLException;
	
	/**
	 * Returns the data with the specified column name.
	 * @param columnName
	 * @return
	 * @throws ETLException
	 */
	public Double getDataAsDouble(String columnName) throws ETLException;

	/**
	 * Returns the data in the specified column.
	 * @param column
	 * @return
	 * @throws ETLException
	 */
	public Double getDataAsDouble(IColumn column) throws ETLException;

	/**
	 * Returns true if this record should be skipped. The normal reason for
	 * this is when the record does not match certain filter criterias.
	 * @return
	 */
	public boolean isSkip();
	
	/**
	 * Set if this record should be skipped. Records that are marked as "skip" are
	 * not passed to the loader(s).
	 * 
	 * @param skip
	 */
	public void setSkip(boolean skip);
	
	/**
	 * Cloning must be implemented.
	 * @return
	 */
	public IRecord clone();
	
	/**
	 * Duplicate this record by calling clone() and adds it to duplicates.
	 * This method should be could during transformer processing 
	 * and are passed to all following transformers.
	 */
	public IRecord duplicate();
	
	/**
	 * Returns the records created with duplicate().
	 * @return
	 */
	public List<IRecord> getDuplicates();
	
	/**
	 * Returns if this record is not the last record provided by IExtractor (default true).
	 * @return
	 */
	public boolean hasNext();

	/**
	 * If set to false (default true) this record is the last record provided by IExtractor.
	 * @param b
	 */
	public void setHasNext(boolean hasNext);
}
