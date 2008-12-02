/*
 * de.xwic.etlgine.IRecord 
 */
package de.xwic.etlgine;

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

}
