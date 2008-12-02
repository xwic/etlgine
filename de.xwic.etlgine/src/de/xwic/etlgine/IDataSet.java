/*
 * de.xwic.etlgine.IDataSet 
 */
package de.xwic.etlgine;

import java.util.List;

/**
 * Defines the data that is handled by the process. 
 * @author lippisch
 */
public interface IDataSet {

	/**
	 * Add a column to the data set. 
	 * @param column
	 * @throws ETLException 
	 */
	public void addColumn(IColumn column) throws ETLException;

	/**
	 * Returns the list of columns.
	 * @return
	 */
	public List<IColumn> getColumns();

	/**
	 * Returns the column with the specified name.
	 * @param name
	 * @return
	 * @throws ETLException 
	 */
	public IColumn getColumn(String name) throws ETLException;

	/**
	 * Add a new column.
	 * @param name
	 * @return
	 * @throws ETLException 
	 */
	public IColumn addColumn(String name) throws ETLException;

	/**
	 * Returns true if a column with that name exists.
	 * @param name
	 * @return
	 */
	public boolean containsColumn(String name);

	/**
	 * Add a new column.
	 * @param name
	 * @param idxT
	 * @return
	 * @throws ETLException 
	 */
	public IColumn addColumn(String name, int idx) throws ETLException;

	/**
	 * Returns the column at the specified column index.
	 * @param idx
	 * @return
	 * @throws ETLException
	 */
	public IColumn getColumnByIndex(int idx) throws ETLException;

}
