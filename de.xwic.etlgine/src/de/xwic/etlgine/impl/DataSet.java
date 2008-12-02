/*
 * de.xwic.etlgine.impl.DataSet 
 */
package de.xwic.etlgine.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IDataSet;

/**
 * @author lippisch
 */
public class DataSet implements IDataSet {

	protected List<IColumn> columns = new ArrayList<IColumn>();
	protected Map<String, IColumn> columnMap = new HashMap<String, IColumn>();
	protected Map<Integer, IColumn> columnIndexMap = new HashMap<Integer, IColumn>();
	
	/**
	 * Add a new column.
	 * @param name
	 * @return
	 * @throws ETLException 
	 */
	public IColumn addColumn(String name) throws ETLException {
		IColumn column = new Column(name); 
		addColumn(column);
		return column;
	}
	
	/**
	 * Add a new column.
	 * @param name
	 * @param idxT
	 * @return
	 * @throws ETLException 
	 */
	public IColumn addColumn(String name, int idx) throws ETLException {
		IColumn column = new Column(name, idx); 
		addColumn(column);
		return column;
	}
	
	/**
	 * Add a column to the data set. 
	 * @param column
	 * @throws ETLException 
	 */
	public void addColumn(IColumn column) throws ETLException {
		if (column == null) {
			throw new NullPointerException("Column must be not null");
		}
		if (column.getName() == null) {
			String baseName = "Col-" + columns.size();
			String name = baseName;
			int idx = 0;
			while (columnMap.containsKey(name)) {
				name = baseName + "." + (idx++);
			}
			column.setName(name);
		} else if (columnMap.containsKey(column.getName())) {
			throw new ETLException("A column with that name already exists in this dataset.");
		}
		
		if (column.getSourceIndex() != -1) {
			if (columnIndexMap.containsKey(column.getSourceIndex())) {
				throw new ETLException("A column with that source index already exists in this dataset.");
			}
			columnIndexMap.put(column.getSourceIndex(), column);
		}
		
		columns.add(column);
		columnMap.put(column.getName(), column);
	}
	
	/**
	 * Returns the list of columns.
	 * @return
	 */
	public List<IColumn> getColumns() {
		return Collections.unmodifiableList(columns);
	}
	
	/**
	 * Returns the column with the specified name.
	 * @param name
	 * @return
	 * @throws ETLException 
	 */
	public IColumn getColumn(String name) throws ETLException {
		IColumn col = columnMap.get(name);
		if (col == null) {
			throw new ETLException("A column with the name " + name + " does not exist.");
		}
		return col;
	}

	/**
	 * Returns the column at the specified column index.
	 * @param idx
	 * @return
	 * @throws ETLException
	 */
	public IColumn getColumnByIndex(int idx) throws ETLException {
		IColumn col = columnIndexMap.get(idx);
		if (col == null) {
			throw new ETLException("A column with the index " + idx + " does not exist.");
		}
		return col;
	}
	
	/**
	 * Returns true if the specified column exists.
	 * @param name
	 * @return
	 */
	public boolean containsColumn(String name) {
		return columnMap.containsKey(name);
	}
	
}
