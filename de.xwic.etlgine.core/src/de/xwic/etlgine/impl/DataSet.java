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
import de.xwic.etlgine.IDataSetColumnAdded;

/**
 * @author lippisch
 */
public class DataSet implements IDataSet {

	protected List<IColumn> columns = new ArrayList<IColumn>();
	protected Map<String, IColumn> columnMap = new HashMap<String, IColumn>();
	protected Map<String, IColumn> aliasMap = new HashMap<String, IColumn>();
	protected Map<Integer, IColumn> columnIndexMap = new HashMap<Integer, IColumn>();
	
	protected List<IDataSetColumnAdded> onColumnAdded = new ArrayList<IDataSetColumnAdded>();
	/**
	 * Add a new column.
	 * @param name
	 * @return
	 * @throws ETLException 
	 */
	public IColumn addColumn(String name) throws ETLException {
		return addColumn(name, -1);
	}
	
	/**
	 * Add a new column.
	 * @param name
	 * @param idxT
	 * @return
	 * @throws ETLException 
	 */
	public IColumn addColumn(String name, int idx) throws ETLException {
		if (name == null) {
			String baseName = "Col-" + columns.size();
			name = baseName;
			int i = 0;
			while (columnMap.containsKey(name)) {
				name = baseName + "." + (i++);
			}
		}
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
		if (columnMap.containsKey(column.getName())) {
			throw new ETLException("A column with name '" + column + "' already exists in this dataset.");
		}

		updateColumn(column);
		
		columns.add(column);
		columnMap.put(column.getName(), column);
		
		// fire column added event
		for (IDataSetColumnAdded columnAdded : onColumnAdded) {
			columnAdded.onDataSetColumnAdded(this, column);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IDataSet#addAlias(de.xwic.etlgine.IColumn, java.lang.String)
	 */
	public void addAlias(IColumn column, String alias) throws ETLException {
		if (!columns.contains(column)) {
			throw new ETLException("The column is not in the column list.");
		}
		if (columnMap.containsKey(alias)) {
			throw new ETLException("A column with this alias name already exists.");
		}
		if (aliasMap.containsKey(alias)) {
			throw new ETLException("This alias is already in use.");
		}
		aliasMap.put(alias, column);
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
			col = aliasMap.get(name);
			if (col == null) {
				throw new ETLException("A column with the name '" + name + "' does not exist.");
			}
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
		return columnMap.containsKey(name) || aliasMap.containsKey(name);
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IDataSet#updateColumn(de.xwic.etlgine.IColumn)
	 */
	public void updateColumn(IColumn column) throws ETLException {
		if (column.getSourceIndex() != -1) {
			if (columnIndexMap.containsKey(column.getSourceIndex())) {
				throw new ETLException("A column with source index '" + column.getSourceIndex() + "' already exists in this dataset.");
			}
			columnIndexMap.put(column.getSourceIndex(), column);
		}
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IDataSet#addOnDataSetColumnAdded(de.xwic.etlgine.IDataSetColumnAdded)
	 */
	@Override
	public void addOnDataSetColumnAdded(IDataSetColumnAdded columnAddedListener) {
		onColumnAdded.add(columnAddedListener);
	}
	
}
