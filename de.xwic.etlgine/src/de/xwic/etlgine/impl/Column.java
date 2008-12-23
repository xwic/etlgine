/*
 * de.xwic.etlgine.impl.Column 
 */
package de.xwic.etlgine.impl;

import de.xwic.etlgine.IColumn;

/**
 * @author lippisch
 */
public class Column implements IColumn {

	public enum DataType {
		UNKNOWN,
		STRING,
		INT,
		LONG,
		DOUBLE,
		DATE,
		DATETIME
	}
	
	private String name = null;
	private String targetName = null;

	private int sourceIndex = -1;
	private boolean exclude = false;
	private DataType typeHint = DataType.UNKNOWN; 

	public Column() {
		
	}
	
	/**
	 * @param name
	 */
	public Column(String name) {
		super();
		if (name == null) {
			throw new NullPointerException("Column name must be not null");
		}
		this.name = name;
	}

	/**
	 * @param name
	 * @param sourceIndex
	 */
	public Column(String name, int sourceIndex) {
		super();
		if (name == null) {
			throw new NullPointerException("Column name must be not null");
		}
		this.name = name;
		this.sourceIndex = sourceIndex;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return the sourceIndex
	 */
	public int getSourceIndex() {
		return sourceIndex;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + sourceIndex;
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Column other = (Column) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (sourceIndex != other.sourceIndex)
			return false;
		return true;
	}

	/**
	 * @return the exclude
	 */
	public boolean isExclude() {
		return exclude;
	}

	/**
	 * If a column is excluded, the loader must ignore the column.
	 * @param exclude the exclude to set
	 */
	public void setExclude(boolean exclude) {
		this.exclude = exclude;
	}

	/**
	 * @return the targetName
	 */
	public String getTargetName() {
		return targetName;
	}

	/**
	 * @param targetName the targetName to set
	 */
	public void setTargetName(String targetName) {
		this.targetName = targetName;
	}

	/**
	 * @return the typeHint
	 */
	public DataType getTypeHint() {
		return typeHint;
	}

	/**
	 * @param typeHint the typeHint to set
	 */
	public void setTypeHint(DataType typeHint) {
		this.typeHint = typeHint;
	}
	
}
