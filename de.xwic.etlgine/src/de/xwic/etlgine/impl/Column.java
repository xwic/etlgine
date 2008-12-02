/*
 * de.xwic.etlgine.impl.Column 
 */
package de.xwic.etlgine.impl;

import de.xwic.etlgine.IColumn;

/**
 * @author lippisch
 */
public class Column implements IColumn {

	private String name = null;
	private int sourceIndex = -1;

	public Column() {
		
	}
	
	/**
	 * @param name
	 */
	public Column(String name) {
		super();
		this.name = name;
	}

	/**
	 * @param name
	 * @param sourceIndex
	 */
	public Column(String name, int sourceIndex) {
		super();
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
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the sourceIndex
	 */
	public int getSourceIndex() {
		return sourceIndex;
	}

	/**
	 * @param sourceIndex the sourceIndex to set
	 */
	public void setSourceIndex(int sourceIndex) {
		this.sourceIndex = sourceIndex;
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
	
	
	
}
