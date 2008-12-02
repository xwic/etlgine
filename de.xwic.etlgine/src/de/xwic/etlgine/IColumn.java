/*
 * de.xwic.etlgine.IColumn 
 */
package de.xwic.etlgine;

/**
 * @author lippisch
 */
public interface IColumn {

	/**
	 * Set the column name.
	 * @param name
	 */
	public abstract void setName(String name);

	/**
	 * Remove the column name.
	 * @return
	 */
	public abstract String getName();

	/**
	 * Set the source index.
	 * @param sourceIndex
	 */
	public abstract void setSourceIndex(int sourceIndex);

	/**
	 * Returns the source index.
	 * @return
	 */
	public abstract int getSourceIndex();

}
