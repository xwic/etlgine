/*
 * de.xwic.etlgine.IColumn 
 */
package de.xwic.etlgine;

/**
 * @author lippisch
 */
public interface IColumn {

	/**
	 * Remove the column name.
	 * @return
	 */
	public abstract String getName();

	/**
	 * Returns the source index.
	 * @return
	 */
	public abstract int getSourceIndex();

	/**
	 * @return the exclude
	 */
	public boolean isExclude();

	/**
	 * If a column is excluded, the loader must ignore the column.
	 * @param exclude the exclude to set
	 */
	public void setExclude(boolean exclude);

}
