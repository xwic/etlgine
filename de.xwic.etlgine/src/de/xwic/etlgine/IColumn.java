/*
 * de.xwic.etlgine.IColumn 
 */
package de.xwic.etlgine;


/**
 * @author lippisch
 */
public interface IColumn {

	public enum DataType {
		UNKNOWN,
		STRING,
		INT,
		LONG,
		DOUBLE,
		DATE,
		DATETIME
	}
	
	
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

	/**
	 * @return the targetName
	 */
	public String getTargetName();

	/**
	 * @param targetName the targetName to set
	 */
	public void setTargetName(String targetName);

	/**
	 * @return the typeHint
	 */
	public DataType getTypeHint();

	/**
	 * @param typeHint the typeHint to set
	 */
	public void setTypeHint(DataType typeHint);

	/**
	 * Returns the name for this column used by the loader.
	 * @return
	 */
	public String computeTargetName();

}
