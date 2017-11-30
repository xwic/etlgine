/**
 * 
 */
package de.xwic.etlgine;

/**
 * @author jbornema
 *
 */
public interface IDataSetColumnAdded {

	/**
	 * Event triggered when column added to DataSet.
	 * @param dataSet
	 * @param column
	 */
	void onDataSetColumnAdded(IDataSet dataSet, IColumn column) throws ETLException;
}
