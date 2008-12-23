/*
 * de.xwic.etlgine.loader.cube.MeasureMapping 
 */
package de.xwic.etlgine.loader.cube;

import de.xwic.cube.ICube;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IRecord;

/**
 * @author lippisch
 */
public class MeasureMapping {

	private String columnName = null;
	private Double fixedValue = null; 
	
	/**
	 * @return the columnName
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * @param columnName the columnName to set
	 */
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	/**
	 * @param cube
	 * @param record
	 * @return
	 * @throws ETLException 
	 */
	public Double getValue(ICube cube, IRecord record) throws ETLException {
		if (fixedValue != null) {
			return fixedValue;
		}
		return record.getDataAsDouble(columnName);
	}

	/**
	 * @return the fixedValue
	 */
	public Double getFixedValue() {
		return fixedValue;
	}

	/**
	 * @param fixedValue the fixedValue to set
	 */
	public void setFixedValue(Double fixedValue) {
		this.fixedValue = fixedValue;
	}

	/**
	 * @param context
	 * @param cube
	 */
	public void afterConfiguration(IContext context, ICube cube) {
		
		
	}
	
}
