/*
 * de.xwic.etlgine.loader.cube.MeasureMapping 
 */
package de.xwic.etlgine.loader.cube;

import de.xwic.cube.ICube;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;

/**
 * @author lippisch
 */
public class MeasureMapping {

	protected String columnName = null;
	protected Double fixedValue = null;
	protected int measureIndex;
	
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
		if (fixedValue != null || columnName == null) {
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
	 * @param processContext
	 * @param cube
	 */
	public void afterConfiguration(IProcessContext processContext, ICube cube) {
		
		
	}

	/**
	 * @return the measureIndex
	 */
	public int getMeasureIndex() {
		return measureIndex;
	}

	/**
	 * @param measureIndex the measureIndex to set
	 */
	public void setMeasureIndex(int measureIndex) {
		this.measureIndex = measureIndex;
	}

}
