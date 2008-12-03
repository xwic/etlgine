/*
 * de.xwic.etlgine.loader.cube.DimensionMapping 
 */
package de.xwic.etlgine.loader.cube;

import de.xwic.cube.ICube;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IRecord;


/**
 * @author lippisch
 */
public class DimensionMapping {

	private IDimension dimension = null;
	private String[] columnNames = null;
	private boolean autoCreate = true;
	private IDimensionElement unmappedElement = null;
	
	/**
	 * Constructor.
	 * @param dimension
	 */
	public DimensionMapping(IDimension dimension) {
		this.dimension = dimension;
	}
	
	/**
	 * @return the columnNames
	 */
	public String[] getColumnNames() {
		return columnNames;
	}
	/**
	 * @param columnNames the columnNames to set
	 */
	public void setColumnNames(String[] columnNames) {
		this.columnNames = columnNames;
	}
	/**
	 * @return the autoCreate
	 */
	public boolean isAutoCreate() {
		return autoCreate;
	}
	/**
	 * @param autoCreate the autoCreate to set
	 */
	public void setAutoCreate(boolean autoCreate) {
		this.autoCreate = autoCreate;
	}
	/**
	 * @return the unmappedElementKey
	 */
	public IDimensionElement getUnmappedElementKey() {
		return unmappedElement;
	}
	/**
	 * @param unmappedElement the unmappedElement to set
	 */
	public void setUnmappedElement(IDimensionElement unmappedElement) {
		this.unmappedElement = unmappedElement;
	}

	/**
	 * Map the element.
	 * @param cube
	 * @param record
	 * @return
	 * @throws ETLException
	 */
	public IDimensionElement mapElement(ICube cube, IRecord record) throws ETLException {
		StringBuilder sb = new StringBuilder();
		for (String col : columnNames) {
			String val = record.getDataAsString(col);
			if (sb.length() != 0) {
				sb.append("/");
			}
			sb.append(val);
		}
		String[] path = sb.toString().split("/");
		IDimensionElement elm = dimension;
		for (String key : path) {
			if (elm.containsDimensionElement(key)) {
				elm = elm.getDimensionElement(key);
			} else {
				if (autoCreate) {
					elm = elm.createDimensionElement(key);
				} else {
					if (unmappedElement != null) {
						return unmappedElement;
					} else {
						record.markInvalid("Element " + sb + " not found in dimension " + dimension.getKey() + " - cant mapp data.");
						return null;
					}
				}
			}
		}
		return elm;
	}
	
}
