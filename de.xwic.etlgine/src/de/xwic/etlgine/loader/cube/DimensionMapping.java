/*
 * de.xwic.etlgine.loader.cube.DimensionMapping 
 */
package de.xwic.etlgine.loader.cube;

import java.util.ArrayList;
import java.util.List;

import de.xwic.cube.ICube;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;


/**
 * @author lippisch
 */
public class DimensionMapping {

	private IDimension dimension = null;
	private String[] columnNames = null;
	private boolean autoCreate = true;
	private String unmappedElementPath = null;
	private String contextPropertyName = null;
	private IDimensionElement unmappedElement = null;
	
	private List<ElementMapping> emList = null;
	
	/**
	 * Constructor.
	 * @param dimension
	 */
	public DimensionMapping(IDimension dimension) {
		this.dimension = dimension;
	}
	
	/**
	 * Add an empty ElementMapping
	 * @return
	 */
	public ElementMapping addElementMapping() {
		ElementMapping em = new ElementMapping(dimension);
		addElementMapping(em);
		return em;
	}
	
	/**
	 * Add an ElementMapping.
	 * @param elementID
	 * @param expression
	 * @return
	 */
	public ElementMapping addElementMapping(String elementID, String expression) {
		return addElementMapping(new ElementMapping(dimension, elementID, expression));
	}
	
	/**
	 * Add an ElementMapping.
	 * @param elementID
	 * @param expression
	 * @return
	 */
	public ElementMapping addElementMapping(String elementID, String expression, boolean isRegExp) {
		return addElementMapping(new ElementMapping(dimension, elementID, expression, isRegExp));
	}
	
	/**
	 * Add an ElementMapping.
	 * @param em
	 */
	private ElementMapping addElementMapping(ElementMapping em) {
		if (emList == null) {
			emList = new ArrayList<ElementMapping>();
		}
		emList.add(em);
		return em;
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
	public IDimensionElement getUnmappedElement() {
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
	public IDimensionElement mapElement(IProcessContext processContext, ICube cube, IRecord record) throws ETLException {
		
		String value;
		if (contextPropertyName != null) {
			value = processContext.getProperty(contextPropertyName);
			if (value == null) {
				throw new ETLException("Specified context property '" + contextPropertyName + "' contains null.");
			}
		} else {
			StringBuilder sb = new StringBuilder();
			for (String col : columnNames) {
				String val = record.getDataAsString(col);
				if (sb.length() != 0) {
					sb.append("/");
				}
				sb.append(val);
			}
			value = sb.toString();
		}
		
		
		if (emList == null) {	// values are the keys.
			String[] path = value.split("/");
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
							record.markInvalid("Element " + value + " not found in dimension " + dimension.getKey() + " - cant map data.");
							return null;
						}
					}
				}
			}
			return elm;
		} else {
			for (ElementMapping em : emList) {
				if (em.match(value)) {
					return em.getElement();
				}
			}
			if (unmappedElement != null) {
				return unmappedElement;
			}
			record.markInvalid("No mapping found for element " + value + " in dimension " + dimension.getKey() + " - cant map data.");
			return null;
		}
	}

	/**
	 * @param processContext
	 * @param cube
	 */
	public void afterConfiguration(IProcessContext processContext, ICube cube) {
		if (emList != null) {
			for (ElementMapping em : emList) {
				em.afterConfiguration(processContext, cube);
			}
		}
		if (unmappedElement == null && unmappedElementPath != null) {
			unmappedElement = dimension.parsePath(unmappedElementPath);
		}
	}

	/**
	 * @return the unmappedElementPath
	 */
	public String getUnmappedElementPath() {
		return unmappedElementPath;
	}

	/**
	 * @param unmappedElementPath the unmappedElementPath to set
	 */
	public void setUnmappedElementPath(String unmappedElementPath) {
		this.unmappedElementPath = unmappedElementPath;
	}

	/**
	 * @return the contextPropertyName
	 */
	public String getContextPropertyName() {
		return contextPropertyName;
	}

	/**
	 * The name of a context property that contains the value.
	 * @param contextPropertyName the contextPropertyName to set
	 */
	public void setContextPropertyName(String contextPropertyName) {
		this.contextPropertyName = contextPropertyName;
	}
	
}
