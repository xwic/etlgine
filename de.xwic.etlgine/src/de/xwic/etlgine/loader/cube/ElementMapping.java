/*
 * de.xwic.etlgine.loader.cube.ElementMapping 
 */
package de.xwic.etlgine.loader.cube;

import java.util.regex.Pattern;

import de.xwic.cube.ICube;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.etlgine.IContext;

/**
 * Maps values into DimensionElements.
 * @author lippisch
 */
public class ElementMapping {

	private IDimension dimension = null;
	private boolean isRegExp = false;
	private boolean ignoreCase = false;
	private String expression = null;
	
	private Pattern pattern = null;
	
	private String elementID = null;
	private IDimensionElement element = null;

	public ElementMapping(IDimension dimension) {
		this.dimension = dimension;
	}
	
	/**
	 * @param expression
	 * @param elementID
	 */
	public ElementMapping(IDimension dimension, String elementID, String expression) {
		super();
		this.dimension = dimension;
		this.expression = expression;
		this.elementID = elementID;
	}

	/**
	 * @param elementID
	 * @param expression
	 * @param isRegExp
	 */
	public ElementMapping(IDimension dimension, String elementID, String expression, boolean isRegExp) {
		super();
		this.dimension = dimension;
		this.elementID = elementID;
		this.expression = expression;
		this.isRegExp = isRegExp;
	}

	/**
	 * @return the elementID
	 */
	public String getElementID() {
		return elementID;
	}
	/**
	 * @param elementID the elementID to set
	 */
	public void setElementID(String elementID) {
		this.elementID = elementID;
	}
	/**
	 * @return the element
	 */
	public IDimensionElement getElement() {
		return element;
	}
	/**
	 * @param element the element to set
	 */
	public void setElement(IDimensionElement element) {
		this.element = element;
	}
	/**
	 * @return the isRegExp
	 */
	public boolean isRegExp() {
		return isRegExp;
	}
	/**
	 * @param isRegExp the isRegExp to set
	 */
	public void setRegExp(boolean isRegExp) {
		this.isRegExp = isRegExp;
	}
	/**
	 * @return the ignoreCase
	 */
	public boolean isIgnoreCase() {
		return ignoreCase;
	}
	/**
	 * @param ignoreCase the ignoreCase to set
	 */
	public void setIgnoreCase(boolean ignoreCase) {
		this.ignoreCase = ignoreCase;
	}
	/**
	 * @return the expression
	 */
	public String getExpression() {
		return expression;
	}
	/**
	 * @param expression the expression to set
	 */
	public void setExpression(String expression) {
		this.expression = expression;
	}
	
	/**
	 * Returns true if the specified value matches this element.
	 * @param value
	 * @return
	 */
	public boolean match(String value) {
		if (isRegExp) {
			if (pattern == null) {
				pattern = Pattern.compile(expression);
			}
			return pattern.matcher(value).matches();
		} else {
			if (ignoreCase) {
				return expression.equalsIgnoreCase(value);
			} else {
				return expression.equals(value);
			}
		}
	}

	/**
	 * @param context
	 * @param cube
	 */
	public void afterConfiguration(IContext context, ICube cube) {
		
		if (element == null && elementID != null) {
			element = dimension.parsePath(elementID);
		}
		
	}

	/**
	 * @return the dimension
	 */
	public IDimension getDimension() {
		return dimension;
	}
	
}
