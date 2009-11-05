/**
 * 
 */
package de.xwic.etlgine.cube.mapping;

import java.util.Date;

/**
 * @author lippisch
 *
 */
public class DimMappingElementDef {

	private int id = 0;
	private String dimMapKey = null;
	private String dimensionKey = null;
	private String expression = null;
	private boolean regExp = false;
	private boolean ignoreCase = false;
	private String elementPath = null;
	private boolean skipRecord = false;
	private Date validFrom = null;
	private Date validTo = null;

	/**
	 * @return the dimMapKey
	 */
	public String getDimMapKey() {
		return dimMapKey;
	}
	/**
	 * @param dimMapKey the dimMapKey to set
	 */
	public void setDimMapKey(String dimMapKey) {
		this.dimMapKey = dimMapKey;
	}
	/**
	 * @return the dimensionKey
	 */
	public String getDimensionKey() {
		return dimensionKey;
	}
	/**
	 * @param dimensionKey the dimensionKey to set
	 */
	public void setDimensionKey(String dimensionKey) {
		this.dimensionKey = dimensionKey;
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
	 * @return the regExp
	 */
	public boolean isRegExp() {
		return regExp;
	}
	/**
	 * @param regExp the regExp to set
	 */
	public void setRegExp(boolean regExp) {
		this.regExp = regExp;
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
	 * @return the elementPath
	 */
	public String getElementPath() {
		return elementPath;
	}
	/**
	 * @param elementPath the elementPath to set
	 */
	public void setElementPath(String elementPath) {
		this.elementPath = elementPath;
	}
	/**
	 * @return the skipRecord
	 */
	public boolean isSkipRecord() {
		return skipRecord;
	}
	/**
	 * @param skipRecord the skipRecord to set
	 */
	public void setSkipRecord(boolean skipRecord) {
		this.skipRecord = skipRecord;
	}
	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(int id) {
		this.id = id;
	}
	/**
	 * @return the validFrom
	 */
	public Date getValidFrom() {
		return validFrom;
	}
	/**
	 * @param validFrom the validFrom to set
	 */
	public void setValidFrom(Date validFrom) {
		this.validFrom = validFrom;
	}
	/**
	 * @return the validTo
	 */
	public Date getValidTo() {
		return validTo;
	}
	/**
	 * @param validTo the validTo to set
	 */
	public void setValidTo(Date validTo) {
		this.validTo = validTo;
	}
	
}
