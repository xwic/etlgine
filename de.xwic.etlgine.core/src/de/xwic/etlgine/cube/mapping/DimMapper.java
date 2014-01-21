/**
 * 
 */
package de.xwic.etlgine.cube.mapping;

import java.util.Date;
import java.util.regex.Pattern;

/**
 * Implements the matching rule based on the specified element Mapping. 
 * This class is used to precompile regular expressions for a faster matching.
 * 
 * @author lippisch
 */
public class DimMapper {

	private final DimMappingElementDef elementDef;
	private Pattern pattern = null;
	private String expression = null;
	
	public DimMapper(DimMappingElementDef elementDef) {
		this.elementDef = elementDef;
		if (elementDef.isRegExp()) {
			pattern = Pattern.compile(elementDef.getExpression(), elementDef.isIgnoreCase() ? Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE : 0);
		} else {
			expression = elementDef.getExpression();
		}
	}
	
	/**
	 * Tests if the specified DimMappingElementDef matches the given value.
	 * @param elmDef
	 * @param value
	 * @return
	 */
	public boolean match(String value) {

		if (elementDef.isRegExp()) {
			return pattern.matcher(value).matches();
		} else {
			if (elementDef.isIgnoreCase()) {
				return expression.equalsIgnoreCase(value);
			} else {
				return expression.equals(value);
			}
		}
		
	}
	
	/**
	 * Tests if this mapping applies to the specified date.
	 * @param date
	 * @return
	 */
	public boolean isValid(Date date) {
		if (date == null) {
			// is only valid if both from/to is null
			return elementDef.getValidFrom() == null && elementDef.getValidTo() == null;
		}
		if (elementDef.getValidFrom() != null && elementDef.getValidFrom().after(date)) {
			return false;
		}
		if (elementDef.getValidTo() != null && elementDef.getValidTo().before(date)) {
			return false;
		}
		
		return true;
	}

	/**
	 * @return the elementDef
	 */
	public DimMappingElementDef getDimMappingElementDef() {
		return elementDef;
	}
	
}
