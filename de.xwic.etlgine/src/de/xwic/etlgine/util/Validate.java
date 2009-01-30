/**
 * 
 */
package de.xwic.etlgine.util;

import de.xwic.etlgine.ETLException;

/**
 * Parameter validation.
 * @author lippisch
 */
public class Validate {

	/**
	 * Returns true if o1 and o2 are both null OR o1.equals(o2).
	 * @param o1
	 * @param o2
	 * @return
	 */
	public static boolean equals(Object o1, Object o2) {
		
		if (o1 == null && o2 == null) {
			return true;
		}
		if (o1 == null || o2 == null) {
			return false;
		}
		return o1.equals(o2);
		
	}
	
	/**
	 * Throws an ETLException if the specified object is null.
	 * @param object
	 * @param message
	 * @throws ETLException
	 */
	public static void notNull(Object object, String message) throws ETLException {
		if (object == null) {
			throw new ETLException(message);
		}
	}
	
}
