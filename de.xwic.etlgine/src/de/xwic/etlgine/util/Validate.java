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
