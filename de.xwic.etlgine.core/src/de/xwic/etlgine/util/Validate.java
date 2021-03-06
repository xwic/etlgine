/**
 * 
 */
package de.xwic.etlgine.util;

import java.util.Collection;
import java.util.Map;

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

	/**
	 * Returns true if the specified Object is a String, isn't null and not empty (trimmed!).
	 * @param string
	 * @return
	 */
	public static boolean validString(Object object) {
		return object != null && object instanceof String && ((String)object).trim().length() > 0;
	}
	
    /**
     * Returns true if the specified Object isn't null and not empty (toString().trim().length() > 0).
     * It's the users responsibility to ensure that toString() delivers a valid value, also "" or null if the
     * object in the case the object is empty/has no value/invalid.
     * @param Object
     * @return
     */
    public static boolean validObject(Object object) {
        return object != null && object.toString() != null && object.toString().trim().length() > 0;
    }
	
	
	/**
	 * Returns true if the collection isn't null and not empty. 
	 * @param collection
	 * @return
	 */
	public static boolean notEmpty(Collection<?> collection) {
		return collection != null && collection.size() > 0;
	}
    /**
     * Returns true if the parameter is a Collection, isn't null and not empty. 
     * @param collection
     * @return
     */
    public static boolean validCollection(Object collection) {
        return collection != null && collection instanceof Collection && ((Collection<?>) collection).size() > 0;
    }

    /**
     * Returns true if the map isn't null and not empty. 
     * @param collection
     * @return
     */
	public static boolean notEmpty(Map<?,?> map) {
	    return map != null && map.size() > 0;
	}

    /**
     * Returns true if the parameter is a Map, isn't null and not empty. 
     * @param collection
     * @return
     */
	public static boolean validMap(Object  map) {
        return map != null && map instanceof Map && ((Map<?, ?>)map).size() > 0;
    }

	/**
	 * Throws an ETLException if the specified string is null or empty.
	 * @param string
	 * @param message
	 * @throws ETLException
	 */
	public static void validString(Object string, String message) throws ETLException {
		if (validString(string) == false) {
			throw new ETLException(message);
		}
	}
	
	/**
	 * Throws an ETLException if the specified collection is null or empty.
	 * @param collection
	 * @param message
	 * @throws ETLException
	 */
	public static void notEmpty(Collection<?> collection, String message) throws ETLException {
		if (notEmpty(collection) == false) {
			throw new ETLException(message);
		}
	}
}
