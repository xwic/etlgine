/**
 * 
 */
package de.xwic.etlgine.impl;

import java.util.HashMap;
import java.util.Map;

import de.xwic.etlgine.IContext;

/**
 * @author Developer
 */
public abstract class Context implements IContext {

	protected IContext parentContext = null; 
	protected Map<String, String> properties = new HashMap<String, String>();
	protected Map<String, Object> globals = new HashMap<String, Object>();
	
	/**
	 * Default Constructor.
	 */
	public Context() {
		
	}
	
	/**
	 * Create a context based on a parent context.
	 * @param parentContext
	 */
	public Context(IContext parentContext) {
		this.parentContext = parentContext;
	}
	
	/**
	 * Set a global property.
	 * @param name
	 * @param value
	 */
	public void setProperty(String name, String value) {
		properties.put(name, value);
	}

	/**
	 * Returns a global property.
	 * @param name
	 * @return
	 */
	public String getProperty(String name) {
		return getProperty(name, null);
	}
	
	/**
	 * Get a global property.
	 * @param name
	 * @param value
	 */
	public String getProperty(String name, String defaultValue) {
		String value = properties.get(name);
		if (value == null) {
			if (parentContext == null) {
				return defaultValue;
			} else {
				return parentContext.getProperty(name, defaultValue);
			}
		}
		return value;
	}
	
	/**
	 * Returns the property value as boolean value. The value is true if
	 * it is either "true", "yes" or "1".
	 * 
	 * @param name
	 * @param defaultValue
	 * @return
	 */
	public boolean getPropertyBoolean(String name, boolean defaultValue) {
		String value = getProperty(name);
		if (value == null) {
			return defaultValue;
		}
		return value.equalsIgnoreCase("true") || value.equalsIgnoreCase("yes") || value.equals("1");
	}

	/**
	 * Set a global object.
	 * @param name
	 * @param object
	 */
	public void setData(String name, Object object) {
		globals.put(name, object);
	}

	/**
	 * Returns a global object.
	 * @param name
	 * @return
	 */
	public Object getData(String name) {
		return getData(name, null);
	}
	
	/**
	 * Returns a global object.
	 * @param name
	 * @param object
	 */
	public Object getData(String name, Object defaultObject) {
		Object value = globals.get(name);
		if (value == null) {
			if (parentContext == null) {
				return defaultObject;
			} else {
				return parentContext.getData(name, defaultObject);
			}
		}
		return value;
	}

}
