/**
 * 
 */
package de.xwic.etlgine;

/**
 * @author Developer
 *
 */
public interface IContext {
	
	/**
	 * Set a global property.
	 * @param name
	 * @param value
	 */
	public void setProperty(String name, String value);

	/**
	 * Returns a global property.
	 * @param name
	 * @return
	 */
	public String getProperty(String name);

	/**
	 * Get a global property.
	 * @param name
	 * @param value
	 */
	public String getProperty(String name, String defaultValue);
	
	/**
	 * Returns the property value as boolean value. The value is true if
	 * it is either "true", "yes" or "1".
	 * 
	 * @param name
	 * @param defaultValue
	 * @return
	 */
	public boolean getPropertyBoolean(String name, boolean defaultValue);
	
	/**
	 * Set a global object.
	 * @param name
	 * @param object
	 */
	public void setData(String name, Object object);

	/**
	 * Returns a global object.
	 * @param name
	 * @return
	 */
	public Object getData(String name);

	/**
	 * Returns a global object.
	 * @param name
	 * @param object
	 */
	public Object getData(String name, Object defaultObject);

}
