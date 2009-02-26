/**
 * 
 */
package de.xwic.etlgine;

/**
 * @author Developer
 *
 */
public interface IContext {
	
	public static final String PROPERTY_SCRIPTPATH = "scriptpath";
	public static final String PROPERTY_ROOTPATH = "rootPath";
	
	public static final String DATA_CUBEHANDLER = "cubeHandler";
	
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

	/**
	 * Returns the parentContext if instance of Context. 
	 * @return
	 */
	public IContext getParentContext();
}
