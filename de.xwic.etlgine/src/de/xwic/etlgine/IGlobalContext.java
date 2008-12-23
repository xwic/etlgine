/*
 * de.xwic.etlgine.IEtlContext 
 */
package de.xwic.etlgine;


/**
 * The context is used by the process participants to share the data.
 * 
 * @author lippisch
 */
public interface IGlobalContext {

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
	 * Set a global object.
	 * @param name
	 * @param object
	 */
	public void setGlobal(String name, Object object);

	/**
	 * Returns a global object.
	 * @param name
	 * @return
	 */
	public Object getGlobal(String name);

	/**
	 * Returns a global object.
	 * @param name
	 * @param object
	 */
	public Object getGlobal(String name, Object defaultObject);

	/**
	 * @return the monitor
	 */
	public IMonitor getMonitor();

	/**
	 * @param monitor the monitor to set
	 */
	public void setMonitor(IMonitor monitor);

}
