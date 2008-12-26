/**
 * 
 */
package de.xwic.etlgine.server;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessChain;

/**
 * 
 * @author Developer
 */
public class ServerContext {

	private Map<String, IProcessChain> processChains = new HashMap<String, IProcessChain>(); 
	protected Map<String, String> properties = new HashMap<String, String>();
	protected Map<String, Object> objects = new HashMap<String, Object>();
	
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
			return defaultValue;
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
		String value = properties.get(name);
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
	public void setObject(String name, Object object) {
		objects.put(name, object);
	}

	/**
	 * Returns a global object.
	 * @param name
	 * @return
	 */
	public Object getObject(String name) {
		return getObject(name, null);
	}
	
	/**
	 * Returns a global object.
	 * @param name
	 * @param object
	 */
	public Object getObject(String name, Object defaultObject) {
		Object value = objects.get(name);
		if (value == null) {
			return defaultObject;
		}
		return value;
	}
	
	/**
	 * Load a ProcessChain from a script.
	 * @param name
	 * @param scriptFile
	 */
	public void loadProcessChain(String name, String scriptFile) throws ETLException {
		
	}
	
	/**
	 * Add a ProcessChain.
	 * @param chain
	 * @throws ETLException
	 */
	public void addProcessChain(IProcessChain chain) throws ETLException {
		if (processChains.containsKey(chain.getName())) {
			throw new ETLException("A ProcessChain with this name already exists.");
		}
		processChains.put(chain.getName(), chain);
	}
	
	/**
	 * Returns the ProcessChain with the specified name.
	 * @param name
	 * @return
	 */
	public IProcessChain getProcessChain(String name) {
		return processChains.get(name);
	}
	
	/**
	 * Returns the list of ProcessChains.
	 * @return
	 */
	public Collection<IProcessChain> getProcessChains() {
		return processChains.values();
	}
	
}
