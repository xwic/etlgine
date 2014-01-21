/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.ei;

import java.util.Properties;

/**
 * Specifies configuration details of a connector.
 * @author lippisch
 */
public class ConnectorConfig {
	
	private String id = null;
	private String connectionId = null;
	private String className = null;
	private String credentials = null;
	private Properties properties = new Properties();
	
	/**
	 * @param id
	 */
	public ConnectorConfig(String id) {
		super();
		this.id = id;
	}
	
	/**
	 * @return the connectionId
	 */
	public String getConnectionId() {
		return connectionId;
	}
	/**
	 * @param connectionId the connectionId to set
	 */
	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}
	/**
	 * @return the className
	 */
	public String getClassName() {
		return className;
	}
	/**
	 * @param className the className to set
	 */
	public void setClassName(String className) {
		this.className = className;
	}
	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @return the properties
	 */
	public Properties getProperties() {
		return properties;
	}

	/**
	 * @param properties the properties to set
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	/**
	 * @param key
	 * @param defaultValue
	 * @return
	 * @see java.util.Properties#getProperty(java.lang.String, java.lang.String)
	 */
	public String getProperty(String key, String defaultValue) {
		return properties.getProperty(key, defaultValue);
	}

	/**
	 * @param key
	 * @param value
	 * @return
	 * @see java.util.Properties#setProperty(java.lang.String, java.lang.String)
	 */
	public void setProperty(String key, String value) {
		properties.setProperty(key, value);
	}

	/**
	 * @return the credentials
	 */
	public String getCredentials() {
		return credentials;
	}

	/**
	 * @param credentials the credentials to set
	 */
	public void setCredentials(String credentials) {
		this.credentials = credentials;
	}

	
}
