/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.ei;

/**
 * Specifies the configuration of a JDBC connection. 
 * @author lippisch
 */
public class ConnectionConfig {

	private String id  = null;
	private boolean traced = false;
	private boolean pooled = false;
	private boolean lazyInit = true;
	
	private String driver = null;
	private String description = null;
	private String url = null;
	
	private String username = null;
	private String password = null;
	
	private int poolSize = 10;
	private int maxWait = 10000;
	private String validationQuery = null;
	
	/**
	 * @param id
	 */
	public ConnectionConfig(String id) {
		super();
		this.id = id;
	}
	
	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}
	/**
	 * @return the traced
	 */
	public boolean isTraced() {
		return traced;
	}
	/**
	 * @param traced the traced to set
	 */
	public void setTraced(boolean traced) {
		this.traced = traced;
	}
	/**
	 * @return the pooled
	 */
	public boolean isPooled() {
		return pooled;
	}
	/**
	 * @param pooled the pooled to set
	 */
	public void setPooled(boolean pooled) {
		this.pooled = pooled;
	}
	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}
	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}
	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}
	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}
	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}
	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}
	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}
	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}
	/**
	 * @return the poolSize
	 */
	public int getPoolSize() {
		return poolSize;
	}
	/**
	 * @param poolSize the poolSize to set
	 */
	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}
	/**
	 * @return the maxWait
	 */
	public int getMaxWait() {
		return maxWait;
	}
	/**
	 * @param maxWait the maxWait to set
	 */
	public void setMaxWait(int maxWait) {
		this.maxWait = maxWait;
	}
	/**
	 * @return the validationQuery
	 */
	public String getValidationQuery() {
		return validationQuery;
	}
	/**
	 * @param validationQuery the validationQuery to set
	 */
	public void setValidationQuery(String validationQuery) {
		this.validationQuery = validationQuery;
	}

	/**
	 * @return the driver
	 */
	public String getDriver() {
		return driver;
	}

	/**
	 * @param driver the driver to set
	 */
	public void setDriver(String driver) {
		this.driver = driver;
	}

	/**
	 * @return the lazyInit
	 */
	public boolean isLazyInit() {
		return lazyInit;
	}

	/**
	 * @param lazyInit the lazyInit to set
	 */
	public void setLazyInit(boolean lazyInit) {
		this.lazyInit = lazyInit;
	}
	
}
