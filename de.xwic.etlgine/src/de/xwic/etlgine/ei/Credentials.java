/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.ei;

/**
 * @author lippisch
 */
public class Credentials {

	private String username = null;
	private String password = null;
	
	/**
	 * 
	 */
	public Credentials() {
	}
	
	/**
	 * @param username
	 * @param password
	 */
	public Credentials(String username, String password) {
		super();
		this.username = username;
		this.password = password;
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
	
	
}
