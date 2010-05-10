/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.ei;

/**
 * @author lippisch
 */
public interface ICredentialStore {

	/**
	 * Returns the credentials with the specified ID or null if those are not defeind.
	 * @param credentialIds
	 * @return
	 */
	public Credentials getCredentials(String credentialIds) throws EIException;
	
	/**
	 * Updates the credentials with the given name.
	 * @param credentialIds
	 * @param username
	 * @param password
	 * @throws EIException
	 */
	public void updateCredentials(String credentialIds, String username, String password) throws EIException;
	
}
