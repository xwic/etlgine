/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.ei;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import de.xwic.etlgine.util.CryptHelper;

/**
 * @author lippisch
 */
public class PropertiesCredentialStore implements ICredentialStore {

	private File credentialFile; 
	
	/**
	 * @param credentialFile
	 */
	public PropertiesCredentialStore(File credentialFile) {
		super();
		this.credentialFile = credentialFile;
	}

	/* (non-Javadoc)
	 * @see com.netapp.pulse.ei.ICrendentialStore#getCredentials(java.lang.String)
	 */
	@Override
	public synchronized Credentials getCredentials(String credentialIds) throws EIException {
		
		// open file and read
		if (credentialFile.exists()) {
			Properties prop = new Properties();
			try {
				FileInputStream fin = new FileInputStream(credentialFile);
				try {
					prop.load(fin);
				} finally {
					fin.close();
				}
			} catch (IOException ie) {
				throw new EIException("Error reading credential store: " + ie, ie);
			}
			
			String userName = prop.getProperty(credentialIds + ".user");
			String password = prop.getProperty(credentialIds + ".password");
			if (userName != null) {
				Credentials cr = new Credentials();
				cr.setUsername(CryptHelper.decryptFromString(userName));
				if (password != null){
					cr.setPassword(CryptHelper.decryptFromString(password));
				}
				return cr;
			}
			
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see com.netapp.pulse.ei.ICrendentialStore#updateCredentials(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public synchronized void updateCredentials(String credentialIds, String username, String password) throws EIException {
		
		Properties prop = new Properties();

		// open file and read
		if (credentialFile.exists()) {
			try {
				FileInputStream fin = new FileInputStream(credentialFile);
				try {
					prop.load(fin);
				} finally {
					fin.close();
				}
			} catch (IOException ie) {
				throw new EIException("Error reading credential store: " + ie, ie);
			}
		}
	
		if (username == null) {
			prop.remove(credentialIds + ".user");
			prop.remove(credentialIds + ".password");
		} else {
			prop.setProperty(credentialIds + ".user", CryptHelper.encryptToString(username));
			if (password == null) {
				prop.remove(credentialIds + ".password");
			} else {
				prop.setProperty(credentialIds + ".password", CryptHelper.encryptToString(password));
			}
		}
		
		try {
			FileOutputStream fos = new FileOutputStream(credentialFile, false);
			try {
				prop.store(fos, "CredentialStoreUpdate");
			} finally {
				fos.close();
			}
		} catch (IOException e) {
			throw new EIException("Error writing credential store!");
		}
		
	}

}
