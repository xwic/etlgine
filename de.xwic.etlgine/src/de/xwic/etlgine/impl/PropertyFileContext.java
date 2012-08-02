/**
 * $Id: $
 *
 * Copyright (c) 2008 Network Appliance.
 * All rights reserved.

 * de.xwic.etlgine.impl.PropertyFileContext.java
 * Created on Jan 22, 2009
 * 
 * @author JBORNEMA
 */
package de.xwic.etlgine.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.IContext;

/**
 * Created on Jan 22, 2009
 * @author JBORNEMA
 */

public class PropertyFileContext extends Context {

	private static final Log log = LogFactory.getLog(PropertyFileContext.class);

	/**
	 * 
	 */
	public PropertyFileContext() {
	}

	/**
	 * @param parentContext
	 */
	public PropertyFileContext(IContext parentContext) {
		super(parentContext);
	}
	
	/**
	 * Just used for spring property setting.
	 * @param name
	 */
	public void setLoadFromFile(String name) {
		loadFromFile(name);
	}
	
	/**
	 * 
	 * @param name
	 * @return
	 */
	public void loadFromFile(String name) {
		Properties props = new Properties();
		try {
			String rootPath = getRootPath();
			if (rootPath != null) {
				File file = new File(rootPath, name);
				if (file.exists()) {
					props.load(new FileInputStream(file));
				} else {
					props.load(new FileInputStream(name));
				}
			} else {
				props.load(new FileInputStream(name));
			}
		} catch (IOException e) {
			log.error("log.error reading server.properties", e);
			return;
		}
		
		// copy properties to context
		for (Object key : props.keySet()) {
			String sKey = (String)key;
			setProperty(sKey, props.getProperty(sKey));
		}
	}
	
	/**
	 * 
	 * @param rootPath
	 */
	public void setRootPath(String rootPath) {
		setProperty(IContext.PROPERTY_ROOTPATH, rootPath);
	}
	
	/**
	 * 
	 * @return
	 */
	public String getRootPath() {
		return getProperty(IContext.PROPERTY_ROOTPATH);
	}
}
