/**
 * $Id: $
 *
 * Copyright (c) 2009 NetApp.
 * All rights reserved.

 * de.xwic.etlgine.server.spring.ETLgineServerFactoryBean.java
 * Created on Feb 27, 2009
 * 
 * @author JBORNEMA
 */
package de.xwic.etlgine.server.spring;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import de.xwic.etlgine.server.ETLgineServer;


/**
 * ETLgine factory bean for spring.
 * 
 * ETLgine Server is launch after properties had been set by calling initialize method.
 * 
 * Created on Feb 27, 2009
 * @author JBORNEMA
 */

public class ETLgineServerFactoryBean implements ApplicationContextAware, FactoryBean, InitializingBean {

	private String rootPath = null;
	private boolean startServer = false;
	
	/* (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	public Object getObject() throws Exception {
		return ETLgineServer.getInstance();
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	@SuppressWarnings("rawtypes")
	public Class getObjectType() {
		return ETLgineServer.class;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	public boolean isSingleton() {
		return true;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		// initialize ETLgine Server
		ETLgineServer server = ETLgineServer.getInstance();
		if (!server.isInitializing() && !server.isInitialized()) {
			if (rootPath != null) {
				server.setRootPath(rootPath);
			}
			server.initialize();
		}
		
		if (startServer && !server.isRunning()) {
			// startup the server.
			Thread serverThread = new Thread(server, "ETLgineServer");
			serverThread.setDaemon(false);
			serverThread.start();		
		}
	}

	/**
	 * @return the rootPath
	 */
	public String getRootPath() {
		return rootPath;
	}

	/**
	 * @param rootPath the rootPath to set
	 */
	public void setRootPath(String rootPath) {
		this.rootPath = rootPath;
	}

	/**
	 * @return the startServer
	 */
	public boolean isStartServer() {
		return startServer;
	}

	/**
	 * @param startServer the startServer to set
	 */
	public void setStartServer(boolean startServer) {
		this.startServer = startServer;
	}
	
}
