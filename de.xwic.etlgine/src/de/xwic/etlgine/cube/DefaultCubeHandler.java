/**
 * $Id: $
 *
 * Copyright (c) 2008 Network Appliance.
 * All rights reserved.

 * de.xwic.etlgine.cube.DefaultCubeHandler.java
 * Created on Jan 22, 2009
 * 
 * @author JBORNEMA
 */
package de.xwic.etlgine.cube;

import de.xwic.cube.ICube;
import de.xwic.cube.IDataPool;
import de.xwic.cube.StorageException;
import de.xwic.etlgine.IContext;

/**
 * Created on Jan 22, 2009
 * @author JBORNEMA
 */

public class DefaultCubeHandler extends CubeHandler {

	protected String defaultDataPoolKey;
	protected String defaultCubeKey;
	
	/**
	 * @param context
	 */
	public DefaultCubeHandler(IContext context) {
		super(context);
	}

	/**
	 * @return the defaultDataPoolKey
	 */
	public String getDefaultDataPoolKey() {
		return defaultDataPoolKey;
	}

	/**
	 * @param defaultDataPoolKey the defaultDataPoolKey to set
	 */
	public void setDefaultDataPoolKey(String defaultDataPool) {
		this.defaultDataPoolKey = defaultDataPool;
	}

	/**
	 * @return the defaultCubeKey
	 */
	public String getDefaultCubeKey() {
		return defaultCubeKey;
	}

	/**
	 * @param defaultCubeKey the defaultCubeKey to set
	 */
	public void setDefaultCubeKey(String defaultCube) {
		this.defaultCubeKey = defaultCube;
	}

	/**
	 * 
	 * @return
	 * @throws StorageException
	 */
	public IDataPool getDefaultDataPool() throws StorageException {
		return getDataPoolManager(defaultDataPoolKey).getDataPool(defaultDataPoolKey);
	}
	
	/**
	 * 
	 * @return
	 * @throws StorageException 
	 */
	public ICube getDefaultCube() throws StorageException {
		return getDefaultDataPool().getCube(defaultCubeKey);
	}
}
