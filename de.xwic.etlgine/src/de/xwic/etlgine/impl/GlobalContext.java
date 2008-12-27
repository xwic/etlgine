/*
 * de.xwic.etlgine.impl.GlobalContext 
 */
package de.xwic.etlgine.impl;

import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IGlobalContext;
import de.xwic.etlgine.IMonitor;

/**
 * @author lippisch
 */
public class GlobalContext extends Context implements IGlobalContext {

	protected IMonitor monitor = null;

	/**
	 * 
	 */
	public GlobalContext() {
		super();
	}

	/**
	 * @param parentContext
	 */
	public GlobalContext(IContext parentContext) {
		super(parentContext);
	}

	/**
	 * @return the monitor
	 */
	public IMonitor getMonitor() {
		return monitor;
	}

	/**
	 * @param monitor the monitor to set
	 */
	public void setMonitor(IMonitor monitor) {
		this.monitor = monitor;
	}

}
