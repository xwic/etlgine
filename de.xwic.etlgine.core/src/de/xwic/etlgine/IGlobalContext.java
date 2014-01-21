/*
 * de.xwic.etlgine.IEtlContext 
 */
package de.xwic.etlgine;


/**
 * The context is used by the process participants to share the data.
 * 
 * @author lippisch
 */
public interface IGlobalContext extends IContext {

	/**
	 * @return the monitor
	 */
	public IMonitor getMonitor();

	/**
	 * @param monitor the monitor to set
	 */
	public void setMonitor(IMonitor monitor);

}
