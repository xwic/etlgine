/**
 * 
 */
package de.xwic.etlgine.server;

/**
 * @author lippisch
 */
public interface IServerContextListener {

	/**
	 * A IJob execution started.
	 * @param event
	 */
	public void jobExecutionStart(ServerContextEvent event);
	
	/**
	 * A IJob execution has ended. The event contains details about the 
	 * job result.
	 * @param event
	 */
	public void jobExecutionEnd(ServerContextEvent event);
	
}
