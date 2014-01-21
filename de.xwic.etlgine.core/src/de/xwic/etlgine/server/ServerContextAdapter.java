/**
 * 
 */
package de.xwic.etlgine.server;

/**
 * Adapter for the IServerContextListener
 * @author lippisch
 */
public abstract class ServerContextAdapter implements IServerContextListener {

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.server.IServerContextListener#jobExecutionEnd(de.xwic.etlgine.server.ServerContextEvent)
	 */
	public void jobExecutionEnd(ServerContextEvent event) {

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.server.IServerContextListener#jobExecutionStart(de.xwic.etlgine.server.ServerContextEvent)
	 */
	public void jobExecutionStart(ServerContextEvent event) {

	}

}
