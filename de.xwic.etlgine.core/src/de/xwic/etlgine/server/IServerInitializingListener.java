/**
 * 
 */
package de.xwic.etlgine.server;

/**
 * @author JBORNEMA
 *
 */
public interface IServerInitializingListener {

	/**
	 * Gets called during ETLgineServer initializeServer method right after the ServerContext is available and before loading the jobs.
	 * @param etLgineServer
	 * @throws Exception
	 */
	void initializingServer(ETLgineServer etLgineServer) throws Exception;

}
