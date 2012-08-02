/**
 * 
 */
package de.xwic.etlgine;

import java.util.List;

/**
 * A process is sequentially executed within a IProcessChain. 
 * @author lippisch
 */
public interface IProcess {

	/**
	 * Start the process. Usualy this method is invoked from the IProcessChain.
	 */
	public Result start() throws ETLException;
	
	/**
	 * After the process has been finished, the result is returned. If the process.start()
	 * method was never called, the result is NULL.
	 * @return
	 */
	public Result getResult();

	/**
	 * @return the monitor
	 */
	public IMonitor getMonitor();

	/**
	 * @param monitor the monitor to set
	 */
	public void setMonitor(IMonitor monitor);

	/**
	 * @return the name
	 */
	public String getName();

	/**
	 * @return the context
	 */
	public IProcessContext getContext();

	/**
	 * Add a process finalizer.
	 */
	public void addProcessFinalizer(IProcessFinalizer finalizer);
	
	/**
	 * Returns the process finalizers.
	 * @return
	 */
	public List<IProcessFinalizer> getProcessFinalizers();

	/**
	 * Returns the process creator information, like the groovy script name.
	 * @return
	 */
	public String getCreatorInfo();
}
