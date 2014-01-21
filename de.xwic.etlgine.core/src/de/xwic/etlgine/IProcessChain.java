/**
 * 
 */
package de.xwic.etlgine;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.List;

/**
 * Adds one or more IProcess into a chain, executing each process
 * after the other, if no error condition is set. 
 * 
 * @author Developer
 */
public interface IProcessChain {

	/**
	 * Returns the global context.
	 * @return
	 */
	public IGlobalContext getGlobalContext();
	
	/**
	 * Returns the name of the chain.
	 * @return
	 */
	public String getName();
	
	/**
	 * Add a custom process.
	 * @param process
	 */
	public void addCustomProcess(IProcess process);
	
	/**
	 * Add a process. 
	 * @param process
	 */
	public IETLProcess createProcess(String name);
	
	/**
	 * Start the process chain.
	 * @throws ETLException
	 */
	public void start() throws ETLException;
	
	/*
	 * Finish the process chain.
	 * This calls also the IJobFinalizer.
	 * @param job
	 */
	public void finish(IJob job);
	
	/**
	 * @return the monitor
	 */
	public IMonitor getMonitor();

	/**
	 * @param monitor the monitor to set
	 */
	public void setMonitor(IMonitor monitor);

	/**
	 * Create a new process from a script file.
	 * @param string
	 * @throws FileNotFoundException 
	 * @throws ETLException 
	 */
	public IETLProcess createProcessFromScript(String name, String filename) throws FileNotFoundException, ETLException;

	/**
	 * Returns the currently active process or null.
	 * @return
	 */
	public IProcess getActiveProcess();

	/**
	 * @return the result
	 */
	public Result getResult();

	/**
	 * Add a job finalizer.
	 */
	public void addJobFinalizer(IJobFinalizer finalizer);

	/**
	 * Returns the job finalizers.
	 * @return
	 */
	public List<IJobFinalizer> getJobFinalizers();
	
	/**
	 * Returns the unmodifiable collection of processes.
	 * @return
	 */
	public Collection<IProcess> getProcesses();

	/**
	 * Returns the process chain creator information, like the groovy script name.
	 * @return
	 */
	public String getCreatorInfo();
}
