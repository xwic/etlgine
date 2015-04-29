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
	 * Add a custom process at the specified index. This is useful when adding processes dynamically.
	 * It is important that the current running process to add a new process only after it in the list of processes.
	 * Adding a new process before it will not work since the index passed that value.
	 *  
	 * @param index 
	 * @param process
	 */
	public void addCustomProcess(int index, IProcess process);
	
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
	 * Create a new process from a script file and adds it at the specified index in the processes list. This is useful for creating
	 * dynamically processes from inside an process. It is important the index to be greater than the curent process index in order 
	 * to be executed.   
	 * 
	 * @param name
	 * @param filename
	 * @param index
	 * @return
	 * @throws FileNotFoundException
	 * @throws ETLException
	 */
	public IETLProcess createProcessFromScript(String name, String filename, int index) throws FileNotFoundException, ETLException;

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
	
	/**
	 * Returns the current job that created the process chain
	 * @return
	 */
	public IJob getJob();
}
