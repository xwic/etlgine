/**
 * 
 */
package de.xwic.etlgine.impl;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IGlobalContext;
import de.xwic.etlgine.IMonitor;
import de.xwic.etlgine.IETLProcess;
import de.xwic.etlgine.IProcess;
import de.xwic.etlgine.IProcessChain;
import de.xwic.etlgine.Result;

/**
 * A chain of processes.
 * @author Florian Lippisch
 */
public class ProcessChain implements IProcessChain {

	private final String name;
	private IMonitor monitor;
	private IGlobalContext globalContext;

	private List<IProcess> processList = new ArrayList<IProcess>();
	
	private IProcess activeProcess = null;
	
	/**
	 * Constructor.
	 * @param name
	 */
	public ProcessChain(String name) {
		this.name = name;
		this.globalContext = new GlobalContext();
	}

	/**
	 * 
	 * @param parentContext
	 * @param name
	 */
	public ProcessChain(IContext parentContext, String name) {
		this.name = name;
		this.globalContext = new GlobalContext(parentContext);
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#addCustomProcess(de.xwic.etlgine.IProcess)
	 */
	public void addCustomProcess(IProcess process) {
		processList.add(process);
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#addProcess(de.xwic.etlgine.IProcess)
	 */
	public IETLProcess createProcess(String name) {
		IETLProcess process = new ETLProcess(globalContext, name);
		processList.add(process);
		return process;
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#createProcessFromScript(java.lang.String)
	 */
	public IETLProcess createProcessFromScript(String name, String filename) throws FileNotFoundException, ETLException {
		
		File jobPath = new File(globalContext.getProperty("scriptpath", "."));
		if (!jobPath.exists()) {
			throw new ETLException("The job path " + jobPath.getAbsolutePath() + " does not exist.");
		}
		
		File file = new File(jobPath, filename);
		if (!file.exists()) {
			throw new FileNotFoundException(file.getAbsolutePath());
		}
		
		IETLProcess process = new ETLProcess(globalContext, name);
		
		Binding binding = new Binding();
		binding.setVariable("context", globalContext);
		binding.setVariable("process", process);
		binding.setVariable("processChain", this);

		GroovyShell shell = new GroovyShell(binding);
		try {
			shell.evaluate(file);
		} catch (Exception e) {
			throw new ETLException("Error evaluating script '" + file.getName() + "':" + e, e);
		}

		processList.add(process);
		return process;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#getGlobalContext()
	 */
	public IGlobalContext getGlobalContext() {
		return globalContext;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#getMonitor()
	 */
	public IMonitor getMonitor() {
		return monitor;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#getName()
	 */
	public String getName() {
		return name;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#setMonitor(de.xwic.etlgine.IMonitor)
	 */
	public void setMonitor(IMonitor monitor) {
		this.monitor = monitor;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#start()
	 */
	public void start() throws ETLException {
		
		try {
			for (IProcess process : processList) {
				activeProcess = process;
				Result result = process.start();
				if (result == Result.FAILED || result == Result.FINISHED_WITH_ERRORS) {
					monitor.logError("Exiting ProcessChain execution because process " + process.getName() + " finished with result: " + result);
					break;
				}
			}
		} finally {
			activeProcess = null;
		}

	}

	/**
	 * @return the activeProcess
	 */
	public IProcess getActiveProcess() {
		return activeProcess;
	}

}
