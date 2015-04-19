/**
 * 
 */
package de.xwic.etlgine.impl;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import de.xwic.etlgine.DefaultMonitor;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IGlobalContext;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IJobFinalizer;
import de.xwic.etlgine.IMonitor;
import de.xwic.etlgine.IETLProcess;
import de.xwic.etlgine.IMonitor.EventType;
import de.xwic.etlgine.IProcess;
import de.xwic.etlgine.IProcessChain;
import de.xwic.etlgine.Result;

/**
 * A chain of processes.
 * @author Florian Lippisch
 */
public class ProcessChain implements IProcessChain {

	private final String name;
	private IMonitor monitor = new DefaultMonitor();
	private IGlobalContext globalContext;

	private List<IProcess> processList = new ArrayList<IProcess>();
	
	private IProcess activeProcess = null;
	private Result result = null;
	
	protected List<IJobFinalizer> finalizers = new ArrayList<IJobFinalizer>();

	private String creatorInfo = null;
	
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
		process.setMonitor(monitor);
		monitor.onEvent(process.getContext(), EventType.PROCESSCHAIN_ADD_CUSTOM_PROCESS, process);
		processList.add(process);
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#addCustomProcess(int, de.xwic.etlgine.IProcess)
	 */
	public void addCustomProcess(int index, IProcess process) {
		process.setMonitor(monitor);
		monitor.onEvent(process.getContext(), EventType.PROCESSCHAIN_ADD_CUSTOM_PROCESS, process);
		processList.add(index, process);
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#addProcess(de.xwic.etlgine.IProcess)
	 */
	public IETLProcess createProcess(String name) {
		IETLProcess process = new ETLProcess(globalContext, name);
		process.setMonitor(monitor);
		monitor.onEvent(process.getContext(), EventType.PROCESSCHAIN_CREATE_PROCESS, process);
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
		
		ETLProcess process = new ETLProcess(globalContext, name);
		process.setMonitor(monitor);
		process.setScriptFilename(file.getAbsolutePath());
		process.setCreatorInfo(filename);
		
		monitor.onEvent(process.getContext(), EventType.PROCESSCHAIN_CREATE_PROCESS_FROM_SCRIPT, process);
		
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
	
	/*
	 * (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#createProcessFromScript(java.lang.String, java.lang.String, int)
	 */
	public IETLProcess createProcessFromScript(String name, String filename, int index) throws FileNotFoundException, ETLException {
		
		File jobPath = new File(globalContext.getProperty("scriptpath", "."));
		if (!jobPath.exists()) {
			throw new ETLException("The job path " + jobPath.getAbsolutePath() + " does not exist.");
		}
		
		File file = new File(jobPath, filename);
		if (!file.exists()) {
			throw new FileNotFoundException(file.getAbsolutePath());
		}
		
		ETLProcess process = new ETLProcess(globalContext, name);
		process.setMonitor(monitor);
		process.setScriptFilename(file.getAbsolutePath());
		process.setCreatorInfo(filename);
		
		monitor.onEvent(process.getContext(), EventType.PROCESSCHAIN_CREATE_PROCESS_FROM_SCRIPT, process);
		
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

		processList.add(index, process);
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
		// keep globalContext's monitor up to date
		if (globalContext != null) {
			globalContext.setMonitor(monitor);
		}
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#start()
	 */
	public void start() throws ETLException {
		
		monitor.onEvent(globalContext, EventType.PROCESSCHAIN_START, this);

		try {
			result = Result.SUCCESSFULL;
			
			/*for (IProcess process : processList)
			  changed to this iteration form to support adding new processes dynamically 	
			*/
			for (int iCnt = 0; iCnt < processList.size(); iCnt++) {
				IProcess process = processList.get(iCnt);
				activeProcess = process;
				Result pResult = process.start();
				if (pResult == Result.FAILED || pResult == Result.FINISHED_WITH_ERRORS) {
					result = pResult;
					monitor.logError("Exiting ProcessChain execution because process " + process.getName() + " finished with result: " + result);
					break;
				}
			}
		} catch (ETLException ee) {
			if (ee.getProcess() == null) {
				ee.setProcess(activeProcess);
			}
			throw ee;
		} catch (Throwable t) {
			ETLException ee = new ETLException("Error executing job: " + t, t);
			ee.setProcess(activeProcess);
			throw ee;
		} finally {
			activeProcess = null;
		}

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#finish(de.xwic.etlgine.IJob)
	 */
	public void finish(IJob job) {
		// run finalizers, allow modification during the loop
		for (int i = 0; i < finalizers.size(); i++) {
			IJobFinalizer finalizer = finalizers.get(i);
			try {
				finalizer.onFinish(job);
			} catch (Throwable t) {
				monitor.logError("Error executing finalizer!", t);
			}
		}
		
		monitor.onEvent(globalContext, EventType.PROCESSCHAIN_FINISHED, this);
	}

	/**
	 * @return the activeProcess
	 */
	public IProcess getActiveProcess() {
		return activeProcess;
	}

	/**
	 * @return the result
	 */
	public Result getResult() {
		return result;
	}

	/**
	 * @param result the result to set
	 */
	public void setResult(Result result) {
		this.result = result;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#addJobFinalizer(de.xwic.etlgine.IJobFinalizer)
	 */
	public void addJobFinalizer(IJobFinalizer finalizer) {
		finalizers.add(finalizer);
		monitor.logInfo("Added job finalizer '" + finalizer + "' at index " + (finalizers.size() - 1));
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#getJobFinalizers()
	 */
	public List<IJobFinalizer> getJobFinalizers() {
		return Collections.unmodifiableList(finalizers);
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessChain#getProcesses()
	 */
	@Override
	public Collection<IProcess> getProcesses() {
		return Collections.unmodifiableCollection(processList);
	}

	/**
	 * @return the creatorInfo
	 */
	public String getCreatorInfo() {
		return creatorInfo;
	}

	/**
	 * @param creatorInfo the creatorInfo to set
	 */
	public void setCreatorInfo(String creatorInfo) {
		this.creatorInfo = creatorInfo;
	}

}
