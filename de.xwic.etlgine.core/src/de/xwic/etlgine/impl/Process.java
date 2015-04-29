/**
 * 
 */
package de.xwic.etlgine.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import de.xwic.etlgine.DefaultMonitor;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IMonitor;
import de.xwic.etlgine.IProcess;
import de.xwic.etlgine.IProcessChain;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IProcessFinalizer;
import de.xwic.etlgine.Result;

/**
 * @author lippisch
 *
 */
public abstract class Process implements IProcess {

	protected String name;

	protected List<IProcessFinalizer> finalizers = new ArrayList<IProcessFinalizer>();
	protected IMonitor monitor = new DefaultMonitor();
	protected ProcessContext processContext;
	protected Result result = null;

	protected String creatorInfo = null;
	
	/**
	 * The process chain to which this process belongs 
	 */
	protected IProcessChain processChain = null;
	
	/**
	 * Construct a new process.
	 * @param name
	 */
	public Process(String name) {
		this.name = name;
		processContext = new ProcessContext(this);
		processContext.setMonitor(monitor);
	}

	/**
	 * Construct a new process.
	 * @param name
	 */
	public Process(IContext context, String name) {
		this.name = name;
		processContext = new ProcessContext(this, context);
		processContext.setMonitor(monitor);
	}
	
	/**
	 * Construct a new process.
	 * @param processChain
	 * @param context
	 * @param name
	 */
	public Process(IProcessChain processChain, IContext context, String name) {
		this.name = name;
		this.processChain = processChain;
		processContext = new ProcessContext(this, context);
		processContext.setMonitor(monitor);
	}

	public abstract Result start() throws ETLException;
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcess#getResult()
	 */
	public Result getResult() {
		return result;
	}
	
	/**
	 * 
	 */
	public Process() {
		this("Unnamed Process");
	}

	public void addProcessFinalizer(IProcessFinalizer finalizer) {
		finalizers.add(finalizer);
		
	}

	public List<IProcessFinalizer> getProcessFinalizers() {
		return Collections.unmodifiableList(finalizers);
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
		processContext.setMonitor(monitor);
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return the context
	 */
	public IProcessContext getContext() {
		return processContext;
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
	
	@Override
	public IProcessChain getProcessChain() {
		return processChain;
	}
	
	@Override
	public void setProcessChain(IProcessChain processChain){
		this.processChain = processChain;
	}

}