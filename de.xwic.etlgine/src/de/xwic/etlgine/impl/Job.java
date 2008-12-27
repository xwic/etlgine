/**
 * 
 */
package de.xwic.etlgine.impl;

import java.util.Date;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IProcessChain;
import de.xwic.etlgine.ITrigger;

/**
 * @author Developer
 *
 */
public class Job implements IJob {

	private IProcessChain processChain = null;
	private ITrigger trigger = null;
	private Date lastRun = null;
	private String name = null;
	private boolean executing = false;
	
	/**
	 * @param name
	 */
	public Job(String name) {
		super();
		this.name = name;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IJob#execute()
	 */
	public synchronized void execute() throws ETLException {
		
		if (executing) {
			throw new ETLException("The job is already beeing executed.");
		}
		executing = true;
		processChain.start();
		executing = false;
		lastRun = new Date();
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IJob#isExecuting()
	 */
	public boolean isExecuting() {
		return executing;
	}
	
	/**
	 * @return the processChain
	 */
	public IProcessChain getProcessChain() {
		return processChain;
	}

	/**
	 * @param processChain the processChain to set
	 */
	public void setProcessChain(IProcessChain processChain) {
		this.processChain = processChain;
	}

	/**
	 * @return the trigger
	 */
	
	public ITrigger getTrigger() {
		return trigger;
	}
	/**
	 * @param trigger the trigger to set
	 */
	public void setTrigger(ITrigger trigger) {
		this.trigger = trigger;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return the lastRun
	 */
	public Date getLastRun() {
		return lastRun;
	}
	
}
