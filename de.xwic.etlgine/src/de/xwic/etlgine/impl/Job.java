/**
 * 
 */
package de.xwic.etlgine.impl;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.io.File;
import java.text.NumberFormat;
import java.util.Date;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.ETLgine;
import de.xwic.etlgine.IContext;
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
	private Date lastStarted = null;
	private Date lastFinished = null;
	private String name = null;
	private boolean executing = false;
	private boolean disabled = false;
	private String chainScriptName = null; 
	
	private State state = State.NEW;
	private Throwable lastException = null;
	
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
	public synchronized void execute(IContext context) throws ETLException {
		
		if (executing) {
			throw new ETLException("The job is already beeing executed.");
		}
		executing = true;
		lastStarted = new Date();
		state = State.RUNNING;
		if (trigger != null) {
			trigger.notifyJobStarted();
		}
		try {
			if (processChain == null) {
				if (chainScriptName == null) {
					throw new ETLException("No processChain or chainScriptName given.");
				}
				loadChainFromScript(context);
			}
			processChain.start();
			state = State.FINISHED;
		} catch (ETLException ee) {
			state = State.ERROR;
			lastException = ee;
			throw ee;
		} catch (Throwable t) {
			state = State.ERROR;
			lastException = t;
			throw new ETLException("Error executing job: " + t, t);
		} finally {
			executing = false;
			lastFinished = new Date();
			processChain = null;
			if (trigger != null) {
				trigger.notifyJobFinished(state == State.ERROR);
			}
		}
	}

	/**
	 * @throws ETLException 
	 * 
	 */
	private void loadChainFromScript(IContext context) throws ETLException {
		
		processChain = ETLgine.createProcessChain(context, chainScriptName);
		
		Binding binding = new Binding();
		binding.setVariable("job", this);
		binding.setVariable("processChain", processChain);
		
		GroovyShell shell = new GroovyShell(binding);
		
		File jobPath = new File(context.getProperty(IContext.PROPERTY_SCRIPTPATH, "."));
		if (!jobPath.exists()) {
			throw new ETLException("The job path " + jobPath.getAbsolutePath() + " does not exist.");
		}
		File file = new File(jobPath, chainScriptName);
		if (!file.exists()) {
			throw new ETLException("The script file " + file.getAbsolutePath() + " does not exist.");
		}
		
		try {
			shell.evaluate(file);
		} catch (Exception e) {
			throw new ETLException("Error evaluating script '" + file.getName() + "':" + e, e);
		}
		
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
	public Date getLastFinished() {
		return lastFinished;
	}

	/**
	 * @return the chainScriptName
	 */
	public String getChainScriptName() {
		return chainScriptName;
	}

	/**
	 * @param chainScriptName the chainScriptName to set
	 */
	public void setChainScriptName(String chainScriptName) {
		this.chainScriptName = chainScriptName;
	}

	/**
	 * @return the state
	 */
	public State getState() {
		return state;
	}

	/**
	 * @param state the state to set
	 */
	public void setState(State state) {
		this.state = state;
	}

	/**
	 * @return the lastException
	 */
	public Throwable getLastException() {
		return lastException;
	}

	/**
	 * @param lastException the lastException to set
	 */
	public void setLastException(Throwable lastException) {
		this.lastException = lastException;
	}

	/**
	 * @return the lastStarted
	 */
	public Date getLastStarted() {
		return lastStarted;
	}

	/**
	 * A disabled job is not scheduled, even if the trigger is due. A manual execution
	 * is still possible.
	 * @return the disabled
	 */
	public boolean isDisabled() {
		return disabled;
	}

	/**
	 * A disabled job is not scheduled, even if the trigger is due. A manual execution
	 * is still possible.
	 * @param disabled the disabled to set
	 */
	public void setDisabled(boolean disabled) {
		this.disabled = disabled;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IJob#notifyEnqueued()
	 */
	public void notifyEnqueued() {
		state = State.ENQUEUED;
		
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IJob#getDurationInfo()
	 */
	public String getDurationInfo() {
		
		NumberFormat nf = NumberFormat.getNumberInstance();
		nf.setMinimumIntegerDigits(2);
		nf.setMaximumFractionDigits(0);
		String sDuration = "";
		if (state == IJob.State.RUNNING) {
			long duration = System.currentTimeMillis() - getLastStarted().getTime();
			int ms = (int)(duration % 1000);
			int sec = (int)((duration / 1000) % 60);
			int min = (int)(duration / 60000);
			sDuration = nf.format(min) + ":" + nf.format(sec) + ":" + nf.format(ms); 
			
		} else if (state == IJob.State.FINISHED || state == IJob.State.FINISHED_WITH_ERROR) {
			long duration = getLastFinished().getTime() - getLastStarted().getTime();
			int ms = (int)(duration % 1000);
			int sec = (int)((duration / 1000) % 60);
			int min = (int)(duration / 60000);
			sDuration = nf.format(min) + ":" + nf.format(sec) + ":" + nf.format(ms);
			
		}
		return sDuration;
		
		
	}
	
}
