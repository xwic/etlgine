/**
 * 
 */
package de.xwic.etlgine.impl;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.io.File;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import de.xwic.etlgine.DefaultMonitor;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.ETLgine;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IJobFinalizer;
import de.xwic.etlgine.IMonitor;
import de.xwic.etlgine.IProcessChain;
import de.xwic.etlgine.ITrigger;
import de.xwic.etlgine.Result;

/**
 * @author Florian Lippisch
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
	private List<String> chainScriptNames = new ArrayList<String>(); 
	
	private State state = State.NEW;
	private Throwable lastException = null;
	private IMonitor monitor = new DefaultMonitor();
	
	private IContext activeContext = null;
	
	private String jobId = null;
	
	protected List<IJobFinalizer> finalizers = new ArrayList<IJobFinalizer>();

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
		ETLgine.integrityTest();
		executing = true;
		activeContext = context;
		lastStarted = new Date();
		lastException = null;
		monitor.reset();
		state = State.RUNNING;
		if (trigger != null) {
			trigger.notifyJobStarted();
		}
		try {
			if (processChain == null) {
				if (chainScriptNames.size() == 0) {
					throw new ETLException("No processChain or chainScriptName given.");
				}
				// TODO move this logic to processChain that can contain processChains
				for (String name : chainScriptNames) {
					loadChainFromScript(context, name);
				}
			}
			processChain.start();
			state = processChain.getResult() != Result.SUCCESSFULL ? State.FINISHED_WITH_ERROR : State.FINISHED;
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
			activeContext = null;
			lastFinished = new Date();
			// run finalizers, allow modification during the loop
			for (int i = 0; i < finalizers.size(); i++) {
				IJobFinalizer finalizer = finalizers.get(i);
				try {
					finalizer.onFinish(this);
				} catch (Throwable t) {
					monitor.logError("Error executing finalizer!", t);
				}
			}
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
	private void loadChainFromScript(IContext context, String chainScriptName) throws ETLException {
		
		if (processChain == null) {
			processChain = ETLgine.createProcessChain(context, chainScriptName);
		}
		processChain.setMonitor(monitor);
		
		Binding binding = new Binding();
		binding.setVariable("context", context);
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
	 * raise stop flag.
	 * @return
	 */
	public boolean stop() {
		IContext ctx = activeContext;
		if (ctx != null) {
			ctx.setStopFlag(true);
			return true;
		}
		return false;
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
		return chainScriptNames == null || chainScriptNames.size() == 0 ? null : chainScriptNames.get(0);
	}

	/**
	 * @param chainScriptName the chainScriptName to set
	 */
	public void setChainScriptName(String chainScriptName) {
		this.chainScriptNames.add(chainScriptName);
	}

	/**
	 * @param chainScriptName
	 */
	public void addChainScriptName(String chainScriptName) {
		chainScriptNames.add(chainScriptName);
	}
	
	/**
	 * @return the chainScriptNames
	 */
	public List<String> getChainScriptNames() {
		return chainScriptNames;
	}

	/**
	 * @param chainScriptName the chainScriptName to set
	 */
	public void setChainScriptNames(List<String> chainScriptName) {
		this.chainScriptNames = chainScriptName;
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

	/**
	 * @return the jobId
	 */
	public String getJobId() {
		return jobId;
	}

	/**
	 * @param jobId the jobId to set
	 */
	public void setJobId(String jobId) {
		this.jobId = jobId;
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
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IJob#addJobFinalizer(de.xwic.etlgine.IJobFinalizer)
	 */
	public void addJobFinalizer(IJobFinalizer finalizer) {
		finalizers.add(finalizer);
		monitor.logInfo("Added job finalizer '" + finalizer + "' at index " + (finalizers.size() - 1));
		
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IJob#getJobFinalizers()
	 */
	public List<IJobFinalizer> getJobFinalizers() {
		return Collections.unmodifiableList(finalizers);
	}
}
