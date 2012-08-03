/**
 * 
 */
package de.xwic.etlgine.impl;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.io.File;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import de.xwic.etlgine.DefaultMonitor;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.ETLgine;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IJobFinalizer;
import de.xwic.etlgine.IMonitor;
import de.xwic.etlgine.IMonitor.EventType;
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
	private long lastDuration = 0;
	private String name = null;
	private boolean executing = false;
	private boolean disabled = false;
	private boolean stopTriggerAfterError = true;
	private List<String> chainScriptNames = new ArrayList<String>(); 
	
	private State state = State.NEW;
	private Throwable lastException = null;
	private IMonitor monitor = new DefaultMonitor();
	
	private String jobId = null;
	
	private String creatorInfo = null;

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
		//activeContext = context; // disabled because context here is ServerContext
		lastStarted = new Date();
		lastException = null;
		monitor.reset();
		state = State.RUNNING;
		monitor.onEvent(context, EventType.JOB_EXECUTION_START, this);
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
			if (ee.getProcess() == null && processChain != null) {
				ee.setProcess(processChain.getActiveProcess());
			}
			throw ee;
		} catch (Throwable t) {
			state = State.ERROR;
			lastException = t;
			ETLException ee = new ETLException("Error executing job: " + t, t);
			if (processChain != null) {
				ee.setProcess(processChain.getActiveProcess());
			}
			throw ee;
		} finally {
			try {
				executing = false;
				lastFinished = new Date();
				lastDuration = lastFinished.getTime() - lastStarted.getTime();
				if (processChain != null) {
					processChain.finish(this);
					processChain = null;
				}
				if (trigger != null) {
					trigger.notifyJobFinished(state == State.ERROR);
				}
			} finally {
				monitor.onEvent(context, EventType.JOB_EXECUTION_END, this);
			}
		}
	}

	/**
	 * @throws ETLException 
	 * 
	 */
	private void loadChainFromScript(IContext context, String chainScriptName) throws ETLException {
		
		File jobPath = new File(context.getProperty(IContext.PROPERTY_SCRIPTPATH, "."));
		if (!jobPath.exists()) {
			throw new ETLException("The job path " + jobPath.getAbsolutePath() + " does not exist.");
		}
		File file = new File(jobPath, chainScriptName);
		if (!file.exists()) {
			throw new ETLException("The script file " + file.getAbsolutePath() + " does not exist.");
		}
		
		if (processChain == null) {
			processChain = ETLgine.createProcessChain(context, chainScriptName);
		}
		processChain.setMonitor(monitor);
		
		// update creator info
		if (processChain instanceof ProcessChain) {
			((ProcessChain)processChain).setCreatorInfo(chainScriptName);
		}
		
		monitor.onEvent(processChain.getGlobalContext(), EventType.PROCESSCHAIN_LOAD_FROM_SCRIPT, processChain);
		
		Binding binding = new Binding();
		binding.setVariable("context", context);
		binding.setVariable("job", this);
		binding.setVariable("processChain", processChain);
		
		GroovyShell shell = new GroovyShell(binding);
		
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
		//IContext ctx = activeContext;
		IContext ctx = processChain != null ? processChain.getGlobalContext() : null;
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

	@Override
	@Deprecated
	public void addJobFinalizer(IJobFinalizer finalizer) {
		processChain.addJobFinalizer(finalizer);
	}
	
	@Override
	@Deprecated
	public List<IJobFinalizer> getJobFinalizers() {
		return processChain.getJobFinalizers();
	}

	/**
	 * @return the lastDuration in milliseconds
	 */
	public long getLastDuration() {
		return lastDuration;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IJob#isStopTriggerAfterError()
	 */
	@Override
	public boolean isStopTriggerAfterError() {
		return stopTriggerAfterError;
	}

	/**
	 * @param stopTriggerAfterError the stopTriggerAfterError to set
	 */
	public void setStopTriggerAfterError(boolean stopTriggerAfterError) {
		this.stopTriggerAfterError = stopTriggerAfterError;
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
