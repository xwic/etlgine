/**
 * 
 */
package de.xwic.etlgine;

import java.util.Date;
import java.util.List;

/**
 * @author Developer
 *
 */
public interface IJob {

	public enum State {
		NEW,
		ENQUEUED,
		RUNNING,
		FINISHED,
		FINISHED_WITH_ERROR,
		ERROR		
	}
	
	/**
	 * Returns true if the job is currently executed.
	 * @return
	 */
	public abstract boolean isExecuting();
	
	/**
	 * Execute the job.
	 * @throws ETLException 
	 */
	public abstract void execute(IContext context) throws ETLException;
	
	/**
	 * Returns the date when the job was executed the last time.
	 * @return
	 */
	public abstract Date getLastFinished();

	/**
	 * Returns the name of the job.
	 * @return
	 */
	public abstract String getName();

	/**
	 * Set the trigger.
	 * @param trigger
	 */
	public abstract void setTrigger(ITrigger trigger);

	/**
	 * Returns the trigger.
	 * @return
	 */
	public abstract ITrigger getTrigger();

	/**
	 * Set the processChain.
	 * @param processChain
	 */
	public abstract void setProcessChain(IProcessChain processChain);

	/**
	 * Returns the processChain.
	 * @return
	 */
	public abstract IProcessChain getProcessChain();

	/**
	 * @return the chainScriptName
	 */
	public String getChainScriptName();

	/**
	 * @param chainScriptName the chainScriptName to set
	 */
	public void setChainScriptName(String chainScriptName);

	/**
	 * @return the state
	 */
	public State getState();

	/**
	 * @return the lastException
	 */
	public Throwable getLastException();

	/**
	 * @return the lastStarted
	 */
	public Date getLastStarted();

	/**
	 * A disabled job is not scheduled, even if the trigger is due. A manual execution
	 * is still possible.
	 * @return the disabled
	 */
	public boolean isDisabled();

	/**
	 * A disabled job is not scheduled, even if the trigger is due. A manual execution
	 * is still possible.
	 * @param disabled the disabled to set
	 */
	public void setDisabled(boolean disabled);

	/**
	 * Invoked when the job was enqueued.
	 */
	public abstract void notifyEnqueued();

	/**
	 * @return
	 */
	public abstract String getDurationInfo();

	/**
	 * @return the jobId
	 */
	public String getJobId();

	/**
	 * @param jobId the jobId to set
	 */
	public void setJobId(String jobId);

	/**
	 * Add a job finalizer.
	 */
	public void addJobFinalizer(IJobFinalizer finalizer);
	
	/**
	 * Returns the job finalizers.
	 * @return
	 */
	public List<IJobFinalizer> getJobFinalizers();
}
