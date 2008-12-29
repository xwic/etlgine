/**
 * 
 */
package de.xwic.etlgine;

import java.util.Date;

/**
 * @author Developer
 *
 */
public interface IJob {

	/**
	 * Returns true if the job is currently executed.
	 * @return
	 */
	public abstract boolean isExecuting();
	
	/**
	 * Execute the job.
	 * @throws ETLException 
	 */
	public abstract void execute() throws ETLException;
	
	/**
	 * Returns the date when the job was executed the last time.
	 * @return
	 */
	public abstract Date getLastRun();

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

}