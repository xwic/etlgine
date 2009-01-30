/**
 * 
 */
package de.xwic.etlgine;

/**
 * Triggers the launch of a job.
 * 
 * @author Developer
 */
public interface ITrigger {

	/**
	 * Returns true if the job should be started.
	 * @return
	 */
	public boolean isDue();
	
	/**
	 * Invoked when the job has been started.
	 */
	public void notifyJobStarted();
	
	/**
	 * Invoked when the job has been finished.
	 * @param withErrors
	 */
	public void notifyJobFinished(boolean withErrors);
	
}
