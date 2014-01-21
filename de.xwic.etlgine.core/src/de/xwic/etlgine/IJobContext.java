/**
 * 
 */
package de.xwic.etlgine;

/**
 * @author jbornema
 *
 */
public interface IJobContext extends IContext {

	/**
	 * Sets the hosting job.
	 * @param job
	 */
	void setJob(IJob job);
	
	/**
	 * @return the hosting job
	 */
	IJob getJob();
}
