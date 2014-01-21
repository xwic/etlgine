/**
 * 
 */
package de.xwic.etlgine;

/**
 * Invoked when the job is done. May be used to remove source files/data after
 * a successfull processing, to send mail notifications or others.
 * @author lippisch
 */
public interface IJobFinalizer {

	/**
	 * Invoked when the job is done.
	 * @param job
	 */
	public void onFinish(IJob job) throws ETLException;
	
}
