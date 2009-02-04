/**
 * 
 */
package de.xwic.etlgine;

/**
 * Invoked when the process is done. May be used to remove source files/data after
 * a successfull processing, to send mail notifications or others.
 * @author lippisch
 */
public interface IProcessFinalizer {

	/**
	 * Invoked when the process is done.
	 * @param context
	 */
	public void onFinish(IProcessContext context) throws ETLException;
	
}
