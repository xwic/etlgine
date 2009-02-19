/**
 * 
 */
package de.xwic.etlgine.server;

import de.xwic.etlgine.IJob;

/**
 * @author lippisch
 */
public class ServerContextEvent {

	private final Object source;
	private IJob job = null;
	private IJob.State result = null;

	/**
	 * @param source
	 */
	public ServerContextEvent(Object source) {
		super();
		this.source = source;
	}
	/**
	 * @param source
	 * @param job
	 */
	public ServerContextEvent(Object source, IJob job) {
		super();
		this.source = source;
		this.job = job;
	}

	/**
	 * @param source
	 * @param job
	 * @param result
	 */
	public ServerContextEvent(Object source, IJob job, IJob.State result) {
		super();
		this.source = source;
		this.job = job;
		this.result = result;
	}
	/**
	 * @return the source
	 */
	public Object getSource() {
		return source;
	}

	/**
	 * @return the job
	 */
	public IJob getJob() {
		return job;
	}

	/**
	 * @return the result
	 */
	public IJob.State getResult() {
		return result;
	}
	
	
	
}
