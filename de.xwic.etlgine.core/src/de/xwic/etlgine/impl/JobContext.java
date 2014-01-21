/**
 * 
 */
package de.xwic.etlgine.impl;

import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IJobContext;

/**
 * @author jbornema
 *
 */
public class JobContext extends Context implements IJobContext {

	private IJob job;

	/**
	 * @param job
	 */
	public JobContext(IJob job) {
		this.job = job;
	}

	/**
	 * @param job
	 * @param parentContext
	 */
	public JobContext(IJob job, IContext parentContext) {
		super(parentContext);
		this.job = job;
	}

	@Override
	public IJob getJob() {
		return job;
	}

	@Override
	public void setJob(IJob job) {
		this.job = job;
	}

}
