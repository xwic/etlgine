/**
 * 
 */
package de.xwic.etlgine.trigger;

import de.xwic.etlgine.IJob;
import de.xwic.etlgine.ITrigger;
import de.xwic.etlgine.server.IServerContextListener;
import de.xwic.etlgine.server.ServerContext;
import de.xwic.etlgine.server.ServerContextEvent;

/**
 * This trigger becomes due when other Job(s) have been finished execution. It
 * might be used to setup an execution chain of single jobs.
 * <p>While the ProcessChain offers more options to share data between the
 * separated processes, chaining Jobs might be more useful if the jobs are
 * managed separately.
 * 
 * @author lippisch
 */
public class JobExecutedTrigger implements ITrigger, IServerContextListener {

	private boolean isDue = false;
	
	private String jobId = null;
	private boolean runOnSuccess = true;
	private boolean runOnError = false;
	
	/**
	 * @param jobId
	 */
	public JobExecutedTrigger(ServerContext context, String jobId) {
		super();
		this.jobId = jobId;
		context.addServerContextListener(this);
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#isDue()
	 */
	public boolean isDue() {
		return isDue;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#notifyJobFinished(boolean)
	 */
	public void notifyJobFinished(boolean withErrors) {

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#notifyJobStarted()
	 */
	public void notifyJobStarted() {
		isDue = false;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.server.IServerContextListener#jobExecutionEnd(de.xwic.etlgine.server.ServerContextEvent)
	 */
	public void jobExecutionEnd(ServerContextEvent event) {
		
		if (jobId != null && jobId.equals(event.getJob().getJobId())) {
			boolean isSuccess = event.getResult() == IJob.State.FINISHED;
			if ((isSuccess && runOnSuccess) || (!isSuccess && runOnError)) {
				isDue = true;
			}
		}

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.server.IServerContextListener#jobExecutionStart(de.xwic.etlgine.server.ServerContextEvent)
	 */
	public void jobExecutionStart(ServerContextEvent event) {
		// to be ignored.
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
	 * @return the runOnSuccess
	 */
	public boolean isRunOnSuccess() {
		return runOnSuccess;
	}

	/**
	 * @param runOnSuccess the runOnSuccess to set
	 */
	public void setRunOnSuccess(boolean runOnSuccess) {
		this.runOnSuccess = runOnSuccess;
	}

	/**
	 * @return the runOnError
	 */
	public boolean isRunOnError() {
		return runOnError;
	}

	/**
	 * @param runOnError the runOnError to set
	 */
	public void setRunOnError(boolean runOnError) {
		this.runOnError = runOnError;
	}

}
