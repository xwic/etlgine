package de.xwic.etlgine.server.appstatus;

import java.io.Serializable;
import java.util.List;

public class ApplicationStatusDetails implements Serializable {
	private static final long serialVersionUID = 3698063507266348889L;
	
	private String instanceID;
	private long instanceInitializedAt;
	private long instanceUptime;
	private int instanceNotificationEnabled;
	private int instanceTriggerEnabled;
	private long instanceMemoryUsedK;
	private long instanceMemoryFreeK;
	private int jobsLoaded;
	private int jobsDisabled;
	private int jobsFailed;
	private long queuesJobMaxExecutionTime;
	private int queuesExecutingJobs;
	private List<ApplicationStatusQueueDetails> queues;

	public ApplicationStatusDetails() {

	}

	public String getInstanceID() {
		return instanceID;
	}

	public void setInstanceID(String instanceID) {
		this.instanceID = instanceID;
	}

	public long getInstanceInitializedAt() {
		return instanceInitializedAt;
	}

	public void setInstanceInitializedAt(long instanceInitializedAt) {
		this.instanceInitializedAt = instanceInitializedAt;
	}

	public long getInstanceUptime() {
		return instanceUptime;
	}

	public void setInstanceUptime(long instanceUptime) {
		this.instanceUptime = instanceUptime;
	}

	public int getInstanceNotificationEnabled() {
		return instanceNotificationEnabled;
	}

	public void setInstanceNotificationEnabled(int instanceNotificationEnabled) {
		this.instanceNotificationEnabled = instanceNotificationEnabled;
	}

	public int getInstanceTriggerEnabled() {
		return instanceTriggerEnabled;
	}

	public void setInstanceTriggerEnabled(int instanceTriggerEnabled) {
		this.instanceTriggerEnabled = instanceTriggerEnabled;
	}

	public long getInstanceMemoryUsedK() {
		return instanceMemoryUsedK;
	}

	public void setInstanceMemoryUsedK(long instanceMemoryUsedK) {
		this.instanceMemoryUsedK = instanceMemoryUsedK;
	}

	public long getInstanceMemoryFreeK() {
		return instanceMemoryFreeK;
	}

	public void setInstanceMemoryFreeK(long instanceMemoryFreeK) {
		this.instanceMemoryFreeK = instanceMemoryFreeK;
	}

	public int getJobsLoaded() {
		return jobsLoaded;
	}

	public void setJobsLoaded(int jobsLoaded) {
		this.jobsLoaded = jobsLoaded;
	}

	public int getJobsDisabled() {
		return jobsDisabled;
	}

	public void setJobsDisabled(int jobsDisabled) {
		this.jobsDisabled = jobsDisabled;
	}

	public int getJobsFailed() {
		return jobsFailed;
	}

	public void setJobsFailed(int jobsFailed) {
		this.jobsFailed = jobsFailed;
	}



	public long getQueuesJobMaxExecutionTime() {
		return queuesJobMaxExecutionTime;
	}

	public void setQueuesJobMaxExecutionTime(long queuesJobMaxExecutionTime) {
		this.queuesJobMaxExecutionTime = queuesJobMaxExecutionTime;
	}

	public int getQueuesExecutingJobs() {
		return queuesExecutingJobs;
	}

	public void setQueuesExecutingJobs(int queuesExecutingJobs) {
		this.queuesExecutingJobs = queuesExecutingJobs;
	}

	public List<ApplicationStatusQueueDetails> getQueues() {
		return queues;
	}

	public void setQueues(List<ApplicationStatusQueueDetails> queues) {
		this.queues = queues;
	}

}
