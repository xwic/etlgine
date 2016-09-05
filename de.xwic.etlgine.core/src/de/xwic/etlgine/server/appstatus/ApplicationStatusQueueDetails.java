package de.xwic.etlgine.server.appstatus;

import java.io.Serializable;

public class ApplicationStatusQueueDetails implements Serializable {

	private static final long serialVersionUID = 4904234574804127115L;
	
	private String queueName;
	private int queueBusy;
	private String queueJobName;
	private String queueJobStatus;
	private long queueJobExecutionTime;
	private String queueJobProcessName;

	public ApplicationStatusQueueDetails() {
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public int getQueueBusy() {
		return queueBusy;
	}

	public void setQueueBusy(int queueBusy) {
		this.queueBusy = queueBusy;
	}

	public String getQueueJobName() {
		return queueJobName;
	}

	public void setQueueJobName(String queueJobName) {
		this.queueJobName = queueJobName;
	}

	public String getQueueJobStatus() {
		return queueJobStatus;
	}

	public void setQueueJobStatus(String queueJobStatus) {
		this.queueJobStatus = queueJobStatus;
	}

	public long getQueueJobExecutionTime() {
		return queueJobExecutionTime;
	}

	public void setQueueJobExecutionTime(long queueJobExecutionTime) {
		this.queueJobExecutionTime = queueJobExecutionTime;
	}

	public String getQueueJobProcessName() {
		return queueJobProcessName;
	}

	public void setQueueJobProcessName(String queueJobProcessName) {
		this.queueJobProcessName = queueJobProcessName;
	}

}
