package de.xwic.etlgine.server.appstatus;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;

import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IProcess;
import de.xwic.etlgine.IJob.State;
import de.xwic.etlgine.server.ETLgineServer;
import de.xwic.etlgine.server.JobQueue;
import de.xwic.etlgine.server.ServerContext;

public class ApplicationStatusListener extends HttpServlet {
	private static final long serialVersionUID = 2128106601867383165L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String responseContent = "";
		ApplicationStatusDetails appDetails = new ApplicationStatusDetails();
		List<ApplicationStatusQueueDetails> appQueues = new ArrayList<ApplicationStatusQueueDetails>();

		ETLgineServer etlInstance = ETLgineServer.getInstance();
		ServerContext etlcontext = etlInstance.getServerContext();

		appDetails.setInstanceID(etlcontext.getProperty("instance.id", ""));
		appDetails.setInstanceInitializedAt(etlInstance.getIntializedTimeInMilis());
		appDetails.setInstanceUptime(getServerUptime(etlInstance.getIntializedTimeInMilis()));
		
		int notificationsEnabled = etlcontext.getPropertyBoolean("notifications.enabled", false)?1:0;
		int triggerEnabled = etlcontext.getPropertyBoolean("trigger.enabled", true)?1:0;
		appDetails.setInstanceNotificationEnabled(notificationsEnabled);
		appDetails.setInstanceTriggerEnabled(triggerEnabled);

		int totalLoadedJobs = 0;
		int totalErrorJobs = 0;
		int totalInactiveJobs = 0;
		
		Collection<IJob> currentJobs =etlcontext.getJobs();
		for (IJob iJob : currentJobs) {
			if (iJob.isDisabled()) {
				totalInactiveJobs ++;
			}
			if(State.ERROR.equals(iJob.getState()) || State.FINISHED_WITH_ERROR.equals(iJob.getState())) {
				totalErrorJobs++;
			}
			totalLoadedJobs++;
		}
		appDetails.setJobsLoaded(totalLoadedJobs);
		appDetails.setJobsDisabled(totalInactiveJobs);
		appDetails.setJobsFailed(totalErrorJobs);
		
		long maxExecutionTime = 0;
		int queueJobsExecuting =0;
		
		for (JobQueue queue : etlcontext.getJobQueues()) {
			ApplicationStatusQueueDetails appQueue = new ApplicationStatusQueueDetails();
			appQueue.setQueueName(queue.getName());
			
			IJob job = queue.getActiveJob();
			if (job == null) {
				appQueue.setQueueBusy(0);
			} else {
				queueJobsExecuting++;
				appQueue.setQueueBusy(1);
				appQueue.setQueueJobName(job.getName());
				appQueue.setQueueJobStatus(job.getState().toString());
				long duration = System.currentTimeMillis() - job.getLastStarted().getTime();
				if (duration>maxExecutionTime) {
					maxExecutionTime = duration;
				}
				appQueue.setQueueJobExecutionTime(duration);
				IProcess p = job.getProcessChain() != null ? job.getProcessChain().getActiveProcess() : null;
				if (p != null) {
					appQueue.setQueueJobProcessName(p.getName());
				}
			}
			appQueues.add(appQueue);
		}
		appDetails.setQueuesJobMaxExecutionTime(maxExecutionTime);
		appDetails.setQueuesExecutingJobs(queueJobsExecuting);
		appDetails.setQueues(appQueues);
		
		Gson gson = new Gson();
		responseContent = gson.toJson(appDetails);
		
		resp.setStatus(HttpServletResponse.SC_OK);
		PrintWriter writer = resp.getWriter();
		writer.print(responseContent);
		writer.close();
	}

	private long getServerUptime(long serverStartedAt) {
		long uptime;

		if (serverStartedAt > 0) {
			uptime = System.currentTimeMillis() - serverStartedAt;

		} else {
			uptime = serverStartedAt;
		}

		return uptime;
	}
}
