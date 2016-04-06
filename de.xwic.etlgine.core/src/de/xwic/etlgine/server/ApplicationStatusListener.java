package de.xwic.etlgine.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;

import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IProcess;
import de.xwic.etlgine.ISource;
import de.xwic.etlgine.IJob.State;

public class ApplicationStatusListener extends HttpServlet {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8150611395013184448L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest
	 * , javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {

		Map<String, String> response = new HashMap<String, String>();
		response.put("Status", "OK");
		response.put("InstanceID", ETLgineServer.getInstance()
				.getServerContext().getProperty("instance.id", ""));

		long serverStartedAt = ETLgineServer.getInstance()
				.getIntializedTimeInMilis();
		
		response.put("startTime", "" + serverStartedAt);
		response.put("upTime", ""+ getServerUptime(serverStartedAt));

		int totalLoadedJobs = 0;
		int totalErrorJobs = 0;
		int totalInactiveJobs = 0;
		
		Collection<IJob> currentJobs =ETLgineServer.getInstance().getServerContext().getJobs();
		for (IJob iJob : currentJobs) {
			if (iJob.isDisabled()) {
				totalInactiveJobs ++;
			}
			if(State.ERROR.equals(iJob.getState()) || State.FINISHED_WITH_ERROR.equals(iJob.getState())) {
				totalErrorJobs++;
			}
			totalLoadedJobs++;
		}
		
		response.put("jobsLoaded",""+totalLoadedJobs);
		response.put("jobsDisabled",""+totalInactiveJobs);
		response.put("jobsFailed",""+totalErrorJobs);

		ETLgineServer server = ETLgineServer.getInstance();
		ServerContext context = server.getServerContext();
		for (JobQueue queue : context.getJobQueues()) {
			
			IJob job = queue.getActiveJob();
			if (job == null) {
				response.put(queue.getName()+"_queueStatus","Empty");
			} else {
				response.put(queue.getName()+"_queueStatus","Executing");
				response.put(queue.getName()+"_queueJobName",job.getName());
				response.put(queue.getName()+"_queueJobState",job.getState().toString());
				long duration = System.currentTimeMillis() - job.getLastStarted().getTime();
				response.put(queue.getName()+"_queueJobDuration",""+duration);
				IProcess p = job.getProcessChain() != null ? job.getProcessChain().getActiveProcess() : null;
				if (p != null) {
					response.put(queue.getName()+"_queueJobProcess",""+p.getName());
				}
			}
		}
		
		String notificationsStatus = ETLgineServer.getInstance().getServerContext().getPropertyBoolean("notifications.enabled", false)?"ON":"OFF";
		String triggerStatus = ETLgineServer.getInstance().getServerContext().getPropertyBoolean("trigger.enabled", true)?"ON":"OFF";
		
		response.put("notificationStatus",notificationsStatus);
		response.put("triggerStatus",triggerStatus);
				
		// prepare JSON
		Gson gson = new Gson();
		String json = gson.toJson(response);

		resp.setStatus(HttpServletResponse.SC_OK);
		PrintWriter writer = resp.getWriter();
		writer.print(json);
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
