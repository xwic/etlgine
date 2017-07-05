/**
 * 
 */
package de.xwic.etlgine.server.admin;

import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONException;
import org.json.JSONWriter;

import de.jwic.base.IControlContainer;
import de.jwic.base.IncludeJsOption;
import de.jwic.base.JavaScriptSupport;
import de.jwic.json.JsonResourceControl;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IJob.State;
import de.xwic.etlgine.IProcess;
import de.xwic.etlgine.ISource;
import de.xwic.etlgine.publish.CubePublishDestination;
import de.xwic.etlgine.publish.CubePublisherManager;
import de.xwic.etlgine.server.ETLgineServer;
import de.xwic.etlgine.server.JobQueue;
import de.xwic.etlgine.server.ServerContext;

/**
 * Displays automatically refreshed status informations.
 * @author Developer
 */
@JavaScriptSupport
public class StatusControl extends JsonResourceControl{
	private long refreshInterval = 1000;
	private String initialClone = null;
	/**
	 * @param container
	 * @param name
	 */
	public StatusControl(IControlContainer container, String name) {
		super(container, name);
		initialClone = ETLgineServer.getInstance().getServerContext().getProperty("ps.proxy.cloning.mode");
	}

	@Override
	public void handleJSONResponse(HttpServletRequest req, JSONWriter res)
			throws JSONException {
		res.object();

        res.key("instanceId").value(ETLgineServer.getInstance().getServerContext().getProperty("instance.id", ""));

        res.key("currentDate").value((new SimpleDateFormat("EEE MMM dd yyyy")).format(new Date()));
        res.key("currentTime").value((new SimpleDateFormat("HH:mm:ss zzz")).format(new Date()));

        res.key("currentUpTime").value(getServerUptime());

		// Memory
		Runtime rt = Runtime.getRuntime();
		NumberFormat nf = NumberFormat.getIntegerInstance();

		res.key("maxMemory").value(nf.format(rt.maxMemory() / 1024) + "k");

		long total = rt.totalMemory() / 1024;
		long free = rt.freeMemory() / 1024;

		res.key("totalMemory").value(nf.format(total) + "k");

		res.key("freeMemory").value(nf.format(free) + "k");

		res.key("usedMemory").value(nf.format(total-free) + "k");

		
		// trigger and publishing
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
		
		res.key("jobsLoaded").value(totalLoadedJobs);
		res.key("jobsDisabled").value(totalInactiveJobs);
		res.key("jobsFailed").value(totalErrorJobs);
		
        res.key("triggerStatus").value(ETLgineServer.getInstance().getServerContext().getPropertyBoolean("trigger.enabled", true)?"Enabled":"Disabled");
        
        res.key("notificationStatus").value(ETLgineServer.getInstance().getServerContext().getPropertyBoolean("notifications.enabled", false)?"Enabled":"Disabled");
        
        if (null != initialClone && !initialClone.isEmpty() && !initialClone.equals("0")){
        	res.key("cloneStatus").value("0".equals(ETLgineServer.getInstance().getServerContext().getProperty("ps.proxy.cloning.mode"))?"Disabled":"Enabled");
        }
        
        List<CubePublishDestination> publishDestinations = CubePublisherManager.getInstance().getPublishTargets();

    	res.key("publishers").array(); // [
        if (null != publishDestinations) {
            for (CubePublishDestination cubePublishDestination : publishDestinations) {
            	res.object();
            	res.key("publishKey").value(cubePublishDestination.getFullKey());
            	res.key("publishStatus").value(cubePublishDestination.isEnabled()?"Enabled":"Disabled");
            	res.endObject();
            	
    		}
        }
        res.endArray();
        
		// Queue info
		ETLgineServer server = ETLgineServer.getInstance();
		ServerContext context = server.getServerContext();
		res.key("queues").array();
		for (JobQueue queue : context.getJobQueues()) {
			res.object();
			res.key("name").value(queue.getName());

			res.key("size").value(queue.getSize());

			IJob job = queue.getActiveJob();
			if (job == null) {
				res.key("status").value("Empty");
			} else {
				res.key("status").value("Executing");
				res.key("jobName").value(job.getName());
				res.key("state").value(job.getState());
				res.key("duration").value(job.getDurationInfo());

				IProcess p = job.getProcessChain() != null ? job.getProcessChain().getActiveProcess() : null;
				if (p != null) {
					res.key("process").value(p.getName());
					ISource source = p.getContext().getCurrentSource();
					if (source != null) {
						String name = source.getName();
						if (name != null) {
							if (name.lastIndexOf('\\') != -1) {
								name = name.substring(name.lastIndexOf('\\') + 1);
							}
							if (name.length() > 30) {
								name = name.substring(name.length() - 30);
							}
							res.key("source").value(name);
						}
					}
					
					long count = p.getContext().getRecordsCount();
					res.key("record").value(nf.format(count));
					long duration = (System.currentTimeMillis() - job.getLastStarted().getTime()) / 1000;
					if(duration > 0) {
                        res.key("processRowsPerSec").value(nf.format(count / duration) + "/sec");
                    }

				}
				
			}
			res.endObject();
		}
       
		res.endArray();
		res.endObject();
	}
	
	@IncludeJsOption
	public long getRefreshInterval() {
		return refreshInterval;
	}
	
	public void setRefreshInterval(long refreshInterval) {
		this.refreshInterval = refreshInterval;
	};

    private String getServerUptime() {
        String uptime = "-";

        long serverStartedAt = ETLgineServer.getInstance().getIntializedTimeInMilis();

        if(serverStartedAt > 0) {
            long duration = System.currentTimeMillis() - serverStartedAt;
            uptime = convertTimeMsIntoString(duration, false);
        }

        return uptime;
    }

    private static String convertTimeMsIntoString(long timeMs, boolean includeMs) {
        int ms = (int) (timeMs % 1000);
        timeMs = timeMs / 1000;
        int sec = (int) (timeMs % 60);
        timeMs = timeMs / 60;
        int min = (int) (timeMs % 60);
        timeMs = timeMs / 60;
        int hour = (int) (timeMs % 24);
        timeMs = timeMs / 24;
        int days = (int) timeMs;

        StringBuilder sb = new StringBuilder();
        if (days != 0) {
            sb.append(days).append("d ");
        }
        if (days != 0 || hour != 0) {
            sb.append(hour).append("h ");
        }
        if (days != 0 || hour != 0 || min != 0) {
            sb.append(min).append("m ");
        }
        sb.append(sec).append("s ");
        if (includeMs) {
            sb.append(ms).append("ms ");
        }
        return sb.toString();
    }
}
