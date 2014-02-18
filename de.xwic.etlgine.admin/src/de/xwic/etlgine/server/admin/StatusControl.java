/**
 * 
 */
package de.xwic.etlgine.server.admin;

import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.Date;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONException;
import org.json.JSONWriter;

import de.jwic.base.IControlContainer;
import de.jwic.base.IncludeJsOption;
import de.jwic.base.JavaScriptSupport;
import de.jwic.json.JsonResourceControl;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IProcess;
import de.xwic.etlgine.ISource;
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
	/**
	 * @param container
	 * @param name
	 */
	public StatusControl(IControlContainer container, String name) {
		super(container, name);
	}

	@Override
	public void handleJSONResponse(HttpServletRequest req, JSONWriter res)
			throws JSONException {
		res.object();
		res.key("currentTime").value(DateFormat.getTimeInstance().format(new Date()));

		// Memory
		Runtime rt = Runtime.getRuntime();
		NumberFormat nf = NumberFormat.getIntegerInstance();

		res.key("maxMemory").value(nf.format(rt.maxMemory() / 1024) + "k");

		long total = rt.totalMemory() / 1024;
		long free = rt.freeMemory() / 1024;

		res.key("totalMemory").value(nf.format(total) + "k");

		res.key("freeMemory").value(nf.format(free) + "k");

		res.key("usedMemory").value(nf.format(total-free) + "k");

		
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
					
					res.key("processDuration").value(nf.format(count / duration) + "/sec");
					
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

}