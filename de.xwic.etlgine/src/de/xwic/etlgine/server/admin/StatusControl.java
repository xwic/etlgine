/**
 * 
 */
package de.xwic.etlgine.server.admin;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.Date;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.jwic.base.Control;
import de.jwic.base.IControlContainer;
import de.jwic.base.IResourceControl;
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
public class StatusControl extends Control implements IResourceControl {

	/**
	 * @param container
	 * @param name
	 */
	public StatusControl(IControlContainer container, String name) {
		super(container, name);
	}

	/* (non-Javadoc)
	 * @see de.jwic.base.IResourceControl#attachResource(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	public void attachResource(HttpServletRequest req, HttpServletResponse res) throws IOException {
		
		PrintWriter pw = res.getWriter();
		
		pw.println("<table width=\"100%\">");
		
		// time
		pw.println("<tr><td class=\"caption\">");
		pw.println("Time:</td><td>");
		pw.println(DateFormat.getTimeInstance().format(new Date()));
		pw.println("</td></tr>");

		// Memory
		Runtime rt = Runtime.getRuntime();
		NumberFormat nf = NumberFormat.getIntegerInstance();

		pw.println("<tr><td class=\"caption\">");
		pw.println("Max:</td><td>");
		pw.println(nf.format(rt.maxMemory() / 1024) + "k");
		pw.println("</td></tr>");

		long total = rt.totalMemory() / 1024;
		long free = rt.freeMemory() / 1024;
		
		pw.println("<tr><td class=\"caption\">");
		pw.println("Total:</td><td>");
		pw.println(nf.format(total) + "k");
		pw.println("</td></tr>");

		pw.println("<tr><td class=\"caption\">");
		pw.println("Free:</td><td>");
		pw.println(nf.format(free) + "k");
		pw.println("</td></tr>");

		pw.println("<tr><td class=\"caption\">");
		pw.println("Used:</td><td>");
		pw.println(nf.format(total - free) + "k");
		pw.println("</td></tr>");

		
		// Queue info
		ETLgineServer server = ETLgineServer.getInstance();
		ServerContext context = server.getServerContext();

		for (JobQueue queue : context.getJobQueues()) {
			pw.println("<tr><td>&nbsp;</td><td>&nbsp;</td></tr>");
			pw.println("<tr><td class=\"caption\">");
			pw.println("Queue:</td><td>");
			pw.println(queue.getName());
			pw.println("</td></tr>");

			pw.println("<tr><td class=\"caption\">");
			pw.println("Size:</td><td>");
			pw.println(queue.getSize());
			pw.println("</td></tr>");
			
			pw.println("<tr><td class=\"caption\">");
			pw.println("Status:</td><td>");
			IJob job = queue.getActiveJob();
			if (job == null) {
				pw.println("Empty");
				pw.println("</td></tr>");
			} else {
				pw.println("Executing");
				pw.println("</td></tr>");

				pw.println("<tr><td class=\"caption\">");
				pw.println("Job:</td><td>");
				pw.println(job.getName());
				pw.println("</td></tr>");

				pw.println("<tr><td class=\"caption\">");
				pw.println("State:</td><td>");
				pw.println(job.getState());
				pw.println("</td></tr>");

				pw.println("<tr><td class=\"caption\">");
				pw.println("Duration:</td><td>");
				pw.println(job.getDurationInfo());
				pw.println("</td></tr>");

				IProcess p = job.getProcessChain() != null ? job.getProcessChain().getActiveProcess() : null;
				if (p != null) {
					pw.println("<tr><td class=\"caption\">");
					pw.println("Process:</td><td>");
					pw.println(p.getName());
					pw.println("</td></tr>");

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
							pw.println("<tr><td class=\"caption\">");
							pw.println("Source:</td><td>");
							pw.println(name);
							pw.println("</td></tr>");
						}
					}
					
					pw.println("<tr><td class=\"caption\">");
					pw.println("Record:</td><td>");
					
					long count = p.getContext().getRecordsCount();
					
					pw.println(nf.format(count));
					
					long duration = (System.currentTimeMillis() - job.getLastStarted().getTime()) / 1000;
					if (duration > 0) {
						pw.println("(" + nf.format(count / duration) + "/sec)");
					}
					
					pw.println("</td></tr>");
				}
				
			}
		}
		
		pw.println("</table>");
		
		
		pw.close();
		
	}

}
