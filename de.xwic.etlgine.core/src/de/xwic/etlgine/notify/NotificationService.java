/**
 * 
 */
package de.xwic.etlgine.notify;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Properties;

import de.jwic.renderer.util.JWicTools;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IJob.State;
import de.xwic.etlgine.IProcess;
import de.xwic.etlgine.mail.IMailManager;
import de.xwic.etlgine.mail.MailFactory;
import de.xwic.etlgine.server.IServerContextListener;
import de.xwic.etlgine.server.ServerContext;
import de.xwic.etlgine.server.ServerContextEvent;

/**
 * The notification service can send Email for various server events such as
 * job completion. Useful to get notified if a job failed.
 * 
 * @author lippisch
 */
public class NotificationService implements IServerContextListener {

	public enum Level {
		ALL,
		WARN,
		ERROR
	}
	
	public enum NotificationEvent {
		JOB_EXECUTION_FINISHED,
		JOB_STARTED
	}
	
	private final ServerContext serverContext;
	private String mailFrom;
	private String mailTo;
	private Level level;
	
	/**
	 * @param serverContext
	 */
	public NotificationService(ServerContext serverContext) {
		this.serverContext = serverContext;
		
		mailFrom = serverContext.getProperty("notifications.from");
		mailTo = serverContext.getProperty("notifications.to");
		level = Level.valueOf(serverContext.getProperty("notifications.level", "WARN"));

		if (mailFrom == null || mailFrom.length() == 0) {
			throw new IllegalStateException("notification.from not specified."); 
		}
		if (mailTo == null || mailTo.length() == 0) {
			throw new IllegalStateException("notification.to not specified."); 
		}
		
		// initialize mail manager
		Properties prop = new Properties();
		prop.setProperty("mail.smtp.host", serverContext.getProperty("mail.smtp.host"));
		MailFactory.initialize(prop);
		
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.server.IServerContextListener#jobExecutionEnd(de.xwic.etlgine.server.ServerContextEvent)
	 */
	public void jobExecutionEnd(ServerContextEvent event) {
	
		boolean doSend = false;
		switch (level) {
		case ALL:
			doSend = true;
			break;
		case WARN:
			doSend = event.getResult() == State.FINISHED_WITH_ERROR ||
				event.getResult() == State.ERROR;
			break;
		case ERROR:
			doSend = event.getResult() == State.ERROR;
			break;
		}
		
		if (doSend) {
			sendNotification(NotificationEvent.JOB_EXECUTION_FINISHED, event.getJob(), event.getResult());
		}
		
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.server.IServerContextListener#jobExecutionStart(de.xwic.etlgine.server.ServerContextEvent)
	 */
	public void jobExecutionStart(ServerContextEvent event) {
		
		// for now, do not send emails on job start
		
	}

	/**
	 * @param job_execution_finished
	 * @param job
	 * @param result
	 */
	private void sendNotification(NotificationEvent nfEvent, IJob job, State result) {

		String subject = "finished with result " + result.toString();
		
		IProcess process = null;
		Throwable t = job.getLastException();
		
		if (t instanceof ETLException) {
			// get process info
			ETLException ee = (ETLException)t;
			process = ee.getProcess();
		}		
		
		sendNotification(job, process, subject, null, t);
		
	}

	/**
	 * 
	 * @param job
	 * @param process
	 * @param subject
	 * @param message
	 * @param t
	 */
	public void sendNotification(IJob job, IProcess process, String subject, String message, Throwable t) {
		// initialize mail...
		IMailManager mailManager = MailFactory.getMailManager();
		JWicTools jt = new JWicTools(Locale.getDefault());
		
		subject = "ETLgine [" + serverContext.getProperty("name", "Unnamed") + "]:" +
				(job != null ? " Job '" + job.getName() + "'" : "") +
				(subject != null ? " " + subject : "");
		
		String content = "<html><body>";
		if (job != null) {
			content += "Job Name: " + job.getName() + "<br>";
			SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM:HH:mm:ss.SSS");
			if (job.getLastStarted() != null) {
				content += "Job Start: " + sdf.format(job.getLastStarted()) + "<br>";
			}
			if (job.getLastFinished() != null) {
				content += "Job End: " + sdf.format(job.getLastFinished()) + "<br>";
			}
			content += "Duration: " + job.getDurationInfo() + "<br>";
		}
		
		if (process != null) {
			// get process info
			content += "Process Name: " + process.getName() + "<br>";
			content += "Process Script: " + process.getCreatorInfo() + "<br>";
		}
		if (message != null) {
			content += "<br>";
			content += "Message:<br>";
			content += "========<br>";
			content += jt.formatHtml(message) + "<br><br>";
		}
		
		if (t != null) {
			// get stack trace
			ByteArrayOutputStream stackTrace = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(stackTrace);
			t.printStackTrace(pw);
			pw.close();
			content += "Last Exception: " + jt.formatHtml(stackTrace.toString());
		}		
		content += "</body></html>";
		
		mailManager.sendEmail(content, subject, mailTo.split(";"), new String[0], mailFrom);		
	}
	
}
