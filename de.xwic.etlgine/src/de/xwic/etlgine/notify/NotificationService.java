/**
 * 
 */
package de.xwic.etlgine.notify;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
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
 * job completion. Usefull to get notified if a job failed.
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

		// initialize mail...
		IMailManager mailManager = MailFactory.getMailManager();
		
		String subject = "ETLgine [" + serverContext.getProperty("name", "Unnamed") + "]: " +
				"Job '" + job.getName() + "' finished with result " + result.toString();
		
		String content = "<html><body>Job Name: " + job.getName() + "<br>";
		if (job.getLastException() != null) {
			// get process info
			IProcess process = null;
			if (job.getLastException() instanceof ETLException) {
				ETLException ee = (ETLException)job.getLastException();
				process = ee.getProcess();
			}
			content += "Process Name: " + (process != null ? process.getName() : "<i>UNKNOWN</i>") + "<br>";
			content += "Duration: " + job.getDurationInfo() + "<br>";
			// get stack trace
			ByteArrayOutputStream stackTrace = new ByteArrayOutputStream();
			PrintWriter pw = new PrintWriter(stackTrace);
			job.getLastException().printStackTrace(pw);
			pw.flush();
			JWicTools jt = new JWicTools(Locale.getDefault());
			content += "Last Exception: " + jt.formatHtml(stackTrace.toString());
		} else {
			content += "Duration: " + job.getDurationInfo() + "<br>";
		}
		content += "</body></html>";
		
		mailManager.sendEmail(content, subject, mailTo.split(";"), new String[0], mailFrom);
		
	}

	
	
}
