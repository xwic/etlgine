/**
 * 
 */
package de.xwic.etlgine.notify;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.jwic.renderer.util.JWicTools;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IJob.State;
import de.xwic.etlgine.IProcess;
import de.xwic.etlgine.jdbc.JDBCUtil;
import de.xwic.etlgine.mail.EmptyMail;
import de.xwic.etlgine.mail.IAttachment;
import de.xwic.etlgine.mail.IMailManager;
import de.xwic.etlgine.mail.MailFactory;
import de.xwic.etlgine.mail.impl.EmailAttachment;
import de.xwic.etlgine.server.IServerContextListener;
import de.xwic.etlgine.server.JobQueue;
import de.xwic.etlgine.server.ServerContext;
import de.xwic.etlgine.server.ServerContextEvent;

/**
 * The notification service can send Email for various server events such as
 * job completion. Useful to get notified if a job failed.
 * 
 * @author lippisch
 */
public class NotificationService implements IServerContextListener {
	
	private static final Log log = LogFactory.getLog(NotificationService.class);
	
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
		
		if (!serverContext.getPropertyBoolean("notifications.enabled", false)) {
			doSend = false;
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
		
		String connectionId = serverContext.getProperty("monitor.connection","");
		
		//if the config for sending the job execution log is activated and we have a valid monitor connection defined 
		if (serverContext.getPropertyBoolean("notifications.attachexecutionlog.enabled", false) && !"".equals(connectionId)){
			
			//create a temporary file to fill it with the execution log
			String tempDir = System.getProperty("java.io.tmpdir");
			String fileName = "ExecutionLog"+System.currentTimeMillis()+".log";
			File logFile = new File(tempDir, fileName);
			try{
				
				extractExecutionLogInFile(job,connectionId, logFile);
				
				//prepare the email
				EmptyMail email = new EmptyMail();
				email.setContent(content);
				email.setSenderAddress(mailFrom);
				email.setSubject(subject);
				email.setToAddresses(Arrays.asList(mailTo.split(";")));
				
				List<IAttachment> attachments = new ArrayList<IAttachment>();
				EmailAttachment att = new EmailAttachment(logFile);
				attachments.add(att);
				email.setAttachments(attachments);
			
				//sending the email
				mailManager.sendEmail(email);
			} catch (Exception e) {
				log.error(e);
			}
		}else{
			mailManager.sendEmail(content, subject, mailTo.split(";"), new String[0], mailFrom);
		}
	}
	
	private void extractExecutionLogInFile(IJob job, String connectionId, File tempLogFile) throws Exception{
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM:hh:mm:ss.SSS ", Locale.ENGLISH);

		Connection conn = null;
		PreparedStatement prepareStatement = null;
		FileOutputStream fos = null;
		try{
			//open the connection to jdbc monitor to get the execution log from db
			conn = JDBCUtil.openConnection(serverContext, connectionId);
			String sql = "select created,level,message from "+ serverContext.getProperty("monitor.table")+ " where job='" + job.getName()
					+ "' and ETLgineId='" + serverContext.getProperty(ServerContext.PROPERTY_SERVER_INSTANCEID) + "'"
					+ " and created >= ? and created <= ? order by created asc";
			Timestamp start = null;
			Timestamp finished = null;
			
			prepareStatement = conn.prepareStatement(sql);
			if (job.getLastStarted() != null) {
				start = new Timestamp(job.getLastStarted().getTime());
			}
			if (job.getLastFinished() != null) {
				finished = new Timestamp(job.getLastFinished().getTime());
			}
			prepareStatement.setTimestamp(1, start);
			prepareStatement.setTimestamp(2, finished);
			
			ResultSet rs = prepareStatement.executeQuery();
			
			fos = new FileOutputStream(tempLogFile);
			StringBuilder sb = new StringBuilder();
			
			//write each record in the temp file
			while (rs.next()){
				sb.setLength(0);
				Timestamp timestamp = rs.getTimestamp("created");
				String level =rs.getString("level");
				String msg = rs.getString("message");
				//prepare the line content as date + logging level + message
				sb.append(sdf.format(timestamp)).append("   ")
				  .append(" [").append(level).append("] ").append(msg)
				  .append("\n");
				fos.write(sb.toString().getBytes());
			}
			
		}finally{
			try {
				//cleanup
				if (null != prepareStatement){
					prepareStatement.close();
				}
				if (null != conn){
					conn.close();
				}
				if (null != fos){
					fos.close();
				}
			} catch (Exception e) {
				log.error(e);
			}
		}
	}
	
}
