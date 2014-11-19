/*
 * Copyright (c) NetApp Inc. - All Rights Reserved
 * 
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 * 
 *  
 */
package de.xwic.etlgine.finalizer;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IProcessFinalizer;
import de.xwic.etlgine.Result;
import de.xwic.etlgine.jdbc.JDBCUtil;

/**
 * Process finalizer that can start an sql server agent job and wait or not for the result.
 * The finalizer calls the following stored procedures:
 * 	-msdb.dbo.sp_start_job
 *  -msdb.dbo.sp_help_job
 *  -msdb.dbo.sp_help_jobhistory
 *  and the used connection must have permissions to do so. The user must be member of sysadmin server role or must be granted one of the roles:
 *  -SQLAgentUserRole
 *  -SQLAgentReaderRole
 *  -SQLAgentOperatorRole
 * 
 * Prerequisites to use this finalizer: The loader must use an shared connection with the same name as the used connection because the share
 * prefix will be automatically added. The last sql finalizer in the job must set the commitOnFinish flag to true.
 * 
 * @author ionut
 *
 */
public class ExecuteSqlServerAgentJobFinalizer implements IProcessFinalizer {

	private static final Log log = LogFactory.getLog(ExecuteSqlServerAgentJobFinalizer.class);

	/**
	 * Stored procedure used to start a sql server agent job
	 */
	private static final String START_JOB_SQL = "EXEC msdb.dbo.sp_start_job @job_name = N'%s'";

	/**
	 * Stored procedure used to check if a sql server agent job is running
	 */
	private static final String IS_RUNNING_JOB_SQL = "EXEC msdb.dbo.sp_help_job @job_name = N'%s'";

	/**
	 * Stored procedure used to get the result of an previously executed sql server agent job
	 */
	private static final String GET_JOB_RESULT = "EXEC msdb.dbo.sp_help_jobhistory @job_name = N'%s'";

	/**
	 * The name of the column containing the current sql server job execution status
	 */
	private static final String EXECUTION_STATUS_COLUMN = "current_execution_status";
	/**
	 * The name of the column containing the result of an sql server agent job
	 */
	private static final String RESULT_STATUS_COLUMN = "run_status";

	/**
	 * Constant defining the SQL server idle execution status
	 */
	private static int IDLE_EXECUTION_STATUS = 4;

	/**
	 * The constant defining the sql server job succeeded status
	 */
	private static int JOB_RESULT_SUCCESS = 1;
	/**
	 * Number of seconds to wait between sending the next query to the server to verify the job execution status; The default value is 60
	 * seconds.
	 */
	private static int POOL_JOB_STATUS_TIME_SEC = 60;

	/**
	 * The connection key from properties file
	 */
	protected String connectionId;

	/**
	 * The job name
	 */
	protected String jobName;
	/**
	 * The custom log message displayed on successful operation. The message can contain one formatting placeholder '%d' that will contain
	 * the number of processed records
	 */
	protected String successMessage;

	/**
	 * This flag indicates that we have used an shared connection and we want to commit the entire transaction after this finalizer
	 */
	protected boolean commitOnFinish;

	/**
	 * An existing sql connection to use if needed
	 */
	protected Connection connection;

	/**
	 * Flag that indicates if the process shall wait for the sql server job to finish. The default value is false.
	 */
	protected boolean waitForJobToFinish;

	/**
	 * The maximum number of seconds that the process must wait for an job to finish; The default value is one hour;
	 */
	protected int timeout = 3600;

	/**
	 * @param connectionId
	 * @param jobName
	 * @param commitOnFinish
	 */
	public ExecuteSqlServerAgentJobFinalizer(String connectionId, String jobName, boolean commitOnFinish) {
		super();
		this.connectionId = connectionId;
		this.jobName = jobName;
		this.commitOnFinish = commitOnFinish;
	}

	/**
	 * 
	 * @param connectionId
	 * @param jobName
	 * @param commitOnFinish
	 * @param waitForJobToFinish
	 */
	public ExecuteSqlServerAgentJobFinalizer(String connectionId, String jobName, boolean commitOnFinish, boolean waitForJobToFinish) {
		super();
		this.connectionId = connectionId;
		this.jobName = jobName;
		this.commitOnFinish = commitOnFinish;
		this.waitForJobToFinish = waitForJobToFinish;
	}

	/**
	 * 
	 * @param connectionId
	 * @param jobName
	 * @param commitOnFinish
	 * @param waitForJobToFinish
	 * @param timeout
	 */
	public ExecuteSqlServerAgentJobFinalizer(String connectionId, String jobName, boolean commitOnFinish, boolean waitForJobToFinish,
			int timeout) {
		super();
		this.connectionId = connectionId;
		this.jobName = jobName;
		this.commitOnFinish = commitOnFinish;
		this.waitForJobToFinish = waitForJobToFinish;
		this.timeout = timeout;
	}

	/**
	 * @param connectionId
	 * @param jobName
	 * @param successMessage
	 * @param commitOnFinish
	 */
	public ExecuteSqlServerAgentJobFinalizer(String connectionId, String jobName, String successMessage, boolean commitOnFinish) {
		super();
		this.connectionId = connectionId;
		this.jobName = jobName;
		this.successMessage = successMessage;
		this.commitOnFinish = commitOnFinish;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.xwic.etlgine.IProcessFinalizer#onFinish(de.xwic.etlgine.IProcessContext)
	 */
	@Override
	public void onFinish(IProcessContext context) throws ETLException {
		Connection con = connection;
		CallableStatement stmt = null;
		try {

			if (null == con) {
				//use the shared connection in order to commit the entire transaction 
				con = JDBCUtil.getSharedConnection(context, connectionId, connectionId);
			}
			//execute the statement only if the current process result is successful
			if (context.getResult() == Result.SUCCESSFULL) {

				if (null != jobName) {
					//check if the job is already running
					if (isRunning(context, con)) {
						throw new ETLException("The sql server agent job " + jobName + " is already running");
					}

					String job = String.format(START_JOB_SQL, jobName);
					stmt = con.prepareCall(job);
					context.getMonitor().logInfo("Executing: " + job);
					//start the job
					boolean isResultSet = stmt.execute();
					int cnt = 0;
					if (!isResultSet) {
						cnt = stmt.getUpdateCount();
					}
					String message = "Processed " + cnt + " records";
					if (null != successMessage) {
						message = String.format(successMessage, cnt);
						successMessage = message;
					}
					if (cnt >=0){
						context.getMonitor().logInfo(message);
					}
					stmt.close();

					//if we must wait for the sql server job to finish
					if (waitForJobToFinish) {
						long startWaitTime = System.currentTimeMillis();
						long endWaitTime = startWaitTime + 1;
						long maxTime = POOL_JOB_STATUS_TIME_SEC * 1000;

						//while we don't reached the max time to wait
						while ((endWaitTime - startWaitTime) < maxTime) {
							//idle current thread
							Thread.sleep(POOL_JOB_STATUS_TIME_SEC * 1000);
							//check if the job is running and get the finished status 
							if (!isRunning(context, con)) {
								int jobResult = getJobResult(context, con);
								//if the job didn't succeeded
								if (JOB_RESULT_SUCCESS != jobResult) {
									throw new ETLException("The sql server agent job " + jobName
											+ " didn't succeeded but finished with the status " + jobResult);
								}
								context.getMonitor().logInfo("The sql server agent job " + jobName + " finished successfuly");
								break;
							}
						}
					}
				}

			}

		} catch (Exception e) {
			context.getMonitor().logError("Exception", e);
			context.setResult(Result.FAILED);
			throw new ETLException(e);
		} finally {
			try {
				//commit or roll back the entire job transaction
				if (commitOnFinish && null != con) {

					if (context.getResult() == Result.SUCCESSFULL) {
						if (!con.isClosed() && !con.getAutoCommit()) {
							con.commit();
						}
					} else {
						if (!con.isClosed() && !con.getAutoCommit()) {
							con.rollback();
							context.getMonitor().logError("ROLLBACK because of unsuccessfull process!");
						}
					}

					if (null != stmt) {
						stmt.close();
					}

					if (!con.isClosed()) {
						con.close();
					}
				}
			} catch (SQLException ex) {
				context.getMonitor().logError("Exception", ex);
				throw new ETLException(ex);
			} finally {
				try {
					if (commitOnFinish && null != con && !con.isClosed()) {
						con.close();
					}
				} catch (SQLException e) {
					log.error("exception on process finalizer when closing the connection", e);
				}
			}
		}

	}

	/**
	 * Checks weather the specified job is running by calling msdb.dbo.sp_help_job
	 * 
	 * @param context
	 * @param con
	 * @return true if the job is running, otherwise false
	 * @throws SQLException
	 */
	private boolean isRunning(IProcessContext context, Connection con) throws SQLException {
		boolean running = false;
		String sql = String.format(IS_RUNNING_JOB_SQL, jobName);
		CallableStatement stmt = null;
		try {
			context.getMonitor().logInfo("Executing: " + sql);
			stmt = con.prepareCall(sql);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				running = (IDLE_EXECUTION_STATUS != rs.getInt(EXECUTION_STATUS_COLUMN));
			}
			rs.close();
		} finally {
			if (null != stmt) {
				stmt.close();
			}
		}
		return running;
	}

	/**
	 * Calls msdb.dbo.sp_help_jobhistory to get the job result status:
	 * 
	 * 0 - Failed 1 - Succeeded 2 - Retry 3 - Canceled
	 * 
	 * @param context
	 * @param con
	 * @return the job status
	 * @throws SQLException
	 */
	private int getJobResult(IProcessContext context, Connection con) throws SQLException {
		int result = -1;
		String sql = String.format(GET_JOB_RESULT, jobName);
		CallableStatement stmt = null;
		try {
			context.getMonitor().logInfo("Executing: " + sql);
			stmt = con.prepareCall(sql);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				result = rs.getInt(RESULT_STATUS_COLUMN);
			}
			rs.close();
		} finally {
			if (null != stmt) {
				stmt.close();
			}
		}
		return result;
	}

	/**
	 * @return the connection
	 */
	public Connection getConnection() {
		return connection;
	}

	/**
	 * Set an existing connection to use
	 * 
	 * @param connection
	 */
	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	/**
	 * @return the connectionId
	 */
	public String getConnectionId() {
		return connectionId;
	}

	/**
	 * @param connectionId
	 *            the connectionId to set
	 */
	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}

	/**
	 * @return the successMessage
	 */
	public String getSuccessMessage() {
		return successMessage;
	}

	/**
	 * @param successMessage
	 *            the successMessage to set
	 */
	public void setSuccessMessage(String successMessage) {
		this.successMessage = successMessage;
	}

	/**
	 * @return the commitOnFinish
	 */
	public boolean isCommitOnFinish() {
		return commitOnFinish;
	}

	/**
	 * @param commitOnFinish
	 *            the commitOnFinish to set
	 */
	public void setCommitOnFinish(boolean commitOnFinish) {
		this.commitOnFinish = commitOnFinish;
	}

}
