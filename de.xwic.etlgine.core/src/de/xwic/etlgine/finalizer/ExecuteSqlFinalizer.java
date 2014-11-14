/*
 * Copyright (c) NetApp Inc. - All Rights Reserved
 * 
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 * 
 *  
 */
package de.xwic.etlgine.finalizer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IProcessFinalizer;
import de.xwic.etlgine.Result;
import de.xwic.etlgine.jdbc.JDBCUtil;

/**
 * Process finalizer allows the execution of an insert, update or delete sql statement on a given shared connection. The finalizer can
 * commit or roll back the entire job transaction.
 * 
 * Prerequisites to use this finalizer: The loader must use an shared connection with the same name as the used connection because the share
 * prefix will be automatically added The last sql finalizer in the job must set the commitOnFinish flag to true.
 * 
 * @author ionut
 *
 */
public class ExecuteSqlFinalizer implements IProcessFinalizer {

	private static final Log log = LogFactory.getLog(ExecuteSqlFinalizer.class);

	/**
	 * The connection key from properties file
	 */
	protected String connectionId;
	/**
	 * The dml statement to execute
	 */
	protected String sql;
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
	 * A list of sql statements that must be executed. If one statement fails the other ones won't be executed and the transaction would be
	 * rolled back
	 */
	protected List<String> sqlStatements;

	/**
	 * 
	 * @param connection
	 * @param sql
	 */
	public ExecuteSqlFinalizer(String connectionId, String sql) {
		this.connectionId = connectionId;
		this.sql = sql;
	}

	/**
	 * 
	 * @param connectionId
	 * @param sqlStatements
	 */
	public ExecuteSqlFinalizer(String connectionId, List<String> sqlStatements) {
		this.connectionId = connectionId;
		this.sqlStatements = sqlStatements;
	}

	/**
	 * 
	 * @param connection
	 * @param sql
	 * @param commitOnFinish
	 */
	public ExecuteSqlFinalizer(String connectionId, String sql, boolean commitOnFinish) {
		this.connectionId = connectionId;
		this.sql = sql;
		this.commitOnFinish = commitOnFinish;
	}

	/**
	 * 
	 * @param connectionId
	 * @param sqlStatements
	 * @param commitOnFinish
	 */
	public ExecuteSqlFinalizer(String connectionId, List<String> sqlStatements, boolean commitOnFinish) {
		this.connectionId = connectionId;
		this.sqlStatements = sqlStatements;
		this.commitOnFinish = commitOnFinish;
	}

	/**
	 * 
	 * @param connection
	 * @param sql
	 * @param successMessage
	 */
	public ExecuteSqlFinalizer(String connectionId, String sql, String successMessage) {
		this.connectionId = connectionId;
		this.sql = sql;
		this.successMessage = successMessage;
	}

	/**
	 * 
	 * @param connection
	 * @param sql
	 * @param successMessage
	 * @param commitOnFinish
	 */
	public ExecuteSqlFinalizer(String connectionId, String sql, String successMessage, boolean commitOnFinish) {
		this.connectionId = connectionId;
		this.sql = sql;
		this.successMessage = successMessage;
		this.commitOnFinish = commitOnFinish;
	}

	/**
	 * 
	 * @param connection
	 * @param sql
	 * @param successMessage
	 * @param commitOnFinish
	 */
	public ExecuteSqlFinalizer(Connection connection, String sql, String successMessage, boolean commitOnFinish) {
		this.connection = connection;
		this.sql = sql;
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
		Statement stmt = null;
		try {

			if (null == con) {
				//use the shared connection in order to commit the entire transaction 
				con = JDBCUtil.getSharedConnection(context, connectionId, connectionId);
			}
			//execute the statement only if the current process result is successful
			if (context.getResult() == Result.SUCCESSFULL) {
				if (null != sql) {
					if (null == sqlStatements) {
						sqlStatements = new ArrayList<String>();
					}
					sqlStatements.clear();
					sqlStatements.add(sql);
				}
				if (null != sqlStatements) {
					for (String sqlStatement : sqlStatements) {
						stmt = con.createStatement();
						context.getMonitor().logInfo("Executing: " + sqlStatement);
						int cnt = stmt.executeUpdate(sqlStatement);
						String message = "Processed " + cnt + " records";
						if (null != successMessage) {
							message = String.format(successMessage, cnt);
							successMessage = message;
						}
						context.getMonitor().logInfo(message);
						stmt.close();
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
						if (!con.isClosed()) {
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
	 * @return the sql
	 */
	public String getSql() {
		return sql;
	}

	/**
	 * @param sql
	 *            the sql to set
	 */
	public void setSql(String sql) {
		this.sql = sql;
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

	/**
	 * @return the sqlStatements
	 */
	public List<String> getSqlStatements() {
		return sqlStatements;
	}

	/**
	 * @param sqlStatements
	 *            the sqlStatements to set
	 */
	public void setSqlStatements(List<String> sqlStatements) {
		this.sqlStatements = sqlStatements;
	}

}
