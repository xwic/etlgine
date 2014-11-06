package de.xwic.etlgine.finalizer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IProcessFinalizer;
import de.xwic.etlgine.Result;
import de.xwic.etlgine.loader.database.transaction.TransactionUtil;

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
	 * A list of sql statements that must be executed. If one statement fails the other ones won't be executed and the transaction would be
	 * rolled back
	 */
	protected List<String> sqlStatements;

	//TODO
	private PlatformTransactionManager transactionManager;

	//TODO
	private TransactionStatus transaction;

	/** A transaction-aware template used to execute DB queries. */
	private JdbcTemplate jdbcTemplate;

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

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.xwic.etlgine.IProcessFinalizer#onFinish(de.xwic.etlgine.IProcessContext)
	 */
	@Override
	public void onFinish(IProcessContext context) throws ETLException {
		initTransactionManager(context);
		initJdbcTemplate(context);
		joinTransaction();

		try {
			if (context.getResult() == Result.SUCCESSFULL) {
				//execute the statement only if the current process result is successful

				if (sql != null) {
					if (sqlStatements == null) {
						sqlStatements = new ArrayList<String>();
					}
					sqlStatements.clear();
					sqlStatements.add(sql);
				}
				if (sqlStatements != null) {
					for (String sqlStatement : sqlStatements) {
						// Execute the database statement
						this.jdbcTemplate.execute(sqlStatement);
						
						context.getMonitor().logInfo("Executed: " + sqlStatement);
					}
				}
			}
		} catch (DataAccessException e) {
			transactionManager.rollback(transaction);

			context.getMonitor().logError("ROLLBACK because of unsuccessfull process!");
			context.getMonitor().logError("Exception", e);

			context.setResult(Result.FAILED);
			throw new ETLException(e);
		}

		if (commitOnFinish) {
			transactionManager.commit(transaction);
			try {
				DataSourceUtils.getConnection(this.jdbcTemplate.getDataSource()).commit();
				DataSourceUtils.getConnection(this.jdbcTemplate.getDataSource()).close();
			} catch (CannotGetJdbcConnectionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void initTransactionManager(IProcessContext context) throws ETLException {
		if (connectionId == null) {
			throw new ETLException("This finalizer needs a 'connectionId'.");
		}

		// TransactionManagers are cached by the DatabaseLoader into the context, during initialization
		PlatformTransactionManager transactionManager = context.getTransactionManager(connectionId);

		if (transactionManager == null) {
			throw new ETLException("No transactionManager could be found for the connectionId: " + connectionId);
		}

		this.transactionManager = transactionManager;
	}

	private void initJdbcTemplate(IProcessContext context) throws ETLException {
		if (connectionId == null) {
			throw new ETLException("This finalizer needs a 'connectionId'.");
		}

		// DataSources are cached by the DatabaseLoader into the context, during initialization
		DataSource dataSource = context.getDataSource(connectionId);

		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	private void joinTransaction() {
		DefaultTransactionDefinition transactionDefinition = TransactionUtil.getDefaultTransactionDefinition();
		TransactionStatus transaction = transactionManager.getTransaction(transactionDefinition);

		this.transaction = transaction;
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
