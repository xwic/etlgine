/**
 * 
 */
package de.xwic.etlgine.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IColumn.DataType;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.impl.Context;
import de.xwic.etlgine.loader.jdbc.SqlDialect;
import de.xwic.etlgine.server.JobQueue;

/**
 * @author Developer
 */
public class JDBCUtil {

	public static String PK_IDENTITY_COLUMN = "Id";
	public static String EXECUTE = "/* execute */";
	public static String IGNORE_ERROR = "/* inore error */";
	public static String EXECUTE_IGNORE_ERROR = EXECUTE + IGNORE_ERROR;
	
	public static Map<String, DbColumnDef> DBCOLUMNDEF_ALTERED = Collections.synchronizedMap(new HashMap<String, DbColumnDef>());
	
	private static final String SHARE_PREFIX = "_sharedConnection.";

	protected final static Log log = LogFactory.getLog(JDBCUtil.class);
	
	/**
	 * Returns the connection with the specified shareName. If the connection does not exist, one is
	 * created. The connection is stored in the context under the key _sharedConnection.[shareName].
	 * 
	 * A 
	 * 
	 * @param context
	 * @param shareName
	 * @param connectName
	 * @return
	 * @throws ETLException
	 * @throws SQLException
	 */
	public static Connection getSharedConnection(IContext context, String shareName, String connectName) throws ETLException, SQLException {
		
		String queueName = getThreadQueueName();
		String sharedKey = SHARE_PREFIX + shareName;
		if (!sharedKey.contains(queueName)){
			sharedKey += queueName;
		}
		Connection con = (Connection) context.getData(sharedKey);
		if (con == null || con.isClosed()) {
			if(log.isDebugEnabled()) {
				log.debug("Shared connection with name: " + sharedKey +
						" was not found, or already closed. Opening a new connection...");
			}
			con = openConnection(context, connectName);
			setSharedConnection(context, shareName, con);
		}
		
		return con;
		
	}
	
	/**
	 * Returns the queue name for this running thread extracted from the thread name
	 * 
	 * @return empty string if no queue name can be extracted from the thread name
	 */
	private static String getThreadQueueName() {
		String queueName = Thread.currentThread().getName();
		if (null != queueName && -1 != queueName.indexOf(JobQueue.QUEUE_THREAD_PREFIX)){
			queueName = queueName.substring(queueName.indexOf(JobQueue.QUEUE_THREAD_PREFIX)+JobQueue.QUEUE_THREAD_PREFIX.length());
		}else{
			queueName="";
		}
		return queueName;
	}

	/**
	 * Sets the connection with specified shareName in context without checking if it already exists.
	 * @param context
	 * @param shareName
	 * @param connection
	 */
	public static synchronized void setSharedConnection(IContext context, String shareName, Connection connection) {
		String queueName = getThreadQueueName();
		String sharedKey = SHARE_PREFIX + shareName;
		if (!sharedKey.contains(queueName)){
			sharedKey += queueName;
		}
		context.setData(sharedKey, connection);
		if(log.isDebugEnabled()) {
			log.debug("Stored shared connection with name: " + SHARE_PREFIX + shareName + " into the context.");
		}
	}
	
	/**
	 * Executes the given SQL statement and returns the value from the first column in the first row
	 * of the ResultSet or NULL if no result is given.
	 * A new connection is opened for the query and closed after processing.
	 * @param con
	 * @param select
	 * @return
	 * @throws SQLException
	 * @throws ETLException 
	 */
	public static Object executeSingleValueQuery(IContext context, String connectionName, String select) throws SQLException, ETLException {
		Connection con = openConnection(context, connectionName);
		try {
			return executeSingleValueQuery(con, select);
		} finally {
			con.close();
		}
	}
	
	/**
	 * Executes the given SQL statement and returns the value from the first column in the first row
	 * of the ResultSet or NULL if no result is given.
	 * @param con
	 * @param select
	 * @return
	 * @throws SQLException
	 */
	public static Object executeSingleValueQuery(Connection con, String select) throws SQLException {
		
		Statement stmt = con.createStatement();
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(select);
			if (rs.next()) {
				return rs.getObject(1);
			}
			return null;
		} finally {
			if (rs != null) {
				rs.close();
			}
			stmt.close();
		}
	}
	
	/**
	 * Open a pre-configured connection. The connection details are obtained from
	 * the context properties in the format: [name].connection.XXX
	 * @param context
	 * @param name
	 * @return
	 */
	public static Connection openConnection(IContext context, String name) throws ETLException, SQLException {
		return openConnection(context, name, true);
	}

	/**
	 * Open a pre-configured connection. The connection details are obtained from
	 * the context properties in the format: [name].connection.XXX
	 * @param context
	 * @param name
	 * @param logging
	 * @return
	 */
	public static Connection openConnection(IContext context, String name, boolean logging) throws ETLException, SQLException {
		
		String driver = context.getProperty(name + ".connection.driver", "net.sourceforge.jtds.jdbc.Driver");
		String url = context.getProperty(name + ".connection.url");
		String username = context.getProperty(name + ".connection.username");
		String password = context.getProperty(name + ".connection.password");
		String catalog = context.getProperty(name + ".connection.catalog");
		String initSessionStmt = context.getProperty(name + ".connection.initSessionStmt");
		int isolationLevel = context.getPropertyInt(name + ".connection.transactionIsolation", -1);
		
		if (url == null) {
			throw new ETLException("The URL is not specified for this connection name. ('" + name + "')");
		}
		/* 2015-05-08 jbornema Disabled to support SSO
		if (username == null) {
			throw new ETLException("The username is not specified for this connection name. ('" + name + "')");
		}
		if (password == null) {
			throw new ETLException("The password is not specified for this connection name. ('" + name + "')");
		}
		*/
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException cnfe) {
			throw new ETLException("Driver " + driver + " can not be found.");
		}
		if (logging){
			log.info("Opening new JDBC connection to: " + url);
		}
		Connection con;
		try {
			Properties props = new Properties();
			if (username != null) {
				props.setProperty("user", username);
			}
			if (password != null) {
				props.setProperty("password", password);
			}
			//props.setProperty("internal_logon", "sysdba");
			con = DriverManager.getConnection(url, props);
		} catch (UnsatisfiedLinkError ule) {
			if (ule.getMessage().contains("java.library.path")) {
				String libPath = System.getProperty("java.library.path");
				throw new ETLException("Problem loading linking library from lava.library.path: " + libPath, ule);
			} else {
				throw new ETLException(ule);
			}
		}

		// set optional transaction isolation level
		if (isolationLevel != -1) {
			con.setTransactionIsolation(isolationLevel);
		}
		
		DatabaseMetaData meta = con.getMetaData();
		String databaseName = meta.getDatabaseProductName();

		String transIso = null;
		switch (con.getTransactionIsolation()) {
		case Connection.TRANSACTION_READ_UNCOMMITTED:
			transIso = "TRANSACTION_READ_UNCOMMITTED";
			break;
		case Connection.TRANSACTION_READ_COMMITTED:
			transIso = "TRANSACTION_READ_COMMITTED";
			break;
		case Connection.TRANSACTION_REPEATABLE_READ:
			transIso = "TRANSACTION_REPEATABLE_READ";
			break;
		case Connection.TRANSACTION_SERIALIZABLE:
			transIso = "TRANSACTION_SERIALIZABLE";
			break;
		case Connection.TRANSACTION_NONE:
			transIso = "TRANSACTION_NONE";
			break;
		case 4096: //4096 corresponds to SQLServerConnection.TRANSACTION_SNAPSHOT
			transIso = "TRANSACTION_SNAPSHOT";
			break;
		default:
			transIso = "Unknown Transaction Isolation!";
			break;
		}
		
		if (logging) {
			String catalogInfo = "";
			if (catalog != null && catalog.length() > 0) {
				con.setCatalog(catalog);
				catalogInfo = ", catalog: " + catalog;
			}
			
			log.info("RDBMS: " + databaseName + catalogInfo + ", version: " + meta.getDatabaseProductVersion().replace("\n", ", ") + 
					", JDBC driver: " + meta.getDriverName() + ", version: " + meta.getDriverVersion() + ", " + transIso);
		}		
		if (initSessionStmt != null && initSessionStmt.trim().length() > 0) {
			Statement stmt = con.createStatement();
			try {
				for (String line : initSessionStmt.split("\n")) {
					log.info("Init session statement: " + line);
					stmt.execute(line);
				}
			} finally {
				stmt.close();
			}
		}
		
		return con;
		
		/*
		return new DelegatingConnection(con) {
			@Override
			public void commit() throws SQLException {
				super.commit();
			}
			@Override
			public void rollback() throws SQLException {
				super.rollback();
			}
			@Override
			public void close() throws SQLException {
				super.close();
			}
		};
		*/
	}
	
	/**
	 * Returns the batch size used for this jdbc statements.
	 * @param context
	 * @param name
	 * @return
	 */
	public static int getBatchSize(IContext context, String name) {
		int batch_size = context.getPropertyInt(name + ".connection.batch_size", 0);
		return batch_size;
	}
	
	/**
	 * Returns the fetch size used for this jdbc statements.
	 * @param context
	 * @param name
	 * @return
	 */
	public static int getFetchSize(IContext context, String name) {
		int fetch_size = context.getPropertyInt(name + ".connection.fetch_size", 0);
		return fetch_size;
	}
	
	/**
	 * Returns the SQL dialect used for this jdbc connection.
	 * @param context
	 * @param name
	 * @return
	 */
	public static String getDialect(IContext context, String name) {
		String dialect = context.getProperty(name + ".connection.dialect", SqlDialect.MSSQL.name());
		return dialect;
	}
	
	/**
	 * Returns the SQL dialect used for this jdbc connection.
	 * @param context
	 * @param name
	 * @return
	 */
	public static SqlDialect getSqlDialect(IContext context, String name) {
		String dialect = getDialect(context, name);
		if (dialect != null && dialect.length() > 0) {
			return SqlDialect.valueOf(dialect);
		}
		return SqlDialect.MSSQL;
	}
	
	/**
	 * @param rs
	 * @throws SQLException 
	 */
	public static void dumpResultSet(ResultSet rs) throws SQLException {
		
		ResultSetMetaData rsmd = rs.getMetaData();
		for (int i = 0; i < rsmd.getColumnCount(); i++) {
			System.out.print(rsmd.getColumnName(i + 1));
			System.out.print(" ");
		}
		System.out.println("");
		while (rs.next()) {
			for (int i = 0; i < rsmd.getColumnCount(); i++) {
				System.out.print(rs.getString(i + 1));
				System.out.print(" ");
			}
			System.out.println();
		}

		
	}

	/**
	 * Checks if the specified column exists.
	 * @param con
	 * @param tableName
	 * @param columnName
	 * @return
	 * @throws SQLException
	 */
	public static boolean columnExists(Connection con, String tableName, String columnName) throws SQLException {
		
		DatabaseMetaData metaData = con.getMetaData();
		ResultSet columns = metaData.getColumns(con.getCatalog(), null, tableName, columnName);
		try {
			if (columns.next()) {
				return true;
			}
			return false;
		} finally {
			columns.close();
		}
		
	}

	public static boolean tableExists(Connection con, String tableName) throws SQLException {
		DatabaseMetaData metaData = con.getMetaData();
		ResultSet tables = metaData.getTables(con.getCatalog(), null, tableName, new String[] {"TABLE"});
		try {
			if (tables.next()) {
				return true;
			}
			return false;
		} finally {
			tables.close();
		}
	}
	
	/**
	 * Checks if the specified column exists and return false if it does.
	 * If not the column is created and true returned.
	 * @param con
	 * @param tableName
	 * @param columnName
	 * @param sqlTypeDef
	 * @return
	 * @throws SQLException
	 */
	public static boolean ensureColumn(Connection con, String tableName, String columnName, String sqlTypeDef) throws SQLException {
		
		boolean exists = columnExists(con, tableName, columnName);
		if (exists) {
			// nothing to do
			return false;
		}
		
		// create column
		String is = getIdentifierSeparator(con);
		Statement stmt = con.createStatement();
		try {
			String sql = "alter table " + is + tableName + is + " add " + is + columnName + is + " " + sqlTypeDef;
			stmt.executeUpdate(sql);
			log.debug("Table column added: " + sql);
		} finally {
			stmt.close();
		}
		
		return true;
	}

	/**
	 * Return quoted String of elements separated by "."
	 * @param quote
	 * @param elements
	 * @return
	 */
	public static String getQuoted(String quote, String... elements) {
		StringBuilder sb = new StringBuilder();
		for (String s : elements) {
			if (sb.length() > 0) {
				sb.append('.');
			}
			if (s == null) {
				continue;
			}
			if (s.length() > 0) {
				sb.append(quote).append(s).append(quote);
			}
		}
		return sb.toString();
	}
	
	/**
	 * 
	 * @param con
	 * @param sqlDialect
	 * @param schema
	 * @param tablename
	 * @param dbColumnsDef
	 * @param pkIdentity
	 * @throws SQLException
	 */
	public static void createTable(Connection con, SqlDialect sqlDialect, String schema, String tablename, Collection<DbColumnDef> dbColumnsDef, String pkIdentity) throws SQLException {
		
		String is = getIdentifierSeparator(con);
		log.info("Creating missing table: " + tablename);

		Statement stmt = con.createStatement();
		StringBuilder sql = new StringBuilder();

		StringBuilder columnsDef = new StringBuilder();

		for (DbColumnDef dbcd : dbColumnsDef) {
			if (pkIdentity != null && dbcd.getName().equals(pkIdentity)) {
				// skip default primary key column
				continue;
			}
			columnsDef.append(", ").append(is).append(dbcd.getName()).append(is).append(" ").append(dbcd.getTypeNameDetails());
		}
		String pkSequence = null;
		if (pkIdentity != null && sqlDialect == SqlDialect.ORACLE) {
			sql.setLength(0);
			pkSequence = "SEQ_" + tablename;
			sql.append("CREATE SEQUENCE " + getQuoted(is, schema, pkSequence));
			log.info("Creating missing sequence for primary key column on table " + getQuoted(is, schema, tablename) + ":\n" + sql.toString());
			stmt.execute(sql.toString());
		}
		
		sql.setLength(0);
		switch (sqlDialect) {
		case MSSQL :
		case H2 :
			sql.append("CREATE TABLE ").append(getQuoted(is, schema, tablename)).append(" (");
			sql.append(getQuoted(is, pkIdentity)).append(" [bigint] IDENTITY(1,1) NOT NULL").append(columnsDef).append(", CONSTRAINT ").append(is).append("PK_").append(tablename).append(is).append(" PRIMARY KEY (").append(getQuoted(is, pkIdentity)).append("))");
			break;
		case ORACLE :
			String seq = "";
			if (pkSequence != null) {
				seq = " DEFAULT " + getQuoted(is, schema, pkSequence) + ".NEXTVAL";
			}
			sql.append("CREATE TABLE ").append(getQuoted(is, schema, tablename)).append(" (");
			sql.append(getQuoted(is, pkIdentity)).append(" NUMBER(20)" + seq + " NOT NULL").append(columnsDef).append(", CONSTRAINT ").append(is).append("PK_").append(tablename).append(is).append(" PRIMARY KEY (").append(is).append(getQuoted(is, pkIdentity)).append("))");
			break;
		}
		
		log.info("Creating missing table sql: \n" + sql.toString());
		
		stmt.execute(sql.toString());
	}
	
	public static boolean syncTableLayout(Connection con, String connectionName, String srcTableName, String dstTableName, String... srcColumnsInclude) throws SQLException {
		String schema = null;
		try {
			schema = con.getSchema();
		} catch (Throwable t) {
			// not implemented/available etc.
		}
		String srcSchema = schema;
		String dstSchema = schema;
		String dstPkIdentity = PK_IDENTITY_COLUMN;
		return syncTableLayoutIncludeColumns(con, connectionName, srcSchema, srcTableName, dstSchema, dstTableName, dstPkIdentity, srcColumnsInclude, null);
	}
	
	public static boolean syncTableLayout2(Connection con, String connectionName, String srcTableName, String dstTableName, String... srcDstColumnsInclude) throws SQLException {
		String schema = null;
		try {
			schema = con.getSchema();
		} catch (Throwable t) {
			// not implemented/available etc.
		}
		String srcSchema = schema;
		String dstSchema = schema;
		String dstPkIdentity = PK_IDENTITY_COLUMN;
		String[] srcColumnsInclude = null;
		String[] dstColumnsInclude = null;
		if (srcDstColumnsInclude != null && srcDstColumnsInclude.length > 0 && (srcDstColumnsInclude.length / 2) * 2 == srcDstColumnsInclude.length) {
			// split src and dst columns
			srcColumnsInclude = new String[srcDstColumnsInclude.length / 2];
			dstColumnsInclude = new String[srcDstColumnsInclude.length / 2];
			for (int i = 0; i < srcDstColumnsInclude.length / 2; i++) {
				srcColumnsInclude[i] = srcDstColumnsInclude[i * 2];
				dstColumnsInclude[i] = srcDstColumnsInclude[i * 2 + 1];
			}
		}
		return syncTableLayoutIncludeColumns(con, connectionName, srcSchema, srcTableName, dstSchema, dstTableName, dstPkIdentity, srcColumnsInclude, dstColumnsInclude);
	}
	
	public static boolean syncTableLayoutIncludeColumns(Connection con, String connectionName, String srcSchema, String srcTableName, String dstSchema, String dstTableName, String dstPkIdentity, String[] srcColumnsInclude, String[] dstColumnsInclude) throws SQLException {
		boolean modified = false;
		IContext context = Context.getThreadContext().get();
		SqlDialect sqlDialect = getSqlDialect(context, connectionName);
		String is = getIdentifierSeparator(con);
		DatabaseMetaData dbmd = con.getMetaData();
		ResultSet rs = null;
		// check source table exists
		rs = dbmd.getTables(con.getCatalog(), srcSchema, srcTableName, new String[]{"TABLE"});
		try {
			if (rs.next()) {
				String tablename = rs.getString("TABLE_NAME");
				String schema = rs.getString("TABLE_SCHEM");
				srcTableName = tablename;
				if (schema != null) {
					srcSchema = schema;
				}
			} else {
				throw new SQLException("Table not found: " + getQuoted(is, srcSchema, srcTableName));
			}
		} finally {
			rs.close();
		}
		
		// check destination table exists
		boolean dstTableExists = false;
		rs = dbmd.getTables(con.getCatalog(), dstSchema, dstTableName, new String[]{"TABLE"});
		try {
			if (rs.next()) {
				dstTableExists = true;
				String tablename = rs.getString("TABLE_NAME");
				String schema = rs.getString("TABLE_SCHEM");
				dstTableName = tablename;
				if (schema != null) {
					dstSchema = schema;
				}
			} else {
				log.warn("Destination table doesn't exist: " + getQuoted(is, dstSchema, dstTableName));
			}
		} finally {
			rs.close();
		}
		
		// get all source table columns into list of DbColumnDef objects
		Map<String, DbColumnDef> srcColumns = getColumns(dbmd, srcSchema, srcTableName, false);
		if (srcColumnsInclude != null) {
			// keep only included columns
			Set<String> srcColNames = new HashSet<String>(srcColumns.keySet());
			Set<String> srcColInc = new HashSet<String>(Arrays.asList(srcColumnsInclude));
			for (String srcCol : srcColNames) {
				if (!srcColInc.contains(srcCol)) {
					srcColumns.remove(srcCol);
				}
			}
		}
		
		if (!dstTableExists) {
			// create destination table
			createTable(con, sqlDialect, dstSchema, dstTableName, srcColumns.values(), dstPkIdentity);
			modified = true;
		} else {
			// load destination columns and create missing or alter existing
			Map<String, DbColumnDef> dstColumns = getColumns(dbmd, dstSchema, dstTableName, false);
			List<DbColumnDef> addColumns = new ArrayList<DbColumnDef>();
			List<DbColumnDef> alterColumns = new ArrayList<DbColumnDef>();
			for (DbColumnDef srcCol : srcColumns.values()) {
				if (dstPkIdentity != null && srcCol.getName().equals(dstPkIdentity)) {
					continue;
				}
				String dstColumnName = srcCol.getName();
				if (dstColumnsInclude != null && srcColumnsInclude != null) {
					// map destination column name
					int idx = ArrayUtils.indexOf(srcColumnsInclude, srcCol.getName());
					if (idx != -1 && idx < dstColumnsInclude.length) {
						dstColumnName = dstColumnsInclude[idx];
					}
				}
				DbColumnDef dstCol = null; // dstColumns.get(dstColumnName);
				for (DbColumnDef colDef : dstColumns.values()) {
					if (colDef.getName().equalsIgnoreCase(dstColumnName)) {
						dstCol = colDef;
						break;
					}
				}
				if (dstCol == null) {
					srcCol.setName(dstColumnName);
					addColumns.add(srcCol);
				} else {
					if (dstCol.isReadOnly() || (dstPkIdentity != null && dstCol.getName().equals(dstPkIdentity))) {
						continue;
					}
					// compare type and check if alter is possible
					String dstTypeNameDetails = dstCol.getTypeNameDetails();
					String srcTypeNameDetails = srcCol.getTypeNameDetails();
					if (dstTypeNameDetails.equals(srcTypeNameDetails)) {
						// nothing to do
						continue;
					}
					if (srcCol.getTypeName().equals(dstCol.getTypeName())) {
						// same column type
						if (srcCol.getSize() > dstCol.getSize() || srcCol.getScale() > dstCol.getScale()) {
							// increase size or scale
							srcCol.setName(dstColumnName);
							alterColumns.add(srcCol);
						}
						continue;
					}
					switch (srcCol.getType()) {
						case Types.VARCHAR:
						case Types.LONGVARCHAR:
						case Types.CHAR:
						case Types.CLOB:
						case Types.NVARCHAR:
						case Types.LONGNVARCHAR:
						case Types.NCHAR:
						case Types.NCLOB: {
							// alter any to text
							srcCol.setName(dstColumnName);
							alterColumns.add(srcCol);
							continue;
						}
						//case Types.NUMERIC:
							
						case Types.BIGINT:
							switch (dstCol.getType()) {
								case Types.BIT:
								case Types.TINYINT:
								case Types.SMALLINT:
								case Types.INTEGER: {
									srcCol.setName(dstColumnName);
									alterColumns.add(srcCol);
									continue;
								}
							}
							break;
						case Types.INTEGER:
							switch (dstCol.getType()) {
								case Types.BIT:
								case Types.TINYINT:
								case Types.SMALLINT: {
									srcCol.setName(dstColumnName);
									alterColumns.add(srcCol);
									continue;
								}
							}
							break;
						case Types.SMALLINT:
							switch (dstCol.getType()) {
								case Types.BIT:
								case Types.TINYINT: {
									srcCol.setName(dstColumnName);
									alterColumns.add(srcCol);
									continue;
								}
							}
							break;
						case Types.TINYINT:
							switch (dstCol.getType()) {
								case Types.BIT: {
									srcCol.setName(dstColumnName);
									alterColumns.add(srcCol);
									continue;
								}
							}
							break;
					}
					log.warn("Cannot alter column " + getQuoted(is, dstTableName, dstCol.getName()) + " " + dstCol.getTypeNameDetails() + " to " + srcCol.getTypeNameDetails());
					continue;
				}
			}
			
			if (addColumns.size()> 0) {
				// add missing columns
				createColumns(con, sqlDialect, dstSchema, dstTableName, addColumns);
				modified = true;
			}
			if (alterColumns.size() > 0) {
				alterColumns(con, sqlDialect, dstSchema, dstTableName, alterColumns);
				modified = true;
			}
		}
		
		return modified;
	}
	
	public static void createColumns(Connection con, SqlDialect sqlDialect, String dstSchema, String dstTableName, List<DbColumnDef> addColumns) throws SQLException {
		String is = getIdentifierSeparator(con);
		StringBuilder sqlAlter = new StringBuilder();
		sqlAlter.append("ALTER TABLE ");
		sqlAlter.append(getQuoted(is, dstSchema, dstTableName));
		sqlAlter.append(" ADD ");
		
		for (DbColumnDef col : addColumns) {
			StringBuilder sql = new StringBuilder(sqlAlter);
			sql.append(getQuoted(is, col.getName())).append(" ").append(col.getTypeNameDetails());
			log.info("Creating missing column: \n" + sql.toString());
			Statement stmt = con.createStatement();
			try {
				stmt.execute(sql.toString());
			} finally {
				stmt.close();
			}
		}
	}

	public static void alterColumns(Connection con, SqlDialect sqlDialect, String dstSchema, String dstTableName, List<DbColumnDef> alterColumns) throws SQLException {
		Statement stmt = con.createStatement();
		try {
			String is = getIdentifierSeparator(con);
			StringBuilder sqlAlter = new StringBuilder();
			sqlAlter.append("ALTER TABLE ").append(getQuoted(is, dstSchema, dstTableName));
			if (sqlDialect == SqlDialect.ORACLE) {
				sqlAlter.append(" MODIFY ");
			} else {
				sqlAlter.append(" ALTER COLUMN ");
			}
			for (DbColumnDef col : alterColumns) {
				StringBuilder sql = new StringBuilder(sqlAlter);
				sql.append(getQuoted(is, col.getName())).append(" ").append(col.getTypeNameDetails());
				log.info("Alter column: " + sql.toString());
				stmt.execute(sql.toString());
			}
		} finally {
			stmt.close();
		}
	}
	
	/**
	 * 
	 * @param metaData
	 * @param schema
	 * @param tablename
	 * @param mapKeyUpperCase
	 * @return
	 * @throws SQLException
	 */
	public static Map<String, DbColumnDef> getColumns(DatabaseMetaData metaData, String schema, String tablename, boolean mapKeyUpperCase) throws SQLException {
		Connection connection = metaData.getConnection();
		String is = getIdentifierSeparator(connection);
		Statement stmt = connection.createStatement();
		try {
			ResultSet rs = stmt.executeQuery("select * from " + getQuoted(is, schema, tablename) + " where 0=1");
			try {
				Map<String, DbColumnDef> columns = new LinkedHashMap<String, DbColumnDef>();
				ResultSetMetaData rsmd = rs.getMetaData();
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					String name = rsmd.getColumnName(i);
					int type = rsmd.getColumnType(i);
					int size = rsmd.getPrecision(i);
					int scale = rsmd.getScale(i);
					boolean allowNull = rsmd.isNullable(i) != ResultSetMetaData.columnNoNulls;
					String typeName = rsmd.getColumnTypeName(i);
					//boolean autoIncrement = rsmd.isAutoIncrement(i);
					boolean readOnly = rsmd.isReadOnly(i);
					DbColumnDef colDef = new DbColumnDef(name, type, typeName, size, scale, allowNull);
					colDef.setReadOnly(readOnly);
					columns.put(mapKeyUpperCase ? name.toUpperCase() : name, colDef);
				}
				return columns;
			} finally {
				rs.close();
			}
		} finally {
			stmt.close();
		}
		
		/* disabled as replaced by dummy select to retrieve ResultSetMetaData
		ResultSet rs = metaData.getColumns(catalogname == null ? connection.getCatalog() : catalogname, null, tablename, null);
		try {
			//dumpResultSet(rs);
			Map<String, DbColumnDef> columns = new LinkedHashMap<String, DbColumnDef>();
			while (rs.next()) {
				String name = rs.getString("COLUMN_NAME");
				int type = rs.getInt("DATA_TYPE");
				int size = rs.getInt("COLUMN_SIZE");
				String allowNull = rs.getString("NULLABLE");
				String typeName = rs.getString("TYPE_NAME");
				String generateColumn = rs.getString("IS_GENERATEDCOLUMN");
				String autoIncrement = rs.getString("IS_AUTOINCREMENT");
				DbColumnDef colDef = new DbColumnDef(name, type, typeName, size, allowNull.equals("YES") || allowNull.equals("1") || allowNull.equals("TRUE"));
				colDef.setReadOnly(generateColumn.equals("YES") || generateColumn.equals("1") || autoIncrement.equals("YES") || autoIncrement.equals("1"));
				columns.put(name.toUpperCase(), colDef);
			}
			return columns;
		} finally {
			rs.close();
		}
		*/
		
	}
	
	/**
	 * @param con
	 * @param string
	 * @throws SQLException 
	 */
	public static int executeUpdate(Connection con, String sql) throws SQLException {
		
		Statement stmt = con.createStatement();
		try {
			int result = stmt.executeUpdate(sql);
			return result;
		} finally {
			stmt.close();
		}
		
	}

	/**
	 * 
	 * @param stmt
	 * @param sql
	 * @return
	 * @throws SQLException 
	 */
	public static int executeUpdate(Statement stmt, String sql) throws SQLException {
		
		int cnt = 0;
		String sqlQuery = null;
		try {
			for (String s : sql.split(Pattern.quote(EXECUTE))) {
				sqlQuery = s;
				if (sqlQuery.contains(IGNORE_ERROR)) {
					try {
						cnt += stmt.executeUpdate(sqlQuery);
					} catch (Throwable t) {
						log.warn("Cannot execute query: " + sqlQuery, t);
					}
				} else {
					cnt += stmt.executeUpdate(sqlQuery);
				}
			}
			return cnt;
		} catch (SQLException se) {
			throw new SQLException("Cannot execute query: " + sqlQuery, se);
		}
		
	}

	public static ResultSet executeQuery(Statement stmt, String sql) throws SQLException {
		try {
			return stmt.executeQuery(sql);
		} catch (SQLException se) {
			throw new SQLException("Cannot execute query: " + sql, se);
		}
	}
	
	/**
	 * Returns the JDBC identifier separator.
	 * On SQLException " is used as fallback.
	 * @param connection
	 * @return
	 */
	public static String getIdentifierSeparator(Connection connection) {
		String identifierSeparator = null;
		try {
			identifierSeparator = connection.getMetaData().getIdentifierQuoteString();
		} catch (SQLException e) {
			identifierSeparator = "\""; // use this
			log.warn("Error reading identifierQuoteString", e);
		}
		return identifierSeparator;
	}

	/**
	 * 
	 * @param connectionName
	 * @param schemaname
	 * @param tablename
	 * @param colDef
	 * @return
	 */
	public static String makeGlobalDbColumnDefKey(String connectionName, String schemaname, String tablename, DbColumnDef colDef) {
		String is = "\"";
		String key = getQuoted(is, connectionName, schemaname, tablename, colDef.getName());
		return key;
	}

	public static boolean updateColumn(IColumn column, int type, int precision, int scale) {
		boolean identified = true;
		IColumn.DataType dt = column.getTypeHint();
		switch (type) {
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.CLOB:
			dt = DataType.STRING;
			break;
		case Types.NUMERIC: // Oracle NUMBER is handled here
			if (scale > 0) {
				dt = DataType.DOUBLE;
			} else if (precision > 10) {
				dt = DataType.LONG;
			} else {
				dt = DataType.INT;
			}
			break;
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
			dt = DataType.INT;
			break;
		case Types.BIGINT:
            dt = DataType.LONG;
           break;
		case Types.FLOAT:
		case Types.DOUBLE:
		case Types.DECIMAL:
			dt = DataType.DOUBLE;
			break;
		case Types.TIMESTAMP:
			dt = DataType.DATETIME;
			break;
		case Types.DATE:
			dt = DataType.DATE;
			break;
		case Types.BIT:
			dt = DataType.BOOLEAN;
			break;
		case Types.BINARY:
		case Types.BLOB:
		case Types.JAVA_OBJECT:
		case Types.LONGVARBINARY:
			dt = DataType.BINARY;
			break;
		default:
			identified = false;
			break;
		}
		column.setLengthHint(precision);
		column.setTypeHint(dt);
		
		return identified;
	}
}
