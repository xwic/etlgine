package de.xwic.etlgine.processes;

import java.util.ArrayList;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IProcessChain;
import de.xwic.etlgine.Result;
import de.xwic.etlgine.extractor.jdbc.JDBCExtractor;
import de.xwic.etlgine.extractor.jdbc.JDBCSource;
import de.xwic.etlgine.finalizer.ExecuteSqlFinalizer;
import de.xwic.etlgine.impl.ETLProcess;

public class CleanUpLogMonitorProcess extends ETLProcess {

	private static int DEFAULT_KEEP_DAYS_SUCCESS = 14;
	private static int DEFAULT_KEEP_DAYS_FAILED = 30;
	private static boolean DEFAULT_ONLY_FLAG_FOR_DELETE = false;
	
	protected String connectionId = null;
	private final String etlGineId;
	private final int noOfDaysSuccess;
	private final int noOfDaysFailed;
	private final boolean lOnlyFlagForDelete;

	public CleanUpLogMonitorProcess(IProcessChain processChain, IContext context, String name, String connectionId, String etlGineId, int noOfDaysSuccess,
			int noOfDaysFailed, boolean lOnlyFlagForDelete) {
		super(processChain, context, name);
		this.connectionId = connectionId;
		this.etlGineId = etlGineId;
		this.noOfDaysSuccess = noOfDaysSuccess;
		this.noOfDaysFailed = noOfDaysFailed;
		this.lOnlyFlagForDelete = lOnlyFlagForDelete;
	}

	public CleanUpLogMonitorProcess(IProcessChain processChain, IContext context, String name, String connectionId) {
		this(processChain, context, name, connectionId, "", DEFAULT_KEEP_DAYS_SUCCESS, DEFAULT_KEEP_DAYS_FAILED, DEFAULT_ONLY_FLAG_FOR_DELETE);
	}

	@Override
	public Result start() throws ETLException {
		try {

			JDBCSource source = new JDBCSource();
			source.setConnectionName(this.connectionId);
			
			this.addSource(source);

			this.setExtractor(new JDBCExtractor());

			ArrayList<String> sqlStatements = new ArrayList<String>();

			StringBuilder sb = new StringBuilder();
			sb.append("UPDATE [dbo].[JDBC_MONITOR] SET ToDelete = 1 WHERE ID IN ");
			sb.append("(SELECT Id FROM ");
			sb.append("(SELECT MAX(jdbc2.created ) as dcreated, jdbc1.created as dend, jdbc1.etlGineId, jdbc1.Hostname, jdbc1.Job ");
			sb.append("FROM [dbo].[JDBC_MONITOR] jdbc1, [dbo].[JDBC_MONITOR] jdbc2 ");
			sb.append("WHERE jdbc1.ToDelete IS NULL AND jdbc2.ToDelete IS NULL AND ISNULL(jdbc1.Event, '') = 'JOB_EXECUTION_END' ");
			sb.append("AND ((jdbc1.State = 'FINISHED' AND DATEADD(d, -" + this.noOfDaysSuccess
					+ ", GETDATE()) > CONVERT(date, jdbc1.Created )) ");
			sb.append("OR (jdbc1.State != 'FINISHED' AND DATEADD(d, -" + this.noOfDaysFailed
					+ ", GETDATE()) > CONVERT(date, jdbc1.Created ))) ");
			sb.append("AND ISNULL(jdbc2.Event, '') = 'JOB_EXECUTION_START' ");
			if (!etlGineId.isEmpty()) {
				sb.append("AND jdbc1.etlGineId = '" + this.etlGineId + "' ");
			}
			sb.append("AND jdbc1.etlGineId = jdbc2.etlGineId AND jdbc1.Hostname = jdbc2.Hostname AND jdbc1.Job = jdbc2.Job AND jdbc2.Created <= jdbc1.Created ");
			sb.append("GROUP BY jdbc1.etlGineId, jdbc1.Hostname, jdbc1.Job, jdbc1.Created ) ids ");
			sb.append(",[dbo].[JDBC_MONITOR] jdbc3 ");
			sb.append("WHERE jdbc3.ToDelete IS NULL AND jdbc3.created between ids.dcreated AND ids.dend ");
			sb.append("AND ids.etlGineId = jdbc3.etlGineId AND ids.Hostname = jdbc3.Hostname ");
			sb.append("AND ids.Job = jdbc3.job AND ISNULL([Event], '') NOT LIKE '%_START%' AND ISNULL([Event], '') NOT LIKE '%_FINISHED%' AND ISNULL([Event], '') != 'JOB_EXECUTION_END'");
			sb.append("UNION ALL ");
			sb.append("SELECT ID FROM [dbo].[JDBC_MONITOR] jdbc WHERE ISNULL([Event], '') = 'JOB_LOAD_FROM_SCRIPT' ");
			if (!etlGineId.isEmpty()) {
				sb.append("AND jdbc.etlGineId = '" + this.etlGineId + "' ");
			}
			sb.append(")");
			sqlStatements.add(sb.toString());
			
			if (!lOnlyFlagForDelete) {
				sb.setLength(0);
				sb.append("DELETE FROM [dbo].[JDBC_MONITOR] WHERE ToDelete = 1");
				if (!etlGineId.isEmpty()) {
					sb.append("AND etlGineId = '" + this.etlGineId + "' ");
				}
				
				sqlStatements.add(sb.toString());
			}

			processContext.getMonitor().logInfo("Log CleanUp SQL: " + sqlStatements);
			ExecuteSqlFinalizer executeSqlFinalizer = new ExecuteSqlFinalizer(connectionId, sqlStatements, true);
			this.addProcessFinalizer(executeSqlFinalizer);

			super.start();

			processContext.getMonitor().logInfo("DB COMMIT done successfully!");
		} catch (Exception se) {
			throw new ETLException("Unhandled SQLException in CleanUpLogMonitorProcess : " + se, se);
		}

		return result;

	}
}
