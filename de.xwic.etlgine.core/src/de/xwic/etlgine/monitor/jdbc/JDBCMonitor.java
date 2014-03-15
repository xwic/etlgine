/**
 * 
 */
package de.xwic.etlgine.monitor.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Date;
import java.util.Stack;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.DefaultMonitor;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IColumn.DataType;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IMonitor;
import de.xwic.etlgine.IProcess;
import de.xwic.etlgine.IProcessChain;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.impl.DataSet;
import de.xwic.etlgine.impl.ETLProcess;
import de.xwic.etlgine.impl.Process;
import de.xwic.etlgine.impl.ProcessContext;
import de.xwic.etlgine.loader.jdbc.JDBCLoader;
import de.xwic.etlgine.server.ETLgineServer;
import de.xwic.etlgine.server.ServerContext;

/**
 * @author JBORNEMA
 *
 */
public class JDBCMonitor extends DefaultMonitor {

	public final static String PROPERTY_MONITOR_CONNECTION = "monitor.connection";
	public final static String PROPERTY_MONITOR_TABLE = "monitor.table";

	protected Log log = LogFactory.getLog(JDBCMonitor.class);

	protected Boolean shareThreadLocal = null;
	protected static ThreadLocal<ThreadLocalData> threadLocal = new ThreadLocal<ThreadLocalData>();
		
	class ThreadLocalData {
		ProcessContext processContext;
		JDBCLoader loader;
		long lastAccessed = 0;
		long period = 3000;
		Timer timer = null;
		
		public void clear() {
			threadLocal.remove();
			processContext = null;
			loader = null;
			lastAccessed = 0;
			if (timer != null) {
				timer.cancel();
				timer = null;
			}
		}
		
		
		public void setLoader(JDBCLoader loader) {
			lastAccessed = System.currentTimeMillis();
			this.loader = loader;
		}
		
		public JDBCLoader getLoader() {
			if (loader != null) {
				lastAccessed = System.currentTimeMillis();
			}
			return loader;
		}
		
		public void scheduleCloseTable() {
			if (timer != null) {
				return;
			}
			timer = new Timer(Thread.currentThread().getName() + ": JDBCMonitor Timer");
			timer.schedule(new TimerTask() {
				@Override
				public void run() {
					if (System.currentTimeMillis() - lastAccessed > period) {
						// close
						try {
							// close table
							closeTable(processContext, loader, ThreadLocalData.this);
							// need to call clear, as closeTable would not retrieve this due to different thread
							clear();
						} catch (Throwable t) {
							_logError(null, t);
						}
					}
				}
			}, period, period);
		}
	}
	
	protected String connectionName;
	protected String tablename;
	
	protected JDBCLoader loader;
	protected ProcessContext processContext;
	
	protected IColumn colLevel;
	protected IColumn colEvent;
	protected IColumn colMessage;
	protected IColumn colException;
	protected IColumn colCreated;
	protected IColumn colStart;
	protected IColumn colFinished;
	protected IColumn colDuration;
	protected IColumn colJob;
	protected IColumn colState;
	protected IColumn colProcess;
	protected IColumn colResult;
	protected IColumn colCreatorInfo;
	protected IColumn colHostname;
	protected IColumn colETLgineName;
    protected IColumn colETLgineId;

	protected EventType eventType;
	protected Object eventSource;
	protected String eventKey;
	protected IJob currentJob;
	protected IProcessChain currentProcessChain;
	protected IProcess currentProcess;
	protected String currentCreatorInfo;
	
	boolean open = false;
	int logging = 0;
	
	protected Stack<Date> startTimes = new Stack<Date>();
	
	/**
	 * Initialize / reset local fields.
	 */
	protected void initialize() {
		startTimes.clear();
		loader = null;
		eventSource = null;
		eventType = null;
		currentJob = null;
		currentProcessChain = null;
		currentProcess = null;
		currentCreatorInfo = null;
		open = false;
		logging = 0;
	}
	
	@Override
	public void reset() {
		super.reset();
		initialize();
	}
	
	@Override
	public void initialize(IContext context) throws ETLException {

		if (connectionName == null) {
			connectionName = context.getProperty(PROPERTY_MONITOR_CONNECTION);
			if (connectionName == null) {
				throw new ETLException("Missing context paramenter '" + PROPERTY_MONITOR_CONNECTION + "' for JDBC connection configuration");
			}
		}
		
		if (tablename == null) {
			tablename = context.getProperty(PROPERTY_MONITOR_TABLE);
			if (tablename == null) {
				throw new ETLException("Missing context paramenter '" + PROPERTY_MONITOR_TABLE + "' as logging table");
			}
		}
	
		Process process = new ETLProcess(context, "JDBCMonitor process");
		process.setMonitor(IMonitor.Empty);
		processContext = (ProcessContext)process.getContext();
		
		IDataSet ds = new DataSet();
		processContext.setDataSet(ds);
		
		colLevel = ds.addColumn("Level");
		colLevel.setTypeHint(DataType.STRING);
		colLevel.setLengthHint(16);
		colEvent = ds.addColumn("Event");
		colEvent.setLengthHint(64);
		colEvent.setTypeHint(DataType.STRING);
		colMessage = ds.addColumn("Message");
		colMessage.setTypeHint(DataType.STRING);
		colMessage.setLengthHint(1024);
		colException = ds.addColumn("Exception");
		colException.setTypeHint(DataType.STRING);
		colException.setLengthHint(4000);
		colCreated = ds.addColumn("Created");
		colCreated.setTypeHint(DataType.DATETIME);
		colStart = ds.addColumn("Start");
		colStart.setTypeHint(DataType.DATETIME);
		colFinished = ds.addColumn("Finished");
		colFinished.setTypeHint(DataType.DATETIME);
		colDuration = ds.addColumn("Duration");
		colDuration.setTypeHint(DataType.INT);
		colJob = ds.addColumn("Job");
		colJob.setTypeHint(DataType.STRING);
		colJob.setLengthHint(64);
		colState = ds.addColumn("State");
		colState.setTypeHint(DataType.STRING);
		colState.setLengthHint(32);
		colProcess = ds.addColumn("Process");
		colProcess.setTypeHint(DataType.STRING);
		colProcess.setLengthHint(64);
		colResult = ds.addColumn("Result");
		colResult.setTypeHint(DataType.STRING);
		colResult.setLengthHint(32);
		colCreatorInfo = ds.addColumn("CreatorInfo");
		colCreatorInfo.setTypeHint(DataType.STRING);
		colCreatorInfo.setLengthHint(128);
		colHostname = ds.addColumn("Hostname");
		colHostname.setTypeHint(DataType.STRING);
		colHostname.setLengthHint(128);
		colETLgineName = ds.addColumn("ETLgineName");
		colETLgineName.setTypeHint(DataType.STRING);
		colETLgineName.setLengthHint(128);
        colETLgineId = ds.addColumn("ETLgineId");
        colETLgineId.setTypeHint(DataType.STRING);
        colETLgineId.setLengthHint(128);
	}
	
	protected Object lock(Object lockObject) {
		if (lockObject != null) {
			return lockObject;
		}
		if (!useThreadLocal()) {
			return this;
		}
		ThreadLocalData data = threadLocal.get();
		if (data != null) {
			return data;
		}
		return this;
	}
	
	/**
	 * @throws ETLException 
	 */
	protected void openTable(Object lockObject) throws ETLException {
		if (open) {
			return;
		}
		synchronized (lock(lockObject)) {
			try {
				JDBCLoader threadLoader = accessThreadLocalLoader();
				if (threadLoader != null) {
					loader = threadLoader;
					return;
				}
				loader = new JDBCLoader();
				loader.setConnectionName(connectionName);
				loader.setTablename(tablename);
				loader.setAutoCreateTable(true);
				loader.setAutoAlterColumns(true);
				loader.setAutoDetectColumnTypes(false);
				loader.setAutoCreateColumns(true);
				loader.setTreatEmptyAsNull(true);
				
				// disable batch
				loader.setBatchSize(0);
				// log that database will be opened
				//_logInfo("Open named connection for logging: " + connectionName);
				loader.initialize(processContext);
				loader.preSourceOpening(processContext);
				loader.preSourceProcessing(processContext);
				
				setThreadLocalData(processContext, loader);
			} finally {
				open = true;
			}
		}
	}

	/**
	 * 
	 * @param processContext
	 * @param loader
	 * @throws ETLException 
	 */
	protected void closeTable(ProcessContext processContext, JDBCLoader loader, Object lockObject) throws ETLException {
		synchronized (lock(lockObject)) {
			loader.postSourceProcessing(processContext);
			loader.onProcessFinished(processContext);
			if (useThreadLocal()) {
				ThreadLocalData data = threadLocal.get();
				if (data != null) {
					data.clear();
				}
			}
		}
	}

	/**
	 * @throws ETLException 
	 */
	protected void closeTable(Object lockObject) throws ETLException {
		if (!open) {
			return;
		}
		try {
			if (loader != null) {
				if (!releaseThreadLocalLoader()) {
					closeTable(processContext, loader, lockObject);
				}
			}
		} finally {
			initialize();
		}
	}
	
	protected void setThreadLocalData(ProcessContext processContext, JDBCLoader loader) {
		if (!useThreadLocal()) {
			return;
		}
		ThreadLocalData data = threadLocal.get();
		if (data == null) {
			data = new ThreadLocalData();
		}
		data.processContext = processContext;
		data.setLoader(loader);
		threadLocal.set(data);	
	}

	protected JDBCLoader accessThreadLocalLoader() {
		if (!useThreadLocal()) {
			return null;
		}
		ThreadLocalData data = threadLocal.get();
		if (data == null) {
			return null;
		}
		return data.getLoader();
	}

	protected boolean releaseThreadLocalLoader() {
		if (!useThreadLocal()) {
			return false;
		}
		ThreadLocalData data = threadLocal.get();
		if (data == null) {
			return false;
		}
		data.getLoader();
		data.scheduleCloseTable();
		return true; 
	}

	@Override
	public void onEvent(IContext context, EventType eventType, Object eventSource) {
		boolean change = this.eventSource != eventSource;
		if (change) {
			// remember primary event source
			if (eventSource instanceof IProcess) {
				currentProcess = (IProcess)eventSource;
				currentCreatorInfo = currentProcess.getCreatorInfo();
			} else if (eventSource instanceof IProcessChain) {
				currentProcessChain = (IProcessChain)eventSource;
				currentCreatorInfo = currentProcessChain.getCreatorInfo();
				currentProcess = null;
			} else if (eventSource instanceof IJob) {
				currentJob = (IJob)eventSource;
				currentCreatorInfo = currentJob.getCreatorInfo();
				currentProcess = null;
			}
			this.eventSource = eventSource;
		}
		change |= this.eventType != eventType;
		// remember event type
		this.eventType = eventType;
		super.onEvent(context, eventType, eventSource);
	}
	
	protected void _logInfo(String message) {
		log.info(message);
		super.logToBuffer("INFO", message, null);
	}
	
	protected void _logError(String message, Throwable t) {
		log.error(message, t);
		super.logToBuffer("ERROR", message, t);
	}
	
	@Override
	protected void logToBuffer(String prefix, String message, Throwable e) {
		super.logToBuffer(prefix, message, e);
		logToTable(prefix, message, e);
	}
	
	/**
	 * 
	 * @param prefix
	 * @param message
	 * @param e
	 */
	public synchronized void logToTable(String prefix, String message, Throwable e) {
		logging++;
		try {
			
			Date created = new Date();
			
			boolean close = false;
			boolean forceClose = false;
			
			String exception = null;
			
			Date start = null;
			Date finished = null;
			Integer duration = null;
			if (eventType != null) {
				switch (eventType) {
					case JOB_EXECUTION_START:
						if (shareThreadLocal == null) {
							// by default disable lazy closing
							shareThreadLocal = false;
						}
					case PROCESSCHAIN_START:
					case PROCESS_START:
					case SOURCE_POST_OPEN: {
						// set start time
						startTimes.push(created);
						break;
					}
					case JOB_LOAD_FROM_SCRIPT:
						close = true;
						if (shareThreadLocal == null) {
							// enable lazy closing for regular ETL job loading
							shareThreadLocal = true;
						}
						break;
					case JOB_EXECUTION_END:
						forceClose = true;
					case PROCESSCHAIN_FINISHED:
					case PROCESS_FINISHED:
					case SOURCE_FINISHED: {
						finished = new Date();
						break;
					}
				}			
				if (finished != null) {
					if (startTimes.size() > 0) {
						start = startTimes.pop();
						duration = (int)(finished.getTime() - start.getTime());
					}
				}
			}
			
			if (e == null && currentProcess != null && currentProcess.getContext() != null && currentProcess.getContext().getLastException() != null) {
				e = currentProcess.getContext().getLastException();
			}
			
			if (e == null && currentJob != null && currentJob.getLastException() != null) {
				e = currentJob.getLastException();
			}
			
			if (e != null) {
				ByteArrayOutputStream bao = new ByteArrayOutputStream();
				PrintWriter pw = new PrintWriter(bao);
				e.printStackTrace(pw);
				pw.close();
				exception = bao.toString();
			}

			// ensure table is open
			openTable(null);
			
			IRecord record = processContext.newRecord(); 
			record.setData(colLevel, prefix);
			record.setData(colEvent, eventType != null ? eventType.name() : null);
			record.setData(colMessage, message);
			record.setData(colException, exception);
			record.setData(colCreated, created);
			record.setData(colStart, start);
			record.setData(colFinished, finished);
			record.setData(colDuration, duration);
			record.setData(colJob, currentJob != null ? currentJob.getName() : null);
			record.setData(colState, currentJob != null && currentJob.getState() != null ? currentJob.getState().name() : null);
			record.setData(colProcess, currentProcess != null ? currentProcess.getName() : null);
			if (eventType != null) {
				switch (eventType) {
					case PROCESS_FINISHED: {
						record.setData(colResult, currentProcess != null && currentProcess.getResult() != null ? currentProcess.getResult().name() : null);
						break;
					}
					case PROCESSCHAIN_FINISHED: {
						record.setData(colResult, currentProcessChain != null && currentProcessChain.getResult() != null ? currentProcessChain.getResult().name() : null);
						break;
					}
				}
			}
			record.setData(colCreatorInfo, currentCreatorInfo);
			record.setData(colHostname, InetAddress.getLocalHost().getCanonicalHostName());
			record.setData(colETLgineName, ETLgineServer.getInstance().getServerContext().getProperty(ServerContext.PROPERTY_SERVER_INSTANCENAME, ServerContext.PROPERTY_SERVER_INSTANCENAME_DEFAULT));
            record.setData(colETLgineId, ETLgineServer.getInstance().getServerContext().getProperty(ServerContext.PROPERTY_SERVER_INSTANCEID, ServerContext.PROPERTY_SERVER_INSTANCEID_DEFAULT));

			loader.processRecord(processContext, record);
			
			if (duration != null || e != null) {
				loader.executeBatch();
			}
			
			if (forceClose) {
				// force close table
				closeTable(processContext, loader, null);
			} else if (close) {
				// close table
				closeTable(null);
			}
			
		} catch (Throwable t) {
			
			_logError("Error logging to table", t);
			
		} finally {			
			logging--;
			// clear event type
			eventType = null;
		}
	}

	protected boolean useThreadLocal() {
		if (shareThreadLocal != null) {
			return shareThreadLocal;
		}
		return false;
	}
	
	/**
	 * @return the shareThreadLocal
	 */
	public Boolean isShareThreadLocal() {
		return shareThreadLocal;
	}

	/**
	 * @param shareThreadLocal the shareThreadLocal to set
	 */
	public void setShareThreadLocal(Boolean shareThreadLocal) {
		this.shareThreadLocal = shareThreadLocal;
	}

	/**
	 * @return the connectionName
	 */
	public String getConnectionName() {
		return connectionName;
	}

	/**
	 * @param connectionName the connectionName to set
	 */
	public void setConnectionName(String connectionName) {
		this.connectionName = connectionName;
	}

	/**
	 * @return the tablename
	 */
	public String getTablename() {
		return tablename;
	}

	/**
	 * @param tablename the tablename to set
	 */
	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	
}
