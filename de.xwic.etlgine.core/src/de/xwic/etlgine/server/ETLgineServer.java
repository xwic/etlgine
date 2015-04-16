/**
 * 
 */
package de.xwic.etlgine.server;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;

import de.xwic.etlgine.DefaultMonitor;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IJob.State;
import de.xwic.etlgine.cube.CubeHandler;
import de.xwic.etlgine.demo.DemoDatabaseUtil;
import de.xwic.etlgine.notify.NotificationService;
import de.xwic.etlgine.publish.CubePublisherManager;

/**
 * The server supports parallel queue by adding in the server properties new keys prefixed by loadJobs.
 *
 * For instance to add two new queues named q1 and parallel:
 * loadJobs_q1=jobscript1.groovy
 * loadJobs_parallel=jobscript2.groovy
 * 
 * Limitations:
 *  Each job can be assigned to one single queue by its name. To add same job to multiple queues use multiple job files and change the job name. 
 *  The jobs running in different queues must use different shared connection names. Each queue must have its own unique shared connection name. 
 * 
 * 
 * @author Developer
 *
 */
public class ETLgineServer implements Runnable {

	private static ETLgineServer instance = null;

	private static final Log log = LogFactory.getLog(ETLgineServer.class);

	private static final long SLEEP_TIME = 5 * 1000; // 5 sec.

	private String rootPath = ".";

	private ServerContext serverContext = new ServerContext();

	private boolean initialized = false;
	private boolean initializing = false;
	private boolean doExit = false;
	private boolean running = false;
	private boolean exitAfterFinish = false;

	private long intializedTimeInMilis = 0;

	public static boolean FORCE_LOG4J_INITIALIZATION = false;

	/**
	 * 
	 */
	public ETLgineServer() {
		instance = this;
	}

	/**
	 * Returns the server instance.
	 * 
	 * @return
	 */
	public static ETLgineServer getInstance() {
		if (instance == null) {
			instance = new ETLgineServer();
		}
		return instance;
	}

	/**
	 * Exit the server.
	 */
	public void exitServer() {
		doExit = true;
	}

	/**
	 * Startup the server.
	 */
	public void run() {

		log.info("Server started.");

		running = true;

		while (!doExit) {

			if (exitAfterFinish) {
				boolean allEmpty = true;
				for (JobQueue queue : serverContext.getJobQueues()) {
					if (!queue.isEmpty() || queue.getActiveJob() != null) {
						allEmpty = false;
						break;
					}
				}
				if (allEmpty) {
					break; // exit the WHILE loop
				}
			}

			// check triggers
			checkTriggers();

			try {
				Thread.sleep(SLEEP_TIME);
			} catch (InterruptedException e) {
				// nothing unexpected.
			}
		}

		// exit all queues
		for (JobQueue queue : serverContext.getJobQueues()) {
			queue.stopQueue();
		}

		stopEmbededWebServer();

		running = false;

	}

	/**
	 * 
	 */
	private void checkTriggers() {

		if (serverContext.getPropertyBoolean("trigger.enabled", true)) {

			for (IJob job : serverContext.getJobs()) {

				// due to OutOfMemoryException the job state can be ENQUEUED
				// without actually sitting in the queue
				if (!job.isDisabled()
						&& !job.isExecuting()
						&& (job.getState() != IJob.State.ENQUEUED || !isJobEnqueued(job))) {
					if ((job.getState() != State.FINISHED_WITH_ERROR && job
							.getState() != State.ERROR)
							|| !job.isStopTriggerAfterError()) {
						if (job.getTrigger() != null
								&& job.getTrigger().isDue()) {
							enqueueJob(job);
						}
					}
				}
			}

		}

	}

	/**
	 * Checks if job is already enqueued.
	 * 
	 * @param job
	 * @return
	 */
	public boolean isJobEnqueued(IJob job) {
		return serverContext.getJobQueueForJob(job.getName()).isJobEnqueued(job);
	}

	/**
	 * Add the job to the default execution queue.
	 * 
	 * @param job
	 */
	public void enqueueJob(IJob job) {
		serverContext.getJobQueueForJob(job.getName()).addJob(job);
	}

	/**
	 * Initialize the server.
	 * 
	 * @throws ETLException
	 */
	public boolean initialize() {
		if (initialized) {
			System.out.println("Server is already initialized.");
			return true;
		}
		try {
			initializing = true;
			setIntializedTimeInMilis(System.currentTimeMillis());
			initialized = initializeServer();
			return initialized;
		} finally {
			initializing = false;
		}
	}

	/**
	 * Initialize the server.
	 * 
	 * @return
	 */
	private boolean initializeServer() {

		File path = new File(rootPath);
		if (!path.exists()) {
			System.out.println("Root path " + path.getAbsolutePath()
					+ " does not exist.");
			return false;
		}

		// set root path of etlgine in system enrionment
		System.setProperty("etlgine_rootPath", path.getAbsolutePath());

		// set root path in server context
		serverContext.setProperty(ServerContext.PROPERTY_ROOTPATH,
				path.getAbsolutePath());

		File pathConfig = new File(path, "config");
		if (!pathConfig.exists()) {
			System.out.println("Config path " + pathConfig.getAbsolutePath()
					+ " does not exist.");
			return false;
		}
		// log.info("Redirecting System.out");
		// System.setOut(new PrintStream(new ScreenOutputStream(screen)));

		// Initialize logging (only if it had not yet been initialized)
		if (FORCE_LOG4J_INITIALIZATION
				|| !Logger.getRootLogger().getAllAppenders().hasMoreElements()) {

			File configFile = new File(pathConfig, "log4j.xml");
			boolean isXml = configFile.exists();
			if (!isXml) {
				configFile = new File(pathConfig, "log4j.properties");
				if (!configFile.exists()) {
					System.out
							.println("Log4J config file log4j.xml or log4j.properties does not exist.");
					return false;
				}
			}

			System.out.println("Initializing Log4J");
			if (isXml) {
				DOMConfigurator.configureAndWatch(configFile.getAbsolutePath(),
						DefaultMonitor.STATUS_INTERVALL / 2);
			} else {
				PropertyConfigurator.configureAndWatch(
						configFile.getAbsolutePath(),
						DefaultMonitor.STATUS_INTERVALL / 2);
			}
		}

		File fileServerConf = new File(pathConfig, "server.properties");
		if (!fileServerConf.exists()) {
			log.error("server.properties not found.");
			return false;
		}

		Properties props = new Properties();
		try {
			props.load(new FileInputStream(fileServerConf));
		} catch (IOException e) {
			log.error("log.error reading server.properties", e);
			return false;
		}

		// search for an "override" file
		File fileServerConfOVR = new File(pathConfig,
				"server.override.properties");
		if (fileServerConfOVR.exists()) {
			Properties props2 = new Properties();
			try {
				props2.load(new FileInputStream(fileServerConfOVR));
			} catch (IOException e) {
				log.error("log.error reading server.override.properties", e);
				return false;
			}
			// merge properties
			for (Enumeration<Object> e = props2.keys(); e.hasMoreElements();) {
				String key = (String) e.nextElement();
				String value = props2.getProperty(key);
				props.setProperty(key, value);
			}
		}

		// copy properties to server context
		for (Object key : props.keySet()) {
			String sKey = (String) key;
			if (serverContext.getProperty(sKey) == null) {
				// set only new properties
				serverContext.setProperty(sKey, props.getProperty(sKey));
			}
		}

		// check if we need to initialize the demo database
		if (serverContext.getPropertyBoolean(
				ServerContext.PROPERTY_SQLITE_DATABASE_INIT, false)
				&& !StringUtils.isEmpty(serverContext.getProperty(
						ServerContext.PROPERTY_SQLITE_DATABSE_CONNECTION, ""))) {
			String conUrl = serverContext.getProperty(
					serverContext.getProperty(
							ServerContext.PROPERTY_SQLITE_DATABSE_CONNECTION,
							"")
							+ ".connection.url", "NOT_FOUND");
			String conDriver = serverContext.getProperty(
					serverContext.getProperty(
							ServerContext.PROPERTY_SQLITE_DATABSE_CONNECTION,
							"")
							+ ".connection.driver", "NOT_FOUND");

			if (!StringUtils.isEmpty(conUrl) && !StringUtils.isEmpty(conDriver)
					&& conUrl.contains(":sqlite:")
					&& conDriver.contains(".sqlite.")) {
				DemoDatabaseUtil.prepareDB(conDriver, conUrl);
			}
		}

		// invoke server initializing listener
		String serverInitializingListener = serverContext
				.getProperty(ServerContext.PROPERTY_INITIALIZING_LISTENER);
		if (serverInitializingListener != null) {
			for (String classname : serverInitializingListener.split(";, ")) {
				IServerInitializingListener listener;
				try {
					listener = (IServerInitializingListener) Class.forName(
							classname).newInstance();
					listener.initializingServer(this);
				} catch (Throwable t) {
					log.error("IServerInitializingListener error for "
							+ classname, t);
				}
			}
		}

		// load jobs
		File jobPath = new File(path, serverContext.getProperty(
				ServerContext.PROPERTY_SCRIPTPATH, "jobs"));
		if (!jobPath.exists()) {
			log.error("The job directory does not exist: "
					+ jobPath.getAbsolutePath());
			return false;
		}
		serverContext.setProperty(ServerContext.PROPERTY_SCRIPTPATH,
				jobPath.getAbsolutePath());
		
		Set<String> queueNames  = new LinkedHashSet<String>();
		queueNames.add(ServerContext.DEFAULT_QUEUE);
		// build a list of queue names
		for (String key : serverContext.getPropertyKeys()) {
			if (key.startsWith("loadJobs_") && !"".equals(serverContext.getProperty(key,"").trim())) {
				queueNames.add(key.substring(9));
			}
		}
		StringTokenizer stk = null;
		String queueProp = "loadJobs";
		for(String qName: queueNames){
			
			if (!ServerContext.DEFAULT_QUEUE.equals(qName)){
				queueProp = "loadJobs_"+qName;
			}
			
			stk = new StringTokenizer(serverContext.getProperty(
					queueProp, ""), ",; ");
			if (stk.hasMoreTokens() && !ServerContext.DEFAULT_QUEUE.equals(qName)){
				try {
					serverContext.addJobQueue(qName, new JobQueue(serverContext, qName));
				} catch (ETLException exc) {
					log.error("Error initializing the etl server ", exc);
					return false;
				}
			}
			while (stk.hasMoreTokens()) {
				String scriptName = stk.nextToken();
				IJob job = loadJob(scriptName, qName);
			}
		}

		// load DataPool(s)
		CubeHandler cubeHandler = CubeHandler.getCubeHandler(serverContext);
		log.info("Loaded " + cubeHandler.getDataPoolManagerKeys().size()
				+ " DataPool(s).");

		for (Iterator iterator = cubeHandler.getDataPoolManagerKeys()
				.iterator(); iterator.hasNext();) {
			String datapoolKey = (String) iterator.next();

			// check publisher settings and set them
			CubePublisherManager.getInstance().fillPublishTargets(
					serverContext, datapoolKey);
		}

		// check if we need to start the webserver
		if (serverContext.getPropertyBoolean(
				ServerContext.PROPERTY_WEBSERVER_START, false)) {
			boolean serverStarted = startEmbededWebServer(path, pathConfig);

			if (!serverStarted) {
				return serverStarted;
			}
		}

		log.info("Notification Services enabled");
		NotificationService nfService = new NotificationService(serverContext);
		serverContext.setData(NotificationService.class.getName(), nfService);
		serverContext.addServerContextListener(nfService);

		// execute run scripts, if specified
		stk = new StringTokenizer(serverContext.getProperty("run", ""), ",; ");
		while (stk.hasMoreTokens()) {
			String scriptName = stk.nextToken();
			try {
				executeStartScript(scriptName);
			} catch (Exception e) {
				log.error("Error running startscript", e);
				return false;
			}
		}

		serverContext.setProperty(ServerContext.PROPERTY_SERVER_VERSION,
				getDeployedVersion());
		
		// System.setOut(oldPS);
		return true;

	}

	private String getDeployedVersion() {
		
        log.info( "  Implementation Title:" + this.getClass().getPackage().getImplementationTitle() );
        log.info( " Implementation Vendor:" + this.getClass().getPackage().getImplementationVendor() );
        log.info( "Implementation Version:" + this.getClass().getPackage().getImplementationVersion() );
        log.info( "    Specification Tile:" + this.getClass().getPackage().getSpecificationTitle() );
        log.info( "  Specification Vendor:" + this.getClass().getPackage().getSpecificationVendor() );
        log.info( " Specification Version:" + this.getClass().getPackage().getSpecificationVersion() );

		String implementationVersion = this.getClass().getPackage().getSpecificationVersion();
		log.info("Got data - " + implementationVersion);
		if (!StringUtils.isEmpty(implementationVersion)) {
			log.info("Not EMPTY!");
			if(implementationVersion.contains("SNAPSHOT")) {
				log.info("SNAPSHOT!");
				implementationVersion = implementationVersion +  "(#" + this.getClass().getPackage().getImplementationVersion()+")";
			} else {
				log.info("NO SNAPSHOT!");
				implementationVersion =  implementationVersion + "." + this.getClass().getPackage().getImplementationVersion();
			}
		} else {
			log.info("EMPTY!");
			implementationVersion = "";
		}
		
		return implementationVersion;
	}

	/**
	 * @param scriptName
	 * @throws Exception
	 */
	private void executeStartScript(String scriptName) throws Exception {

		log.info("Executing Startscript " + scriptName);

		Binding binding = new Binding();
		binding.setVariable("context", serverContext);

		GroovyShell shell = new GroovyShell(binding);

		File file = new File(new File(rootPath), scriptName);
		if (!file.exists()) {
			throw new ETLException("The script file " + file.getAbsolutePath()
					+ " does not exist.");
		}

		try {
			shell.evaluate(file);
		} catch (Exception e) {
			throw new ETLException("Error evaluating script '" + file.getName()
					+ "':" + e, e);
		}

	}

	/**
	 * @return the rootPath
	 */
	public String getRootPath() {
		return rootPath;
	}

	/**
	 * @param rootPath
	 *            the rootPath to set
	 */
	public void setRootPath(String rootPath) {
		this.rootPath = rootPath;
	}

	/**
	 * @return the serverContext
	 */
	public ServerContext getServerContext() {
		return serverContext;
	}

	/**
	 * @return the exitAfterFinish
	 */
	public boolean isExitAfterFinish() {
		return exitAfterFinish;
	}

	/**
	 * @param exitAfterFinish
	 *            the exitAfterFinish to set
	 */
	public void setExitAfterFinish(boolean exitAfterFinish) {
		this.exitAfterFinish = exitAfterFinish;
	}

	/**
	 * @return the initialized
	 */
	public boolean isInitialized() {
		return initialized;
	}

	/**
	 * @return the initializing
	 */
	public boolean isInitializing() {
		return initializing;
	}

	public boolean isRunning() {
		return running;
	}

	/**
	 * Loads a job from groovy script and returns it.
	 * 
	 * @param scriptName
	 * @param queueName 
	 * @return
	 */
	public IJob loadJob(String scriptName, String queueName) {
		String jobName = scriptName;
		if (jobName.toLowerCase().endsWith(".groovy")) {
			jobName = jobName.substring(0,
					jobName.length() - ".groovy".length());
		}
		try {
			log.info("Loading Job " + jobName + " from file " + scriptName + " to queue "+queueName);
			return serverContext.loadJob(jobName, scriptName, queueName);
		} catch (Throwable e) {
			log.error("An error occured during loading of the job "
					+ scriptName, e);
		}
		return null;
	}

	protected boolean startEmbededWebServer(File path, File pathConfig) {
		return false;
	}

	protected void stopEmbededWebServer() {

	}

	public long getIntializedTimeInMilis() {
		return intializedTimeInMilis;
	}

	public void setIntializedTimeInMilis(long intializedTimeInMilis) {
		this.intializedTimeInMilis = intializedTimeInMilis;
	}
}
