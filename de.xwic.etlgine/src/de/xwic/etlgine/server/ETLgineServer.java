/**
 * 
 */
package de.xwic.etlgine.server;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.xml.XmlConfiguration;

import de.xwic.etlgine.DefaultMonitor;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IJob.State;
import de.xwic.etlgine.cube.CubeHandler;

/**
 * @author Developer
 *
 */
public class ETLgineServer implements Runnable {
	
	private static ETLgineServer instance = null;
	
	private static final Log log = LogFactory.getLog(ETLgineServer.class);
	
	private static final long SLEEP_TIME = 5 * 1000; // 5 sec.
	
	private String rootPath = ".";
	private Server jetty;
	
	private ServerContext serverContext = new ServerContext();
	
	private boolean initialized = false;
	private boolean initializing = false;
	private boolean doExit = false;
	private boolean running = false;
	private boolean exitAfterFinish = false;

	/**
	 * 
	 */
	public ETLgineServer() {
		instance = this;
	}
	
	/**
	 * Returns the server instance.
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
		
		try {
			if (jetty != null) {
				jetty.stop();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		running = false;
		
	}
	
	/**
	 * 
	 */
	private void checkTriggers() {

		for (IJob job : serverContext.getJobs()) {
			if (!job.isDisabled() && !job.isExecuting() && !(job.getState() == IJob.State.ENQUEUED)) {
				if (job.getState() != State.FINISHED_WITH_ERROR && job.getState() != State.ERROR) {
					if (job.getTrigger() != null && job.getTrigger().isDue()) {
						enqueueJob(job);
					}
				}
			}
		}
		
	}

	/**
	 * Add the job to the default execution queue.
	 * @param job
	 */
	public void enqueueJob(IJob job) {
		serverContext.getDefaultJobQueue().addJob(job);
	}

	/**
	 * Initialize the server.
	 * @throws ETLException 
	 */
	public boolean initialize() {
		if (initialized) {
			System.out.println("Server is already initialized.");
			return true;
		}
		try {
			initializing = true;
			initialized = initializeServer();
			return initialized;
		} finally {
			initializing = false;
		}
	}
	
	/**
	 * Initialize the server.
	 * @return
	 */
	private boolean initializeServer() {
		
		File path = new File(rootPath);
		if (!path.exists()) {
			System.out.println("Root path " + path.getAbsolutePath() + " does not exist.");
			return false;
		}
		
		serverContext.setProperty(ServerContext.PROPERTY_ROOTPATH, path.getAbsolutePath());
		
		File pathConfig = new File(path, "config");
		if (!pathConfig.exists()) {
			System.out.println("Config path " + pathConfig.getAbsolutePath() + " does not exist.");
			return false;
		}
		//log.info("Redirecting System.out");
		//System.setOut(new PrintStream(new ScreenOutputStream(screen)));

		// Initialize logging (only if it had not yet been initialized)
		if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {

			File configFile = new File(pathConfig, "log4j.xml");
			boolean isXml = configFile.exists(); 
			if (!isXml) {
				configFile = new File(pathConfig, "log4j.properties");
				if (!configFile.exists()) {
					System.out.println("Log4J config file log4j.xml or log4j.properties does not exist.");
					return false;
				}
			}

			System.out.println("Initializing Log4J");
			if (isXml) {
				DOMConfigurator.configureAndWatch(configFile.getAbsolutePath(), DefaultMonitor.STATUS_INTERVALL / 2);
			} else {
				PropertyConfigurator.configureAndWatch(configFile.getAbsolutePath(), DefaultMonitor.STATUS_INTERVALL / 2);
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
			log.error("log.error reading server.properties: " + e);
			return false;
		}

		
		// copy properties to server context
		for (Object key : props.keySet()) {
			String sKey = (String)key;
			if (serverContext.getProperty(sKey) == null) {
				// set only new properties
				serverContext.setProperty(sKey, props.getProperty(sKey));
			}
		}

		// load jobs
		File jobPath = new File(path, serverContext.getProperty(ServerContext.PROPERTY_SCRIPTPATH, "jobs"));
		if (!jobPath.exists()) {
			log.error("The job directory does not exist: " + jobPath.getAbsolutePath());
			return false;
		}
		serverContext.setProperty(ServerContext.PROPERTY_SCRIPTPATH, jobPath.getAbsolutePath());
		
		StringTokenizer stk = new StringTokenizer(serverContext.getProperty("loadJobs", ""), ",; ");
		while (stk.hasMoreTokens()) {
			String scriptName = stk.nextToken();
			String jobName = scriptName;
			if (jobName.toLowerCase().endsWith(".groovy")) {
				jobName = jobName.substring(0, jobName.length() - ".groovy".length());
			}
			try {
				log.info("Loading Job " + jobName + " from file " + scriptName);
				serverContext.loadJob(jobName, scriptName);
			} catch (Throwable e) {
				log.error("An error occured during loading of the job " + scriptName + ": " + e, e);
			}
		}

		// load DataPool(s)
		CubeHandler cubeHandler = CubeHandler.getCubeHandler(serverContext);
		log.info("Loaded " + cubeHandler.getDataPoolManagerKeys().size() + " DataPool(s).");
		
		// load webserver
		if (serverContext.getPropertyBoolean(ServerContext.PROPERTY_WEBSERVER_START, false)) {
			log.info("Initializing Webserver (Jetty)");
			try {
				jetty = new Server();
				/*
				 * If ETLgine is launched from a different "current directory",
				 * this is the the only way to set jetty home directoy.
				 * Might have side effects on the "hosting" application.
				 */
				String userDir = System.getProperty("user.dir");
				String newUserDir = new File(rootPath, "..").getCanonicalPath();
				if (!newUserDir.startsWith(userDir)) {
					// etl server is located in different path
					// workaround for now is to set user.dir to newUserDir
					// FIXME do it better ;-)
					System.setProperty("user.dir", newUserDir);
				}
				XmlConfiguration conf = new XmlConfiguration(new FileInputStream(new File(pathConfig, "jetty.xml")));
				conf.configure(jetty);
	
				Handler[] handlers = jetty.getHandlers();
			    ContextHandlerCollection context = null;
			    for (Handler h : handlers) {
			    	if (h instanceof ContextHandlerCollection) {
			    		context = (ContextHandlerCollection)h;
			    		break;
			    	}
			    }
			    if (context == null) {
			    	log.error("Invalid Jetty Configuration - no ContextHandlerCollection found.");
			    	return false;
			    }
				WebAppContext wc = new WebAppContext(context, new File(path, "web").getAbsolutePath(), "/etlgine");
				wc.setDefaultsDescriptor(new File(pathConfig, "webdefault.xml").getAbsolutePath());
				
				jetty.start();
				
			} catch (Exception e) {
				log.error("Error starting webserver: " + e, e);
			}
		}
		
		// execute run scripts, if specified
		
		stk = new StringTokenizer(serverContext.getProperty("run", ""), ",; ");
		while (stk.hasMoreTokens()) {
			String scriptName = stk.nextToken();
			try {
				executeStartScript(scriptName);
			} catch (Exception e) {
				log.error("Error running startscript: " + e, e);
				return false;
			}
		}
		//System.setOut(oldPS);
		return true;
		
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
			throw new ETLException("The script file " + file.getAbsolutePath() + " does not exist.");
		}
		
		try {
			shell.evaluate(file);
		} catch (Exception e) {
			throw new ETLException("Error evaluating script '" + file.getName() + "':" + e, e);
		}

		
	}

	/**
	 * @return the rootPath
	 */
	public String getRootPath() {
		return rootPath;
	}

	/**
	 * @param rootPath the rootPath to set
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
	 * @param exitAfterFinish the exitAfterFinish to set
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
}
