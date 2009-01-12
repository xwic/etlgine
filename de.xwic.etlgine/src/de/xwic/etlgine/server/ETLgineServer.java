/**
 * 
 */
package de.xwic.etlgine.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.xml.XmlConfiguration;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IJob;
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
	
	private boolean doExit = false;
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

		File configFile = new File(pathConfig, "log4j.properties");
		if (!configFile.exists()) {
			System.out.println("Log4J config file log4j.properties does not exist.");
			return false;
		}
		
		// Initialize logging
		System.out.println("Initializing Log4J");
		PropertyConfigurator.configure(configFile.getAbsolutePath());
		
	
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
			serverContext.setProperty(sKey, props.getProperty(sKey));
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
			} catch (Exception e) {
				log.error("An log.error occured during loading of the job " + scriptName + ": " + e);
				e.printStackTrace();
				return false;
			}
		}

		// load DataPool(s)
		CubeHandler cubeHandler = CubeHandler.getCubeHandler(serverContext);
		log.info("Loaded " + cubeHandler.getDataPoolManagerKeys().size() + " DataPool(s).");
		
		// load webserver
		if (serverContext.getPropertyBoolean("webserver.start", false)) {
			log.info("Initializing Webserver (Jetty)");
			try {
				jetty = new Server();
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
				e.printStackTrace();
			}
		}
		
		//System.setOut(oldPS);
		return true;
		
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

	
}
