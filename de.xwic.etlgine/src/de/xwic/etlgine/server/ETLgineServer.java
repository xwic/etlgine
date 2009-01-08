/**
 * 
 */
package de.xwic.etlgine.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.log4j.PropertyConfigurator;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.xml.XmlConfiguration;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.cube.CubeHandler;
import eu.lippisch.jscreen.Color;
import eu.lippisch.jscreen.Input;
import eu.lippisch.jscreen.Key;
import eu.lippisch.jscreen.Screen;
import eu.lippisch.jscreen.app.JScreenApplication;
import eu.lippisch.jscreen.util.InputUtil;

/**
 * @author Developer
 *
 */
public class ETLgineServer extends JScreenApplication {
	
	private static ETLgineServer instance = null;
	
	private String rootPath = ".";
	private Screen screen;
	private Input input;
	private Server jetty;
	
	private ServerContext serverContext = new ServerContext();

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
	
	/* (non-Javadoc)
	 * @see eu.lippisch.jscreen.app.JScreenApplication#run(eu.lippisch.jscreen.Screen, eu.lippisch.jscreen.Input)
	 */

	@Override
	public void run(Screen screen, Input input) {

		this.screen = screen;
		this.input = input;
		
		if (!initialize()) {
			screen.println("Server start failed. Press any key to exit.");
			input.readKey(true);
			return;
		}
		
		screen.println("Server started. Press ESC to exit, F5 to start a job.");
		
		handleKeyboardInput();
		
		try {
			if (jetty != null) {
				screen.println("Stopping Webserver...");
				jetty.stop();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 
	 */
	private void handleKeyboardInput() {

		boolean exit = false;
		while (!exit) {
			Key key = input.readKey(true);
			
			if (key.action != null) {
				switch (key.action) {
				case F5:
					screen.println("Joblist:");
					int idx = 1;
					for (IJob job : serverContext.getJobs()) {
						screen.printf(" %d - %s%n", idx++, job.getName());
					}
					screen.print("Enter job name to start: ");
					String inpName = InputUtil.inputLine(screen, input);
					if (inpName.trim().length() != 0) {
						IJob job = serverContext.getJob(inpName);
						if (job != null) {
							screen.println("Executing Job " + job.getName());
							try {
								job.execute();
							} catch (ETLException e) {
								error("Error during execution: " + e);
							}
						} else {
							error("Job with the name " + inpName + " does not exist.");
						}
					}
					break;
				case ESCAPE:
					screen.print("Are you sure? (Enter yes to exit): ");
					String inp = InputUtil.inputLine(screen, input);
					if (inp.startsWith("y")) {
						exit = true;
					}
					break;
				}
			}
			
		}
		
	}

	/**
	 * Initialize the server.
	 * @throws ETLException 
	 */
	private boolean initialize() {
		
		screen.setColor(Color.WHITE, Color.BLACK);
		screen.println("Loading ETLgine Server");
		screen.println("======================");
		
		screen.setForegroundColor(Color.GRAY);
		
		File path = new File(rootPath);
		if (!path.exists()) {
			error("Root path " + path.getAbsolutePath() + " does not exist.");
			return false;
		}
		
		serverContext.setProperty(ServerContext.PROPERTY_ROOTPATH, path.getAbsolutePath());
		
		File pathConfig = new File(path, "config");
		if (!pathConfig.exists()) {
			error("Config path " + pathConfig.getAbsolutePath() + " does not exist.");
			return false;
		}
		//screen.println("Redirecting System.out");
		//System.setOut(new PrintStream(new ScreenOutputStream(screen)));

		File configFile = new File(pathConfig, "log4j.properties");
		if (!configFile.exists()) {
			error("Log4J config file log4j.properties does not exist.");
			return false;
		}
		
		File fileServerConf = new File(pathConfig, "server.properties");
		if (!fileServerConf.exists()) {
			error("server.properties not found.");
			return false;
		}
		
		Properties props = new Properties();
		try {
			props.load(new FileInputStream(fileServerConf));
		} catch (IOException e) {
			error("Error reading server.properties: " + e);
			return false;
		}

		
		// copy properties to server context
		for (Object key : props.keySet()) {
			String sKey = (String)key;
			serverContext.setProperty(sKey, props.getProperty(sKey));
		}

		// Initialize logging
		screen.println("Initializing Log4J");
		PropertyConfigurator.configure(configFile.getAbsolutePath());
		
		// load jobs
		File jobPath = new File(path, serverContext.getProperty(ServerContext.PROPERTY_SCRIPTPATH, "jobs"));
		if (!jobPath.exists()) {
			error("The job directory does not exist: " + jobPath.getAbsolutePath());
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
				screen.println("Loading Job " + jobName + " from file " + scriptName);
				serverContext.loadJob(jobName, scriptName);
			} catch (Exception e) {
				error("An error occured during loading of the job " + scriptName + ": " + e);
				e.printStackTrace();
				return false;
			}
		}

		// load DataPool(s)
		CubeHandler cubeHandler = CubeHandler.getCubeHandler(serverContext);
		screen.println("Loaded " + cubeHandler.getDataPoolManagerKeys().size() + " DataPool(s).");
		
		// load webserver
		if (serverContext.getPropertyBoolean("webserver.start", false)) {
			screen.println("Initializing Webserver (Jetty)");
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
			    	error("Invalid Jetty Configuration - no ContextHandlerCollection found.");
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
	
	/* (non-Javadoc)
	 * @see eu.lippisch.jscreen.app.JScreenApplication#getPreferedColumns()
	 */
	@Override
	public int getPreferedColumns() {
		return 120;
	}
	
	/* (non-Javadoc)
	 * @see eu.lippisch.jscreen.app.JScreenApplication#getPreferedRows()
	 */
	@Override
	public int getPreferedRows() {
		return 50;
	}

	/**
	 * @param string
	 */
	private void error(String string) {
		
		screen.setForegroundColor(Color.HI_RED);
		screen.println(string);
		screen.setForegroundColor(Color.GRAY);
		
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

	
}
