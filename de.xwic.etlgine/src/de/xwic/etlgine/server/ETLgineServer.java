/**
 * 
 */
package de.xwic.etlgine.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.xml.XmlConfiguration;

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
	
	private String rootPath = ".";
	private Screen screen;
	private Input input;
	private Server jetty;
	
	private ServerContext serverContext = new ServerContext();

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
		
		screen.println("Server started. Press ESC to exit.");
		
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
			
			switch (key.action) {
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

	/**
	 * Initialize the server.
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
		
		File pathConfig = new File(path, "config");
		if (!pathConfig.exists()) {
			error("Config path " + pathConfig.getAbsolutePath() + " does not exist.");
			return false;
		}
		screen.println("Redirecting System.out");
		System.setOut(new PrintStream(new ScreenOutputStream(screen)));

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
			props.load(new FileReader(fileServerConf));
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
		File jobPath = new File(path, serverContext.getProperty("jobpath", "jobs"));
		if (!jobPath.exists()) {
			error("The job directory does not exist: " + jobPath.getAbsolutePath());
			return false;
		}

		
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
				WebAppContext wc = new WebAppContext(context, new File(path, "web").getAbsolutePath(), "etlgine");
				wc.setDefaultsDescriptor(new File(pathConfig, "webdefault.xml").getAbsolutePath());
				
				jetty.start();
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		
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
