/**
 * 
 */
package de.xwic.etlgine.server;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import de.xwic.etlgine.IJob;

/**
 * Starting ETLgine Server.
 * @author Florian Lippisch
 */
public class Launch {

	private final static String PARAM_PATH = "path";
	private final static String PARAM_CONSOLE = "console";
	private final static String PARAM_RUN = "run";
	private final static String PARAM_HELP = "help";
	
	private static Map<String, String> PARAMS = new HashMap<String, String>();
	static {
		PARAMS.put(PARAM_PATH, PARAM_PATH);
		PARAMS.put("p", PARAM_PATH);
		PARAMS.put(PARAM_CONSOLE, PARAM_CONSOLE);
		PARAMS.put("con", PARAM_CONSOLE);
		PARAMS.put(PARAM_RUN, PARAM_RUN);
		PARAMS.put("r", PARAM_RUN);
		PARAMS.put("h", PARAM_HELP);
		PARAMS.put(PARAM_HELP, PARAM_HELP);
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// force log4j initialization
		ETLgineServer.FORCE_LOG4J_INITIALIZATION = true;
		
		System.out.println("ETLgine - XWic ETL Tool");
		System.out.println("-----------------------");
		System.out.println();
		
		// parse arguments
		Properties prop;
		try {
			prop = getLaunchArgs(args);
		} catch (IllegalArgumentException e) {
			System.out.println("Invalid arguments specified: " + e.getMessage());
			return;
		}
		
		if (prop.getProperty(PARAM_HELP, null) != null) {
			System.out.println("Arguments: [-help][-path <base path>] [-run <job name>] [-console [swing]]");
			return;
		}
		
		String path = prop.getProperty(PARAM_PATH, ".");
		String rootPath = new File(path).getAbsolutePath();
		try {
			rootPath = new File(path).getCanonicalPath();
		} catch (IOException ioe) {}
		
		System.setProperty("root", rootPath);
		ETLgineServerJetty server = ETLgineServerJetty.getInstance();
		server.setRootPath(rootPath);
		if (!server.initialize()) {
			System.out.println("Server start failed.");
			return;
		}
		

		String runJob = prop.getProperty(PARAM_RUN);

		// startup the server.
		Thread serverThread = new Thread(server, "ETLgineServer");
		serverThread.setDaemon(false);
		serverThread.start();
		
		if (runJob != null) {
			for (String rJob : runJob.split("[;,]")) {
				IJob job = server.getServerContext().getJob(rJob);
				if (job == null) {
					job = server.loadJob(rJob + ".groovy");
					//System.out.println("The specified job does not exist: " + runJob);
				}
				if (job != null) {
					server.enqueueJob(job);
				}
			}
			server.setExitAfterFinish(true);
		}

	}

	/**
	 * @param args
	 * @return
	 */
	private static Properties getLaunchArgs(String[] args) {
		
		Properties prop = new Properties();
		
		String cmd = null;
		for (int i = 0 ; i < args.length; i++) {
			
			if (args[i].startsWith("-")) {
				// is command
				if (cmd != null && prop.getProperty(cmd, null) == null) {
					prop.setProperty(cmd, "1");
				}
				if (args[i].length() <= 1) {
					throw new IllegalArgumentException("The argument #" + i + " is a '-' only, which is not allowed.");
				}
				String key = args[i].substring(1);
				if (!PARAMS.containsKey(key)) {
					throw new IllegalArgumentException("Unknown argument: " + key);
				}
				cmd = PARAMS.get(key);
			} else {
				if (cmd == null) {
					throw new IllegalArgumentException("No key value specified for argument #" + i + ". Enter -help for more infomrations.");
				}
				String old = prop.getProperty(cmd, null);
				if (old != null) {
					prop.setProperty(cmd, old + " " + args[i]);
				} else {
					prop.setProperty(cmd, args[i]);
				}
			}
		}
		if (cmd != null) {
			String old = prop.getProperty(cmd, null);
			if (old == null) {
				prop.setProperty(cmd, "1");
			}
		}
		
		return prop;
	}

}
