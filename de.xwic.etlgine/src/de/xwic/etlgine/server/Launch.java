/**
 * 
 */
package de.xwic.etlgine.server;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import eu.lippisch.jscreen.runner.SwingRunner;
import eu.lippisch.jscreen.runner.SwtRunner;

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
		
		
		System.setProperty("root", new File(path).getAbsolutePath());
		
		ETLgineServer server = new ETLgineServer();
		server.setRootPath(path);
		
		String runJob = prop.getProperty(PARAM_RUN);
		if (runJob != null) {
			// only execute the specified job
			
		} else {
			// startup the server.
			Thread serverThread = new Thread(server, "ETLgineServer");
			serverThread.setDaemon(false);
			serverThread.start();
			if (prop.getProperty(PARAM_CONSOLE) != null) {
				ETLgineConsole console = new ETLgineConsole(server);
				if ("swing".equals(prop.getProperty(PARAM_CONSOLE))) {
					SwingRunner.launch(console, true);
				} else {
					SwtRunner.launch(console, true);
				}
			}
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
