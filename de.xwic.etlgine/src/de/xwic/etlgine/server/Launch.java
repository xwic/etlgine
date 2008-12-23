/**
 * 
 */
package de.xwic.etlgine.server;

import java.io.File;

import eu.lippisch.jscreen.runner.SwtRunner;

/**
 * Starting ETLgine Server.
 * @author Florian Lippisch
 */
public class Launch {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String path = "config";
		if (args.length > 0) {
			path = args[0];
		}
		
		System.setProperty("root", new File(".").getAbsolutePath());
		
		ETLgineServer server = new ETLgineServer();
		server.setConfigPath(path);
		
		// start ETLgine server.
		SwtRunner.launch(server, true);

	}

}
