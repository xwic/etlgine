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
		
		String path = "server";
		if (args.length > 0) {
			path = args[0];
		}
		
		System.setProperty("root", new File(path).getAbsolutePath());
		
		ETLgineServer server = new ETLgineServer();
		server.setRootPath(path);
		
		// start ETLgine server.
		SwtRunner.launch(server, true);

	}

}
