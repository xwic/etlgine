/**
 * 
 */
package de.xwic.etlgine.server;

import java.io.File;
import java.io.PrintStream;

import org.apache.log4j.PropertyConfigurator;

import eu.lippisch.jscreen.Color;
import eu.lippisch.jscreen.Input;
import eu.lippisch.jscreen.Key;
import eu.lippisch.jscreen.Screen;
import eu.lippisch.jscreen.app.JScreenApplication;

/**
 * @author Developer
 *
 */
public class ETLgineServer extends JScreenApplication {
	
	private String configPath = "config";
	private Screen screen;
	private Input input;

	/* (non-Javadoc)
	 * @see eu.lippisch.jscreen.app.JScreenApplication#run(eu.lippisch.jscreen.Screen, eu.lippisch.jscreen.Input)
	 */

	@Override
	public void run(Screen screen, Input input) {

		this.screen = screen;
		this.input = input;
		
		initialize();
		
		screen.println("Server started. Press ESC to exit.");
		
		boolean exit = false;
		while (!exit) {
			Key key = input.readKey(true);
			
			switch (key.action) {
			case ESCAPE:
				exit = true;
				break;
			}
			
		}
		
	}
	
	/**
	 * Initialize the server.
	 */
	private void initialize() {
		
		screen.setColor(Color.WHITE, Color.BLACK);
		screen.println("Loading ETLgine Server");
		screen.println("======================");
		
		screen.setForegroundColor(Color.GRAY);
		
		File path = new File(configPath);
		if (!path.exists()) {
			error("Config path " + path.getAbsolutePath() + " does not exist.");
			return;
		}
		
		screen.println("Redirecting System.out");
		System.setOut(new PrintStream(new ScreenOutputStream(screen)));

		File configFile = new File(path, "log4j.properties");
		if (!configFile.exists()) {
			error("Log4J config file log4j.properties does not exist.");
			return;
		}
		
		screen.println("Initializing Log4J");
		PropertyConfigurator.configure(configFile.getAbsolutePath());
		
		
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
	 * @return the configPath
	 */
	public String getConfigPath() {
		return configPath;
	}

	/**
	 * @param configPath the configPath to set
	 */
	public void setConfigPath(String configPath) {
		this.configPath = configPath;
	}

	
}
