/**
 * 
 */
package de.xwic.etlgine.server;

import de.xwic.etlgine.IJob;
import eu.lippisch.jscreen.Input;
import eu.lippisch.jscreen.Key;
import eu.lippisch.jscreen.Screen;
import eu.lippisch.jscreen.app.JScreenApplication;
import eu.lippisch.jscreen.util.InputUtil;

/**
 * @author Developer
 *
 */
public class ETLgineConsole extends JScreenApplication {

	private Screen screen;
	private Input input;
	private ServerContext serverContext;
	private final ETLgineServer server;
	

	/**
	 * @param server
	 */
	public ETLgineConsole(ETLgineServer server) {
		this.server = server;
		this.serverContext = server.getServerContext();
	}

	/* (non-Javadoc)
	 * @see eu.lippisch.jscreen.app.JScreenApplication#run(eu.lippisch.jscreen.Screen, eu.lippisch.jscreen.Input)
	 */
	@Override
	public void run(Screen screen, Input input) {
		this.screen = screen;
		this.input = input;
		
		handleKeyboardInput();

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
	 * 
	 */
	private void handleKeyboardInput() {

		screen.println("ETLgine Server - Console");
		screen.println("  F4 - View Jobs");
		screen.println("  F5 - Run Job");
		screen.println(" ESC - Exit Console");
		screen.println(" F10 - Shutdown Server");

		String inp = null;
		boolean exit = false;
		while (!exit) {
			Key key = input.readKey(true);
			
			if (key.action != null) {
				switch (key.action) {
				case F4:
				case F5:
					screen.println("Joblist:");
					int idx = 1;
					for (IJob job : serverContext.getJobs()) {
						screen.printf(" %d - %s%n", idx++, job.getName());
					}
					if (key.action == Key.Action.F5) {
						screen.print("Enter job name to start: ");
						String inpName = InputUtil.inputLine(screen, input);
						if (inpName.trim().length() != 0) {
							IJob job = serverContext.getJob(inpName);
							if (job != null) {
								screen.println("Executing Job " + job.getName());
								try {
									job.execute();
								} catch (Throwable e) {
									screen.println("Error during execution: " + e);
								}
							} else {
								screen.println("Job with the name " + inpName + " does not exist.");
							}
						}
					}
					break;
				case ESCAPE:
					screen.print("Are you sure? (Enter yes to exit): ");
					inp = InputUtil.inputLine(screen, input);
					if (inp.startsWith("y")) {
						exit = true;
					}
					break;
				case F10:
					screen.print("Are you sure? (Enter yes to exit): ");
					inp = InputUtil.inputLine(screen, input);
					if (inp.startsWith("y")) {
						server.exitServer();
						exit = true;
					}
					break;
				}
			}
			
		}
		
	}

}
