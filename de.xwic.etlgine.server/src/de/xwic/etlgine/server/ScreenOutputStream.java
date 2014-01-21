/**
 * 
 */
package de.xwic.etlgine.server;

import java.io.IOException;
import java.io.OutputStream;

import eu.lippisch.jscreen.Screen;

/**
 * @author Developer
 *
 */
public class ScreenOutputStream extends OutputStream {

	private Screen screen;
	
	/**
	 * @param screen
	 */
	public ScreenOutputStream(Screen screen) {
		super();
		this.screen = screen;
	}

	/* (non-Javadoc)
	 * @see java.io.OutputStream#write(int)
	 */
	@Override
	public void write(int b) throws IOException {
		char c = (char)b;
		screen.print(c);		
	}

}
