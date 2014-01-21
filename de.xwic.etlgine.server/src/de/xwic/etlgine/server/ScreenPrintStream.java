/**
 * 
 */
package de.xwic.etlgine.server;

import java.io.PrintStream;

import eu.lippisch.jscreen.Screen;

/**
 * This PrintStream implementation directly delegates the print
 * calls to the screen, skipping the way over the output stream
 * which results in less screen updates, making the printing
 * faster.
 * @author Developer
 */
public class ScreenPrintStream extends PrintStream {

	private final Screen screen;
	
	/**
	 * Constructor.
	 * @param screen
	 */
	public ScreenPrintStream(Screen screen) {
		super(new ScreenOutputStream(screen));
		this.screen = screen;
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#print(boolean)
	 */
	@Override
	public void print(boolean b) {
		screen.print(b);
	}

	/* (non-Javadoc)
	 * @see java.io.PrintStream#print(char)
	 */
	@Override
	public void print(char c) {
		screen.print(c);
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#print(char[])
	 */
	@Override
	public void print(char[] s) {
		screen.print(new String(s));
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#print(double)
	 */
	@Override
	public void print(double d) {
		screen.print(d);
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#print(float)
	 */
	@Override
	public void print(float f) {
		screen.print(f);
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#print(int)
	 */
	@Override
	public void print(int i) {
		screen.print(i);
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#print(long)
	 */
	@Override
	public void print(long l) {
		screen.print(l);
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#print(java.lang.Object)
	 */
	@Override
	public void print(Object obj) {
		screen.print(obj);
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#print(java.lang.String)
	 */
	@Override
	public void print(String s) {
		screen.print(s);
	}
	
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#println()
	 */
	@Override
	public void println() {
		screen.println();
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#println(boolean)
	 */
	@Override
	public void println(boolean x) {
		screen.println(x);
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#println(char)
	 */
	@Override
	public void println(char x) {
		screen.println(x);
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#println(char[])
	 */
	@Override
	public void println(char[] x) {
		screen.println(new String(x));
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#println(double)
	 */
	@Override
	public void println(double x) {
		screen.println(x);
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#println(float)
	 */
	@Override
	public void println(float x) {
		screen.println(x);
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#println(int)
	 */
	@Override
	public void println(int x) {
		screen.println(x);
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#println(long)
	 */
	@Override
	public void println(long x) {
		screen.println(x);
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#println(java.lang.Object)
	 */
	@Override
	public void println(Object x) {
		screen.println(x);
	}
	
	/* (non-Javadoc)
	 * @see java.io.PrintStream#println(java.lang.String)
	 */
	@Override
	public void println(String x) {
		screen.println(x);
	}
	
}


