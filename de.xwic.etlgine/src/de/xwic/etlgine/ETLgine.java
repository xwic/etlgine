/*
 * de.xwic.etlgine.ETLgine 
 */
package de.xwic.etlgine;

import java.io.InputStream;
import java.util.Properties;

import de.xwic.etlgine.impl.ProcessChain;
import de.xwic.etlgine.mail.MailFactory;

/**
 * 
 * @author lippisch
 */
public class ETLgine {

	/**
	 * Create a new ProcessChain.
	 * @return
	 */
	public static IProcessChain createProcessChain(String name) {
		return new ProcessChain(name);
	}
	
	/**
	 * Create a new ProcessChain.
	 * @return
	 */
	public static IProcessChain createProcessChain(IContext parentContext, String name) {
		return new ProcessChain(parentContext, name);
	}

	/**
	 * Perform a system integrity test. If the ETLgine is running from a network drive
	 * or in a virtualized environment with attached virtual drives, it can happen that
	 * these drives get disconnected and the Java VM is loosing connection to the opened
	 * JAR files. In such a case, embedded resources and class files can not be read anymore,
	 * causing unexpected errors.
	 * 
	 * This test will perform a number of tests to ensure the system integrity. If the
	 * test fails, a RuntimeException is thrown. 
	 */
	public static void integrityTest() {
		
		// try to read an embedded resource.
		try {
			InputStream in = new ETLgine().getClass().getResourceAsStream("Integrity.properties");
			if (in == null) {
				throw new IllegalStateException("Can not find embedded resource!");
			}
			Properties prop = new Properties();
			prop.load(in);
			in.close();
			if (!prop.getProperty("integrity", "").equals("1")) {
				throw new IllegalStateException("Reading embedded resource failed - unexpected content.");
			}
			
			// initialize Velocity to check 3rd party jars
			MailFactory.getTemplateEngine();
			
		} catch (Throwable t) {
			throw new RuntimeException("Integrity Check failed - System may be in unstable state! (" + t.getMessage() + ")", t);
		}
		
		
	}

}
