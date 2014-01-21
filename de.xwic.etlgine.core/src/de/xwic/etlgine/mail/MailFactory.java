/**
 * 
 */
package de.xwic.etlgine.mail;

import java.util.Properties;

import de.xwic.etlgine.mail.impl.JavaMailManager;
import de.xwic.etlgine.mail.impl.VelocityTemplateEngine;


/**
 * Factory for email access.
 * 
 * @author Ronny Pfretzschner
 *
 */
public class MailFactory {

	private static IMailManager instance;
	private static Properties mailProperties = null;
	
	/**
	 * 
	 * @return the mail Manager as singleton.
	 */
	public static IMailManager getMailManager() {
		if (instance == null) {
			
			if (mailProperties == null) {
				throw new IllegalStateException("Mailproperties are not set! Call initialize first.");
			}
			
			instance = new JavaMailManager(mailProperties);
		}
		return instance;
	}
	
	/**
	 * 
	 * @return a new instance of the template engine.
	 */
	public static ITemplateEngine getTemplateEngine() {
		return new VelocityTemplateEngine();
	}

	/**
	 * Initializes the factory.
	 * 
	 * @param mailProperties
	 */
	public static void initialize(Properties mailProperties) {
		MailFactory.mailProperties = mailProperties;
	}
}
