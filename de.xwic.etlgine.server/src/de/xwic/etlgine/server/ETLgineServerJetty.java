/**
 * 
 */
package de.xwic.etlgine.server;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.xml.XmlConfiguration;

/**
 * @author Developer
 *
 */
public class ETLgineServerJetty extends ETLgineServer {
	private static ETLgineServerJetty instance = null;
	private static final Log log = LogFactory.getLog(ETLgineServerJetty.class);
	private Server jetty;
	
	/**
	 * 
	 */
	public ETLgineServerJetty() {
		instance = this;
	}
	
	/**
	 * Returns the server instance.
	 * @return
	 */
	public static ETLgineServerJetty getInstance() {
		if (instance == null) {
			instance = new ETLgineServerJetty();
		}
		return instance;
	}
	
	protected boolean startEmbededWebServer(File path, File pathConfig) {
		log.info("Initializing Webserver (Jetty)");
		try {
			jetty = new Server();
			/*
			 * If ETLgine is launched from a different "current directory",
			 * this is the the only way to set jetty home directoy.
			 * Might have side effects on the "hosting" application.
			 */
			/*
			String userDir = System.getProperty("user.dir");
			String newUserDir = new File(rootPath).getCanonicalPath();
			if (!newUserDir.startsWith(userDir)) {
				// etl server is located in different path
				// workaround for now is to set user.dir to newUserDir
				// FIX-ME do it better ;-)
				System.setProperty("user.dir", newUserDir);
			}
			
			2009-10-13 jbornema
				- fixed in using jetty <SystemProperty name="etlgine_rootPath" default="."/> in xml files
			*/
			XmlConfiguration conf = new XmlConfiguration(new FileInputStream(new File(pathConfig, "jetty.xml")));
			conf.configure(jetty);

			Handler[] handlers = jetty.getHandlers();
		    ContextHandlerCollection context = null;
		    for (Handler h : handlers) {
		    	if (h instanceof ContextHandlerCollection) {
		    		context = (ContextHandlerCollection)h;
		    		break;
		    	}
		    }
		    if (context == null) {
		    	log.error("Invalid Jetty Configuration - no ContextHandlerCollection found.");
		    	return false;
		    }
			WebAppContext wc = new WebAppContext(context, new File(path, "web").getAbsolutePath(), "/etlgine");
			wc.setDefaultsDescriptor(new File(pathConfig, "webdefault.xml").getAbsolutePath());
			
			jetty.start();
			
			return true;
			
		} catch (Exception e) {
			log.error("Error starting webserver", e);
			return false;
		}
	}
	protected void stopEmbededWebServer() {
		try {
			if (jetty != null) {
				jetty.stop();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
