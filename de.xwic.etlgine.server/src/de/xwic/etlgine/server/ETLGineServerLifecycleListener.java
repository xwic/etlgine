package de.xwic.etlgine.server;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

public class ETLGineServerLifecycleListener implements ServletContextListener {
	private static Log log = LogFactory.getLog(ETLGineServerLifecycleListener.class);
	Thread serverThread;
	ServletContext context;
	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		log.info("Destroy context");
		if (serverThread != null && serverThread.isAlive()) {
			serverThread.interrupt();
		}
		
	}

	@Override
	public void contextInitialized(ServletContextEvent event) {
		context = event.getServletContext();
		String rootPath = context.getRealPath("") + "\\..";

		System.setProperty("root", rootPath);

		initLog4J();
		
		initServerThread(rootPath);
	}

	private void initServerThread(String rootPath) {
		// Initialize the ETL server
		log.info("Start ETLGine server - " + getInstanceName());

		ETLgineServer server = ETLgineServer.getInstance();
		server.setRootPath(rootPath);
		if (!server.initialize()) {
			log.error("Start ETLGine server failed. - " + getInstanceName());
			return;
		} 
		// startup the server.
		serverThread = new Thread(server, getInstanceName());
		serverThread.setDaemon(false);
		serverThread.start();
		log.info("Start ETLGine server success. - " + getInstanceName());

		server.getServerContext().setProperty("server.starttimeetl", Long.toString(System.currentTimeMillis()));

		server.getServerContext().setProperty(ServerContext.PROPERTY_SERVER_VERSION, getDeployedVersion());

	}
	

	private void initLog4J() {
		String log4jFile = "WEB-INF/log4j.properties";
		if (log4jFile != null && log4jFile.length() != 0) {
			PropertyConfigurator.configure(context.getRealPath(log4jFile));
		}
		log = LogFactory.getLog(getClass());
	}

	protected String getInstanceName() {
		return "ETLNAgineServer";
	}
	

	private String getDeployedVersion() {

		log.info("  Implementation Title:" + this.getClass().getPackage().getImplementationTitle());
		log.info(" Implementation Vendor:" + this.getClass().getPackage().getImplementationVendor());
		log.info("Implementation Version:" + this.getClass().getPackage().getImplementationVersion());
		log.info("    Specification Tile:" + this.getClass().getPackage().getSpecificationTitle());
		log.info("  Specification Vendor:" + this.getClass().getPackage().getSpecificationVendor());
		log.info(" Specification Version:" + this.getClass().getPackage().getSpecificationVersion());

		String implementationVersion = this.getClass().getPackage().getSpecificationVersion();
		log.info("Got data - " + implementationVersion);
		if (!StringUtils.isEmpty(implementationVersion)) {
			log.info("Not EMPTY!");
			if (implementationVersion.contains("SNAPSHOT")) {
				log.info("SNAPSHOT!");
				implementationVersion = implementationVersion + "(#" + this.getClass().getPackage().getImplementationVersion() + ")";
			} else {
				log.info("NO SNAPSHOT!");
				implementationVersion = implementationVersion + "." + this.getClass().getPackage().getImplementationVersion();
			}
		} else {
			log.info("EMPTY!");
			implementationVersion = "";
		}

		return implementationVersion;
	}
}
