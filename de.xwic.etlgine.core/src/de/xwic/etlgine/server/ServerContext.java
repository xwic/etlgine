/**
 * 
 */
package de.xwic.etlgine.server;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.DefaultMonitor;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.IMonitor;
import de.xwic.etlgine.impl.Context;
import de.xwic.etlgine.impl.Job;

/**
 * 
 * @author Developer
 */
public class ServerContext extends Context {

	enum EventType {
		JOB_EXECUTION_START,
		JOB_EXECUTION_END
	}
	
	public final static String DEFAULT_QUEUE = "default";
	public static final String PROPERTY_INITIALIZING_LISTENER = "initializing.listener.classnames";
	public final static String PROPERTY_WEBSERVER_START = "webserver.start";
	public final static String PROPERTY_MONITOR_CLASSNAME = "monitor.classname";

	public final static String PROPERTY_SERVER_INSTANCEID = "instance.id";
	public final static String PROPERTY_SERVER_INSTANCENAME = "name";

	public final static String PROPERTY_SERVER_INSTANCEID_DEFAULT = "DID";
	public final static String PROPERTY_SERVER_INSTANCENAME_DEFAULT = "DEFAULT";

    public final static String PROPERTY_SQLITE_DATABASE_INIT = "sqlite.database.init";
    public final static String PROPERTY_SQLITE_DATABSE_CONNECTION = "sqlite.database.connection";

    public final static String PROPERTY_SERVER_VERSION = "property.server.deployed.version";
    public final static String PROPERTY_SERVER_VERSION_DEFAULT = "5.0.0.0.x";
    

	protected final Log log = LogFactory.getLog(getClass());
	
	private Map<String, IJob> jobs = new HashMap<String, IJob>();
	private Map<String, JobQueue> queues = new HashMap<String, JobQueue>();

	private List<IServerContextListener> listeners = new ArrayList<IServerContextListener>();
	
	/**
	 * 
	 */
	public ServerContext() {
		queues.put(DEFAULT_QUEUE, new JobQueue(this, DEFAULT_QUEUE));
	}
	
	/**
	 * Add a IServerContextListener.
	 * @param listener
	 */
	public synchronized void addServerContextListener(IServerContextListener listener) {
		listeners.add(listener);
	}
	
	/**
	 * Remove a listener.
	 * @param listener
	 */
	public synchronized void removeServerContextListener(IServerContextListener listener) {
		listeners.remove(listener);
	}
	
	/**
	 * Notify listeners of a new event.
	 * @param et
	 * @param event
	 */
	void fireEvent(EventType et, ServerContextEvent event) {
		
		IServerContextListener[] ls = new IServerContextListener[listeners.size()];
		ls = listeners.toArray(ls);
		for (IServerContextListener listener : ls) {
			switch (et) {
			case JOB_EXECUTION_END:
				listener.jobExecutionEnd(event);
				break;
			case JOB_EXECUTION_START:
				listener.jobExecutionStart(event);
				break;
			}
		}
		
	}
	
	/**
	 * Load a Job from a script.
	 * @param name
	 * @param scriptFile
	 */
	public IJob loadJob(String name, String scriptFile) throws ETLException {
		
		if (jobs.containsKey(name)) {
			throw new ETLException("A job with the name already exist. (" + name + ")");
		}

		File jobPath = new File(getProperty(PROPERTY_SCRIPTPATH, "."));
		if (!jobPath.exists()) {
			throw new ETLException("The job path " + jobPath.getAbsolutePath() + " does not exist.");
		}
		File file = new File(jobPath, scriptFile);
		if (!file.exists()) {
			throw new ETLException("The script file " + file.getAbsolutePath() + " does not exist.");
		}
		
		Job job = new Job(name);
		job.setMonitor(createDefaultMonitor());
		job.setCreatorInfo(scriptFile);
		
		job.setServerInstanceId(getProperty(PROPERTY_SERVER_INSTANCEID, PROPERTY_SERVER_INSTANCEID_DEFAULT));
		job.setServerInstanceName(getProperty(PROPERTY_SERVER_INSTANCENAME, PROPERTY_SERVER_INSTANCENAME_DEFAULT));
		
		job.getMonitor().onEvent(this, de.xwic.etlgine.IMonitor.EventType.JOB_LOAD_FROM_SCRIPT, job);
		
		Binding binding = new Binding();
		binding.setVariable("context", this);
		binding.setVariable("job", job);

		GroovyShell shell = new GroovyShell(binding);
		
		try {
			shell.evaluate(file);
		} catch (Exception e) {
			throw new ETLException("Error evaluating script '" + file.getName() + "':" + e, e);
		}

		jobs.put(name, job);
		return job;
	}
	
	/**
	 * Returns the default job queue.
	 * @return
	 */
	public JobQueue getDefaultJobQueue() {
		return queues.get(DEFAULT_QUEUE);
	}
	
	/**
	 * Returns the queue with the specified name.
	 * @param name
	 * @return
	 */
	public JobQueue getJobQueue(String name) {
		return queues.get(name);
	}
	
	/**
	 * Returns all job queues.
	 * @return
	 */
	public Collection<JobQueue> getJobQueues() {
		return queues.values();
	}
	
	/**
	 * Add a ProcessChain.
	 * @param chain
	 * @throws ETLException
	 */
	public void addJob(IJob job) throws ETLException {
		if (jobs.containsKey(job.getName())) {
			throw new ETLException("A job with this name already exists. (" + job.getName() + ")");
		}
		jobs.put(job.getName(), job);
	}

	/**
	 * Remove a ProcessChain.
	 * @param chain
	 * @throws ETLException
	 */
	public void removeJob(String jobName) throws ETLException {
		if (!jobs.containsKey(jobName)) {
			throw new ETLException("A job with this name does not exists. (" + jobName + ")");
		}
		jobs.remove(jobName);
	}
	
	/**
	 * Returns the ProcessChain with the specified name.
	 * @param name
	 * @return
	 */
	public IJob getJob(String name) {
		return jobs.get(name);
	}
	
	/**
	 * Returns the list of ProcessChains.
	 * @return
	 */
	public Collection<IJob> getJobs() {
		return jobs.values();
	}

	
	/**
	 * Creates a new default IMonitor.
	 * @return
	 * @throws ETLException 
	 */
	public IMonitor createDefaultMonitor() throws ETLException {
		IContext context = this;
		IMonitor monitor = null;
		String classname = context.getProperty(PROPERTY_MONITOR_CLASSNAME);
		if (classname != null) {
			try {
				monitor = (IMonitor)Class.forName(classname).newInstance();
				monitor.initialize(context);
				return monitor;
			} catch (Throwable e) {
				log.error("Cannot create new instance of monitor " + classname + ", " + DefaultMonitor.class.getName() + " is used instead", e);
			}
		}
		monitor = new DefaultMonitor();
		monitor.initialize(context);
		return monitor;
	}
}
