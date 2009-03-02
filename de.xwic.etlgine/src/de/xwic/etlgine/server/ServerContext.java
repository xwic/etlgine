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

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IJob;
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
	public final static String PROPERTY_WEBSERVER_START = "webserver.start";
	
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
		IJob job = new Job(name);
		
		Binding binding = new Binding();
		binding.setVariable("context", this);
		binding.setVariable("job", job);

		GroovyShell shell = new GroovyShell(binding);
		
		File jobPath = new File(getProperty(PROPERTY_SCRIPTPATH, "."));
		if (!jobPath.exists()) {
			throw new ETLException("The job path " + jobPath.getAbsolutePath() + " does not exist.");
		}
		File file = new File(jobPath, scriptFile);
		if (!file.exists()) {
			throw new ETLException("The script file " + file.getAbsolutePath() + " does not exist.");
		}
		
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

	
}
