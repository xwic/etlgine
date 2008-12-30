/**
 * 
 */
package de.xwic.etlgine.server;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.ETLgine;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.impl.Context;
import de.xwic.etlgine.impl.Job;

/**
 * 
 * @author Developer
 */
public class ServerContext extends Context {

	protected final Log log = LogFactory.getLog(getClass());
	
	private Map<String, IJob> jobs = new HashMap<String, IJob>();

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
		job.setProcessChain(ETLgine.createProcessChain(this, name));
		
		Binding binding = new Binding();
		binding.setVariable("job", job);
		binding.setVariable("processChain", job.getProcessChain());

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
