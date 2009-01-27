/**
 * 
 */
package de.xwic.etlgine.server;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.IJob;

/**
 * 
 * @author lippisch
 */
public class JobQueue implements Runnable {

	private static final int SLEEP_TIME = 30 * 1000;
	
	private static final Log log = LogFactory.getLog(JobQueue.class);
	
	private Queue<IJob> queue = new ConcurrentLinkedQueue<IJob>();

	private final String name;
	private Thread myThread;
	private boolean exitFlag = false;
	private IJob activeJob = null;
	private final ServerContext context; 
	
	/**
	 * Constructor.
	 * @param name
	 */
	public JobQueue(ServerContext context, String name) {
		this.context = context;
		this.name = name;
		
		myThread = new Thread(this, "jobQueue-" + name);
		myThread.start();
	}
	
	/**
	 * Add a job to the queue. If the queue is empty, the job is immidiately processed.
	 * @param job
	 */
	public void addJob(IJob job) {
		if (!queue.contains(job)) {
			job.notifyEnqueued();
			log.debug("Adding job " + job.getName() + " to queue " + name);
			queue.add(job);
			myThread.interrupt();
		} else {
			throw new IllegalStateException("The specified job (" + job.getName() + ") is already queued for processing.");
		}
	}
	
	/**
	 * Returns true if the queue is empty.
	 * @return
	 */
	public boolean isEmpty() {
		return queue.isEmpty();
	}

	/**
	 * Exits the queue. If a job is currently beeing processed, the
	 * queue is terminated after the job has finished.
	 */
	public void stopQueue() {
		log.info("Stopping queue " + name);
		exitFlag = true;
		myThread.interrupt();
	}
	

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
	
		while (!exitFlag) {
			
			activeJob = queue.poll();
			if (activeJob != null) {
				try {
					log.info("[Queue " + name +"]: Executing Job " + activeJob.getName());
					activeJob.execute(context);
					log.info("[Queue " + name +"]: Job " + activeJob.getName() + " finished execution.");
				} catch (Throwable t) {
					log.error("Error executing job " + activeJob.getName() + " in queue " + name + ": " + t, t);
				}
				activeJob = null;
			} else {
				try {
					Thread.sleep(SLEEP_TIME);
				} catch (InterruptedException e) {
					// nothing unexpected...
				}
			}
			 
		}
		
	}

	/**
	 * @return the activeJob
	 */
	public IJob getActiveJob() {
		return activeJob;
	}
	
}
