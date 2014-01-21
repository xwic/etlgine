package de.xwic.etlgine.trigger;

import java.io.File;

import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.ITrigger;
import de.xwic.etlgine.IJob.State;
import de.xwic.etlgine.impl.Job;

public class ScheduledFileChangedTrigger implements ITrigger {

	private ScheduledTrigger scheduledTrigger;
	private FileLookupTrigger fileLookUpTrigger;
	private long fileLastModified = -1;
	private boolean execute = true;
	private Job job;
	
	public ScheduledFileChangedTrigger(int intervallInSeconds, String filepath, Job job) {
		this.scheduledTrigger = new ScheduledTrigger(intervallInSeconds);
		this.fileLookUpTrigger = new FileLookupTrigger(filepath);
		this.job = job;
	}
		
	public boolean isDue() {
		if(this.execute && this.fileLookUpTrigger.isDue() && this.scheduledTrigger.isDue()) {
			return hasFileDateChanged(fileLookUpTrigger.getFile());
		} else {
			return false;
		}		
	}

	private boolean hasFileDateChanged(File file) {
		if( this.fileLastModified < 0) {
			this.fileLastModified  = file.lastModified();
			return false;
		} else {
			if(this.fileLookUpTrigger.isDue()) {
				long lastMod = this.fileLastModified;
				this.fileLastModified = file.lastModified();
				return this.fileLastModified  != lastMod;
			} else {
				return false;
			}
		}		
	}

	public void notifyJobFinished(boolean withErrors) {
		if(withErrors) {
			LogFactory.getLog(LogFactory.FACTORY_DEFAULT).error("### Job " + job.getName() + " failed. Setting state FINISHED for next execution. ###");
			job.setState(State.FINISHED);
		}
		this.scheduledTrigger.notifyJobFinished(withErrors);
		this.fileLastModified = fileLookUpTrigger.getFile().lastModified();
		this.execute = true;
	}

	public void notifyJobStarted() {
		this.scheduledTrigger.notifyJobStarted();
		this.execute = false;
	}

}
