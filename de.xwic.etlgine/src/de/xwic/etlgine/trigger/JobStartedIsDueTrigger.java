/**
 * 
 */
package de.xwic.etlgine.trigger;

import de.xwic.etlgine.ITrigger;

/**
 * @author JBORNEMA
 *
 * JobStartedIsDueTrigger wraps another ITrigger and ensures that when a job starts execution
 * the original isDue() state persists till notifyJobFinished(boolean).
 * This behavior enables during job executing to check for the original isDue() state.
 * The general use is to implement different behavior when the job was manually enqueued (then isDue() is false).
 */
public class JobStartedIsDueTrigger implements ITrigger {

	protected ITrigger trigger = null;
	protected Boolean due = null;
	
	/**
	 * 
	 */
	public JobStartedIsDueTrigger() {
	}

	/**
	 * @param trigger
	 */
	public JobStartedIsDueTrigger(ITrigger trigger) {
		this.trigger = trigger;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#isDue()
	 */
	@Override
	public boolean isDue() {
		if (due != null) {
			return due;
		}
		return trigger.isDue();
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#notifyJobStarted()
	 */
	@Override
	public void notifyJobStarted() {
		due = trigger.isDue();
		trigger.notifyJobStarted();
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#notifyJobFinished(boolean)
	 */
	@Override
	public void notifyJobFinished(boolean withErrors) {
		due = null;
		trigger.notifyJobFinished(withErrors);
	}

	/**
	 * @return the trigger
	 */
	public ITrigger getTrigger() {
		return trigger;
	}

	/**
	 * @param trigger the trigger to set
	 */
	public void setTrigger(ITrigger trigger) {
		this.trigger = trigger;
	}

}
