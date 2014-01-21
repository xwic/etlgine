/**
 * 
 */
package de.xwic.etlgine.trigger;

import java.util.ArrayList;
import java.util.Collection;

import de.xwic.etlgine.ITrigger;

/**
 * @author jbornema
 *
 * Wraps one or more ITrigger.
 * 
 * Also ensures that when a job starts execution the original isDue() state persists till notifyJobFinished(boolean).
 * This behavior enables during job executing to check for the original isDue() state.
 * The general use is to implement different behavior when the job was manually enqueued (then isDue() is false).
 */
public class TriggerList implements ITrigger {

	protected Collection<ITrigger> triggers = new ArrayList<ITrigger>();
	protected ITrigger dueTrigger = null;
	protected Boolean due = null;

	/**
	 * 
	 */
	public TriggerList() {
	}

	/**
	 * 
	 * @param triggers
	 */
	public TriggerList(ITrigger... triggers) {
		for (ITrigger trigger : triggers) {
			this.triggers.add(trigger);
		}
	}
	
	/**
	 * Adds trigger to list of triggers checked.
	 * @param trigger
	 * @return
	 */
	public ITrigger addTrigger(ITrigger trigger) {
		triggers.add(trigger);
		return trigger;
	}
	
	/**
	 * Removes the trigger from list.
	 * @param trigger
	 * @return
	 */
	public ITrigger removeTrigger(ITrigger trigger) {
		if (triggers.remove(trigger)) {
			if (dueTrigger == trigger) {
				dueTrigger = null;
			}
			return trigger;
		}
		return null;
	}
	
	/**
	 * @return the triggers
	 */
	public Collection<ITrigger> getTriggers() {
		return triggers;
	}

	/**
	 * @param triggers the triggers to set
	 */
	public void setTriggers(Collection<ITrigger> triggers) {
		this.triggers = triggers;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#isDue()
	 */
	@Override
	public boolean isDue() {
		if (due != null) {
			return due;
		}
		for (ITrigger trigger : triggers) {
			if (trigger.isDue()) {
				setDueTrigger(trigger);
				return true;
			}
		}
		return false;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#notifyJobStarted()
	 */
	@Override
	public void notifyJobStarted() {
		ITrigger trigger = getDueTrigger();
		if (trigger != null) {
			due = trigger.isDue();
			trigger.notifyJobStarted();
		} else {
			due = null;
		}
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#notifyJobFinished(boolean)
	 */
	@Override
	public void notifyJobFinished(boolean withErrors) {
		due = null;
		ITrigger trigger = getDueTrigger();
		if (trigger != null) {
			trigger.notifyJobFinished(withErrors);
		}
	}

	/**
	 * @return the dueTrigger
	 */
	public ITrigger getDueTrigger() {
		return dueTrigger;
	}

	/**
	 * @param dueTrigger the dueTrigger to set
	 */
	public void setDueTrigger(ITrigger dueTrigger) {
		this.dueTrigger = dueTrigger;
	}

}
