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
 */
public class TriggerList implements ITrigger {

	protected Collection<ITrigger> triggers = new ArrayList<ITrigger>();
	protected ITrigger dueTrigger = null;
	
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
			trigger.notifyJobStarted();
		}
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#notifyJobFinished(boolean)
	 */
	@Override
	public void notifyJobFinished(boolean withErrors) {
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
