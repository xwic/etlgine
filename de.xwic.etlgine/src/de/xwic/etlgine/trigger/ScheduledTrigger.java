/**
 * 
 */
package de.xwic.etlgine.trigger;

import java.util.Calendar;
import java.util.Date;

import de.xwic.etlgine.ITrigger;

/**
 * @author lippisch
 *
 */
public class ScheduledTrigger implements ITrigger {

	public enum Type {
		DAILY, // fixed time daily execution at a  
		INTERVALL, // starts the job again every x seconds after the last execution
		WEEKLY,
		MONTLY,
		ONCE
	}
	
	private Type type = Type.DAILY;
	private Date notBefore = null;
	private Date notAfter = null;
	
	private int hourOfDay = 0;
	private int minuteOfDay = 0;
	
	private int intervallInSeconds;
	
	private Date nextStart = null;
	private Date lastRun = null;
	
	/**
	 * Constructor.
	 */
	public ScheduledTrigger() {
		
	}
	
	/**
	 * @param hourOfDay
	 * @param minuteOfDay
	 */
	public ScheduledTrigger(int hourOfDay, int minuteOfDay) {
		super();
		this.hourOfDay = hourOfDay;
		this.minuteOfDay = minuteOfDay;
		type = Type.DAILY;
		calculateNextStart();
	}

	/**
	 * @param intervallInSeconds
	 */
	public ScheduledTrigger(int intervallInSeconds) {
		super();
		this.intervallInSeconds = intervallInSeconds;
		type = Type.INTERVALL;
		calculateNextStart();
	}


	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#isDue()
	 */
	public boolean isDue() {
		if (nextStart == null) {
			calculateNextStart();
		}
		
		Date now = new Date();
		if (notBefore != null && notBefore.after(now)) {
			return false;
		}
		if (notAfter != null && notAfter.before(now)) {
			return false;
		}
		
		return nextStart.before(now);
	}

	/**
	 * 
	 */
	private void calculateNextStart() {

		Date now = new Date();
		
		switch (type) {
		case DAILY: {
			
			Date last = lastRun != null ? lastRun : now;
			
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.HOUR_OF_DAY, hourOfDay);
			cal.set(Calendar.MINUTE, minuteOfDay);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			
			if (cal.getTime().before(last)) {
				cal.add(Calendar.DAY_OF_MONTH, 1);
			}
			nextStart = cal.getTime();
			
		}; break;
		case INTERVALL : {
			
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.SECOND, this.intervallInSeconds);
			
			nextStart = cal.getTime();
			
		}; break;
			
		}
		
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#notifyJobFinished(boolean)
	 */
	public void notifyJobFinished(boolean withErrors) {
		
		calculateNextStart();
		
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#notifyJobStarted()
	 */
	public void notifyJobStarted() {
		
		lastRun = new Date();
		calculateNextStart();

	}

	/**
	 * @return the type
	 */
	public Type getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * @return the notBefore
	 */
	public Date getNotBefore() {
		return notBefore;
	}

	/**
	 * @param notBefore the notBefore to set
	 */
	public void setNotBefore(Date notBefore) {
		this.notBefore = notBefore;
	}

	/**
	 * @return the notAfter
	 */
	public Date getNotAfter() {
		return notAfter;
	}

	/**
	 * @param notAfter the notAfter to set
	 */
	public void setNotAfter(Date notAfter) {
		this.notAfter = notAfter;
	}

	/**
	 * @return the hourOfDay
	 */
	public int getHourOfDay() {
		return hourOfDay;
	}

	/**
	 * @param hourOfDay the hourOfDay to set
	 */
	public void setHourOfDay(int hourOfDay) {
		this.hourOfDay = hourOfDay;
	}

	/**
	 * @return the minuteOfDay
	 */
	public int getMinuteOfDay() {
		return minuteOfDay;
	}

	/**
	 * @param minuteOfDay the minuteOfDay to set
	 */
	public void setMinuteOfDay(int minuteOfDay) {
		this.minuteOfDay = minuteOfDay;
	}

	/**
	 * @return the nextStart
	 */
	public Date getNextStart() {
		return nextStart;
	}

	/**
	 * @return the intervallInSeconds
	 */
	public int getIntervallInSeconds() {
		return intervallInSeconds;
	}

	/**
	 * @param intervallInSeconds the intervallInSeconds to set
	 */
	public void setIntervallInSeconds(int intervallInSeconds) {
		this.intervallInSeconds = intervallInSeconds;
	}
	
}
