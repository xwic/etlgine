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
		INTERVAL, // starts the job again every x seconds after the last execution
		WEEKLY,
		MONTLY,
		ONCE
	}
	
	private Type type = Type.DAILY;
	private Date notBefore = null;
	private Date notAfter = null;
	
	private int hourOfDay = 0;
	private int minuteOfDay = 0;
	private Integer hourOfDayAfterError;
	private Integer minuteOfDayAfterError;
	
	private int intervalInSeconds;
	private Integer intervalInSecondsAfterError;

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
	 * @param hourOfDay
	 * @param minuteOfDay
	 * @param hourOfDayAfterError
	 * @param minuteOfDayAfterError
	 */
	public ScheduledTrigger(int hourOfDay, int minuteOfDay, int hourOfDayAfterError, int minuteOfDayAfterError) {
		this(hourOfDay, minuteOfDay);
		this.hourOfDayAfterError = hourOfDayAfterError;
		this.minuteOfDayAfterError = minuteOfDayAfterError;
	}
	
	/**
	 * @param intervallInSeconds
	 */
	public ScheduledTrigger(int intervalInSeconds) {
		super();
		this.intervalInSeconds = intervalInSeconds;
		type = Type.INTERVAL;
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
		calculateNextStart(false);
	}
	
	/**
	 * @param withErrors
	 */
	private void calculateNextStart(boolean withErrors) {

		Date now = new Date();
		
		switch (type) {
		case MONTLY:
		case WEEKLY:
		case DAILY: {
			
			Date last = lastRun != null ? lastRun : now;
			
			Calendar cal = Calendar.getInstance();
			if (withErrors && hourOfDayAfterError != null && minuteOfDayAfterError != null) {
				cal.set(Calendar.HOUR_OF_DAY, hourOfDayAfterError);
				cal.set(Calendar.MINUTE, minuteOfDayAfterError);
			} else {
				cal.set(Calendar.HOUR_OF_DAY, hourOfDay);
				cal.set(Calendar.MINUTE, minuteOfDay);
			}
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			
			if (cal.getTime().before(last)) {
				switch (type) {
				case MONTLY:
					cal.add(Calendar.MONTH, 1);
					break;
				case WEEKLY:
					cal.add(Calendar.WEEK_OF_MONTH, 1);
					break;
				case DAILY:
					cal.add(Calendar.DAY_OF_MONTH, 1);
					break;
				}
			}
			nextStart = cal.getTime();
			
		}; break;
		case INTERVAL : {
			
			Calendar cal = Calendar.getInstance();
			if (withErrors && intervalInSecondsAfterError != null) {
				cal.add(Calendar.SECOND, intervalInSecondsAfterError);
			} else {
				cal.add(Calendar.SECOND, intervalInSeconds);
			}
			
			nextStart = cal.getTime();
			
		}; break;
			
		}
		
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ITrigger#notifyJobFinished(boolean)
	 */
	public void notifyJobFinished(boolean withErrors) {
		
		calculateNextStart(withErrors);
		
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
	 * @deprecated
	 */
	public int getIntervallInSeconds() {
		return intervalInSeconds;
	}

	/**
	 * @param intervallInSeconds the intervallInSeconds to set
	 * @deprecated
	 */
	public void setIntervallInSeconds(int intervallInSeconds) {
		this.intervalInSeconds = intervallInSeconds;
	}

	/**
	 * @return the intervalInSeconds
	 */
	public int getIntervalInSeconds() {
		return intervalInSeconds;
	}

	/**
	 * @param intervalInSeconds the intervalInSeconds to set
	 */
	public void setIntervalInSeconds(int intervalInSeconds) {
		this.intervalInSeconds = intervalInSeconds;
	}

	/**
	 * @return the intervalInSecondsAfterError
	 */
	public Integer getIntervalInSecondsAfterError() {
		return intervalInSecondsAfterError;
	}

	/**
	 * @param intervalInSecondsAfterError the intervalInSecondsAfterError to set
	 */
	public ScheduledTrigger setIntervalInSecondsAfterError(Integer intervalInSecondsAfterError) {
		this.intervalInSecondsAfterError = intervalInSecondsAfterError;
		return this;
	}

	/**
	 * @return the hourOfDayAfterError
	 */
	public Integer getHourOfDayAfterError() {
		return hourOfDayAfterError;
	}

	/**
	 * @param hourOfDayAfterError the hourOfDayAfterError to set
	 */
	public void setHourOfDayAfterError(Integer hourOfDayAfterError) {
		this.hourOfDayAfterError = hourOfDayAfterError;
	}

	/**
	 * @return the minuteOfDayAfterError
	 */
	public Integer getMinuteOfDayAfterError() {
		return minuteOfDayAfterError;
	}

	/**
	 * @param minuteOfDayAfterError the minuteOfDayAfterError to set
	 */
	public void setMinuteOfDayAfterError(Integer minuteOfDayAfterError) {
		this.minuteOfDayAfterError = minuteOfDayAfterError;
	}
	
}
