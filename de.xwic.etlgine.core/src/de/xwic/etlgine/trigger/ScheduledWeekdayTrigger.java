package de.xwic.etlgine.trigger;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import de.xwic.etlgine.ITrigger;

public class ScheduledWeekdayTrigger implements ITrigger {

	private int hourOfDay = 0;
	private int minuteOfDay = 0;

	private Date nextStart = null;
	private Date lastRun = null;

	private List<Integer> runOnDays = new ArrayList<Integer>();

	public ScheduledWeekdayTrigger() {

	}

	public ScheduledWeekdayTrigger(List<Integer> runOnDays, int hourOfDay,
			int minuteOfDay) {
		super();
		this.runOnDays = runOnDays;
		this.hourOfDay = hourOfDay;
		this.minuteOfDay = minuteOfDay;
	}

	public List<Integer> getRunOnDays() {
		return runOnDays;
	}

	public void setRunOnDays(List<Integer> runOnDays) {
		this.runOnDays = runOnDays;
	}

	@Override
	public boolean isDue() {
		if (nextStart == null) {
			calculateNextStart();
		}

		Date now = new Date();

		return nextStart.before(now);
	}

	@Override
	public void notifyJobStarted() {

		lastRun = new Date();
		calculateNextStart();

	}

	@Override
	public void notifyJobFinished(boolean withErrors) {

		calculateNextStart(withErrors);

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
		Calendar cal = Calendar.getInstance();
		int weekday = cal.get(Calendar.DAY_OF_WEEK);

		// if we need to run today - then check if current time is before run
		// scheduled time
		if (runOnDays.contains(new Integer(weekday))
				&& ((cal.get(Calendar.HOUR_OF_DAY) < hourOfDay) || (cal
						.get(Calendar.HOUR_OF_DAY) == hourOfDay && cal
						.get(Calendar.MINUTE) < minuteOfDay))) {

			cal.set(Calendar.HOUR_OF_DAY, hourOfDay);
			cal.set(Calendar.MINUTE, minuteOfDay);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);

		} else {
			// get next run day from today - smallest diff to current day from
			// the scheduled days

			// check if at least one entry is after the current day as day
			// number
			int futureEntryFound = -1;
			int pastEntryFound = -1;
			for (Integer runOnDay : runOnDays) {
				if (runOnDay.intValue() > weekday) {
					if (futureEntryFound == -1) {
						futureEntryFound = runOnDay.intValue();
					} else if (runOnDay.intValue() < futureEntryFound) {
						futureEntryFound = runOnDay.intValue();
					}
				} else {
					if (pastEntryFound == -1) {
						pastEntryFound = runOnDay.intValue();
					} else if (runOnDay.intValue() < pastEntryFound) {
						pastEntryFound = runOnDay.intValue();
					}
				}
			}
			int days = 1;
			if (futureEntryFound != -1) {
				// we got a day in the current week we need to run on
				days = (futureEntryFound - weekday);

			} else if (futureEntryFound != -1) {
				// we got a day in the current week we need to run on
				days = (7 - weekday + pastEntryFound);
			}

			cal.add(Calendar.DAY_OF_YEAR, days);

			cal.set(Calendar.HOUR_OF_DAY, hourOfDay);
			cal.set(Calendar.MINUTE, minuteOfDay);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);

		}

		nextStart = cal.getTime();
	}

}
