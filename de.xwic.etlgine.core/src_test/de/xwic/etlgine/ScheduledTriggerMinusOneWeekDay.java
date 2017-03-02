/*
 * Copyright (c) NetApp Inc. - All Rights Reserved
 * 
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 * 
 * com.netapp.ngs.etl.trigger.ScheduledTriggerMinusOneWeekDay 
 */
package de.xwic.etlgine;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.Calendar;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.xwic.etlgine.trigger.ScheduledTrigger;
import de.xwic.etlgine.trigger.ScheduledTrigger.Type;

/**
 * @author ionut
 *
 */
public class ScheduledTriggerMinusOneWeekDay {

	private ScheduledTrigger trigger;

	@Before
	public void setup() {
	}

	@Test
	public void testBeforeCurrentTimeSkipToday() {
		Calendar today = Calendar.getInstance();
		int dayOfWeek = today.get(Calendar.DAY_OF_WEEK);
		//-1 hour
		today.add(Calendar.HOUR_OF_DAY, -1);

		//schedule daily except today and one hour in the past of crt time
		trigger = new ScheduledTrigger(Type.DAILY_EXCEPT_ONE_WEEK_DAY, dayOfWeek, today.get(Calendar.HOUR_OF_DAY),
				today.get(Calendar.MINUTE));
		//the last run is set to current time on creation therefore the next start is set to next day
		assertFalse(trigger.isDue());
		
		today.add(Calendar.DAY_OF_WEEK, -1);
		
		//set the flag to indicate that the job has run yesterday
		setLastRun(trigger, today);
		
		//this will trigger the computing of next start which should skip the current day and should be set on tomorrow 
		trigger.notifyJobFinished(false);
		
		//isDue has a condition that check if the current scheduled date is before last run date in order to skip all passed time jobs
		assertFalse(trigger.isDue());
	}

	@Test
	public void testAfterCurrentTimeSkipToday() {
		Calendar today = Calendar.getInstance();
		int dayOfWeek = today.get(Calendar.DAY_OF_WEEK);
		//+1 hour
		today.add(Calendar.HOUR_OF_DAY, 1);

		//schedule daily except today and one hour in the future from crt time
		trigger = new ScheduledTrigger(Type.DAILY_EXCEPT_ONE_WEEK_DAY, dayOfWeek, today.get(Calendar.HOUR_OF_DAY),
				today.get(Calendar.MINUTE));
		//the last run is set to current time on creation and the current day is skipped therefore the next start is set to next day
		assertFalse(trigger.isDue());
		
		//make it yesterday
		today.add(Calendar.DAY_OF_WEEK, -1);
		//set the flag to indicate that the job has run yesterday
		setLastRun(trigger, today);
		//this will trigger the computing of next start
		trigger.notifyJobFinished(false);
		assertFalse(trigger.isDue());
	}

	@Test
	public void testBeforeCurrentTime() {
		Calendar today = Calendar.getInstance();

		//-1 hour
		today.add(Calendar.HOUR_OF_DAY, -1);
		int hour = today.get(Calendar.HOUR_OF_DAY);
		int minute = today.get(Calendar.MINUTE);

		//make it yesterday
		today.add(Calendar.DAY_OF_WEEK, -1);
		int dayOfWeek = today.get(Calendar.DAY_OF_WEEK);

		//schedule daily except yesterday and one hour in the past of crt time
		trigger = new ScheduledTrigger(Type.DAILY_EXCEPT_ONE_WEEK_DAY, dayOfWeek, hour, minute);

		//the last run is set to current time on creation but all passed time triggers for today are scheduled in the next day
		assertFalse(trigger.isDue());
		
		//set the flag to indicate that the job has run yesterday
		setLastRun(trigger, today);
		//this will trigger the computing of next start
		trigger.notifyJobFinished(false);
		//now that the last run is set on yesterday the date is compared with the current time and set as due
		assertTrue(trigger.isDue());

	}

	@Test
	public void testAfterCurrentTime() {
		Calendar today = Calendar.getInstance();

		//+1 hour
		today.add(Calendar.HOUR_OF_DAY, 1);
		int hour = today.get(Calendar.HOUR_OF_DAY);
		int minute = today.get(Calendar.MINUTE);

		//make it yesterday
		today.add(Calendar.DAY_OF_WEEK, -1);
		int dayOfWeek = today.get(Calendar.DAY_OF_WEEK);

		//schedule daily except yesterday and one hour in the future
		trigger = new ScheduledTrigger(Type.DAILY_EXCEPT_ONE_WEEK_DAY, dayOfWeek, hour, minute);
		//the last run is set to current time on creation therefore the next start is set to today but in the future so it is 
		//not yet due
		assertFalse(trigger.isDue());
		
		today.add(Calendar.DAY_OF_WEEK, -1);
		
		//set the flag to indicate that the job has run yesterday
		setLastRun(trigger, today);
		//this will trigger the computing of next start for today in the future
		trigger.notifyJobFinished(false);
		
		//scheduled but not yet due
		assertFalse(trigger.isDue());
	}
	

	@After
	public void clean() {

		//daily except Sunday = 1
		trigger = null;
	}

	/**
	 * @param today
	 */
	private void setLastRun(ScheduledTrigger trigger, Calendar today) {
		try {
			Field lastRunField = trigger.getClass().getDeclaredField("lastRun");
			lastRunField.setAccessible(true);
			lastRunField.set(trigger, today.getTime());
			lastRunField.setAccessible(true);
		} catch (Exception e) {
			fail("Cannot set lastRun");
		}
	}

}
