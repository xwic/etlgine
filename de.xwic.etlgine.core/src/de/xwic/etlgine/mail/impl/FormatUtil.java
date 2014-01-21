/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.mail.impl;

import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * @author lippisch
 */
public class FormatUtil {

	private Locale locale;

	/**
	 * @param locale
	 */
	public FormatUtil(Locale locale) {
		super();
		this.locale = locale;
	}
	
	/**
	 * Format a date in short format.
	 * @param date
	 * @return
	 */
	public String date(Date date) {
		if (date == null) {
			return "";
		}
		return DateFormat.getDateInstance(DateFormat.SHORT, locale).format(date);
	}

	/**
	 * Format a date in short format.
	 * @param date
	 * @return
	 */
	public String dateTime(Date date) {
		if (date == null) {
			return "";
		}
		return DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT, locale).format(date);
	}

	/**
	 * Format a date in short format.
	 * @param date
	 * @return
	 */
	public String time(Date date) {
		if (date == null) {
			return "";
		}
		return DateFormat.getTimeInstance(DateFormat.SHORT, locale).format(date);
	}

}
