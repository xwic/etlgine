/**
 * $Id: $
 *
 * Copyright (c) 2008 Network Appliance.
 * All rights reserved.

 * de.xwic.etlgine.transformer.DateTransformer.java
 * Created on Jan 26, 2009
 * 
 * @author JBORNEMA
 */
package de.xwic.etlgine.transformer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import de.xwic.etlgine.AbstractTransformer;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;

/**
 * Created on Jan 26, 2009
 * @author JBORNEMA
 */

public class DateTransformer extends AbstractTransformer {

	protected SimpleDateFormat dateFormat;
	
	protected String pattern = null;
	
	protected List<SimpleDateFormat> dateFormats = new ArrayList<SimpleDateFormat>();
	
	protected String[] columns;
	
	protected boolean checkDate = false;
	
	protected boolean retryOnError = false;
	
	@Override
	public void initialize(IProcessContext processContext) throws ETLException {
		super.initialize(processContext);
		
		// initialize default patterns
		dateFormats.add(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
		dateFormats.add(new SimpleDateFormat("MM/dd/yyyy HH:mm:ss"));
		dateFormats.add(new SimpleDateFormat("dd.MM.yyyy HH:mm:ss"));
		dateFormats.add(new SimpleDateFormat("MM/dd/yyyy"));
		dateFormats.add(new SimpleDateFormat("yyyy-MM-dd"));
		dateFormats.add(new SimpleDateFormat("dd-MMM-yyyy"));
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.AbstractTransformer#processRecord(de.xwic.etlgine.IProcessContext, de.xwic.etlgine.IRecord)
	 */
	@Override
	public void processRecord(IProcessContext processContext, IRecord record) throws ETLException {
		super.processRecord(processContext, record);
		
		// iterate columns and generate Date objects
		for (String name : columns) {
			Object value = record.getData(name);
			if (value instanceof Date || value == null || !(value instanceof String)) {
				continue;
			}

			String s = (String)value;

			if (s.length() == 0) {
				continue;
			}
			
			try {
				Date d = parse(s);
			
				record.setData(name, d);
			} catch (ParseException e) {
				throw new ETLException("Error parsing date string '" + s + "' in field " + name, e);
			}
		}
	}

	/**
	 * Parse date using current dateFormat (initializes if null).
	 * @param s
	 * @param retried 
	 * @return
	 * @throws ETLException 
	 * @throws ParseException 
	 */
	public Date parse(String s) throws ETLException, ParseException {
		if (s == null || s.length() == 0) {
			return null;
		}
		
		if (dateFormat == null) { // try to find a format
			if (pattern != null) {
				dateFormat = new SimpleDateFormat(pattern);
			} else {
				// try to find one that works...
				for (SimpleDateFormat df : getDateFormats()) {
					try {
						df.parse(s);
						dateFormat = df;
						pattern = df.toPattern();
						break; // found one!
					} catch (Exception e) {
						// continue and check other formats.
					}
				}
				if (dateFormat == null) { // none found
					throw new ETLException("Cannot find a matching date pattern for string: " + s);
				}
			}
		}
		Date d = null;
		try {
			d = dateFormat.parse(s);
		} catch (ParseException pe) {
			if (retryOnError) {
				// recursive call that doesn't end in infinite loop because dateFormat and pattern are null'd
				dateFormat = null;
				pattern = null;
				d = parse(s);
			} else {
				throw pe;
			}
		}
		
		// check date
		d = checkDate(d);
	
		return d;
	}
	
	/**
	 * @return the columns
	 */
	public String[] getColumns() {
		return columns;
	}
	
	/**
	 * @param columns the columns to set
	 */
	public void setColumns(String... columns) {
		this.columns = columns;
	}
	
	/**
	 * @param pattern the pattern to set
	 */
	public void setPattern(String format) {
		this.pattern = format;
	}
	
	/**
	 * @return the pattern
	 */
	public String getPattern() {
		return pattern;
	}
	
	/**
	 * @return the dateFormat
	 */
	public SimpleDateFormat getDateFormat() {
		if (dateFormat == null) {
			dateFormat = new SimpleDateFormat(pattern);
		}
		return dateFormat;
	}
	
	/**
	 * @param dateFormat the dateFormat to set
	 */
	public void setDateFormat(SimpleDateFormat dateFormat) {
		this.dateFormat = dateFormat;
	}

	/**
	 * @param checkDate the checkDate to set
	 */
	public void setCheckDate(boolean checkDate) {
		this.checkDate = checkDate;
	}
	
	/**
	 * @return the checkDate
	 */
	public boolean isCheckDate() {
		return checkDate;
	}
	
	/**
	 * Check date with custom logic.
	 * @param d
	 * @return
	 */
	protected Date checkDate(Date d) {
		if (checkDate) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(d);
			int year = cal.get(Calendar.YEAR);
			if (year < 100) {
				// ups, looks like 2000 is missing
				cal.add(Calendar.YEAR, 2000);
				d = cal.getTime();
			}
		}
		return d;
	}

	/**
	 * @return the dateFormats
	 */
	public List<SimpleDateFormat> getDateFormats() {
		return dateFormats;
	}

	/**
	 * @param dateFormats the dateFormats to set
	 */
	public void setDateFormats(List<SimpleDateFormat> dateFormats) {
		this.dateFormats = dateFormats;
	}

	/**
	 * @return the retryOnError
	 */
	public boolean isRetryOnError() {
		return retryOnError;
	}

	/**
	 * @param retryOnError the retryOnError to set
	 */
	public void setRetryOnError(boolean retryOnError) {
		this.retryOnError = retryOnError;
	}
}
