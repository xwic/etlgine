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
import java.util.Calendar;
import java.util.Date;

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
	
	protected String[] patternTrials = { "yyyy-MM-dd HH:mm:ss", "MM/dd/yyyy HH:mm:ss", "dd.MM.yyyy HH:mm:ss", "MM/dd/yyyy", "yyyy-MM-dd" };
	
	protected String[] columns;
	
	protected boolean checkDate = false;
	
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
			

			if (dateFormat == null) { // try to find a format
				if (pattern != null) {
					dateFormat = new SimpleDateFormat(pattern);
				} else {
					// try to find one that works...
					for (String p : patternTrials) {
						SimpleDateFormat df = new SimpleDateFormat(p);
						try {
							df.parse(s);
							dateFormat = df;
							pattern = p;
							break; // found one!
						} catch (Exception e) {
							// continue and check other formats.
						}
					}
					if (dateFormat == null) { // none found
						throw new ETLException("Can not find a matching date pattern for string: " + s);
					}
				}
			}

			
			try {
				Date d = dateFormat.parse(s);
				
				// check date
				d = checkDate(d);
			
				record.setData(name, d);
			} catch (ParseException e) {
				throw new ETLException("Error parsing date string '" + s + "' in field " + name, e);
			}
		}
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
}
