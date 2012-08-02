/**
 * 
 */
package de.xwic.etlgine;

import java.util.Calendar;
import java.util.Random;


/**
 * Used in test cases. Returns 10.000 random entries.
 * @author Developer
 */
public class TestRndExtractor extends AbstractExtractor {

	private int entries = 1;
	private int count = 0;
	private Random random = new Random(System.currentTimeMillis());
	
	private String[] NAMES = { "Jens", "Florian", "Peter", "Heinz", "Paul", "Martin", "Alfred", "Simon", "Hugo" };
	
	public void close() throws ETLException {

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IExtractor#getNextRecord()
	 */
	public IRecord getNextRecord() throws ETLException {
		
		if (count >= entries) {
			return null;
		}
		count++;

		IRecord record = context.newRecord();
		
		// randomize date
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, random.nextInt(365));
		
		record.setData("Date", cal.getTime());
		record.setData("Bookings", new Double(random.nextDouble() * 100000));
		record.setData("Name", NAMES[random.nextInt(NAMES.length)]);
		
		return record;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IExtractor#openSource(de.xwic.etlgine.ISource, de.xwic.etlgine.IDataSet)
	 */
	public void openSource(ISource source, IDataSet dataSet) throws ETLException {

		if (!(source instanceof TestRndSource)) {
			throw new ETLException("The source must be of type TestRndSource");
		}
		TestRndSource trs = (TestRndSource)source;
		entries = trs.getEntries();
		
		dataSet.addColumn("Name");
		dataSet.addColumn("Date")
			.setTypeHint(IColumn.DataType.DATETIME);
		dataSet.addColumn("Bookings")
			.setTypeHint(IColumn.DataType.DOUBLE);
		
		count = 0;
		
	}

}
