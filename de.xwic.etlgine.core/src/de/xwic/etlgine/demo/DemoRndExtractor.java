package de.xwic.etlgine.demo;

import de.xwic.etlgine.*;

import java.util.Calendar;
import java.util.Random;

/**
 * Used in test cases and the demo jobs. Returns 10.000 random entries.
 *
 */
public class DemoRndExtractor  extends AbstractExtractor {

    private int entries = 1;
    private int count = 0;
    private Random random = new Random(System.currentTimeMillis());
    private int cycles = 0;

    private String[] NAMES = { "Jens", "Florian", "Peter", "Heinz", "Paul", "Martin", "Alfred", "Simon", "Hugo", null /* dynamic size based on cycles */ };

    public void close() throws ETLException {
    	cycles++;
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
        String name = NAMES[random.nextInt(NAMES.length)];
        if (name == null) {
        	for (String n : NAMES) {
        		if (n == null) continue;
        		for (int i = 0; i <= cycles; i++) {
        			if (name == null) {
        				name = n;
        			} else {
        				name += "-" + n;
        			}
        		}
        	}
        }
        record.setData("Name", name);

        return record;
    }

    /* (non-Javadoc)
     * @see de.xwic.etlgine.IExtractor#openSource(de.xwic.etlgine.ISource, de.xwic.etlgine.IDataSet)
     */
    public void openSource(ISource source, IDataSet dataSet) throws ETLException {

        if (!(source instanceof DemoRndSource)) {
            throw new ETLException("The source must be of type DemoRndSource");
        }
        DemoRndSource trs = (DemoRndSource)source;
        entries = trs.getEntries();

        dataSet.addColumn("Name");
        dataSet.addColumn("Date")
                .setTypeHint(IColumn.DataType.DATETIME);
        dataSet.addColumn("Bookings")
                .setTypeHint(IColumn.DataType.DOUBLE);

        count = 0;

    }
}
