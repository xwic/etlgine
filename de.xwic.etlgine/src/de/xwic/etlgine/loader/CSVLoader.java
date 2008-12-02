/**
 * 
 */
package de.xwic.etlgine.loader;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import au.com.bytecode.opencsv.CSVWriter;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IETLContext;
import de.xwic.etlgine.ILoader;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.impl.AbstractLoader;
import de.xwic.etlgine.impl.Context;

/**
 * Writes the data into a CSV file.
 * @author Lippisch
 */
public class CSVLoader extends AbstractLoader implements ILoader {

	private boolean containsHeader = true;
	private char separator = ',';
	private char quoteChar = '"';
	private String filename = null;
	private CSVWriter writer;
	
	/**
	 * @return the filename
	 */
	public String getFilename() {
		return filename;
	}

	/**
	 * @param filename the filename to set
	 */
	public void setFilename(String filename) {
		this.filename = filename;
	}

	/**
	 * @return the containsHeader
	 */
	public boolean isContainsHeader() {
		return containsHeader;
	}

	/**
	 * @param containsHeader the containsHeader to set
	 */
	public void setContainsHeader(boolean containsHeader) {
		this.containsHeader = containsHeader;
	}

	/**
	 * @return the separator
	 */
	public char getSeparator() {
		return separator;
	}

	/**
	 * @param separator the separator to set
	 */
	public void setSeparator(char separator) {
		this.separator = separator;
	}

	/**
	 * @return the quoteChar
	 */
	public char getQuoteChar() {
		return quoteChar;
	}

	/**
	 * @param quoteChar the quoteChar to set
	 */
	public void setQuoteChar(char quoteChar) {
		this.quoteChar = quoteChar;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#initialize(de.xwic.etlgine.IETLContext)
	 */
	@Override
	public void initialize(IETLContext context) throws ETLException {
		
		try {
			writer = new CSVWriter(new FileWriter(filename), separator, quoteChar);
		} catch (IOException e) {
			throw new ETLException("Error creating file " + filename + ": " + e, e);
		}

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#onProcessFinished(de.xwic.etlgine.impl.Context)
	 */
	@Override
	public void onProcessFinished(Context context) throws ETLException {
		try {
			writer.flush();
			writer.close();
		} catch (IOException e) {
			throw new ETLException("Error closing writer: " + e, e);
		}
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#preSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	@Override
	public void preSourceProcessing(IETLContext context) {

		if (containsHeader) {
			List<IColumn> columns = context.getDataSet().getColumns();
			String[] data = new String[columns.size()];
			for (int i = 0; i < data.length; i++) {
				data[i] = columns.get(i).getName();
			}
			writer.writeNext(data);
		}
		
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#processRecord(de.xwic.etlgine.IETLContext, de.xwic.etlgine.IRecord)
	 */
	public void processRecord(IETLContext context, IRecord record) throws ETLException {

		List<IColumn> columns = context.getDataSet().getColumns();
		String[] data = new String[columns.size()];
		for (int i = 0; i < data.length; i++) {
			IColumn column = columns.get(i);
			Object value = record.getData(column);
			data[i] = value != null ? value.toString() : "";
		}
		writer.writeNext(data);

		
	}

}
