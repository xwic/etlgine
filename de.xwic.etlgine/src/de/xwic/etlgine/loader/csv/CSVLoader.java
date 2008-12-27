/**
 * 
 */
package de.xwic.etlgine.loader.csv;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import au.com.bytecode.opencsv.CSVWriter;
import de.xwic.etlgine.AbstractLoader;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.ILoader;
import de.xwic.etlgine.IRecord;

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
	
	private int colCount = 0;
	private IColumn[] exportCols = null;
	
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
	public void initialize(IProcessContext processContext) throws ETLException {
		
		try {
			writer = new CSVWriter(new FileWriter(filename), separator, quoteChar);
		} catch (IOException e) {
			throw new ETLException("Error creating file " + filename + ": " + e, e);
		}

	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#onProcessFinished(de.xwic.etlgine.IETLContext)
	 */
	@Override
	public void onProcessFinished(IProcessContext processContext) throws ETLException {
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
	public void preSourceProcessing(IProcessContext processContext) {

		if (containsHeader) {
			List<IColumn> columns = processContext.getDataSet().getColumns();
			for (IColumn col : columns) {
				if (!col.isExclude()) {
					colCount++;
				}
			}
			String[] data = new String[colCount];
			exportCols = new IColumn[colCount];
			int i = 0;
			for (IColumn col : columns) {
				if (!col.isExclude()) {
					exportCols[i] = col;
					data[i++] = col.getName();
				}
			}
			writer.writeNext(data);
		}
		
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#processRecord(de.xwic.etlgine.IETLContext, de.xwic.etlgine.IRecord)
	 */
	public void processRecord(IProcessContext processContext, IRecord record) throws ETLException {

		String[] data = new String[colCount];
		for (int i = 0; i < data.length; i++) {
			Object value = record.getData(exportCols[i]);
			data[i] = value != null ? value.toString() : "";
		}
		writer.writeNext(data);
		
	}

}
