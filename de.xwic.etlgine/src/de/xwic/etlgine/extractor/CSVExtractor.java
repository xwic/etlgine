/*
 * de.xwic.etlgine.loader.CSVExtractor 
 */
package de.xwic.etlgine.extractor;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import sun.nio.cs.StreamDecoder;
import au.com.bytecode.opencsv.CSVReader;
import de.xwic.etlgine.AbstractExtractor;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IExtractor;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.ISource;
import de.xwic.etlgine.sources.FileSource;

/**
 * Extract data from a CSV file using OpenCSV Reader.
 * @author lippisch
 */
public class CSVExtractor extends AbstractExtractor implements IExtractor {

	private CSVReader reader = null;

	private boolean containsHeader = true;
	private boolean reachedEnd = false;
	private char separator = ',';
	private char quoteChar = '"';
	private int skipLines = 0;
	private int recordNumber = 0;

	private int expectedColumns = -1;
	
	private IDataSet dataSet;
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#close()
	 */
	public void close() throws ETLException {
		if (reader != null) {
			try {
				reader.close();
			} catch (IOException e) {
				throw new ETLException("Error closing loader: " + e, e);
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#getNextRecord()
	 */
	public IRecord getNextRecord() throws ETLException {
		if (!reachedEnd) {
			recordNumber++;
			try {
				IRecord record = context.newRecord();
				String[] data = reader.readNext();
				if (data == null) {
					reachedEnd = true;
				} else { 
					if (containsHeader && data.length != expectedColumns) {
						record.markInvalid("Expected " + expectedColumns + " but record contained " + data.length + " columns. (row=" + recordNumber + ")");
					}
					
					// try to load what's there
					int max = Math.min(expectedColumns, data.length);
					for (int i = 0; i < max; i++) {
						IColumn column = dataSet.getColumnByIndex(i);
						record.setData(column, data[i]);
					}
					
					return record;
				}
			} catch (IOException e) {
				throw new ETLException("Error reading record at row " + recordNumber + ": " + e, e);
			}
			
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#openSource(de.xwic.etlgine.ISource)
	 */
	public void openSource(ISource source, IDataSet dataSet) throws ETLException {
		
		this.dataSet = dataSet;
		
		if (!(source instanceof FileSource)) {
			throw new ETLException("Can not handle a source of this type - FileSource type required.");
		}
		FileSource fsrc = (FileSource)source;
		try {
			recordNumber = 0;
			reachedEnd = false;
			
			InputStream in = new FileInputStream(fsrc.getFile());
			
			// determine encoding
			if (fsrc.getEncoding() == null) {
				String encoding = null; // java default
				byte[] encoding_tag = new byte[2];
				// find source encoding
				if (in.read(encoding_tag, 0, 2) == 2 && encoding_tag[0] == -1 && encoding_tag[1] == -2) {
					// used by Cognos CSV reports
					encoding = "UTF-16LE";
				} else {
					in.close();
					in = new FileInputStream(fsrc.getFile());
				}
				fsrc.setEncoding(encoding);
			}
			reader  = new CSVReader(new BufferedReader(StreamDecoder.forInputStreamReader(in, fsrc.getFile(), fsrc.getEncoding())), separator, quoteChar, skipLines);
			if (containsHeader) {
				String[] header = reader.readNext();
				if (header == null) {
					// file is empty!
					reachedEnd = true;
				} else {
					expectedColumns = header.length;
					int idx = 0;
					for (String colName : header) {
						dataSet.addColumn(colName, idx++);
					}
				}
			}
		} catch (FileNotFoundException e) {
			throw new ETLException("Source file not found (" + source.getName() + ") : " + e, e);
		} catch (IOException e) {
			throw new ETLException("Error reading file (" + source.getName() + ") : " + e, e);
		}
		
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

	/**
	 * @return the skipLines
	 */
	public int getSkipLines() {
		return skipLines;
	}

	/**
	 * @param skipLines the skipLines to set
	 */
	public void setSkipLines(int skipLines) {
		this.skipLines = skipLines;
	}

	
}
