/*
 * de.xwic.etlgine.loader.CSVExtractor 
 */
package de.xwic.etlgine.extractor;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import sun.nio.cs.StreamDecoder;
import au.com.bytecode.opencsv.CSVReader;
import de.xwic.etlgine.AbstractExtractor;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IExtractor;
import de.xwic.etlgine.IProcessContext;
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

	private InputStream input = null;
	
	private boolean autoMode = false;
	private boolean trimColumnName = false;
	private char initial_separator = separator;
	private char initial_quoteChar = quoteChar;
	

	public List<ICSVExtractorListener> listeners = new ArrayList<ICSVExtractorListener>();
	
	/**
	 * Add a listener.
	 * @param listener
	 */
	public void addCSVExtractorListener(ICSVExtractorListener listener) {
		listeners.add(listener);
	}
	
	/**
	 * Remove listener.
	 * @param listener
	 */
	public void removeCSVExtractorListener(ICSVExtractorListener listener) {
		listeners.remove(listener);
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.ILoader#close()
	 */
	public void close() throws ETLException {
		if (reader != null) {
			try {
				reader.close();
				reader = null;
			} catch (IOException e) {
				throw new ETLException("Error closing loader: " + e, e);
			}
		}
		if (input != null) {
			try {
				input.close();
				input = null;
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
					record.resetChangeFlag();
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
			throw new ETLException("Cannot handle a source of this type - FileSource type required.");
		}
		FileSource fsrc = (FileSource)source;
		try {
			recordNumber = 0;
			reachedEnd = false;

			BufferedReader rawReader = initializeStream(fsrc);
			
			class Helper {
				public void prepareRawReader(BufferedReader rawReader) throws IOException {
					for (int i = 0; i < skipLines; i++) {
						String skipLine = rawReader.readLine();
						for (ICSVExtractorListener listener : listeners) {
							listener.onLineSkipped(skipLine, i);
						}
					}
				}
				public int count(String s, String c) {
					return (s.length() - s.replace(c, "").length()) / c.length();
				}
				
				public Character find(String s, char[] chars) {
					Character found = null;
					int i = 0;
					for (char ch : chars) {
						Character c = ch;
						int j = count(s, c.toString());
						if (j > 0 && j > i) {
							i = j;
							found = c;
						}
					}
					return found;
				}
			};
			Helper helper = new Helper();
			helper.prepareRawReader(rawReader);
			
			if (autoMode) {
				// detect separator and quoteChar from first line
				String line = rawReader.readLine();
				if (line != null && line.length() > 0) {
					// check for separator and quoteChar
					char[] separators = new char[]{';',',','\t'};
					char[] quoteChars = new char[]{'"','\''};
					Character new_separator = helper.find(line, separators);
					Character new_quoteChar = helper.find(line, quoteChars);
					if (new_separator != null && new_quoteChar != null) {
						// check that it makes sense
						int count = helper.count(line, new_separator.toString() + new_quoteChar);
						if (count == 0) {
							throw new ETLException("AutoMode failed with detected separator (" + new_separator + ") and quoteChar (" + new_quoteChar + ")");
						}
					}
					// restore initial settings
					separator = initial_separator;
					quoteChar = initial_quoteChar;
					if (new_separator != null) {
						separator = new_separator;
						context.getMonitor().logInfo("AutoMode identified separator " + new_separator + " (0x" + Integer.toHexString(new_separator) + ")");
					}
					if (new_quoteChar != null) {
						quoteChar = new_quoteChar;
						context.getMonitor().logInfo("AutoMode identified quoteChar " + new_quoteChar + " (0x" + Integer.toHexString(new_quoteChar) + ")");
					}
				}
				rawReader = initializeStream(fsrc);
				helper.prepareRawReader(rawReader);
			}
			
			context.getMonitor().logInfo("CSVExtractor uses separator " + separator + " (0x" + Integer.toHexString(separator) + ") and quoteChar " + quoteChar + " (0x" + Integer.toHexString(quoteChar) + ")");
			reader = new CSVReader(rawReader, separator, quoteChar, 0);
			if (containsHeader) {
				String[] header = reader.readNext();
				if (header == null) {
					// file is empty!
					reachedEnd = true;
				} else {
					expectedColumns = header.length;
					int idx = 0;
					for (String colName : header) {
						if (trimColumnName) {
							colName = colName.trim();
						}
						int i = 1;
						for (String name = colName; dataSet.containsColumn(name); i++) {
							name = colName + i;
						}
						if (i > 1) {
							// correct column name
							colName = colName + i;
						}
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
	 * 
	 * @param fsrc
	 * @return
	 * @throws IOException
	 */
	protected BufferedReader initializeStream(FileSource fsrc) throws IOException {
		if (input != null) {
			input.close();
		}
		input = fsrc.getInputStream();
		
		// determine encoding
		String encoding = fsrc.getEncoding(); // null is java default
		byte[] encoding_tag = new byte[2];
		// find source encoding
		if (input.read(encoding_tag, 0, 2) == 2) {
			if (encoding_tag[0] == (byte)0xFF && encoding_tag[1] == (byte)0xFE) {
				// used by Cognos CSV reports
				encoding = "UTF-16LE";
			} else if (encoding_tag[0] == (byte)0xFE && encoding_tag[1] == (byte)0xFF) {
				// used by SFDC as of 2012-07-26
				encoding = "UTF-16BE";
				if (input.read(encoding_tag, 0, 2) == 2 && encoding_tag[0] == (byte)0xFE && encoding_tag[1] == (byte)0xFF) {
					// 2012-10-10: on 2012-07-26 an incorrect encoding tag (BOM) of -2,-1,-2,-1 instead of -2,-1 was multiple times downloaded, so that get fixed here...
				} else {
					input.close();
					input = fsrc.getInputStream();
					input.read(encoding_tag, 0, 2);
				}
			} else if (encoding_tag[0] == (byte)0xEF && encoding_tag[1] == (byte)0xBB) {
				// check for UTF-8
				if (input.read(encoding_tag, 0, 1) == 1 && encoding_tag[0] == (byte)0xBF) {
					// it's UTF-8, first encountered 2012-12-11 from the eBI answers report download format CSV
					encoding = "UTF-8";
				} else {
					// should never happen
					input.close();
					input = fsrc.getInputStream();
					input.read(encoding_tag, 0, 2);
				}
			} else {
				input.close();
				input = fsrc.getInputStream();
			}
		} else {
			input.close();
			input = fsrc.getInputStream();
		}
		fsrc.setEncoding(encoding);
		
		BufferedReader rawReader = new BufferedReader(StreamDecoder.forInputStreamReader(input, fsrc, fsrc.getEncoding()));
		
		return rawReader;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.AbstractExtractor#postSourceProcessing(de.xwic.etlgine.IProcessContext)
	 */
	@Override
	public void postSourceProcessing(IProcessContext processContext) throws ETLException {
		super.postSourceProcessing(processContext);
		
		close();
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
		initial_separator = separator;
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
		initial_quoteChar = quoteChar;
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

	/**
	 * @return the autoMode
	 */
	public boolean isAutoMode() {
		return autoMode;
	}

	/**
	 * @param autoMode the autoMode to set
	 */
	public void setAutoMode(boolean autoMode) {
		this.autoMode = autoMode;
	}

	/**
	 * @return the trimColumnName
	 */
	public boolean isTrimColumnName() {
		return trimColumnName;
	}

	/**
	 * @param trimColumnName the trimColumnName to set
	 */
	public void setTrimColumnName(boolean trimColumnName) {
		this.trimColumnName = trimColumnName;
	}

}
