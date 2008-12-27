/**
 * 
 */
package de.xwic.etlgine.extractor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

import de.xwic.etlgine.AbstractExtractor;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.ISource;
import de.xwic.etlgine.sources.FileSource;

/**
 * Extract data from an XLS file.
 * @author lippisch
 */
public class XLSExtractor extends AbstractExtractor {

	private IDataSet dataSet;
	private InputStream inputStream;
	private HSSFWorkbook workbook;
	
	private boolean containsHeader = true;
	private String sheetName = null;
	private int sheetIndex = 0;
	private int startRow = 0;
	
	private int currRow = 0;
	private int maxRow = 0;
	private int expectedColumns = 0;

	private HSSFSheet sheet;
	private boolean reachedEnd = false;

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IExtractor#close()
	 */
	public void close() throws ETLException {
		try {
			if (inputStream != null) {
				inputStream.close();
				workbook = null;
				sheet = null;
				inputStream = null;
			}
		} catch (IOException e) {
			throw new ETLException("Error closing input stream: " + e, e);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.impl.AbstractExtractor#postSourceProcessing(de.xwic.etlgine.IETLContext)
	 */
	@Override
	public void postSourceProcessing(IProcessContext processContext) throws ETLException {
		super.postSourceProcessing(processContext);
		close();
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IExtractor#getNextRecord()
	 */
	public IRecord getNextRecord() throws ETLException {
		
		if (!reachedEnd) {
			try {
				IRecord record = processContext.newRecord();
				HSSFRow row;
				// read until we find a row that contains data.
				while ((row = sheet.getRow(currRow)) == null && currRow <= maxRow) {
					currRow++;
				}
				
				if (row == null) {
					reachedEnd = true;
				} else { 
					if (containsHeader && row.getLastCellNum() < expectedColumns) {
						record.markInvalid("Expected " + expectedColumns + " columns but row contained " + row.getLastCellNum() + " columns. (row=" + currRow + ")");
					}
					
					// try to load what's there
					int max = Math.min(expectedColumns, row.getLastCellNum()) + 1;
					for (int i = 0; i < max; i++) {
						IColumn column = dataSet.getColumnByIndex(i);
						record.setData(column, readObject(row, i));
					}
					
					currRow++;
					if (currRow > maxRow) {
						reachedEnd = true;
					}
					return record;
				}
				
				
			} catch (Exception e) {
				throw new ETLException("Error reading record at row " + currRow + ": " + e, e);
			}
			
		}

		return null;
	}

	/**
	 * @param row
	 * @param i
	 * @return
	 */
	private Object readObject(HSSFRow row, int col) {
		HSSFCell cell = row.getCell(col);
		if (cell != null) {
			switch (cell.getCellType()) {
			case HSSFCell.CELL_TYPE_NUMERIC:
				return cell.getNumericCellValue();
			case HSSFCell.CELL_TYPE_STRING:
				return cell.getRichStringCellValue().getString();
			}
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IExtractor#openSource(de.xwic.etlgine.ISource, de.xwic.etlgine.IDataSet)
	 */
	public void openSource(ISource source, IDataSet dataSet) throws ETLException {
		
		this.dataSet = dataSet;
		
		if (!(source instanceof FileSource)) {
			throw new ETLException("Can not handle a source of this type - FileSource type required.");
		}
		FileSource fsrc = (FileSource)source;
		try {
			inputStream = new FileInputStream(fsrc.getFile());
			workbook = new HSSFWorkbook(new POIFSFileSystem(inputStream));
			
			if (containsHeader) {
				if (sheetName != null) {
					sheet = workbook.getSheet(sheetName);
				} else {
					sheet = workbook.getSheetAt(sheetIndex);
				}
				if (sheet == null) {
					processContext.getMonitor().logError("The specified sheet was not found!");
					reachedEnd = true;
				} else {
					HSSFRow row = sheet.getRow(startRow);
					if (row == null) {
						// file is empty!
						processContext.getMonitor().logWarn("The specified header row does not exist. Assume that the file is empty.");
						reachedEnd = true;
					} else {
						int lastNum = row.getLastCellNum();
						int lastCol = 0;
						for (int c = 0; c < lastNum; c++) {
							HSSFCell cell = row.getCell(c);
							if (cell != null) {
								if (cell.getCellType() == HSSFCell.CELL_TYPE_STRING) {
									HSSFRichTextString value = cell.getRichStringCellValue();
									String text = value != null && value.getString() != null ? value.getString() : "";
									if (text.length() != 0) {
										if (c > lastCol) {
											lastCol = c;
										}
										dataSet.addColumn(text, c);
									}
								}
							}
						}
						expectedColumns = lastCol;
						currRow = startRow + 1;
						maxRow = sheet.getLastRowNum();
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
	 * @return the sheetName
	 */
	public String getSheetName() {
		return sheetName;
	}

	/**
	 * @param sheetName the sheetName to set
	 */
	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	/**
	 * @return the sheetIndex
	 */
	public int getSheetIndex() {
		return sheetIndex;
	}

	/**
	 * @param sheetIndex the sheetIndex to set
	 */
	public void setSheetIndex(int sheetIndex) {
		this.sheetIndex = sheetIndex;
	}

	/**
	 * @param startRow the startRow to set
	 */
	public void setStartRow(int startRow) {
		this.startRow = startRow;
	}

	/**
	 * @return the startRow
	 */
	public int getStartRow() {
		return startRow;
	}

}
