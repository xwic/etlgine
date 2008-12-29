/**
 * 
 */
package de.xwic.etlgine.extractor.xls;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

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

/**
 * Extract data from an XLS file.
 * @author lippisch
 */
public class XLSExtractor extends AbstractExtractor {

	public final static String COL_SHEETNAME = "_XLS_SHEETNAME";
	
	private IDataSet dataSet;
	private InputStream inputStream;
	private HSSFWorkbook workbook;
	
	private XLSFileSource currSource = null;
	
	private int currRow = 0;
	private int maxRow = 0;
	private int expectedColumns = 0;

	private List<String> currCols = null;
	private List<HSSFSheet> sheets = null;
	private List<String> sheetNames = null;
	private int sheetIdx = 0;
	private HSSFSheet currSheet = null;
	private boolean reachedEnd = false;

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IExtractor#close()
	 */
	public void close() throws ETLException {
		try {
			if (inputStream != null) {
				inputStream.close();
				workbook = null;
				sheets = null;
				currSheet = null;
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
				while ((row = currSheet.getRow(currRow)) == null) {
					currRow++;
					if (currRow > maxRow) {
						sheetIdx++;
						if (sheetIdx >= sheets.size()) {
							reachedEnd = true;
						} else {
							initSheet(sheetIdx);
						}
					}
				}
				
				if (row == null) {
					reachedEnd = true;
				} else {
					record.setData(COL_SHEETNAME, sheetNames.get(sheetIdx));
					if (currSource.isContainsHeader() && row.getLastCellNum() < expectedColumns) {
						record.markInvalid("Expected " + expectedColumns + " columns but row contained " + row.getLastCellNum() + " columns. (row=" + currRow + ")");
					}
					
					// try to load what's there
					int max = Math.min(expectedColumns, row.getLastCellNum()) + 1;
					for (int i = 0; i < max; i++) {
						String name = this.currCols.get(i);
						record.setData(name, XLSTool.getObject(row, i));
					}
					
					currRow++;
					if (currRow > maxRow) {
						sheetIdx++;
						if (sheetIdx >= sheets.size()) {
							reachedEnd = true;
						} else {
							initSheet(sheetIdx);
						}
					}
					return record;
				}
				
				
			} catch (Exception e) {
				throw new ETLException("Error reading record at row " + currRow + ": " + e, e);
			}
			
		}

		return null;
	}


	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IExtractor#openSource(de.xwic.etlgine.ISource, de.xwic.etlgine.IDataSet)
	 */
	public void openSource(ISource source, IDataSet dataSet) throws ETLException {
		
		this.dataSet = dataSet;
		
		IColumn col = dataSet.addColumn(COL_SHEETNAME);
		col.setExclude(true);
		
		if (!(source instanceof XLSFileSource)) {
			throw new ETLException("Can not handle a source of this type - XLSFileSource type required.");
		}
		currSource = (XLSFileSource)source;
		try {
			inputStream = new FileInputStream(currSource.getFile());
			workbook = new HSSFWorkbook(new POIFSFileSystem(inputStream));
			
			if (currSource.isContainsHeader()) {
				sheets = new ArrayList<HSSFSheet>();
				sheetNames = new ArrayList<String>();
				if (currSource.getSheetNames() != null && currSource.getSheetNames().size() > 0) {
					for (String sheetName : currSource.getSheetNames()) {
						sheets.add(workbook.getSheet(sheetName));
						sheetNames.add(sheetName);
					}
				} else {
					sheets.add(workbook.getSheetAt(currSource.getSheetIndex()));
					sheetNames.add(Integer.toString(currSource.getSheetIndex()));
				}
				if (sheets.size() == 0) {
					processContext.getMonitor().logError("The specified sheet(s) can not be found!");
					reachedEnd = true;
				} else {
					for (HSSFSheet sheet : sheets) {
						HSSFRow row = sheet.getRow(currSource.getStartRow());
						if (row == null) {
							// file is empty!
							processContext.getMonitor().logWarn("The specified header row does not exist. Assume that the file is empty.");
							reachedEnd = true;
						} else {
							int lastNum = row.getLastCellNum();
							for (int c = 0; c < lastNum; c++) {
								HSSFCell cell = row.getCell(c);
								if (cell != null) {
									if (cell.getCellType() == HSSFCell.CELL_TYPE_STRING) {
										HSSFRichTextString value = cell.getRichStringCellValue();
										String text = value != null && value.getString() != null ? value.getString() : "";
										if (text.length() != 0) {
											if (!dataSet.containsColumn(text)) {
												dataSet.addColumn(text);
											}
										}
									}
								}
							}
						}
					}
					if (!reachedEnd) {
						initSheet(0);
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
	 * @param i
	 */
	private void initSheet(int idx) {
		
		sheetIdx = idx;
		currSheet = sheets.get(sheetIdx);
		
		currRow = currSource.getStartRow() + 1;
		maxRow = currSheet.getLastRowNum();

		currCols = new ArrayList<String>();
		// re-read column headers for this sheet.
		
		HSSFRow row = currSheet.getRow(currSource.getStartRow());
		int lastNum = row.getLastCellNum();
		int lastCol = 0;
		for (int c = 0; c < lastNum; c++) {
			HSSFCell cell = row.getCell(c);
			if (cell != null) {
				if (cell.getCellType() == HSSFCell.CELL_TYPE_STRING) {
					HSSFRichTextString value = cell.getRichStringCellValue();
					String text = value != null && value.getString() != null ? value.getString() : "";
					if (text.length() != 0) {
						currCols.add(text);
						lastCol = c;
					} else {
						currCols.add(null);
					}
				}
			}
		}
		expectedColumns = lastCol;
		
	}


}
