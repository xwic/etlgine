/**
 * 
 */
package de.xwic.etlgine.extractor.xls;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.RichTextString;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

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

	public final static String COL_SHEETNAME = "_XLS_SHEETNAME";
	
	private InputStream inputStream;
	private Workbook workbook;
	
	private XLSFileSource currSource = null;
	
	private int currRow = 0;
	private int maxRow = 0;
	private int expectedColumns = 0;

	private List<String> currCols = null;
	private List<Sheet> sheets = null;
	private List<String> sheetNames = null;
	private int sheetIdx = 0;
	private Sheet currSheet = null;
	private boolean reachedEnd = false;
	private boolean expectAllColumns = true;
	private boolean dedupeColumnName = false;

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IExtractor#close()
	 */
	public void close() throws ETLException {
		try {
			if (inputStream != null) {
				inputStream.close();
				inputStream = null;
				workbook = null;
				currSource = null;
				currRow = 0;
				maxRow = 0;
				expectedColumns = 0;
				currCols = null;
				sheets = null;
				sheetNames = null;
				sheetIdx = 0;
				currSheet = null;
				reachedEnd = false;
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
				IRecord record = context.newRecord();
				Row row;
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
				
				record.setData(COL_SHEETNAME, sheetNames.get(sheetIdx));
				if (currSource.isContainsHeader() && expectAllColumns && (row.getLastCellNum() < expectedColumns)) {
					record.markInvalid("Expected " + expectedColumns + " columns but row contained " + row.getLastCellNum() + " columns. (row=" + currRow + ")");
				}
				
				// try to load what's there
				int max = Math.min(expectedColumns, row.getLastCellNum()) + 1;
				IDataSet ds = context.getDataSet();
				for (int i = 0; i < max; i++) {
					IColumn column = null;
					String name = i < currCols.size() ? currCols.get(i) : null;
					//String name = currCols.get(i);
					if (name == null) {
						// missing column name, auto generate now
						name = "Column" + i;
						if (!ds.containsColumn(name)) {
							column = ds.addColumn(name);
						}
					}
					if (column == null) {
						column = ds.getColumn(name);
					}
					record.setData(column, XLSTool.getObject(row, i, column.getTypeHint()));
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
				record.resetChangeFlag();
				return record;
				
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
		
		reachedEnd = false;
		
		IColumn col = dataSet.addColumn(COL_SHEETNAME);
		col.setExclude(true);
		
		if (!(source instanceof FileSource)) {
			throw new ETLException("Can not handle a source of this type - FileSource type required.");
		}
		
		FileSource fileSource = (FileSource)source;
		
		if (source instanceof XLSFileSource) {
			currSource = (XLSFileSource)source;
		} else {
			currSource = new XLSFileSource(fileSource);
			// call isAvailable
			if (!currSource.isAvailable()) {
				throw new ETLException("XLSFileSource is not available for FileSource, please use XLSFileSource instead");
			}
		}
		try {
			inputStream = currSource.getInputStream();
			workbook = WorkbookFactory.create(inputStream);
			
			if (currSource.isContainsHeader()) {
				sheets = new ArrayList<Sheet>();
				sheetNames = new ArrayList<String>();
				int sheetCnt = workbook.getNumberOfSheets();
				context.getMonitor().logInfo("Found " + sheetCnt + " sheets in " + currSource);
				for (int i = 0; i < sheetCnt; i++) {
					String sheetName = workbook.getSheetName(i);
					context.getMonitor().logInfo("Found sheet '" + sheetName + "' at index " + i);
				}
				if (currSource.getSheetNames() != null && currSource.getSheetNames().size() > 0) {
					for (String sheetName : currSource.getSheetNames()) {
						Sheet sheet = workbook.getSheet(sheetName);
						if (sheet == null) {
							context.getMonitor().logError("Sheet '" + sheetName + "' not found");
							continue;
						}
						sheets.add(sheet);
						sheetNames.add(sheetName);
					}
				} else {
					sheets.add(workbook.getSheetAt(currSource.getSheetIndex()));
					sheetNames.add(Integer.toString(currSource.getSheetIndex()));
				}
				if (sheets.size() == 0) {
					if (!source.isOptional()) {
						if (currSource.getSheetName() != null) {
							throw new ETLException("The specified sheet(s) cannot be found (" + currSource.getSheetName() + ")!");	
						}
						throw new ETLException("The specified sheet(s) cannot be found!");	
					}
					context.getMonitor().logError("The specified sheet(s) cannot be found!");
					reachedEnd = true;
				} else {
					for (Sheet sheet : sheets) {
						Row row = sheet.getRow(currSource.getStartRow());
						Set<String> addedColumnNames = new HashSet<String>();
						if (row == null) {
							// file is empty!
							context.getMonitor().logWarn("The specified header row does not exist. Assume that the file is empty.");
							reachedEnd = true;
						} else {
							int lastNum = row.getLastCellNum();
							for (int c = 0; c < lastNum; c++) {
								Cell cell = row.getCell(c);
								if (cell != null) {
									if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
										RichTextString value = cell.getRichStringCellValue();
										String text = value != null && value.getString() != null ? value.getString() : "";
										if (text.length() != 0) {
											if (dedupeColumnName && addedColumnNames.contains(text)) {
												String newText = null;
												for (int i = 2; ; i++) {
													newText = text + i;
													if (!addedColumnNames.contains(newText)) {
														text = newText;
														break;
													}
												}
											}
											if (!dataSet.containsColumn(text)) {
												dataSet.addColumn(text);
											}
											addedColumnNames.add(text);
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
		} catch (InvalidFormatException e) {
			throw new ETLException("File has no valid excel format for POI! (" + source.getName() + ") : " + e, e);
		}
	}

	/**
	 * @param i
	 */
	private void initSheet(int idx) {
		
		String sheetName = sheetNames.get(idx);
		context.getMonitor().logInfo("Now switching to sheet '" + sheetName + "'");
		
		sheetIdx = idx;
		currSheet = sheets.get(sheetIdx);
		
		currRow = currSource.getStartRow() + 1;
		maxRow = currSheet.getLastRowNum();

		currCols = new ArrayList<String>();
		// re-read column headers for this sheet.
		
		Row row = currSheet.getRow(currSource.getStartRow());
		int lastNum = row.getLastCellNum();
		int lastCol = 0;
		for (int c = 0; c < lastNum; c++) {
			Cell cell = row.getCell(c);
			if (cell != null) {
				if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
					RichTextString value = cell.getRichStringCellValue();
					String text = value != null && value.getString() != null ? value.getString() : "";
					if (text.length() != 0) {
						if (dedupeColumnName && currCols.contains(text)) {
							String newText = null;
							for (int i = 2; ; i++) {
								newText = text + i;
								if (!currCols.contains(newText)) {
									text = newText;
									break;
								}
							}
						}
						currCols.add(text);
						lastCol = c;
					} else {
						currCols.add(null);
					}
				} else {
					currCols.add(null);
				}
			} else {
				currCols.add(null);
			}
		}
		expectedColumns = lastCol;
		
	}

	/**
	 * @return the expectAllColumns
	 */
	public boolean isExpectAllColumns() {
		return expectAllColumns;
	}

	/**
	 * @param expectAllColumns the expectAllColumns to set
	 */
	public void setExpectAllColumns(boolean expectAllColumns) {
		this.expectAllColumns = expectAllColumns;
	}

	/**
	 * @return the dedupeColumnName
	 */
	public boolean isDedupeColumnName() {
		return dedupeColumnName;
	}

	/**
	 * @param dedupeColumnName the dedupeColumnName to set
	 */
	public void setDedupeColumnName(boolean dedupeColumnName) {
		this.dedupeColumnName = dedupeColumnName;
	}

}
