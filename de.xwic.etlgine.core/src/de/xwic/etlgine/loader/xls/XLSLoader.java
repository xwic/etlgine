/*
 * Copyright (c) NetApp Inc. - All Rights Reserved
 * 
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 * 
 * de.xwic.etlgine.loader.xls.XLSLoader 
 */
package de.xwic.etlgine.loader.xls;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import de.xwic.etlgine.AbstractLoader;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IColumn.DataType;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;

/**
 * Produces an excel file on the specified location containing all the records extracted from the source.
 * 
 * @author ionut
 *
 */
public class XLSLoader extends AbstractLoader {

	private static final Log log = LogFactory.getLog(XLSLoader.class);

	/**
	 * Keys of the used cell styles
	 */
	private static final String HEADER_STYLE = "HEADER_STYLE";
	private static final String ROW_STYLE = "ROW_STYLE";
	private static final String INT_STYLE = "INT_STYLE";
	private static final String DOUBLE_STYLE = "DOUBLE_STYLE";
	private static final String DATE_STYLE = "DATE_STYLE";
	private static final int FONT_SIZE = 10;
	private static final String FONT = "Calibri";

	/**
	 * The full path of the xls file that will be created
	 */
	protected String fileName;

	/**
	 * The reference to the xls workbook
	 */
	protected Workbook workbook;

	/**
	 * The first sheet
	 */
	protected Sheet sheet;
	/**
	 * The output file content stream
	 */
	protected FileOutputStream fileOutStream;

	/**
	 * Flag to indicate if the header was already written
	 */
	protected boolean headerWritten;

	/**
	 * The first sheet name
	 */
	protected String sheetName = "Sheet1";

	/**
	 * If we should override existing file or if we should load it and add the new sheet to it
	 */
	protected boolean overrideExistingFile = true;

	/**
	 * Holds the number of rows
	 */
	protected int rowCount = 0;

	/**
	 * Formatter for the cell data
	 */
	protected DataFormatter formatter;

	/**
	 * Holds the actual used date format
	 */
	protected String dateFormat = "mm/dd/yyyy";

	/**
	 * The name of the font to use for the entire sheet
	 */
	protected String fontName = FONT;

	/**
	 * The size of the font to be used
	 */
	protected int fontSize = FONT_SIZE;

	/**
	 * Keeps the cell styles
	 */
	protected Map<String, CellStyle> styles = new HashMap<String, CellStyle>();

	/**
	 * 
	 * @param fileName
	 */
	public XLSLoader(String fileName) {
		this.fileName = fileName;
	}

	/**
	 * 
	 * @param fileName
	 * @param sheetName
	 */
	public XLSLoader(String fileName, String sheetName) {
		this.fileName = fileName;
		this.sheetName = sheetName;
	}

	public XLSLoader(String fileName, String sheetName, boolean overrideExistingFile) {
		this.fileName = fileName;
		this.sheetName = sheetName;
		this.overrideExistingFile = overrideExistingFile;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.xwic.etlgine.AbstractLoader#initialize(de.xwic.etlgine.IProcessContext)
	 */
	public void initialize(IProcessContext processContext) throws ETLException {
		super.initialize(processContext);
		
		File file = new File(fileName);
		
		workbook = new XSSFWorkbook();
		if(!overrideExistingFile) {
			if(file.exists()) {
				try {
					InputStream st = new FileInputStream(file);
					workbook = new XSSFWorkbook(st);
					st.close();
				} catch (FileNotFoundException e) {
					throw new ETLException("Error loading file:" + fileName, e);
				} catch (IOException e) {
					throw new ETLException("Error loading file:" + fileName, e);
				}
			}
		}
		sheet = workbook.createSheet(sheetName);
		formatter = new DataFormatter(true);
		createCellStyles();
		try {
			fileOutStream = new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			throw new ETLException("Error creating file:" + fileName, e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.xwic.etlgine.ILoader#processRecord(de.xwic.etlgine.IProcessContext, de.xwic.etlgine.IRecord)
	 */
	@Override
	public void processRecord(IProcessContext processContext, IRecord record) throws ETLException {
		List<IColumn> columns = processContext.getDataSet().getColumns();
		Cell currentCell = null;
		int cellIdx = 0;

		//write the header
		if (!headerWritten) {
			createHeader(columns);
		}

		//write the current row
		Row currentRow = sheet.createRow(rowCount++);
		for (IColumn col : columns) {
			if (!col.isExclude()) {
				currentCell = currentRow.createCell(cellIdx++);
				currentCell.setCellStyle(styles.get(ROW_STYLE));
				Object value = record.getData(col);

				//set cell type, style and value
				if (null == value) {
					currentCell.setCellType(Cell.CELL_TYPE_BLANK);
				} else if (col.getTypeHint().equals(DataType.STRING)) {
					currentCell.setCellValue((String) value);
				} else if (col.getTypeHint().equals(DataType.BOOLEAN)) {
					currentCell.setCellType(Cell.CELL_TYPE_BOOLEAN);
					currentCell.setCellValue((Boolean) value);
				} else if (col.getTypeHint().equals(DataType.DATE) || col.getTypeHint().equals(DataType.DATETIME)) {
					currentCell.setCellStyle(styles.get(DATE_STYLE));
					currentCell.setCellValue((Date) value);
				} else if (col.getTypeHint().equals(DataType.DOUBLE) || col.getTypeHint().equals(DataType.INT)
						|| col.getTypeHint().equals(DataType.LONG)) {
					currentCell.setCellType(Cell.CELL_TYPE_NUMERIC);
					if (value instanceof Integer) {
						currentCell.setCellStyle(styles.get(INT_STYLE));
						currentCell.setCellValue(((Integer) value));
					} else if (value instanceof Long) {
						currentCell.setCellStyle(styles.get(INT_STYLE));
						currentCell.setCellValue(((Long) value));
					} else if (value instanceof Double) {
						currentCell.setCellStyle(styles.get(DOUBLE_STYLE));
						currentCell.setCellValue(((Double) value));
					}
				} else {
					currentCell.setCellValue(value.toString());
				}

			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.xwic.etlgine.AbstractLoader#onProcessFinished(de.xwic.etlgine.IProcessContext)
	 */
	public void onProcessFinished(IProcessContext processContext) throws ETLException {
		try {
			workbook.write(fileOutStream);
		} catch (IOException e) {
			throw new ETLException("Error writing the xls file " + fileName, e);
		} finally {
			try {
				if (null != fileOutStream) {
					fileOutStream.close();
				}
			} catch (IOException e) {
				log.error(e);
			}
		}
	}

	/**
	 * @return the fileName
	 */
	public String getFileName() {
		return fileName;
	}

	/**
	 * @param fileName
	 *            the fileName to set
	 */
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	/**
	 * @return the sheetName
	 */
	public String getSheetName() {
		return sheetName;
	}

	/**
	 * @param sheetName
	 *            the sheetName to set
	 */
	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	/**
	 * @return the dateFormat
	 */
	public String getDateFormat() {
		return dateFormat;
	}

	/**
	 * @return the fontName
	 */
	public String getFontName() {
		return fontName;
	}

	/**
	 * @param fontName
	 *            the fontName to set
	 */
	public void setFontName(String fontName) {
		this.fontName = fontName;
	}

	/**
	 * @return the fontSize
	 */
	public int getFontSize() {
		return fontSize;
	}

	/**
	 * @param fontSize
	 *            the fontSize to set
	 */
	public void setFontSize(int fontSize) {
		this.fontSize = fontSize;
	}

	/**
	 * @param dateFormat
	 *            the dateFormat to set
	 */
	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	private void createCellStyles() {
		XSSFFont defaultFont = createFont(false);
		
		//header cell type
		CellStyle headerStyle = workbook.createCellStyle();
		headerStyle.setFont(createFont(true));
		headerStyle.setAlignment(CellStyle.ALIGN_CENTER);
		styles.put(HEADER_STYLE, headerStyle);
		
		//general row style
		CellStyle rowStyle = workbook.createCellStyle();
		rowStyle.setFont(defaultFont);
		styles.put(ROW_STYLE, rowStyle);

		//integer numbers
		CellStyle intStyle = workbook.createCellStyle();
		intStyle.setFont(defaultFont);
		intStyle.setDataFormat(workbook.createDataFormat().getFormat("0"));
		intStyle.setAlignment(CellStyle.ALIGN_LEFT);
		styles.put(INT_STYLE, intStyle);
		
		//double values
		CellStyle doubleStyle = workbook.createCellStyle();
		doubleStyle.setFont(defaultFont);
		//doubleStyle.setDataFormat(workbook.createDataFormat().getFormat("$##,##0.00"));
		doubleStyle.setAlignment(CellStyle.ALIGN_LEFT);
		styles.put(DOUBLE_STYLE, doubleStyle);

		//date cell type
		CellStyle dateStyle = workbook.createCellStyle();
		dateStyle.setFont(defaultFont);
		dateStyle.setAlignment(CellStyle.ALIGN_CENTER);
		dateStyle.setDataFormat(workbook.createDataFormat().getFormat(dateFormat));
		
		styles.put(DATE_STYLE, dateStyle);
	}

	private int createHeader(List<IColumn> columns) {
		int cellIdx = 0;
		Cell currentCell = null;
		Row headerRow = sheet.createRow(rowCount++);

		for (IColumn col : columns) {
			if (!col.isExclude()) {
				currentCell = headerRow.createCell(cellIdx++);
				currentCell.setCellStyle(styles.get(HEADER_STYLE));
				currentCell.setCellValue(col.computeTargetName());
			}
		}
		headerWritten = true;
		return cellIdx;
	}

	private XSSFFont createFont(boolean bold) {
		XSSFFont defaultFont = (XSSFFont) workbook.createFont();
		defaultFont.setFontHeightInPoints((short) getFontSize());
		defaultFont.setFontName(getFontName());
		defaultFont.setColor(IndexedColors.BLACK.getIndex());
		if (bold) {
			defaultFont.setBold(true);
			defaultFont.setBoldweight((short) 5);
		}
		defaultFont.setItalic(false);
		return defaultFont;
	}

}
