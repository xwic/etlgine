/**
 * 
 */
package de.xwic.etlgine.extractor.xls;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.sources.FileSource;

/**
 * @author lippisch
 *
 */
public class XLSFileSource extends FileSource {

	public static final String XLS_EXTENSION = ".xls";
	public static final String XLSX_EXTENSION = ".xlsx";
	
	
	protected final Log log = LogFactory.getLog(getClass());
	
	protected List<String> sheetNames = null;
	protected boolean containsHeader = true;
	protected int sheetIndex = 0;
	protected int startRow = 0;

	protected boolean available = false;
	protected String endsWith = XLS_EXTENSION;
	
	protected boolean isOOXML = false;
	
	public XLSFileSource() {
		
	}

	public XLSFileSource(File file) {
		super(file);
	}

	public XLSFileSource(String filename) {
		super(filename);
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.sources.FileSource#isAvailable()
	 */
	@Override
	public boolean isAvailable() {
		checkSource();
		return available;
	}
	
	/**
	 * 
	 */
	public void checkSource() {
		
		available = false;
		if (file.exists()) {
			
			if (file.isFile()) {
				// autodetect xls and xlsx
				boolean checked = file.getName().toLowerCase().endsWith(endsWith);
				if (!checked) {
					// try xls
					checked = file.getName().toLowerCase().endsWith(XLS_EXTENSION);
					if (checked) {
						// adjust extension
						setEndsWith(XLS_EXTENSION);
					} else {
						// try xlsx
						checked = file.getName().toLowerCase().endsWith(XLSX_EXTENSION);
						if (checked) {
							// adjust extension
							setEndsWith(XLSX_EXTENSION);
						}
					}
				}
				if (checked && checkFilename(file.getName())) {
					available = true;
				}
			} else if (file.isDirectory()) {
				File[] files = file.listFiles(new FilenameFilter() {
					public boolean accept(File dir, String name) {
						return name.toLowerCase().endsWith(endsWith);
					}
				});
				for(File xlsFile : files) {
					if (checkFilename(xlsFile.getName())) {
						available = true;
						file = xlsFile;
						break;
					}
				}
			}
			
			if (available) {
				try {
					determineSheetNames(file);
				} catch (Exception e) {
					log.error("Error determining sheets: " + e, e);
					available = false;
					file = null;
				}
			}
			
		} else {
			log.warn("The sourcePath " + file.getAbsolutePath() + " does not exist.");
		}
		
	}

	
	/**
	 * @param file
	 */
	protected void determineSheetNames(File file) throws Exception {

		// do nothing.
		
	}

	/**
	 * Check the filename and return true if the file is valid.
	 * @param name
	 * @return
	 */
	protected boolean checkFilename(String name) {
		return true;
	}

	/**
	 * @return the sheetNames
	 */
	public List<String> getSheetNames() {
		return sheetNames;
	}

	/**
	 * @param sheetNames the sheetNames to set
	 */
	public void setSheetNames(List<String> sheetNames) {
		this.sheetNames = sheetNames;
	}
	/**
	 * @return the sheetName
	 */
	public String getSheetName() {
		if (sheetNames != null && sheetNames.size() != 0) {
			if (sheetNames.size() == 1) {
				return sheetNames.get(0);
			} else {
				StringBuilder sb = new StringBuilder();
				for (String s : sheetNames) {
					if (sb.length() != 0) {
						sb.append(";");
					}
					sb.append(s);
				}
				return sb.toString();
			}
		}
		return null;
	}

	/**
	 * @param sheetName the sheetName to set
	 */
	public void setSheetName(String sheetName) {
		if (sheetNames == null) {
			sheetNames = new ArrayList<String>();
		}
		sheetNames.clear();
		sheetNames.add(sheetName);
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
	 * @return the endsWith
	 */
	public String getEndsWith() {
		return endsWith;
	}

	/**
	 * @param endsWith the endsWith to set
	 */
	public void setEndsWith(String endsWith) {
		this.endsWith = endsWith;
		if (endsWith != null && endsWith.toLowerCase().endsWith(XLSX_EXTENSION)) {
			isOOXML = true;
		}
	}

	/**
	 * @return the isOOXML
	 */
	public boolean isOOXML() {
		return isOOXML;
	}

	/**
	 * @param isOOXML the isOOXML to set
	 */
	public void setOOXML(boolean isOOXML) {
		this.isOOXML = isOOXML;
	}

}
