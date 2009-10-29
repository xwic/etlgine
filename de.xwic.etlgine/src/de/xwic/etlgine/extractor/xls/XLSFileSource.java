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

	protected final Log log = LogFactory.getLog(getClass());
	
	protected List<String> sheetNames = null;
	protected boolean containsHeader = true;
	protected int sheetIndex = 0;
	protected int startRow = 0;

	protected boolean available = false;
	protected String endsWith = ".xls";
	
	protected File sourcePath = null;
	
	public XLSFileSource() {
		
	}
	
	public XLSFileSource(String fileName) {
		sourcePath = new File(fileName);
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
		if (sourcePath.exists()) {
			
			if (sourcePath.isFile()) {
				if (sourcePath.getName().toLowerCase().endsWith(endsWith) && checkFilename(sourcePath.getName())) {
					available = true;
					file = sourcePath;
				}
			} else if (sourcePath.isDirectory()) {
				File[] files = sourcePath.listFiles(new FilenameFilter() {
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
			
			if (available && file != null) {
				try {
					determineSheetNames(file);
				} catch (Exception e) {
					log.error("Error determining sheets: " + e, e);
					available = false;
					file = null;
				}
			}
			
		} else {
			log.warn("The sourcePath " + sourcePath.getAbsolutePath() + " does not exist.");
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
	}


}
