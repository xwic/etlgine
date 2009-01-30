/**
 * 
 */
package de.xwic.etlgine.extractor.xls;

import java.util.ArrayList;
import java.util.List;

import de.xwic.etlgine.sources.FileSource;

/**
 * @author lippisch
 *
 */
public class XLSFileSource extends FileSource {

	protected List<String> sheetNames = null;
	protected boolean containsHeader = true;
	protected int sheetIndex = 0;
	protected int startRow = 0;

	public XLSFileSource() {
		
	}
	
	public XLSFileSource(String fileName) {
		super(fileName);
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


}
