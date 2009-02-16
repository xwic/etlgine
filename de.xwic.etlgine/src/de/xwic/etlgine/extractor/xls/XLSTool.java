/**
 * 
 */
package de.xwic.etlgine.extractor.xls;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFRow;

/**
 * @author lippisch
 *
 */
public class XLSTool {

	/**
	 * Returns the cell value as string.
	 * @param row
	 * @param col
	 * @return
	 */
	public static String getString(HSSFRow row, int col) {
		Object obj = getObject(row, col);
		if (obj != null) {
			return obj.toString();
		}
		return null;
	}
	
	/**
	 * @param row
	 * @param i
	 * @return
	 */
	public static Object getObject(HSSFRow row, int col) {
		if (row != null) {
			HSSFCell cell = row.getCell(col);
			if (cell != null) {
				switch (cell.getCellType()) {
				case HSSFCell.CELL_TYPE_NUMERIC:
					if (HSSFDateUtil.isCellDateFormatted(cell)) {
						return HSSFDateUtil.getJavaDate(cell.getNumericCellValue());
					} 
					return cell.getNumericCellValue();
				case HSSFCell.CELL_TYPE_STRING:
					return cell.getRichStringCellValue().getString();
				case HSSFCell.CELL_TYPE_FORMULA:
					// try string first
					String s = cell.getRichStringCellValue().getString();
					if (s == null || s.length() == 0) {
						return cell.getNumericCellValue();
					} else {
						return s;
					}
				}
			}
		}
		return null;
	}

}
