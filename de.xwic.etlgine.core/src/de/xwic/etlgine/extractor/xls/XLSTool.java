/**
 * 
 */
package de.xwic.etlgine.extractor.xls;

import java.util.Date;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import de.xwic.etlgine.IColumn.DataType;

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
	
	public static Object getObject(Row row, int col) {
		return getObject(row, col, null);
	}
	
	/**
	 * @param row
	 * @param i
	 * @return
	 */
	public static Object getObject(Row row, int col, DataType dataType) {
		if (row != null) {
			Cell cell = row.getCell(col);
			if (cell != null) {
				switch (cell.getCellType()) {
				case HSSFCell.CELL_TYPE_ERROR:
					return null;
				case HSSFCell.CELL_TYPE_NUMERIC:
					if (HSSFDateUtil.isCellDateFormatted(cell)) {
						return HSSFDateUtil.getJavaDate(cell.getNumericCellValue());
					} 
					if (dataType != null && dataType == DataType.STRING) {
						// dirty trick to get cell.getRichStringCellValue() working
						cell.setCellType(Cell.CELL_TYPE_STRING);
						return cell.getRichStringCellValue().getString();
					}
					return cell.getNumericCellValue();
				case HSSFCell.CELL_TYPE_STRING:
					return cell.getRichStringCellValue().getString();
                case HSSFCell.CELL_TYPE_BOOLEAN: // Add by R.Martin Aug 2011
                    return cell.getBooleanCellValue();
				case HSSFCell.CELL_TYPE_FORMULA:
					// try string first
					String s = null;
					try {
						s = cell.getRichStringCellValue().getString();
					} catch (Exception e) {
						try {
							return cell.getNumericCellValue();
						} catch (Throwable t) {
							// fix #N/A
							//int err = cell.getErrorCellValue();
							return null;
						}
					}
					
					return s;
				}
			}
		}
		return null;
	}

	/**
	 * @param row
	 * @param i
	 * @return
	 */
	public static Date getDate(HSSFRow row, int i) {
		HSSFCell cell = row.getCell(i);
		if (cell != null) {
			if (HSSFDateUtil.isCellDateFormatted(cell)) {
				return HSSFDateUtil.getJavaDate(cell.getNumericCellValue());
			}
		}
		return null;
	}

}
