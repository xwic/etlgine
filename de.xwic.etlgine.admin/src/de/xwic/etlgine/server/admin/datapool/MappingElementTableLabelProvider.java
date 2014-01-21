/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

import de.jwic.ecolib.tableviewer.CellLabel;
import de.jwic.ecolib.tableviewer.ITableLabelProvider;
import de.jwic.ecolib.tableviewer.RowContext;
import de.jwic.ecolib.tableviewer.TableColumn;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.etlgine.cube.mapping.DimMappingElementDef;

/**
 * @author lippisch
 *
 */
public class MappingElementTableLabelProvider implements ITableLabelProvider {

	private IDimension dimension = null;
	private String testString = null;
	private DateFormat df = new SimpleDateFormat("dd-MMM-yyyy");
	
	/* (non-Javadoc)
	 * @see de.jwic.ecolib.tableviewer.ITableLabelProvider#getCellLabel(java.lang.Object, de.jwic.ecolib.tableviewer.TableColumn, de.jwic.ecolib.tableviewer.RowContext)
	 */
	public CellLabel getCellLabel(Object row, TableColumn column, RowContext rowContext) {
		CellLabel cell = new CellLabel();
		DimMappingElementDef dmd = (DimMappingElementDef)row;
		if ("match".equals(column.getUserObject())) {
			boolean match = false;
			boolean error = false;
			if (testString != null && testString.length() != 0) {
				// test
				if (dmd.isRegExp()) {
					try {
						Pattern pattern = Pattern.compile(dmd.getExpression(), dmd.isIgnoreCase() ? Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE : 0);
						match = pattern.matcher(testString).matches();
					} catch (Throwable t) {
						error = true;
					}
				} else {
					if (dmd.isIgnoreCase()) {
						match = dmd.getExpression().equalsIgnoreCase(testString);
					} else {
						match = dmd.getExpression().equals(testString);
					}
				}
			}
			if (match) {
				cell.text = "M";
			} else if (error) {
				cell.text = "E";
			} else {
				cell.text = "";
			}
			
		} else if ("exp".equals(column.getUserObject())) {
			
			boolean match = false;
			boolean error = false;
			if (testString != null && testString.length() != 0) {
				// test
				if (dmd.isRegExp()) {
					try {
						Pattern pattern = Pattern.compile(dmd.getExpression(), dmd.isIgnoreCase() ? Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE : 0);
						match = pattern.matcher(testString).matches();
					} catch (Throwable t) {
						error = true;
					}
				} else {
					if (dmd.isIgnoreCase()) {
						match = dmd.getExpression().equalsIgnoreCase(testString);
					} else {
						match = dmd.getExpression().equals(testString);
					}
				}
			} 
			if (match) {
				cell.text =  "<span style=\"color: #00E000; \">" + dmd.getExpression() + "</span>";
			} else if (error) {
				cell.text =  "<span style=\"color: #FF0000; \">" + dmd.getExpression() + "</span>";
			} else {
				cell.text = dmd.getExpression();
			}
			
		} else if ("path".equals(column.getUserObject())) {
			String path = dmd.getElementPath();
			cell.text = path;
			if (dimension != null) {
				try {
					IDimensionElement elm = dimension.parsePath(path);
					if (!elm.isLeaf()) {
						cell.text = "<span style=\"color: blue\">" + path + "</span>";
					}
				} catch (Throwable t) {
					// the path does not work
					cell.text = "<span style=\"color: red\">" + path + "</span>";
				}
			}
			
		} else if ("regExp".equals(column.getUserObject())) {
			cell.text = dmd.isRegExp() ? "Yes" : "No";
		} else if ("ignoreCase".equals(column.getUserObject())) {
			cell.text = dmd.isIgnoreCase() ? "Yes" : "No";
		} else if ("skip".equals(column.getUserObject())) {
			cell.text = dmd.isSkipRecord() ? "Yes" : "No";
		} else if ("validFrom".equals(column.getUserObject())) {
			cell.text = dmd.getValidFrom() != null ? df.format(dmd.getValidFrom()) : "";
		} else if ("validTo".equals(column.getUserObject())) {
			cell.text = dmd.getValidTo() != null ? df.format(dmd.getValidTo()) : "";
		}
		return cell;
	}

	/**
	 * @return the dimension
	 */
	public IDimension getDimension() {
		return dimension;
	}

	/**
	 * @param dimension the dimension to set
	 */
	public void setDimension(IDimension dimension) {
		this.dimension = dimension;
	}

	/**
	 * @param text
	 */
	public void setTestString(String text) {
		testString = text; 
	}

}
