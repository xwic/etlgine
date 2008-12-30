/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import de.jwic.ecolib.tableviewer.CellLabel;
import de.jwic.ecolib.tableviewer.ITableLabelProvider;
import de.jwic.ecolib.tableviewer.RowContext;
import de.jwic.ecolib.tableviewer.TableColumn;
import de.xwic.etlgine.cube.mapping.DimMappingElementDef;

/**
 * @author lippisch
 *
 */
public class MappingElementTableLabelProvider implements ITableLabelProvider {

	/* (non-Javadoc)
	 * @see de.jwic.ecolib.tableviewer.ITableLabelProvider#getCellLabel(java.lang.Object, de.jwic.ecolib.tableviewer.TableColumn, de.jwic.ecolib.tableviewer.RowContext)
	 */
	public CellLabel getCellLabel(Object row, TableColumn column, RowContext rowContext) {
		CellLabel cell = new CellLabel();
		DimMappingElementDef dmd = (DimMappingElementDef)row;
		if ("exp".equals(column.getUserObject())) {
			cell.text = dmd.getExpression();
		} else if ("path".equals(column.getUserObject())) {
			cell.text = dmd.getElementPath();
		} else if ("regExp".equals(column.getUserObject())) {
			cell.text = dmd.isRegExp() ? "Yes" : "No";
		} else if ("ignoreCase".equals(column.getUserObject())) {
			cell.text = dmd.isIgnoreCase() ? "Yes" : "No";
		} else if ("skip".equals(column.getUserObject())) {
			cell.text = dmd.isSkipRecord() ? "Yes" : "No";
		}
		return cell;
	}

}
