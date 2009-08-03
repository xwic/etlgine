/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

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
	
	/* (non-Javadoc)
	 * @see de.jwic.ecolib.tableviewer.ITableLabelProvider#getCellLabel(java.lang.Object, de.jwic.ecolib.tableviewer.TableColumn, de.jwic.ecolib.tableviewer.RowContext)
	 */
	public CellLabel getCellLabel(Object row, TableColumn column, RowContext rowContext) {
		CellLabel cell = new CellLabel();
		DimMappingElementDef dmd = (DimMappingElementDef)row;
		if ("exp".equals(column.getUserObject())) {
			cell.text = dmd.getExpression();
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

}
