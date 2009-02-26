/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import de.jwic.ecolib.tableviewer.CellLabel;
import de.jwic.ecolib.tableviewer.ITableLabelProvider;
import de.jwic.ecolib.tableviewer.RowContext;
import de.jwic.ecolib.tableviewer.TableColumn;
import de.xwic.cube.IDimensionElement;

/**
 * @author Developer
 *
 */
public class DimensionEditorLabelProvider implements ITableLabelProvider {

	/* (non-Javadoc)
	 * @see de.jwic.ecolib.tableviewer.ITableLabelProvider#getCellLabel(java.lang.Object, de.jwic.ecolib.tableviewer.TableColumn, de.jwic.ecolib.tableviewer.RowContext)
	 */
	public CellLabel getCellLabel(Object row, TableColumn column, RowContext rowContext) {
		CellLabel cell = new CellLabel();
		IDimensionElement elm = (IDimensionElement)row;
		if ("key".equals(column.getUserObject())) {
			cell.text = elm.getKey();
		} else if ("title".equals(column.getUserObject())) {
			cell.text = elm.getTitle();
		} else if ("weight".equals(column.getUserObject())) {
			cell.text = Double.toString(elm.getWeight());
		}
		return cell;
	}

}
