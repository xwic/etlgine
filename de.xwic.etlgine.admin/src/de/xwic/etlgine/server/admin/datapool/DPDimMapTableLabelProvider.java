/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import de.jwic.controls.tableviewer.CellLabel;
import de.jwic.controls.tableviewer.ITableLabelProvider;
import de.jwic.controls.tableviewer.RowContext;
import de.jwic.controls.tableviewer.TableColumn;
import de.xwic.etlgine.cube.mapping.DimMappingDef;
import de.xwic.etlgine.server.admin.ImageLibrary;

/**
 * @author Developer
 *
 */
public class DPDimMapTableLabelProvider implements ITableLabelProvider {

	/* (non-Javadoc)
	 * @see de.jwic.ecolib.tableviewer.ITableLabelProvider#getCellLabel(java.lang.Object, de.jwic.ecolib.tableviewer.TableColumn, de.jwic.ecolib.tableviewer.RowContext)
	 */
	public CellLabel getCellLabel(Object row, TableColumn column, RowContext rowContext) {
		CellLabel cell = new CellLabel();
		DimMappingDef dmd = (DimMappingDef)row;
		if ("key".equals(column.getUserObject())) {
			cell.text = dmd.getKey();
			cell.image = ImageLibrary.IMAGE_TABLE;
			
		} else if ("dimension".equals(column.getUserObject())) {
			cell.text = dmd.getDimensionKey();
		} else if ("description".equals(column.getUserObject())) {
			cell.text = dmd.getDescription();
		} else if ("action".equals(column.getUserObject())) {
			cell.text = dmd.getOnUnmapped().name();
		}
		return cell;
	}

}
