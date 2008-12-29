/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import de.jwic.ecolib.tableviewer.CellLabel;
import de.jwic.ecolib.tableviewer.ITableLabelProvider;
import de.jwic.ecolib.tableviewer.RowContext;
import de.jwic.ecolib.tableviewer.TableColumn;
import de.xwic.cube.IDataPoolManager;
import de.xwic.cube.StorageException;
import de.xwic.etlgine.server.ETLgineServer;
import de.xwic.etlgine.server.ServerContext;
import de.xwic.etlgine.server.admin.ImageLibrary;

/**
 * @author Developer
 *
 */
public class DPTableLabelProvider implements ITableLabelProvider {

	/* (non-Javadoc)
	 * @see de.jwic.ecolib.tableviewer.ITableLabelProvider#getCellLabel(java.lang.Object, de.jwic.ecolib.tableviewer.TableColumn, de.jwic.ecolib.tableviewer.RowContext)
	 */
	public CellLabel getCellLabel(Object row, TableColumn column, RowContext rowContext) {
		CellLabel cell = new CellLabel();
		String key = (String)row;
		if ("name".equals(column.getUserObject())) {
			cell.text = key;
			cell.image = ImageLibrary.IMAGE_DATABASE;
			
		} else if ("key".equals(column.getUserObject())) {
			String dpKey = ETLgineServer.getInstance().getServerContext().getProperty(key + ".datapool.key", null);
			if (dpKey == null) {
				cell.text = "<i>Not Specified</i>";
			} else {
				cell.text = dpKey;
			}
		} else if ("status".equals(column.getUserObject())) {
			ServerContext context = ETLgineServer.getInstance().getServerContext(); 
			String dpKey = context.getProperty(key + ".datapool.key", null);
			IDataPoolManager dpm = context.getDataPoolManager(key);
			if (dpm == null) {
				cell.text = "NOT LOADED!";
			} else {
				try {
					cell.text = dpm.containsDataPool(dpKey) ? "Initialized" : "Not Initialized";
				} catch (StorageException e) {
					cell.text = "Error";
				}
			}
		}
		return cell;
	}

}