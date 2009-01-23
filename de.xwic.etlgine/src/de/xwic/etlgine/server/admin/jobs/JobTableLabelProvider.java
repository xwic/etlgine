/**
 * 
 */
package de.xwic.etlgine.server.admin.jobs;

import java.text.DateFormat;

import de.jwic.ecolib.tableviewer.CellLabel;
import de.jwic.ecolib.tableviewer.ITableLabelProvider;
import de.jwic.ecolib.tableviewer.RowContext;
import de.jwic.ecolib.tableviewer.TableColumn;
import de.xwic.etlgine.IJob;

/**
 * @author Developer
 *
 */
public class JobTableLabelProvider implements ITableLabelProvider {

	/* (non-Javadoc)
	 * @see de.jwic.ecolib.tableviewer.ITableLabelProvider#getCellLabel(java.lang.Object, de.jwic.ecolib.tableviewer.TableColumn, de.jwic.ecolib.tableviewer.RowContext)
	 */
	public CellLabel getCellLabel(Object row, TableColumn column, RowContext rowContext) {
		CellLabel cell = new CellLabel();
		IJob job = (IJob)row;
		if ("name".equals(column.getUserObject())) {
			cell.text = job.getName();
		} else if ("lastRun".equals(column.getUserObject())) {
			if (job.getLastRun() != null) {
				DateFormat df = DateFormat.getDateTimeInstance();
				cell.text = df.format(job.getLastRun());
			} else {
				cell.text = "never";
			}
		} else if ("state".equals(column.getUserObject())) {
			cell.text = job.getState().name();
		}
		return cell;
	}

}
