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
import de.xwic.etlgine.server.admin.ImageLibrary;
import de.xwic.etlgine.trigger.ScheduledTrigger;

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
			if (job.isDisabled() && !job.isExecuting()) {
				cell.image = ImageLibrary.IMAGE_CONTROL_PAUSE;
			} else {
				switch (job.getState()) {
				case ENQUEUED:
					cell.image = ImageLibrary.IMAGE_SCRIPT_GEAR;
					break;
				case RUNNING:
					cell.image = ImageLibrary.IMAGE_SCRIPT_LIGHTNING;
					break;
				case FINISHED_WITH_ERROR:
					cell.image = ImageLibrary.IMAGE_SCRIPT_ERROR;
					break;
				case ERROR:
					cell.image = ImageLibrary.IMAGE_SCRIPT_RED;
					break;
				default:
					cell.image = ImageLibrary.IMAGE_SCRIPT;
					break;
				}
			}
		} else if ("lastFinish".equals(column.getUserObject())) {
			if (job.getLastFinished() != null) {
				DateFormat df = DateFormat.getDateTimeInstance();
				cell.text = df.format(job.getLastFinished());
			} else {
				cell.text = "never";
			}
		} else if ("state".equals(column.getUserObject())) {
			IJob.State state = job.getState();
			cell.text = state.name();
			if (state == IJob.State.RUNNING) {
				long duration = System.currentTimeMillis() - job.getLastStarted().getTime();
				int ms = (int)(duration % 1000);
				int sec = (int)((duration / 1000) % 60);
				int min = (int)(duration / 60000);
				cell.text = cell.text + " (" + min + "m " + sec + "s " + ms + "ms)";
			} else if (state == IJob.State.FINISHED || state == IJob.State.FINISHED_WITH_ERROR) {
				long duration = job.getLastFinished().getTime() - job.getLastStarted().getTime();
				int ms = (int)(duration % 1000);
				int sec = (int)((duration / 1000) % 60);
				int min = (int)(duration / 60000);
				cell.text = cell.text + " (" + min + "m " + sec + "s " + ms + "ms)";
				
			}
		
		} else if ("nextRun".equals(column.getUserObject())) {
			cell.text = "-";
			if (job.getTrigger() != null) {
				if (job.getTrigger() instanceof ScheduledTrigger) {
					ScheduledTrigger st = (ScheduledTrigger)job.getTrigger();
					if (st.getNextStart() != null) {
						cell.text = DateFormat.getDateTimeInstance().format(st.getNextStart());
					}
				}
			}
			
		}
		return cell;
	}

}
