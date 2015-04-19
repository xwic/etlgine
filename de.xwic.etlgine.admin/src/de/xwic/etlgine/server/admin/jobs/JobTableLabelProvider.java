/**
 * 
 */
package de.xwic.etlgine.server.admin.jobs;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;

import de.jwic.controls.tableviewer.CellLabel;
import de.jwic.controls.tableviewer.ITableLabelProvider;
import de.jwic.controls.tableviewer.RowContext;
import de.jwic.controls.tableviewer.TableColumn;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.ITrigger;
import de.xwic.etlgine.server.ETLgineServer;
import de.xwic.etlgine.server.admin.ImageLibrary;
import de.xwic.etlgine.trigger.ScheduledTrigger;
import de.xwic.etlgine.trigger.TriggerList;

/**
 * @author Developer
 *
 */
public class JobTableLabelProvider implements ITableLabelProvider {

	private static final long serialVersionUID = 1L;

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
				cell.text = cell.text + " (" + job.getDurationInfo() + ")";
			} else if (state == IJob.State.FINISHED || state == IJob.State.FINISHED_WITH_ERROR) {
				cell.text = cell.text + " (" + job.getDurationInfo() + ")";
			}

            if(state == IJob.State.FINISHED_WITH_ERROR || state == IJob.State.ERROR) {
                if(job.isStopTriggerAfterError()) {
                    cell.text = cell.text + " [TRIGGER OFF]";
                } else {
                    cell.text = cell.text + " [TRIGGER ON]";
                }

            }

		} else if ("nextRun".equals(column.getUserObject())) {
			cell.text = "-";
			if (job.getTrigger() != null) {
				Collection<ITrigger> triggers = null;
				if (job.getTrigger() instanceof TriggerList) {
					triggers = ((TriggerList)job.getTrigger()).getTriggers();
				} else {
					triggers = new ArrayList<ITrigger>();
					triggers.add(job.getTrigger());
				}
				for (ITrigger trigger : triggers) {
					if (trigger instanceof ScheduledTrigger) {
						ScheduledTrigger st = (ScheduledTrigger)trigger;
						if (st.getNextStart() != null) {
							String time = new SimpleDateFormat("dd-MMM-yyyy HH:mm").format(st.getNextStart());
							if (cell.text.equals("-")) {
								cell.text = time;
							} else {
								if (cell.text.length() > 12 && time.length() > 12 && cell.text.substring(0,12).equals(time.substring(0,12))) {
									// remove same date
									cell.text += ", " + time.substring(12);
								} else {
									cell.text += ", " + time;
								}
							}
						}
					}
				}
			}
			
		} else if ("queue".equals(column.getUserObject())) {
			cell.text = ETLgineServer.getInstance().getServerContext().getJobQueueForJob(job.getName()).getName();
		}
		return cell;
	}

}
