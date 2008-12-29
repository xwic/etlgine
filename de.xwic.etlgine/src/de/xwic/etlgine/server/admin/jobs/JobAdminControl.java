/**
 * 
 */
package de.xwic.etlgine.server.admin.jobs;

import java.util.ArrayList;
import java.util.List;

import de.jwic.base.IControlContainer;
import de.jwic.controls.ActionBarControl;
import de.jwic.controls.ButtonControl;
import de.jwic.ecolib.tableviewer.TableColumn;
import de.jwic.ecolib.tableviewer.TableModel;
import de.jwic.ecolib.tableviewer.TableViewer;
import de.jwic.ecolib.tableviewer.defaults.ListContentProvider;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.server.ETLgineServer;
import de.xwic.etlgine.server.admin.BaseContentContainer;
import de.xwic.etlgine.server.admin.ImageLibrary;

/**
 * @author Developer
 *
 */
public class JobAdminControl extends BaseContentContainer {

	private TableViewer table;
	
	/**
	 * @param container
	 * @param name
	 */
	public JobAdminControl(IControlContainer container, String name) {
		super(container, name);
		
		setTitle("Job Overview");
		
		ActionBarControl abar = new ActionBarControl(this, "actionBar");
		ButtonControl btReturn = new ButtonControl(abar, "return");
		btReturn.setIconEnabled(ImageLibrary.IMAGE_RETURN);
		btReturn.setTitle("Return");
		btReturn.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				close();
			}
		});
		
		table = new TableViewer(this, "table");
		
		List<IJob> jobList = new ArrayList<IJob>();
		jobList.addAll(ETLgineServer.getInstance().getServerContext().getJobs());
		
		table.setContentProvider(new ListContentProvider(jobList));
		table.setTableLabelProvider(new JobTableLabelProvider());
		table.setWidth(799);
		table.setHeight(300);
		table.setResizeableColumns(true);
		table.setScrollable(true);
		table.setShowStatusBar(false);
		
		TableModel model = table.getModel();
		model.setSelectionMode(TableModel.SELECTION_SINGLE);
		model.addColumn(new TableColumn("Job Name", 300, "name"));
		model.addColumn(new TableColumn("Last Run", 100, "lastRun"));
		
	}

	/**
	 * 
	 */
	protected void close() {
		destroy();
	}

}
