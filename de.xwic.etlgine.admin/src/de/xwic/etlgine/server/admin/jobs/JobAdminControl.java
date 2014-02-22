/**
 * 
 */
package de.xwic.etlgine.server.admin.jobs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import de.jwic.base.IControlContainer;
import de.jwic.controls.Button;
import de.jwic.controls.ErrorWarning;
import de.jwic.controls.ToolBar;
import de.jwic.controls.ToolBarGroup;
import de.jwic.controls.tableviewer.TableColumn;
import de.jwic.controls.tableviewer.TableModel;
import de.jwic.controls.tableviewer.TableViewer;
import de.jwic.data.ListContentProvider;
import de.jwic.events.ElementSelectedEvent;
import de.jwic.events.ElementSelectedListener;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.server.ETLgineServer;
import de.xwic.etlgine.server.JobQueue;
import de.xwic.etlgine.server.admin.BaseContentContainer;
import de.xwic.etlgine.server.admin.ImageLibrary;
import de.xwic.etlgine.server.admin.StackedContentContainer;

/**
 * @author Developer
 *
 */
public class JobAdminControl extends BaseContentContainer {

	private TableViewer table;
	private Button btRun;
	private Button btStopJob;
	private Button btViewJob;
    private Button btReactivateJobTrigger;
	private List<IJob> jobList;
	private ErrorWarning errInfo;
	
	/**
	 * @param container
	 * @param name
	 */
	public JobAdminControl(IControlContainer container, String name) {
		super(container, name);
		
		setTitle("Job Overview");

		createActionBar();
		
		errInfo = new ErrorWarning(this, "errorInfo");
		
		table = new TableViewer(this, "table");
		
		jobList = new ArrayList<IJob>();
		jobList.addAll(ETLgineServer.getInstance().getServerContext().getJobs());
		
		Collections.sort(jobList, new Comparator<IJob>() {
			public int compare(IJob o1, IJob o2) {
				return o1.getName().toLowerCase().compareTo(o2.getName().toLowerCase());
			}
		});
		
		
		table.setContentProvider(new ListContentProvider<IJob>(jobList));
		table.setTableLabelProvider(new JobTableLabelProvider());
		table.setWidth(949);
		table.setHeight(500);
		table.setResizeableColumns(true);
		table.setScrollable(true);
		table.setShowStatusBar(false);
		
		TableModel model = table.getModel();
		model.setSelectionMode(TableModel.SELECTION_SINGLE);
		model.addColumn(new TableColumn("Job Name", 450, "name"));
		model.addColumn(new TableColumn("State", 150, "state"));
		model.addColumn(new TableColumn("Finished", 130, "lastFinish"));
		model.addColumn(new TableColumn("Next Run", 130, "nextRun"));
		
		model.addElementSelectedListener(new ElementSelectedListener() {
			private static final long serialVersionUID = 1L;
			public void elementSelected(ElementSelectedEvent event) {
				updateButtons();
			}
		});
		
	}

	/**
	 * 
	 */
	protected void updateButtons() {
		boolean selected = table.getModel().getFirstSelectedKey() != null && table.getModel().getFirstSelectedKey().length() != 0;
		btRun.setEnabled(selected);
		btViewJob.setEnabled(selected);
	}

	/**
	 * 
	 */
	private void createActionBar() {
		ToolBar abar = new ToolBar(this, "actionBar");
		ToolBarGroup group = abar.addGroup();
		Button btReturn = group.addButton();
		btReturn.setIconEnabled(ImageLibrary.IMAGE_RETURN);
		btReturn.setTitle("Return");
		btReturn.addSelectionListener(new SelectionListener() {
			private static final long serialVersionUID = 1L;
			public void objectSelected(SelectionEvent event) {
				close();
			}
		});

		btRun = group.addButton();
		btRun.setIconEnabled(ImageLibrary.IMAGE_SCRIPT_GO);
		btRun.setTitle("Run Job");
		btRun.addSelectionListener(new SelectionListener() {
			private static final long serialVersionUID = 1L;
			public void objectSelected(SelectionEvent event) {
				onRunJob();
			}
		});

		Button btRefresh = group.addButton(); 
		btRefresh.setIconEnabled(ImageLibrary.IMAGE_REFRESH);
		btRefresh.setTitle("Refresh");
		btRefresh.addSelectionListener(new SelectionListener() {
			private static final long serialVersionUID = 1L;
			public void objectSelected(SelectionEvent event) {
				onRefresh();
			}
		});

		btViewJob = group.addButton();
		btViewJob.setIconEnabled(ImageLibrary.IMAGE_ZOOM);
		btViewJob.setTitle("View Job");
		btViewJob.addSelectionListener(new SelectionListener() {
			private static final long serialVersionUID = 1L;
			public void objectSelected(SelectionEvent event) {
				onViewJob();
			}
		});
		
		btStopJob = group.addButton();
		btStopJob.setIconEnabled(ImageLibrary.IMAGE_CANCEL);
		btStopJob.setTitle("Stop Job");
		btStopJob.addSelectionListener(new SelectionListener() {
			private static final long serialVersionUID = 1L;
			public void objectSelected(SelectionEvent event) {
				onStopJob();
			}
		});

        btReactivateJobTrigger = group.addButton();
        btReactivateJobTrigger.setIconEnabled(ImageLibrary.IMAGE_APP_GO);
        btReactivateJobTrigger.setTitle("Reactivate Job Trigger");
        btReactivateJobTrigger.addSelectionListener(new SelectionListener() {
            private static final long serialVersionUID = 1L;
            public void objectSelected(SelectionEvent event) {
                onReactivateJobTrigger();
            }
        });

	}

	/**
	 * 
	 */
	protected void onStopJob() {
		
		String selection = table.getModel().getFirstSelectedKey();
		if (selection != null) {
			int idx = Integer.parseInt(selection);
			IJob job = jobList.get(idx);
			if (job.isExecuting()) {
				if (job.getProcessChain().getGlobalContext().isStopFlag()) {
					// force Thread.interrupt() on JobQueue's thread
					for (JobQueue queue : ETLgineServer.getInstance().getServerContext().getJobQueues()) {
						if (queue.getActiveJob() == job) {
							// queue found, trigger interrupt
							queue.getThread().interrupt();
							errInfo.showWarning("JobQueue '" + queue.getName() + "' Thread.interrupt() invoked.");
							break;
						}
					}
				} else {
					if (job.stop()) {
						errInfo.showWarning("Stop-Flag set.");
					} else {
						errInfo.showError("Unable to stop job.");
					}
				}
			} else {
				errInfo.showError("The selected job is not running at the moment.");
			}
		}

		
	}

	/**
	 * 
	 */
	protected void onViewJob() {
		
		String selection = table.getModel().getFirstSelectedKey();
		if (selection != null) {
			int idx = Integer.parseInt(selection);
			IJob job = jobList.get(idx);
			StackedContentContainer sc = (StackedContentContainer)getContainer();
			JobDetailsControl jobDetails = new JobDetailsControl (sc, "jd", job);
			sc.setCurrentControlName(jobDetails.getName());
		}

		
	}

	/**
	 * 
	 */
	protected void onRefresh() {

		table.setRequireRedraw(true);
		
	}

	/**
	 * 
	 */
	protected void onRunJob() {
		String selection = table.getModel().getFirstSelectedKey();
		if (selection != null) {
			int idx = Integer.parseInt(selection);
			IJob job = jobList.get(idx);
			if (job.isExecuting()) {
				errInfo.showError("The selected job is currently executing.");
			} else {
				ETLgineServer.getInstance().enqueueJob(job);
				//errInfo.showWarning("The job has been enqueued");
				table.setRequireRedraw(true);
			}
		}
	}

	/**
	 * 
	 */
	protected void close() {
		destroy();
	}

    /**
     *
     */
    protected void onReactivateJobTrigger() {
        String selection = table.getModel().getFirstSelectedKey();
        if (selection != null) {
            int idx = Integer.parseInt(selection);
            IJob job = jobList.get(idx);
            if (job.isExecuting()) {
                errInfo.showError("The selected job is currently executing.");
            } else {
                job.setStopTriggerAfterError(false);

                errInfo.showWarning("The job trigger has been activated");
                table.setRequireRedraw(true);
            }
        }
    }
}
