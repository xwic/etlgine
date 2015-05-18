/**
 * 
 */
package de.xwic.etlgine.server.admin.jobs;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import de.jwic.base.IControlContainer;
import de.jwic.controls.Button;
import de.jwic.controls.ErrorWarning;
import de.jwic.controls.ToolBar;
import de.jwic.controls.ToolBarGroup;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.etlgine.IJob;
import de.xwic.etlgine.server.admin.BaseContentContainer;
import de.xwic.etlgine.server.admin.ImageLibrary;

/**
 * Displays Job Details.
 * @author lippisch
 */
public class JobDetailsControl extends BaseContentContainer {

	private final IJob job;
	private Button btDisable;
    private Button btReactivateJobTrigger;
	private ErrorWarning errInfo;

	/**
	 * @param container
	 * @param name
	 */
	public JobDetailsControl(IControlContainer container, String name, IJob job) {
		super(container, name);
		this.job = job;

		setTitle("Job Details (" + job.getName() + ")");
		
		
		createActionBar();
		errInfo = new ErrorWarning(this, "errorInfo");

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
				onReturn();
			}
		});

		btDisable = group.addButton();
		btDisable.setIconEnabled(ImageLibrary.IMAGE_CONTROL_PAUSE);
		btDisable.setTitle(job.isDisabled() ? "Enable" : "Disable");
		btDisable.addSelectionListener(new SelectionListener() {
			private static final long serialVersionUID = 1L;
			public void objectSelected(SelectionEvent event) {
				onDisable();
			}
		});

       /* btReactivateJobTrigger = group.addButton();
        btReactivateJobTrigger.setIconEnabled(ImageLibrary.IMAGE_SCRIPT_RED);
        btReactivateJobTrigger.setTitle("Activate Trigger for Failed Job");
        btReactivateJobTrigger.addSelectionListener(new SelectionListener() {
            private static final long serialVersionUID = 1L;
            public void objectSelected(SelectionEvent event) {
                onActivateJobTrigger();
            }
        });*/

		
	}

	/**
	 * 
	 */
	protected void onDisable() {
		job.setDisabled(!job.isDisabled());
		btDisable.setTitle(job.isDisabled() ? "Enable" : "Disable");
	}
    /**
    *
    */
   protected void onActivateJobTrigger() {
       if (job.isExecuting()) {
           errInfo.showError("The selected job is currently executing.");
       } else {
           job.setStopTriggerAfterError(false);

           // Hack so that the job next run time is calculated and the job is not started immediately
           if(null != job.getTrigger()) {
               job.getTrigger().notifyJobFinished(true);
           }

           errInfo.showWarning("The job trigger has been activated");
           this.setRequireRedraw(true);
       }
   }

	/**
	 * 
	 */
	protected void onReturn() {
		destroy();
	}

	/**
	 * @return the job
	 */
	public IJob getJob() {
		return job;
	}
	
	/**
	 * @return the last exception's stack trace
	 */
	public String getLastExceptionStackTrace() {
		if (job == null || job.getLastException() == null) {
			return null;
		}
		ByteArrayOutputStream stackTrace = new ByteArrayOutputStream();
		PrintWriter pw = new PrintWriter(stackTrace);
		job.getLastException().printStackTrace(pw);
		pw.flush();
		
		return stackTrace.toString();
	}
	
	public String getJobState(IJob job) {
		String jobState = job.getState().name();
		if(IJob.State.FINISHED_WITH_ERROR.equals(job.getState()) || IJob.State.ERROR.equals(job.getState())) {
            if(job.isStopTriggerAfterError()) {
            	jobState = jobState + " [TRIGGER OFF]";
            } else {
            	jobState = jobState + " [TRIGGER ON]";
            }

        }	
		return jobState;
	}
}