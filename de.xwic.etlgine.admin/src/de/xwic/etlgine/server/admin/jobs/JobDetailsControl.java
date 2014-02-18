/**
 * 
 */
package de.xwic.etlgine.server.admin.jobs;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import de.jwic.base.IControlContainer;
import de.jwic.controls.ActionBarControl;
import de.jwic.controls.ButtonControl;
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
	private ButtonControl btDisable;

	/**
	 * @param container
	 * @param name
	 */
	public JobDetailsControl(IControlContainer container, String name, IJob job) {
		super(container, name);
		this.job = job;

		setTitle("Job Details (" + job.getName() + ")");
		
		
		createActionBar();
		
	}

	/**
	 * 
	 */
	private void createActionBar() {

		ActionBarControl abar = new ActionBarControl(this, "actionBar");
		
		ButtonControl btReturn = new ButtonControl(abar, "return");
		btReturn.setIconEnabled(ImageLibrary.IMAGE_RETURN);
		btReturn.setTitle("Return");
		btReturn.addSelectionListener(new SelectionListener() {
			private static final long serialVersionUID = 1L;
			public void objectSelected(SelectionEvent event) {
				onReturn();
			}
		});

		btDisable = new ButtonControl(abar);
		btDisable.setIconEnabled(ImageLibrary.IMAGE_CONTROL_PAUSE);
		btDisable.setTitle(job.isDisabled() ? "Enable" : "Disable");
		btDisable.addSelectionListener(new SelectionListener() {
			private static final long serialVersionUID = 1L;
			public void objectSelected(SelectionEvent event) {
				onDisable();
			}
		});

		
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
}