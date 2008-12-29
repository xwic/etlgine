/**
 * 
 */
package de.xwic.etlgine.server.admin;

import de.jwic.base.IControlContainer;
import de.jwic.base.ImageRef;
import de.jwic.controls.ButtonControl;
import de.jwic.ecolib.controls.StackedContainer;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.etlgine.server.admin.datapool.DPAdminControl;
import de.xwic.etlgine.server.admin.jobs.JobAdminControl;

/**
 * @author Developer
 *
 */
public class MainMenuControl extends BaseContentContainer {

	/**
	 * @param container
	 * @param name
	 */
	public MainMenuControl(IControlContainer container, String name) {
		super(container, name);
		
		setTitle("Home");
		
		ButtonControl btJobs = new ButtonControl(this, "btJobs");
		btJobs.setTitle("Jobs");
		btJobs.setWidth(120);
		btJobs.setIconEnabled(new ImageRef("img/script.png"));
		btJobs.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onJobsSelection();
			}
		});
		ButtonControl btDataPools = new ButtonControl(this, "btDataPools");
		btDataPools.setTitle("Data Pools");
		btDataPools.setWidth(120);
		btDataPools.setIconEnabled(new ImageRef("img/database.png"));
		btDataPools.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onDPSelection();
			}
		});

	}

	/**
	 * 
	 */
	protected void onJobsSelection() {
		StackedContentContainer sc = (StackedContentContainer)getContainer();
		JobAdminControl jobAdmin = new JobAdminControl(sc, "joa");
		sc.setCurrentControlName(jobAdmin.getName());		
	}

	/**
	 * 
	 */
	protected void onDPSelection() {
		StackedContentContainer sc = (StackedContentContainer)getContainer();
		DPAdminControl dpAdmin = new DPAdminControl(sc, "dp");
		sc.setCurrentControlName(dpAdmin.getName());		
	}
	
	
}
