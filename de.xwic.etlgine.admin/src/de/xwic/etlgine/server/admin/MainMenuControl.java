/**
 * 
 */
package de.xwic.etlgine.server.admin;

import de.jwic.base.IControlContainer;
import de.jwic.base.ImageRef;
import de.jwic.controls.Button;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.etlgine.server.ETLgineServer;
import de.xwic.etlgine.server.admin.datapool.DPAdminControl;
import de.xwic.etlgine.server.admin.jobs.JobAdminControl;
import de.xwic.etlgine.server.admin.users.UsersListControl;

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
		
		Button btJobs = new Button(this, "btJobs");
		btJobs.setTitle("Jobs");
		btJobs.setWidth(120);
		btJobs.setIconEnabled(new ImageRef("img/script.png"));
		btJobs.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onJobsSelection();
			}
		});
		Button btDataPools = new Button(this, "btDataPools");
		btDataPools.setTitle("Data Pools");
		btDataPools.setWidth(120);
		btDataPools.setIconEnabled(new ImageRef("img/database.png"));
		btDataPools.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onDPSelection();
			}
		});

		Button btShutDown = new Button(this, "btShutDown");
		btShutDown.setTitle("Shut Down");
		btShutDown.setWidth(120);
		btShutDown.setConfirmMsg("Do you realy want to SHUT DOWN the ETLgine Server?");
		btShutDown.setIconEnabled(ImageLibrary.IMAGE_RETURN);
		btShutDown.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onShutDown();
			}
		});

		Button btUserList = new Button(this, "btUserList");
		btUserList.setTitle("Users");
		btUserList.setWidth(120);
		btUserList.setIconEnabled(ImageLibrary.IMAGE_TABLE);
		btUserList.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onUsersListSelection();
			}
		});
		
	}

	/**
	 * 
	 */
	protected void onShutDown() {
		
		ETLgineServer.getInstance().exitServer();
		getSessionContext().exit();
		
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
	protected void onUsersListSelection() {
		StackedContentContainer sc = (StackedContentContainer)getContainer();
		
		UsersListControl usersList = new UsersListControl(sc, "acl");
		sc.setCurrentControlName(usersList.getName());		
	}
	
	
}
