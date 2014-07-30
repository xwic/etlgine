/**
 * 
 */
package de.xwic.etlgine.server.admin;

import org.apache.commons.lang.StringUtils;

import de.jwic.base.IControlContainer;
import de.jwic.base.Page;
import de.jwic.controls.Button;
import de.jwic.controls.Label;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.etlgine.server.ETLgineServer;
import de.xwic.etlgine.server.ServerContext;

/**
 * @author Developer
 *
 */
public class AdminPage extends Page {

	private StackedContentContainer content;
	
	/**
	 * @param container
	 */
	public AdminPage(IControlContainer container) {
		this(container, null);
	}

	/**
	 * @param container
	 * @param name
	 */
	public AdminPage(IControlContainer container, String name) {
		super(container, name);
		setTitle("ETLgine (" + getServerInstance() +")");
		
		content = new StackedContentContainer(this, "content");
		
		new BreadCrumpControl(this, "breadcrump", content);
		new MainMenuControl(content, "mm");
		new StatusControl(this, "status");

        //Garbage collector button
		Button btGC = new Button(this, "btGC");
		btGC.setTitle("Run GC");
		btGC.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				System.gc();
			}
		});

		new OnOffControl(this, "on_off");
		
		createVersionInfoControl(this);
	}
	
	/**
	 * Returns the server name.
	 * @return
	 */
	public String getServerName() {
		return ETLgineServer.getInstance().getServerContext().getProperty(ServerContext.PROPERTY_SERVER_INSTANCENAME, ServerContext.PROPERTY_SERVER_INSTANCENAME_DEFAULT);
	}
	public String getServerInstance() {
		return ETLgineServer.getInstance().getServerContext().getProperty(ServerContext.PROPERTY_SERVER_INSTANCEID, ServerContext.PROPERTY_SERVER_INSTANCEID_DEFAULT);
	}
	
	protected void createVersionInfoControl(IControlContainer container) {
		// set the version label
		Label verInfo = new Label(container, "lblVersionInfo");
		
		String implementationVersion = ETLgineServer.getInstance().getServerContext().getProperty(ServerContext.PROPERTY_SERVER_VERSION, ServerContext.PROPERTY_SERVER_VERSION_DEFAULT);

		if (StringUtils.isEmpty(implementationVersion)) {
			implementationVersion = ServerContext.PROPERTY_SERVER_VERSION_DEFAULT;
		}
		verInfo.setText(getServerInstance() 
				+ " | v"
				+ implementationVersion );
	}

}
