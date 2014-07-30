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
        log.info( "  Implementation Title:" + this.getClass().getPackage().getImplementationTitle() );
        log.info( " Implementation Vendor:" + this.getClass().getPackage().getImplementationVendor() );
        log.info( "Implementation Version:" + this.getClass().getPackage().getImplementationVersion() );
        log.info( "    Specification Tile:" + this.getClass().getPackage().getSpecificationTitle() );
        log.info( "  Specification Vendor:" + this.getClass().getPackage().getSpecificationVendor() );
        log.info( " Specification Version:" + this.getClass().getPackage().getSpecificationVersion() );

		String implementationVersion =this.getClass().getPackage().getSpecificationVersion();
		log.info("Got data - " + implementationVersion);
		if (!StringUtils.isEmpty(implementationVersion)) {
			log.info("Not EMPTY!");
			if(implementationVersion.contains("SNAPSHOT")) {
				log.info("SNAPSHOT!");
				implementationVersion = implementationVersion +  "(#" + this.getClass().getPackage().getImplementationVersion()+")";
			} else {
				log.info("NO SNAPSHOT!");
				implementationVersion =  implementationVersion + "." + this.getClass().getPackage().getImplementationVersion();
			}
		} else {
			log.info("EMPTY!");
			implementationVersion = "5.0.0.0-SNAPSHOT";
		}
		verInfo.setText(getServerInstance() 
				+ " | v"
				+ implementationVersion );
	}

}
