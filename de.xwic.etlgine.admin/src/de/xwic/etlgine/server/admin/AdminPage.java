/**
 * 
 */
package de.xwic.etlgine.server.admin;

import de.jwic.base.IControlContainer;
import de.jwic.base.Page;
import de.jwic.controls.Button;
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
		setTitle("ETLgine Administration");
		
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


        // Activate trigger
        Button btEnableTRG = new Button(this, "btEnableTRG");
        btEnableTRG.setTitle("Enable Trigger");
        btEnableTRG.addSelectionListener(new SelectionListener() {
            public void objectSelected(SelectionEvent event) {
                ServerContext serverContext = ETLgineServer.getInstance().getServerContext();
                serverContext.setProperty("trigger.enabled", "true");
            }
        });

        Button btDisableTRG = new Button(this, "btDisableTRG");
        btDisableTRG.setTitle("Disable Trigger");
        btDisableTRG.addSelectionListener(new SelectionListener() {
            public void objectSelected(SelectionEvent event) {
                ServerContext serverContext = ETLgineServer.getInstance().getServerContext();
                serverContext.setProperty("trigger.enabled", "false");
            }
        });

	}
	
	/**
	 * Returns the server name.
	 * @return
	 */
	public String getServerName() {
		return ETLgineServer.getInstance().getServerContext().getProperty("name", "unnamed");
	}

    public boolean showEnable() {
        return ETLgineServer.getInstance().getServerContext().getPropertyBoolean("trigger.enabled", true);
    }
}
