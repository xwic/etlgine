/**
 * 
 */
package de.xwic.etlgine.server.admin;

import java.util.List;

import de.jwic.base.IControlContainer;
import de.jwic.base.Page;
import de.jwic.controls.Button;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.etlgine.publish.CubePublishDestination;
import de.xwic.etlgine.publish.CubePublisherHelper;
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
        btEnableTRG.setTitle("On");
        btEnableTRG.addSelectionListener(new SelectionListener() {
            public void objectSelected(SelectionEvent event) {
                ServerContext serverContext = ETLgineServer.getInstance().getServerContext();
                serverContext.setProperty("trigger.enabled", "true");
            }
        });

        Button btDisableTRG = new Button(this, "btDisableTRG");
        btDisableTRG.setTitle("Off");
        btDisableTRG.addSelectionListener(new SelectionListener() {
            public void objectSelected(SelectionEvent event) {
                ServerContext serverContext = ETLgineServer.getInstance().getServerContext();
                serverContext.setProperty("trigger.enabled", "false");
            }
        });

        for (final CubePublishDestination cubePublishDestination : getPublishDestinations()) {
            Button btPublishEnable = new Button(this, "btPublishEnable_"+cubePublishDestination.getKey());
            btPublishEnable.setTitle("On");
            btPublishEnable.addSelectionListener(new SelectionListener() {
                public void objectSelected(SelectionEvent event) {
                	setPublishStatus(cubePublishDestination.getKey(), true);
                }
            });
        		
            Button btPublishDisable = new Button(this, "btPublishDisable_"+cubePublishDestination.getKey());
            btPublishDisable.setTitle("Off");
            btPublishDisable.addSelectionListener(new SelectionListener() {
                public void objectSelected(SelectionEvent event) {
                	setPublishStatus(cubePublishDestination.getKey(), false);
                }
            });
        	
			
		}
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
    
    public List<CubePublishDestination> getPublishDestinations() {
    	return CubePublisherHelper.getInstance().getPublishTargets();
    }
    
    public void setPublishStatus(String targetKey, boolean publishEnabled) {
    	System.out.println(""+targetKey+publishEnabled);
    	CubePublisherHelper.getInstance().setTargetEnabled(targetKey, publishEnabled);
    }
}
