/**
 * 
 */
package de.xwic.etlgine.server.admin;

import de.jwic.base.IControlContainer;
import de.jwic.base.Page;
import de.jwic.ecolib.controls.StackedContainer;

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
		
		new MainMenuControl(content, "mm");
		
	}

}
