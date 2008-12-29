/**
 * 
 */
package de.xwic.etlgine.server.admin;

import de.jwic.base.Application;
import de.jwic.base.Control;
import de.jwic.base.IControlContainer;

/**
 * @author Developer
 *
 */
public class AdminApplication extends Application {

	/* (non-Javadoc)
	 * @see de.jwic.base.Application#createRootControl(de.jwic.base.IControlContainer)
	 */
	@Override
	public Control createRootControl(IControlContainer container) {
		AdminPage page = new AdminPage(container);
		return page;
	}

}
