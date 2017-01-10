/**
 * 
 */
package de.xwic.etlgine.server.admin;

import de.jwic.base.Control;
import de.jwic.base.IControlContainer;
import de.xwic.appkit.webbase.toolkit.app.ExtendedApplication;
import de.xwic.appkit.webbase.toolkit.app.Site;

/**
 * @author Developer
 *
 */
public class AdminApplication extends ExtendedApplication {

	/* (non-Javadoc)
	 * @see de.jwic.base.Application#createRootControl(de.jwic.base.IControlContainer)
	 */
	@Override
	public Control createRootControl(IControlContainer container) {
		container.getSessionContext().setExitURL("byebye.html");
		AdminPage page = new AdminPage(container);
		return page;
	}

	@Override
	public String getPageTitle() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getHelpURL() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void loadApp(Site site) {
		// TODO Auto-generated method stub
		
	}

}
