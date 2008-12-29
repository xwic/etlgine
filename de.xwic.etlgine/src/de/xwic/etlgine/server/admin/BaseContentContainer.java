/**
 * 
 */
package de.xwic.etlgine.server.admin;

import de.jwic.base.ControlContainer;
import de.jwic.base.IControlContainer;
import de.jwic.base.IOuterLayout;

/**
 * @author Developer
 *
 */
public abstract class BaseContentContainer extends ControlContainer implements IOuterLayout {

	protected String title = null;
	
	/**
	 * @param container
	 */
	public BaseContentContainer(IControlContainer container) {
		super(container);
		setRendererId(DEFAULT_OUTER_RENDERER);
	}

	/**
	 * @param container
	 * @param name
	 */
	public BaseContentContainer(IControlContainer container, String name) {
		super(container, name);
		setRendererId(DEFAULT_OUTER_RENDERER);
	}
	
	/* (non-Javadoc)
	 * @see de.jwic.base.IOuterLayout#getOuterTemplateName()
	 */
	public String getOuterTemplateName() {
		return BaseContentContainer.class.getName();
	}

	/**
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * @param title the title to set
	 */
	public void setTitle(String title) {
		this.title = title;
	}

}
