/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import de.jwic.base.IControlContainer;
import de.jwic.controls.ActionBarControl;
import de.jwic.controls.ButtonControl;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.cube.ICube;
import de.xwic.etlgine.server.admin.BaseContentContainer;
import de.xwic.etlgine.server.admin.ImageLibrary;
import de.xwic.etlgine.server.admin.StackedContentContainer;

/**
 * Show cube details.
 * @author lippisch
 */
public class CubeDetailsControl extends BaseContentContainer {

	private final ICube cube;

	/**
	 * @param container
	 * @param name
	 */
	public CubeDetailsControl(IControlContainer container, String name, ICube cube) {
		super(container, name);
		this.cube = cube;

		setTitle("Cube [" + cube.getKey() + "]");
		
		createActionBar();
		
	}

	/**
	 * 
	 */
	private void createActionBar() {

		ActionBarControl abar = new ActionBarControl(this, "actionBar");
		
		ButtonControl btReturn = new ButtonControl(abar, "return");
		btReturn.setIconEnabled(ImageLibrary.IMAGE_RETURN);
		btReturn.setTitle("Return");
		btReturn.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				close();
			}
		});

		ButtonControl action = new ButtonControl(abar);
		//action.setIconEnabled(ImageLibrary.IMAGE_RETURN);
		action.setTitle("Import from CSV");
		action.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onImportFromCSV();
			}
		});

		
	}

	/**
	 * 
	 */
	protected void onImportFromCSV() {
		
		StackedContentContainer sc = (StackedContentContainer)getContainer();
		CubeImportControl control = new CubeImportControl(sc, null, cube);
		sc.setCurrentControlName(control.getName());		

		
	}

	/**
	 * 
	 */
	protected void close() {
		destroy();
	}

	
	/**
	 * @return the cube
	 */
	public ICube getCube() {
		return cube;
	}

}
