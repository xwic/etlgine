/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import de.jwic.base.IControlContainer;
import de.jwic.controls.Button;
import de.jwic.controls.ToolBar;
import de.jwic.controls.ToolBarGroup;
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

		ToolBar abar = new ToolBar(this, "actionBar");
		ToolBarGroup group = abar.addGroup();
		
		Button btReturn = group.addButton();
		btReturn.setIconEnabled(ImageLibrary.IMAGE_RETURN);
		btReturn.setTitle("Return");
		btReturn.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				close();
			}
		});

		Button action = group.addButton();
		//action.setIconEnabled(ImageLibrary.IMAGE_RETURN);
		action.setTitle("Import from CSV");
		action.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onImportFromCSV();
			}
		});

		Button delete = group.addButton();
		delete.setIconEnabled(ImageLibrary.IMAGE_TABLE_DELETE);
		delete.setTitle("Delete Cube");
		delete.setConfirmMsg("Do you really want to delete this cube from the datapool.");
		delete.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onDeleteCube();
			}
		});
		
		Button btClear = group.addButton();
		btClear.setIconEnabled(ImageLibrary.IMAGE_TABLE_DELETE);
		btClear.setTitle("Clear Cube");
		btClear.setConfirmMsg("Do you really want to delete ALL DATA (cells) from this cube.");
		btClear.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onClearCube();
			}
		});
		
	}

	/**
	 * 
	 */
	protected void onClearCube() {
		
		cube.clear();
		close();
		
	}

	/**
	 * 
	 */
	protected void onDeleteCube() {
		
		cube.remove();
		close();
		
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
