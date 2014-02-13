/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import de.jwic.base.IControlContainer;
import de.jwic.controls.ActionBarControl;
import de.jwic.controls.ButtonControl;
import de.jwic.controls.FileUpload;
import de.jwic.controls.ErrorWarning;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.cube.ICube;
import de.xwic.etlgine.cube.CubeImportUtil;
import de.xwic.etlgine.server.admin.BaseContentContainer;
import de.xwic.etlgine.server.admin.ImageLibrary;

/**
 * @author lippisch
 */
public class CubeImportControl extends BaseContentContainer {

	private final ICube cube;
	private FileUpload fileUpload;
	private ErrorWarning errInfo;

	/**
	 * @param container
	 * @param name
	 */
	public CubeImportControl(IControlContainer container, String name, ICube cube) {
		super(container, name);
		this.cube = cube;
		
		setTitle("Data Import into Cube [" + cube.getKey() + "]");
		
		ActionBarControl abar = new ActionBarControl(this, "actionBar");
		
		ButtonControl btReturn = new ButtonControl(abar, "return");
		btReturn.setIconEnabled(ImageLibrary.IMAGE_RETURN);
		btReturn.setTitle("Return");
		btReturn.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onReturn();
			}
		});

		errInfo = new ErrorWarning(this, "errInfo");
		
		fileUpload = new FileUpload(this, "fileUpload");
		
		ButtonControl btImport = new ButtonControl(this, "btImport");
		btImport.setTitle("Import File");
		btImport.addSelectionListener(new SelectionListener() {
			/* (non-Javadoc)
			 * @see de.jwic.events.SelectionListener#objectSelected(de.jwic.events.SelectionEvent)
			 */
			public void objectSelected(SelectionEvent event) {
				onImportFile();
			}
		});
		
		
	}

	/**
	 * 
	 */
	protected void onImportFile() {
		if (!fileUpload.isFileUploaded()) {
			errInfo.showError("You must select a file.");
		} else {
			try {
			
				cube.clear();
				CubeImportUtil.importCSV(fileUpload.getInputStream(), cube);
				errInfo.showWarning("The data has been imported successfully. (Hit Return)");
				
			} catch (Exception e) {
				errInfo.showError(e);
			}
		}
		
	}

	
	/**
	 * 
	 */
	protected void onReturn() {

		destroy();
		
	}

}
