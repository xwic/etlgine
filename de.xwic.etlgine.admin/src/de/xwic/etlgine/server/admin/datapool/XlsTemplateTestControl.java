/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.jwic.base.IControlContainer;
import de.jwic.controls.Button;
import de.jwic.controls.ErrorWarning;
import de.jwic.controls.FileUpload;
import de.jwic.controls.ToolBar;
import de.jwic.controls.ToolBarGroup;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.cube.IDataPool;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.cube.webui.controls.DimensionElementSelector;
import de.xwic.etlgine.server.admin.BaseContentContainer;
import de.xwic.etlgine.server.admin.ImageLibrary;

/**
 * @author lippisch
 *
 */
public class XlsTemplateTestControl extends BaseContentContainer {

	private final IDataPool dataPool;
	private FileUpload fileUpload;
	private ErrorWarning errInfo;
	private XlsDownloadControl downloadCtrl;

	private Map<String, String> dimCtrlMap = new HashMap<String, String>();
	
	/**
	 * @param container
	 * @param name
	 */
	public XlsTemplateTestControl(IControlContainer container, String name, IDataPool dataPool) {
		super(container, name);
		this.dataPool = dataPool;
		
		setTitle("Excel Template Test");
		
		ToolBar abar = new ToolBar(this, "actionBar");
		ToolBarGroup group = abar.addGroup();
		Button  btReturn = group.addButton();
		btReturn.setIconEnabled(ImageLibrary.IMAGE_RETURN);
		btReturn.setTitle("Return");
		btReturn.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onReturn();
			}
		});

		errInfo = new ErrorWarning(this, "errInfo");
		
		fileUpload = new FileUpload(this, "fileUpload");
		
		Button btTest = group.addButton();
		btTest.setTitle("Process File");
		btTest.addSelectionListener(new SelectionListener() {
			/* (non-Javadoc)
			 * @see de.jwic.events.SelectionListener#objectSelected(de.jwic.events.SelectionEvent)
			 */
			public void objectSelected(SelectionEvent event) {
				onProcessTemplate();
			}
		});
		
		
		downloadCtrl = new XlsDownloadControl(this, "download");
		
		for (IDimension dim : dataPool.getDimensions()) {
			DimensionElementSelector dimSel = new DimensionElementSelector(this, null, dim);
			dimCtrlMap.put(dim.getKey(), dimSel.getName());
		}
		
	}

	/**
	 * 
	 */
	protected void onProcessTemplate() {
		if (!fileUpload.isFileUploaded()) {
			errInfo.showError("You must select a file.");
		} else {
			try {
				
				// build filter list
				List<IDimensionElement> filters = new ArrayList<IDimensionElement>();
				for (String selName : dimCtrlMap.values()) {
					DimensionElementSelector dse = (DimensionElementSelector)getControl(selName);
					filters.add(dse.getDimensionElement());
				}
				
				downloadCtrl.startDownload(fileUpload.getInputStream(), dataPool, filters);
			} catch (IOException e) {
				errInfo.showError(e);
			}
		}
		
	}

	/**
	 * Returns the list of dimensions.
	 * @return
	 */
	public Collection<IDimension> getDimensions() {
		return dataPool.getDimensions();
	}
	
	/**
	 * Returns the key to the specified dimension element selector.
	 * @param key
	 * @return
	 */
	public String getDimCtrlId(String key) {
		return dimCtrlMap.get(key);
	}
	
	/**
	 * 
	 */
	protected void onReturn() {

		destroy();
		
	}

}
