/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import de.jwic.base.IControlContainer;
import de.jwic.controls.ActionBarControl;
import de.jwic.controls.ButtonControl;
import de.jwic.ecolib.controls.ErrorWarningControl;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.cube.IDataPool;
import de.xwic.cube.IDataPoolManager;
import de.xwic.cube.IDimension;
import de.xwic.cube.util.JDBCSerializerUtil;
import de.xwic.cube.webui.controls.DimensionElementSelector;
import de.xwic.etlgine.jdbc.JDBCUtil;
import de.xwic.etlgine.server.ETLgineServer;
import de.xwic.etlgine.server.ServerContext;
import de.xwic.etlgine.server.admin.BaseContentContainer;
import de.xwic.etlgine.server.admin.ImageLibrary;
import de.xwic.etlgine.server.admin.StackedContentContainer;

/**
 * @author Developer
 *
 */
public class DPDetailsControl extends BaseContentContainer {

	private final String dataPoolManagerKey;
	private IDataPool dataPool;

	private Map<IDimension, DimensionElementSelector> selectorMap = new HashMap<IDimension, DimensionElementSelector>();
	private ButtonControl btSave;
	private String syncTableConnectionName;
	private ServerContext context;
	
	private ErrorWarningControl errInfo;
	
	/**
	 * @param container
	 * @param name
	 */
	public DPDetailsControl(IControlContainer container, String name, String dataPoolManagerKey) {
		super(container, name);
		this.dataPoolManagerKey = dataPoolManagerKey;
		
		context = ETLgineServer.getInstance().getServerContext();
		setTitle("DataPool Details (" + dataPoolManagerKey + ")");
		
		errInfo = new ErrorWarningControl(this, "errInfo");
		
		ActionBarControl abar = new ActionBarControl(this, "actionBar");
		
		ButtonControl btReturn = new ButtonControl(abar, "return");
		btReturn.setIconEnabled(ImageLibrary.IMAGE_RETURN);
		btReturn.setTitle("Return");
		btReturn.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				close();
			}
		});

		btSave = new ButtonControl(abar, "save");
		btSave.setIconEnabled(ImageLibrary.IMAGE_DATABASE_SAVE);
		btSave.setTitle("Save to InitTable");
		btSave.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onSaveToInit();
			}
		});
		
		loadDataPoolInfo();
		
	}
	
	/**
	 * 
	 */
	protected void onSaveToInit() {
		
		try {
			Connection connection = JDBCUtil.openConnection(context, syncTableConnectionName);
			try {
				JDBCSerializerUtil.storeMeasures(connection, dataPool, "XCUBE_MEASURES");
				
				JDBCSerializerUtil.storeDimensions(connection, dataPool, "XCUBE_DIMENSIONS", "XCUBE_DIMENSION_ELEMENTS");
				
				errInfo.showWarning("Database Updated");
			} finally {
				connection.close();
			}
		} catch (Exception e) {
			log.error("Error saving to sync table.", e);
			errInfo.showError(e);
		}
		
	}

	/**
	 * 
	 */
	public void actionDimEdit(String dimKey) {
		
		StackedContentContainer sc = (StackedContentContainer)getContainer();
		DimensionEditorControl dimEditor = new DimensionEditorControl(sc, null, dataPool.getDimension(dimKey));
		sc.setCurrentControlName(dimEditor.getName());		
		
	}

	/**
	 * 
	 */
	private void loadDataPoolInfo() {
		
		String key = context.getProperty(dataPoolManagerKey + ".datapool.key", null);
		syncTableConnectionName = context.getProperty(dataPoolManagerKey + ".datapool.syncTables.connection");
		
		if (key != null) {
			try {
				IDataPoolManager dpm = context.getDataPoolManager(dataPoolManagerKey);
				if (dpm.containsDataPool(key)) {
					dataPool = dpm.getDataPool(key);
					for (IDimension dim : dataPool.getDimensions()) {
						DimensionElementSelector dsc = new DimensionElementSelector(this, null, dim);
						dsc.setWidth(148);
						selectorMap.put(dim, dsc);
					}
				}
			} catch (Exception e) {
				log.error("Error loading datapool: " + e, e);
			}
		}
		
		btSave.setEnabled(syncTableConnectionName != null);
	}

	/**
	 * Returns the name of the Selector.
	 * @param dim
	 * @return
	 */
	public String getSelectorName(IDimension dim) {
		return selectorMap.get(dim).getName();
	}
	
	/**
	 * 
	 */
	protected void close() {
		destroy();
	}

	/**
	 * @return the dataPoolManagerKey
	 */
	public String getDataPoolManagerKey() {
		return dataPoolManagerKey;
	}

	/**
	 * @return the dataPool
	 */
	public IDataPool getDataPool() {
		return dataPool;
	}

}
