/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import de.jwic.base.IControlContainer;
import de.jwic.controls.ActionBarControl;
import de.jwic.controls.ButtonControl;
import de.jwic.ecolib.tableviewer.TableColumn;
import de.jwic.ecolib.tableviewer.TableModel;
import de.jwic.ecolib.tableviewer.TableViewer;
import de.jwic.ecolib.tableviewer.defaults.ListContentProvider;
import de.jwic.events.ElementSelectedEvent;
import de.jwic.events.ElementSelectedListener;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.cube.IDataPool;
import de.xwic.cube.IDataPoolManager;
import de.xwic.cube.StorageException;
import de.xwic.etlgine.cube.CubeHandler;
import de.xwic.etlgine.loader.cube.DataPoolInitializer;
import de.xwic.etlgine.server.ETLgineServer;
import de.xwic.etlgine.server.ServerContext;
import de.xwic.etlgine.server.admin.BaseContentContainer;
import de.xwic.etlgine.server.admin.ImageLibrary;
import de.xwic.etlgine.server.admin.StackedContentContainer;

/**
 * @author Developer
 *
 */
public class DPAdminControl extends BaseContentContainer {

	private TableViewer table;
	private ButtonControl btInitialize;
	private ButtonControl btOpen;
	private ButtonControl btRelease;
	private TableModel tableModel;
	private CubeHandler cubeHandler;
	
	/**
	 * @param container
	 * @param name
	 */
	public DPAdminControl(IControlContainer container, String name) {
		super(container, name);
		cubeHandler = CubeHandler.getCubeHandler(ETLgineServer.getInstance().getServerContext());
		
		setTitle("DataPoolManager Overview");
		setupActionBar();
		
		table = new TableViewer(this, "table");
		List<String> keyList = new ArrayList<String>();
		keyList.addAll(cubeHandler.getDataPoolManagerKeys());
		
		table.setContentProvider(new ListContentProvider(keyList) {
			public String getUniqueKey(Object object) {
				return (String)object;
			}
		});
		table.setTableLabelProvider(new DPTableLabelProvider());
		table.setWidth(949);
		table.setHeight(300);
		table.setResizeableColumns(true);
		table.setScrollable(true);
		table.setShowStatusBar(false);
		
		tableModel = table.getModel();
		tableModel.setSelectionMode(TableModel.SELECTION_SINGLE);
		tableModel.addColumn(new TableColumn("Data Pool Manager", 400, "name"));
		tableModel.addColumn(new TableColumn("Key", 240, "key"));
		tableModel.addColumn(new TableColumn("Status", 150, "status"));
		tableModel.addElementSelectedListener(new ElementSelectedListener() {
			public void elementSelected(ElementSelectedEvent event) {
				updateButtonState();
			}
		});
		
		updateButtonState();
	}

	/**
	 * 
	 */
	protected void updateButtonState() {
		boolean selected = tableModel.getSelection().size() > 0;
		btInitialize.setEnabled(selected);
		btOpen.setEnabled(selected);
	}

	/**
	 * 
	 */
	private void setupActionBar() {

		ActionBarControl abar = new ActionBarControl(this, "actionBar");
		
		ButtonControl btReturn = new ButtonControl(abar, "return");
		btReturn.setIconEnabled(ImageLibrary.IMAGE_RETURN);
		btReturn.setTitle("Return");
		btReturn.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				close();
			}
		});
		
		btOpen= new ButtonControl(abar, "open");
		btOpen.setIconEnabled(ImageLibrary.IMAGE_DATABASE);
		btOpen.setTitle("Open");
		btOpen.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				doDbOpen();
			}
		});

		btInitialize = new ButtonControl(abar, "init");
		btInitialize.setIconEnabled(ImageLibrary.IMAGE_DATABASE_INIT);
		btInitialize.setTitle("Initialize");
		btInitialize.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				doDbInitialize();
			}
		});
		
		btRelease = new ButtonControl(abar, "release");
		btRelease.setTitle("Release");
		btRelease.addSelectionListener(new SelectionListener() {
			/* (non-Javadoc)
			 * @see de.jwic.events.SelectionListener#objectSelected(de.jwic.events.SelectionEvent)
			 */
			public void objectSelected(SelectionEvent event) {
				doRelease();
			}
		});
	}

	/**
	 * 
	 */
	protected void doRelease() {
		
		ServerContext context = ETLgineServer.getInstance().getServerContext();
		String dpmKey = tableModel.getFirstSelectedKey();
		String key = context.getProperty(dpmKey + ".datapool.key", null);
		
		if (key != null) {
			try {
				IDataPoolManager dpm = cubeHandler.getDataPoolManager(dpmKey);
				IDataPool pool;
				if (dpm.isDataPoolLoaded(key)) {
					pool = dpm.getDataPool(key);
					dpm.releaseDataPool(pool);
				}
				table.setRequireRedraw(true);
			} catch (StorageException se) {
				//log.error("Error releasing dataPool", se);
				throw new RuntimeException("Error releasing DP", se);
			}
		}
	}

	/**
	 * 
	 */
	protected void doDbOpen() {
		
		String dpmKey = tableModel.getFirstSelectedKey();
		StackedContentContainer sc = (StackedContentContainer)getContainer();
		DPDetailsControl dpDetails = new DPDetailsControl(sc, "dpd", dpmKey);
		sc.setCurrentControlName(dpDetails.getName());		

	}

	/**
	 * 
	 */
	protected void doDbInitialize() {
		
		
		ServerContext context = ETLgineServer.getInstance().getServerContext();
		String dpmKey = tableModel.getFirstSelectedKey();
		String key = context.getProperty(dpmKey + ".datapool.key", null);
		
		if (key != null) {
			try {
				IDataPoolManager dpm = cubeHandler.getDataPoolManager(dpmKey);
				IDataPool pool;
				if (!dpm.containsDataPool(key)) {
					pool = dpm.createDataPool(key);
				} else {
					pool = dpm.getDataPool(key);
				}
		
				String initScript = context.getProperty(dpmKey + ".datapool.initScript", null);
				if (initScript != null) {
					String path = context.getProperty(dpmKey + ".datapool.path", null);
					File fRoot = new File(context.getProperty(ServerContext.PROPERTY_ROOTPATH, "."));
					File fDP = new File(fRoot, path);
					File fScript = new File(fDP, initScript);
					if (fScript.exists()) {
						DataPoolInitializer dpInit = new DataPoolInitializer(context, fScript);
						dpInit.verify(pool);
					} else {
						log.warn("Init Script: " + initScript + " does not exist.");
					}

				}
				
				pool.save();
				table.setRequireRedraw(true);
			} catch (Exception e) {
				//log.error("Error initializing pool: " + e, e);
				throw new RuntimeException("Error initializing pool", e);
			}
		}
		
	}

	/**
	 * 
	 */
	protected void close() {
		destroy();
	}

}
