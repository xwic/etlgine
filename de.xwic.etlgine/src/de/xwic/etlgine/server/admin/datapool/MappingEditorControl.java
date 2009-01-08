/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import java.sql.Connection;
import java.util.List;

import de.jwic.base.IControlContainer;
import de.jwic.controls.ActionBarControl;
import de.jwic.controls.ButtonControl;
import de.jwic.controls.InputBoxControl;
import de.jwic.controls.LabelControl;
import de.jwic.controls.ListBoxControl;
import de.jwic.controls.RadioGroupControl;
import de.jwic.ecolib.controls.ErrorWarningControl;
import de.jwic.events.ElementSelectedEvent;
import de.jwic.events.ElementSelectedListener;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.cube.IDataPool;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.cube.webui.controls.DimensionElementSelector;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.cube.CubeHandler;
import de.xwic.etlgine.cube.mapping.DimMappingDef;
import de.xwic.etlgine.cube.mapping.DimMappingDefDAO;
import de.xwic.etlgine.cube.mapping.DimMappingElementDef;
import de.xwic.etlgine.cube.mapping.DimMappingElementDefDAO;
import de.xwic.etlgine.jdbc.JDBCUtil;
import de.xwic.etlgine.server.ETLgineServer;
import de.xwic.etlgine.server.ServerContext;
import de.xwic.etlgine.server.admin.BaseContentContainer;
import de.xwic.etlgine.server.admin.ImageLibrary;

/**
 * @author lippisch
 *
 */
public class MappingEditorControl extends BaseContentContainer {

	private DimMappingDef dimMapping;
	
	private InputBoxControl inpKey;
	private InputBoxControl inpDescription;
	private ListBoxControl lbcDimension;
	private RadioGroupControl chkOptions;
	private DimensionElementSelector elmSelector;
	private ErrorWarningControl errInfo;

	private MappingElementEditorControl mapEditor;
	
	private ButtonControl btSave;
	
	private IDataPool dataPool;

	private final String dpManagerKey;

	private boolean isNew;

	/**
	 * @param container
	 * @param name
	 */
	public MappingEditorControl(IControlContainer container, String name, String dpManagerKey, DimMappingDef dimMapping) {
		super(container, name);
		this.dpManagerKey = dpManagerKey;
		this.dimMapping = dimMapping;
		isNew = dimMapping.getKey() == null;
		

		try {
			dataPool = CubeHandler.getCubeHandler(ETLgineServer.getInstance().getServerContext()).openDataPool(dpManagerKey);
		} catch (ETLException e) {
			throw new RuntimeException("Error Reading DataPool: " + e, e);
		}
		
		String key = dimMapping.getKey() != null ? dimMapping.getKey() : null;
		setTitle("Mapping Editor (" + key + ")");
		
		errInfo = new ErrorWarningControl(this, "errInfo");
		mapEditor = new MappingElementEditorControl(this, "mapEditor");

		setupActionBar();
		createDimMappingEditor();
		
		
		// load childs if its not a new one
		if (!isNew) {
			loadMappingElements();
		}
	}

	/**
	 * 
	 */
	private void loadMappingElements() {
		ServerContext context = ETLgineServer.getInstance().getServerContext();
		String syncTableConnectionName = context.getProperty(dpManagerKey + ".datapool.syncTables.connection");
		try {
			Connection connection = JDBCUtil.openConnection(context, syncTableConnectionName);
			try {
				DimMappingElementDefDAO dao = new DimMappingElementDefDAO(connection);
				List<DimMappingElementDef> list = dao.listMappings(dimMapping.getKey());
				for (DimMappingElementDef me : list) {
					me.setDimensionKey(dimMapping.getDimensionKey());	// for security...
				}
				mapEditor.setMappingList(list);
			} finally {
				connection.close();
			}
		} catch (Exception e) {
			errInfo.showError(e);
			log.error("Error loading mapping elements: ", e);
		}
		
	}

	/**
	 * Setup the ActionBar.
	 */
	private void setupActionBar() {
		ActionBarControl abar = new ActionBarControl(this, "actionBar");
		
		ButtonControl btReturn = new ButtonControl(abar, "return");
		btReturn.setIconEnabled(ImageLibrary.IMAGE_RETURN);
		btReturn.setTitle("Return");
		btReturn.setConfirmMsg("Changes will get lost!");
		btReturn.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				close();
			}
		});

		btSave = new ButtonControl(abar, "save");
		btSave.setIconEnabled(ImageLibrary.IMAGE_TABLE_SAVE);
		btSave.setTitle("Save & Close");
		btSave.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onSaveAndClose();
			}
		});

		ButtonControl btAdd = new ButtonControl(abar, "insert");
		btAdd.setTitle("Create Element");
		btAdd.setIconEnabled(ImageLibrary.IMAGE_ADD);
		btAdd.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				mapEditor.createNewElement();
			}
		});
		
	}
	/**
	 * 
	 */
	protected void onSaveAndClose() {
		
		String key = inpKey.getText().trim();
		if (key.length() == 0) {
			errInfo.showError("You must specify a key.");
			return;
		}
		
		
		String dimKey = lbcDimension.getSelectedKey();
		if (dimKey == null || dimKey.length() == 0) {
			errInfo.showError("You must select a dimension.");
			return;
		}
		DimMappingDef.Action onUnmapped = DimMappingDef.Action.valueOf(chkOptions.getSelectedKey());
		
		String unmappedElement = elmSelector.getDimensionElement() != null ? elmSelector.getDimensionElement().getPath() : null;
		if (onUnmapped == DimMappingDef.Action.ASSIGN) {
			if (unmappedElement == null) {
				errInfo.showError("An Unmapped Element must be specified.");
				return;
			} else if (!elmSelector.getDimensionElement().isLeaf()) {
				errInfo.showError("An Unmapped Element must be specified that is NOT a leaf!");
				return;
			}
		}
		
		// now check the dimension mapping table
		List<DimMappingElementDef> mappingList = mapEditor.getMappingList();
		for (DimMappingElementDef me : mappingList) {
			if (me.getDimensionKey() == null || !me.getDimensionKey().equals(dimKey)) {
				errInfo.showError("The mapping table contains elements for other dimensions then the selected one.");
				return;
			}
			me.setDimMapKey(key);
		}
		
		dimMapping.setKey(key);
		dimMapping.setDimensionKey(dimKey);
		dimMapping.setDescription(inpDescription.getText());
		dimMapping.setOnUnmapped(onUnmapped);
		dimMapping.setUnmappedPath(unmappedElement);
		
		ServerContext context = ETLgineServer.getInstance().getServerContext();
		String syncTableConnectionName = context.getProperty(dpManagerKey + ".datapool.syncTables.connection");
		try {
			Connection connection = JDBCUtil.openConnection(context, syncTableConnectionName);
			try {
				connection.setAutoCommit(false);
				DimMappingDefDAO dao = new DimMappingDefDAO(connection);
				if (isNew) {
					dao.insert(dimMapping);
				} else {
					dao.update(dimMapping);
				}
				
				// insert dimMappings
				DimMappingElementDefDAO daoME = new DimMappingElementDefDAO(connection);
				daoME.deleteByDimMapKey(key);
				for (DimMappingElementDef me : mappingList) {
					daoME.insert(me);
				}
				
				connection.commit();
				connection.setAutoCommit(true);
				close();
			} finally {
				if (!connection.getAutoCommit()) {
					connection.rollback();
				}
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
	protected void close() {
		destroy();
	}

	/**
	 * 
	 */
	private void createDimMappingEditor() {
		inpKey = new InputBoxControl(this, "inpKey");
		inpKey.setWidth(300);
		
		inpDescription = new InputBoxControl(this, "inpDescription");
		inpDescription.setMultiLine(true);
		inpDescription.setRows(3);
		inpDescription.setWidth(300);
		
		lbcDimension = new ListBoxControl(this, "lbcDimension");
		lbcDimension.setChangeNotification(true);
		lbcDimension.addElementSelectedListener(new ElementSelectedListener() {
			public void elementSelected(ElementSelectedEvent event) {
				onDimensionSelection((String)event.getElement());
			}
		});
		for (IDimension dim : dataPool.getDimensions()) {
			String title = dim.getTitle() != null ? dim.getKey() + "(" + dim.getTitle() + ")" : dim.getKey();
			lbcDimension.addElement(title, dim.getKey());
		}
		
		chkOptions = new RadioGroupControl(this, "chkOptions");
		chkOptions.setChangeNotification(true);
		chkOptions.addElement("Create", "CREATE");
		chkOptions.addElement("Skip", "SKIP");
		chkOptions.addElement("Assign To", "ASSIGN");
		chkOptions.addElementSelectedListener(new ElementSelectedListener() {
			public void elementSelected(ElementSelectedEvent event) {
				boolean isAssign = ("ASSIGN".equals(chkOptions.getSelectedKey()));
				if (elmSelector != null) {
					elmSelector.setVisible(isAssign);
				}
			}
		});
		
		new LabelControl(this, "elmSelector").setText("");
		elmSelector = null;
		
		/*
		 * Load Initial Values 
		 */
		if (dimMapping.getKey() != null) {
			inpKey.setText(dimMapping.getKey());
			inpKey.setEnabled(false);
		}
		inpDescription.setText(dimMapping.getDescription() != null ? dimMapping.getDescription() : "");
		lbcDimension.setSelectedKey(dimMapping.getDimensionKey() != null ? dimMapping.getDimensionKey() : "");
		
		chkOptions.setSelectedKey(dimMapping.getOnUnmapped().name());
		
		if (elmSelector != null && dimMapping.getUnmappedPath() != null && dimMapping.getUnmappedPath().length() != 0) {
			IDimension dimension = elmSelector.getDimension();
			try {
				IDimensionElement elm = dimension.parsePath(dimMapping.getUnmappedPath());
				elmSelector.setDimensionElement(elm);
			} catch (Exception e) {
				errInfo.showError("Error restoring unmapped value - element removed?: " + e);
			}
		}
		
	}

	/**
	 * @param element
	 */
	protected void onDimensionSelection(String element) {
		
		removeControl("elmSelector");
		if (element != null && element.length() != 0) {
			IDimension dimension = dataPool.getDimension(element);
			elmSelector = new DimensionElementSelector(this, "elmSelector", dimension);
			elmSelector.setSelectLeafsOnly(true);
			mapEditor.setDimension(dimension);
		} else {
			elmSelector = null;
			new LabelControl(this, "elmSelector").setText("");
		}
		
	}

}