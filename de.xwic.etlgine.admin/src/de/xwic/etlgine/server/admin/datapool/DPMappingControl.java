/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import java.sql.Connection;
import java.util.Iterator;
import java.util.List;

import de.jwic.base.IControlContainer;
import de.jwic.controls.Button;
import de.jwic.controls.ToolBar;
import de.jwic.controls.ToolBarGroup;
import de.jwic.controls.tableviewer.TableColumn;
import de.jwic.controls.tableviewer.TableModel;
import de.jwic.controls.tableviewer.TableViewer;
import de.jwic.data.IContentProvider;
import de.jwic.data.Range;
import de.jwic.events.ElementSelectedEvent;
import de.jwic.events.ElementSelectedListener;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.etlgine.cube.mapping.DimMappingDef;
import de.xwic.etlgine.cube.mapping.DimMappingDefDAO;
import de.xwic.etlgine.jdbc.JDBCUtil;
import de.xwic.etlgine.server.ETLgineServer;
import de.xwic.etlgine.server.admin.BaseContentContainer;
import de.xwic.etlgine.server.admin.ImageLibrary;
import de.xwic.etlgine.server.admin.StackedContentContainer;

/**
 * @author lippisch
 *
 */
public class DPMappingControl extends BaseContentContainer {

	private TableViewer table;
	private TableModel tableModel;
	private final String syncTableConnectionName;
	private Button btEdit;
	private Button btAdd;
	private final String dpManagerKey;
	private MappingContentProvider contentProvider;

	private class MappingContentProvider implements IContentProvider {

		private List<DimMappingDef> lastList;
		/* (non-Javadoc)
		 * @see de.jwic.ecolib.tableviewer.IContentProvider#getChildren(java.lang.Object)
		 */
		public Iterator<?> getChildren(Object object) {
			return null;
		}

		/* (non-Javadoc)
		 * @see de.jwic.ecolib.tableviewer.IContentProvider#getContentIterator(de.jwic.ecolib.tableviewer.Range)
		 */
		public Iterator<?> getContentIterator(Range range) {
			// load the List
			try {
				Connection connection = JDBCUtil.openConnection(ETLgineServer.getInstance().getServerContext(), syncTableConnectionName);
				DimMappingDefDAO dao = new DimMappingDefDAO(connection);
				lastList = dao.listMappings();
			} catch (Exception se) {
				//log.error("Error in DPMappingControl.setupTable()", se);
				throw new RuntimeException("Error reading data", se);
			}
			
			return lastList.iterator();
		}

		/* (non-Javadoc)
		 * @see de.jwic.ecolib.tableviewer.IContentProvider#getTotalSize()
		 */
		public int getTotalSize() {
			return lastList.size();
		}

		/* (non-Javadoc)
		 * @see de.jwic.ecolib.tableviewer.IContentProvider#getUniqueKey(java.lang.Object)
		 */
		public String getUniqueKey(Object object) {
			return Integer.toString(lastList.indexOf(object));
		}

        @Override
        public Object getObjectFromKey(String s) {
            //TODO - on change to jWic 5.2 this needed to be implemented
            throw new UnsupportedOperationException();
        }

        /* (non-Javadoc)
         * @see de.jwic.ecolib.tableviewer.IContentProvider#hasChildren(java.lang.Object)
         */
		public boolean hasChildren(Object object) {
			return false;
		}
		
		/**
		 * Returns the DimMappingDef from the last retrieved list.
		 * @param index
		 * @return
		 */
		public DimMappingDef getDimMappingDef(int index) {
			return lastList.get(index);
		}
		
	}
	
	/**
	 * @param container
	 * @param name
	 */
	public DPMappingControl(IControlContainer container, String name, String dpManagerKey, String syncTableConnectionName) {
		super(container, name);
		this.dpManagerKey = dpManagerKey;
		this.syncTableConnectionName = syncTableConnectionName;
		
		setTitle("Mapping Overview");
		
		setupActionBar();
		setupTable();
		updateButtonState();
	}
	
	/**
	 * 
	 */
	private void setupTable() {

		table = new TableViewer(this, "table");
		
		contentProvider = new MappingContentProvider();
		table.setContentProvider(contentProvider);
		table.setTableLabelProvider(new DPDimMapTableLabelProvider());
		table.setWidth(949);
		table.setHeight(500);
		table.setResizeableColumns(true);
		table.setScrollable(true);
		table.setShowStatusBar(false);
		
		tableModel = table.getModel();
		tableModel.setSelectionMode(TableModel.SELECTION_SINGLE);
		tableModel.addColumn(new TableColumn("Key", 200, "key"));
		tableModel.addColumn(new TableColumn("Dimension", 150, "dimension"));
		tableModel.addColumn(new TableColumn("Description", 240, "description"));
		tableModel.addColumn(new TableColumn("On Unmapped", 80, "action"));
		tableModel.addElementSelectedListener(new ElementSelectedListener() {
			public void elementSelected(ElementSelectedEvent event) {
				updateButtonState();
			}
		});

	
	}

	/**
	 * 
	 */
	protected void updateButtonState() {
		boolean selected = tableModel.getFirstSelectedKey() != null;
		btEdit.setEnabled(selected);		
	}

	/**
	 * Setup the ActionBar.
	 */
	private void setupActionBar() {
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

		btAdd = group.addButton();
		btAdd.setIconEnabled(ImageLibrary.IMAGE_TABLE_ADD);
		btAdd.setTitle("Add");
		btAdd.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onAddMapping();
			}
		});

		btEdit = group.addButton();
		btEdit.setIconEnabled(ImageLibrary.IMAGE_TABLE_EDIT);
		btEdit.setTitle("Edit");
		btEdit.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onEditMapping();
			}
		});

	}
	
	/**
	 * 
	 */
	protected void onAddMapping() {

		DimMappingDef dmd = new DimMappingDef();
		
		StackedContentContainer sc = (StackedContentContainer)getContainer();
		MappingEditorControl dpMapEditor = new MappingEditorControl(sc, null, dpManagerKey, dmd);
		sc.setCurrentControlName(dpMapEditor.getName());
		
	}

	/**
	 * 
	 */
	protected void onEditMapping() {
		
		String selection = tableModel.getFirstSelectedKey();
		if (selection != null) {
			DimMappingDef dmd = contentProvider.getDimMappingDef(Integer.parseInt(selection));
			
			StackedContentContainer sc = (StackedContentContainer)getContainer();
			MappingEditorControl dpMapEditor = new MappingEditorControl(sc, null, dpManagerKey, dmd);
			sc.setCurrentControlName(dpMapEditor.getName());
			
		}

	}

	/**
	 * 
	 */
	protected void close() {
		destroy();
	}


}
