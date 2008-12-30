/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import java.util.ArrayList;
import java.util.List;

import de.jwic.base.ControlContainer;
import de.jwic.base.IControlContainer;
import de.jwic.ecolib.tableviewer.TableColumn;
import de.jwic.ecolib.tableviewer.TableModel;
import de.jwic.ecolib.tableviewer.TableViewer;
import de.jwic.ecolib.tableviewer.defaults.ListContentProvider;
import de.jwic.events.ElementSelectedEvent;
import de.jwic.events.ElementSelectedListener;
import de.xwic.cube.IDataPool;
import de.xwic.cube.IDimension;
import de.xwic.etlgine.cube.mapping.DimMappingElementDef;

/**
 * @author lippisch
 *
 */
public class MappingElementEditorControl extends ControlContainer {

	private final IDataPool dataPool;

	private List<DimMappingElementDef> mappingList = new ArrayList<DimMappingElementDef>();
	private IDimension dimension = null;
	
	private TableViewer table;
	private TableModel tableModel;

	/**
	 * @param container
	 * @param name
	 */
	public MappingElementEditorControl(IControlContainer container, String name, IDataPool dataPool) {
		super(container, name);
		this.dataPool = dataPool;
		setupTable();
	}
	
	/**
	 * 
	 */
	private void setupTable() {

		table = new TableViewer(this, "table");
		
		table.setContentProvider(new ListContentProvider(mappingList));
		table.setTableLabelProvider(new MappingElementTableLabelProvider());
		table.setWidth(793);
		table.setHeight(300);
		table.setResizeableColumns(true);
		table.setScrollable(true);
		table.setShowStatusBar(false);
		
		tableModel = table.getModel();
		tableModel.setSelectionMode(TableModel.SELECTION_SINGLE);
		tableModel.addColumn(new TableColumn("Expression", 250, "exp"));
		tableModel.addColumn(new TableColumn("Path", 250, "path"));
		tableModel.addColumn(new TableColumn("RegExp", 90, "regExp"));
		tableModel.addColumn(new TableColumn("Ignore Case", 90, "ignoreCase"));
		tableModel.addColumn(new TableColumn("Skip Record", 90, "skip"));
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
		// TODO Auto-generated method stub
		
	}

	/**
	 * @return the mappingList
	 */
	public List<DimMappingElementDef> getMappingList() {
		return mappingList;
	}

	/**
	 * @param mappingList the mappingList to set
	 */
	public void setMappingList(List<DimMappingElementDef> mappingList) {
		this.mappingList = mappingList;
		table.setContentProvider(new ListContentProvider(mappingList));
	}

	/**
	 * @return the dimension
	 */
	public IDimension getDimension() {
		return dimension;
	}

	/**
	 * @param dimension the dimension to set
	 */
	public void setDimension(IDimension dimension) {
		this.dimension = dimension;
	}

}
