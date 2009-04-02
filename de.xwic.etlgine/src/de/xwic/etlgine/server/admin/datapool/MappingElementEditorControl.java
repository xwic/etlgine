/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import java.util.ArrayList;
import java.util.List;

import de.jwic.base.ControlContainer;
import de.jwic.base.IControlContainer;
import de.jwic.controls.ButtonControl;
import de.jwic.controls.CheckboxControl;
import de.jwic.controls.InputBoxControl;
import de.jwic.ecolib.tableviewer.TableColumn;
import de.jwic.ecolib.tableviewer.TableModel;
import de.jwic.ecolib.tableviewer.TableViewer;
import de.jwic.ecolib.tableviewer.defaults.ListContentProvider;
import de.jwic.events.ElementSelectedEvent;
import de.jwic.events.ElementSelectedListener;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.cube.webui.controls.DimensionElementSelector;
import de.xwic.etlgine.cube.mapping.DimMappingElementDef;

/**
 * @author lippisch
 *
 */
public class MappingElementEditorControl extends ControlContainer {

	private List<DimMappingElementDef> mappingList = new ArrayList<DimMappingElementDef>();
	private IDimension dimension = null;
	
	private TableViewer table;
	private TableModel tableModel;
	
	private InputBoxControl inpExpression;
	private DimensionElementSelector selElement;
	private CheckboxControl chkOptions;
	private ButtonControl btUpdate, btDelete, btMoveUp, btMoveDown;
	
	private DimMappingElementDef currElement = null;
	private MappingElementTableLabelProvider labelProvider;

	/**
	 * @param container
	 * @param name
	 */
	public MappingElementEditorControl(IControlContainer container, String name) {
		super(container, name);
		setupTable();
		setupEditor();
		
		createNewElement();
	}
	
	/**
	 * 
	 */
	private void setupEditor() {
		
		inpExpression = new InputBoxControl(this, "inpExpression");
		inpExpression.setMultiLine(true);
		inpExpression.setWidth(400);
		inpExpression.setRows(4);
		
		selElement = null; // see setDimension(..)
		
		chkOptions = new CheckboxControl(this, "chkOptions");
		chkOptions.setColumns(1);
		chkOptions.addElement("Is RegExp", "regExp");
		chkOptions.addElement("Ignore Case", "ignoreCase");
		chkOptions.addElement("Skip Record", "skipRecord");
		
		btUpdate = new ButtonControl(this, "btUpdate");
		btUpdate.setTitle("Update");
		btUpdate.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onUpdate();
			}
		});
		
		btDelete = new ButtonControl(this, "btDelete");
		btDelete.setTitle("Delete");
		btDelete.setConfirmMsg("Are you sure?");
		btDelete.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onDelete();
			}
		});
		
		btMoveUp = new ButtonControl(this, "btMoveUp");
		btMoveUp.setTitle("Move Up");
		btMoveUp.addSelectionListener(new SelectionListener() {
			/* (non-Javadoc)
			 * @see de.jwic.events.SelectionListener#objectSelected(de.jwic.events.SelectionEvent)
			 */
			public void objectSelected(SelectionEvent event) {
				onMoveUp();
			} 
		});
		
		btMoveDown = new ButtonControl(this, "btMoveDown");
		btMoveDown.setTitle("Move Down");
		btMoveDown.addSelectionListener(new SelectionListener() {
			/* (non-Javadoc)
			 * @see de.jwic.events.SelectionListener#objectSelected(de.jwic.events.SelectionEvent)
			 */
			public void objectSelected(SelectionEvent event) {
				onMoveDown();				
			}
		});
		
	}

	/**
	 * 
	 */
	protected void onDelete() {
		mappingList.remove(currElement);
		table.setRequireRedraw(true);
		tableModel.clearSelection();
	}

	/**
	 * 
	 */
	protected void onUpdate() {
		boolean insert = false;
		if (currElement == null) {
			insert = true;
			currElement = new DimMappingElementDef();
		}
		currElement.setExpression(inpExpression.getText().trim());
		currElement.setDimensionKey(dimension.getKey());
		currElement.setDimMapKey(null);
		currElement.setElementPath(selElement.getDimensionElement().getPath());
		currElement.setIgnoreCase(chkOptions.isKeySelected("ignoreCase"));
		currElement.setRegExp(chkOptions.isKeySelected("regExp"));
		currElement.setSkipRecord(chkOptions.isKeySelected("skipRecord"));
		
		if (insert) {
			mappingList.add(currElement);
		}
		table.setRequireRedraw(true);
		tableModel.clearSelection();
		createNewElement();
	}

	/**
	 * Invoke to enable the editor for a new element.
	 */
	public void createNewElement() {
		currElement = null;
		inpExpression.setText("");
		inpExpression.forceFocus();
		chkOptions.setSelectedKey("ignoreCase");
		if (selElement != null) {
			selElement.actionFirst();
		}
		btUpdate.setTitle("Insert");
		btDelete.setEnabled(false);
		refreshMoveButtons();
	}
	
	/**
	 * 
	 */
	private void setupTable() {

		table = new TableViewer(this, "table");
		
		table.setContentProvider(new ListContentProvider(mappingList));
		labelProvider = new MappingElementTableLabelProvider();
		
		table.setTableLabelProvider(labelProvider);
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
				onElementSelection((String)event.getElement());
			}
		});

	
	}

	/**
	 * @param selection 
	 * 
	 */
	protected void onElementSelection(String selection) {
		
		int idx = Integer.parseInt(selection);
		currElement = mappingList.get(idx);
		inpExpression.setText(currElement.getExpression());
		inpExpression.forceFocus();
		
		StringBuilder sb = new StringBuilder();
		if (currElement.isRegExp()) sb.append("regExp;");
		if (currElement.isIgnoreCase()) sb.append("ignoreCase;");
		if (currElement.isSkipRecord()) sb.append("skipRecord;");
		chkOptions.setSelectedKey(sb.toString());
		
		try {
			IDimensionElement elm = dimension.parsePath(currElement.getElementPath());
			selElement.setDimensionElement(elm);
		} catch (Exception e) {
			log.error("Error loading dimension element");
		}
		
		btUpdate.setTitle("Update");
		btDelete.setEnabled(true);
	
		refreshMoveButtons();
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
		// remove old
		removeControl("selElement");
		selElement = new DimensionElementSelector(this, "selElement", dimension);
		selElement.setWidth(400);
		
		labelProvider.setDimension(dimension);
		
	}

	/**
	 * 
	 */
	protected void onMoveDown() {
		int idx = mappingList.indexOf(currElement) + 1;
		reindex(currElement, idx);
		tableModel.selection(String.valueOf(idx)); // set new index selected
		table.setRequireRedraw(true);
		refreshMoveButtons(); // refresh buttons
		
	}

	/**
	 * 
	 */
	protected void onMoveUp() {
		int idx = mappingList.indexOf(currElement) - 1;
		if (idx >= 0) {
			reindex(currElement, idx);
			tableModel.selection(String.valueOf(idx)); // set new index selected
			table.setRequireRedraw(true);
			refreshMoveButtons(); // refresh buttons
		}
		
	}

	/**
	 * 
	 */
	protected void refreshMoveButtons() {
		int idx = currElement == null ? -1 : mappingList.indexOf(currElement);
		btMoveUp.setEnabled(currElement != null && idx > 0);
		btMoveDown.setEnabled(currElement != null && idx + 1 < mappingList.size());
	}
	
	/**
	 * @param mapping
	 * @param newIndex
	 */
	protected void reindex(DimMappingElementDef mapping, int newIndex) {
		if (newIndex > (mappingList.size() - 1)) {
			// simply put at the end of the list.
			mappingList.remove(mapping);
			mappingList.add(mapping);
		} else {
			mappingList.remove(mapping);
			mappingList.add(newIndex, mapping);
		}
	}
}
