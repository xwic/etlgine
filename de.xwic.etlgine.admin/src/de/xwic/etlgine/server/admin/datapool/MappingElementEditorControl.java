/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import de.jwic.base.ControlContainer;
import de.jwic.base.IControlContainer;
import de.jwic.controls.Button;
import de.jwic.controls.CheckboxControl;
import de.jwic.controls.DateInputBoxControl;
import de.jwic.controls.InputBoxControl;
import de.jwic.controls.tableviewer.TableColumn;
import de.jwic.controls.tableviewer.TableModel;
import de.jwic.controls.tableviewer.TableViewer;
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
	private DateInputBoxControl inpValidFrom;
	private DateInputBoxControl inpValidTo;
	private Button btUpdate, btMassInsert, btDelete, btMoveUp, btMoveDown, btMoveTop, btMoveBtm;
	
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
		
		inpValidFrom = new DateInputBoxControl(this, "inpValidFrom");
		inpValidTo = new DateInputBoxControl(this, "inpValidTo");
		
		selElement = null; // see setDimension(..)
		
		chkOptions = new CheckboxControl(this, "chkOptions");
		chkOptions.setColumns(1);
		chkOptions.addElement("Is RegExp", "regExp");
		chkOptions.addElement("Ignore Case", "ignoreCase");
		chkOptions.addElement("Skip Record", "skipRecord");
		chkOptions.addElement("Auto Assign Dimension (On Insert)", "autoAssign");
		chkOptions.addElement("Escape '/'", "escape");
		
		btUpdate = new Button(this, "btUpdate");
		btUpdate.setTitle("Update");
		btUpdate.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onUpdate();
			}
		});
		
		btMassInsert = new Button(this, "btMassInsert");
		btMassInsert.setTitle("Mass Insert");
		btMassInsert.addSelectionListener(new SelectionListener() {
			@Override
			public void objectSelected(SelectionEvent event) {
				onMassInsert();
			}
		});
		
		btDelete = new Button(this, "btDelete");
		btDelete.setTitle("Delete");
		btDelete.setConfirmMsg("Are you sure?");
		btDelete.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onDelete();
			}
		});
		
		btMoveUp = new Button(this, "btMoveUp");
		btMoveUp.setTitle("Move Up");
		btMoveUp.addSelectionListener(new SelectionListener() {
			/* (non-Javadoc)
			 * @see de.jwic.events.SelectionListener#objectSelected(de.jwic.events.SelectionEvent)
			 */
			public void objectSelected(SelectionEvent event) {
				onMoveUp();
			} 
		});
		
		btMoveDown = new Button(this, "btMoveDown");
		btMoveDown.setTitle("Move Down");
		btMoveDown.addSelectionListener(new SelectionListener() {
			/* (non-Javadoc)
			 * @see de.jwic.events.SelectionListener#objectSelected(de.jwic.events.SelectionEvent)
			 */
			public void objectSelected(SelectionEvent event) {
				onMoveDown();				
			}
		});

		btMoveTop = new Button(this, "btMoveTop");
		btMoveTop.setTitle("Move Top");
		btMoveTop.addSelectionListener(new SelectionListener() {
			/* (non-Javadoc)
			 * @see de.jwic.events.SelectionListener#objectSelected(de.jwic.events.SelectionEvent)
			 */
			public void objectSelected(SelectionEvent event) {
				onMoveTop();				
			}
		});

		btMoveBtm = new Button(this, "btMoveBtm");
		btMoveBtm.setTitle("Move Bottom");
		btMoveBtm.addSelectionListener(new SelectionListener() {
			/* (non-Javadoc)
			 * @see de.jwic.events.SelectionListener#objectSelected(de.jwic.events.SelectionEvent)
			 */
			public void objectSelected(SelectionEvent event) {
				onMoveBtm();				
			}
		});

	}

	protected void onMassInsert() {
		
		String text = inpExpression.getText();
		if (text.isEmpty()) {
			return; // ignore
		}

		boolean autoAssign = chkOptions.isKeySelected("autoAssign");
		boolean escape = chkOptions.isKeySelected("escape");
		
		BufferedReader reader = new BufferedReader(new StringReader(text));

		try {
			String line;
			while ((line = reader.readLine()) != null) {
				String key = line.trim();
				if (escape) {
					key = key.replace('/', '&');
				}

				if (!key.isEmpty()) {
					String dimPath = null;
					if (autoAssign) {
						// search matching element
						dimPath = findDimensionMatch(dimension, key);
					}
					if (dimPath == null) {
						dimPath = selElement.getDimensionElement().getPath();
					}
					
					currElement = new DimMappingElementDef();
					
					currElement.setExpression(key);
					currElement.setDimensionKey(dimension.getKey());
					currElement.setDimMapKey(null);
					currElement.setElementPath(dimPath);
					currElement.setIgnoreCase(chkOptions.isKeySelected("ignoreCase"));
					currElement.setRegExp(chkOptions.isKeySelected("regExp"));
					currElement.setSkipRecord(chkOptions.isKeySelected("skipRecord"));
					currElement.setValidFrom(inpValidFrom.getDate());
					currElement.setValidTo(inpValidTo.getDate());
					
					mappingList.add(currElement);
				}
				
			}
			
			table.setRequireRedraw(true);
			tableModel.clearSelection();
			createNewElement();

		} catch (Exception e) { 
			//log.error("Error creating elements", e);
			throw new RuntimeException("Error creating elements: " + e, e);
		}
		
	}

	/**
	 * Tries to find a dimension element that matches the key.
	 * @param elm
	 * @param key
	 * @return
	 */
	private String findDimensionMatch(IDimensionElement elm, String key) {

		for (IDimensionElement child : elm.getDimensionElements()) {
			if (child.getKey().equalsIgnoreCase(key)) {
				return child.getPath();
			}
			String result = findDimensionMatch(child, key);
			if (result != null) {
				return result;
			}
		}
		return null;
		
	}

	public void doSort(final boolean byExpression) {
		
		Collections.sort(mappingList, new Comparator<DimMappingElementDef>() {
			public int compare(DimMappingElementDef o1, DimMappingElementDef o2) {
				String e1 = byExpression ? o1.getExpression() : o1.getElementPath();
				String e2 = byExpression ? o2.getExpression() : o2.getElementPath();
				if (e1 == null || e2 == null) {
					return e1 == null ? -1 : 1;
				}
				return e1.toUpperCase().compareTo(e2.toUpperCase());
			}
		});
		tableModel.clearSelection();
		table.setRequireRedraw(true);
		refreshMoveButtons(); // refresh buttons

	}
	
	/**
	 * 
	 */
	protected void onMoveBtm() {
		int idx = mappingList.size() - 1;
		reindex(currElement, idx);
		tableModel.selection(String.valueOf(idx)); // set new index selected
		table.setRequireRedraw(true);
		refreshMoveButtons(); // refresh buttons
	}

	/**
	 * 
	 */
	protected void onMoveTop() {
		reindex(currElement, 0);
		tableModel.selection(String.valueOf(0)); // set new index selected
		table.setRequireRedraw(true);
		refreshMoveButtons(); // refresh buttons
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
		boolean escape = chkOptions.isKeySelected("escape");
		String exp = inpExpression.getText().trim();
		if (escape) {
			exp = exp.replace('/', '&');
		}
		currElement.setExpression(exp);
		currElement.setDimensionKey(dimension.getKey());
		currElement.setDimMapKey(null);
		
		String path = selElement.getDimensionElement().getPath();
		if (chkOptions.isKeySelected("autoAssign")) {
			path = findDimensionMatch(dimension, inpExpression.getText().trim());
			if (path == null) {
				path = "NOT FOUND";
			}
		}
		
		currElement.setElementPath(path);
		currElement.setIgnoreCase(chkOptions.isKeySelected("ignoreCase"));
		currElement.setRegExp(chkOptions.isKeySelected("regExp"));
		currElement.setSkipRecord(chkOptions.isKeySelected("skipRecord"));
		currElement.setValidFrom(inpValidFrom.getDate());
		currElement.setValidTo(inpValidTo.getDate());
		
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
		inpValidFrom.setDate(null);
		inpValidTo.setDate(null);
		btUpdate.setTitle("Insert");
		btDelete.setEnabled(false);
		btMassInsert.setEnabled(true);
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
		table.setWidth(943);
		table.setHeight(300);
		table.setResizeableColumns(true);
		table.setScrollable(true);
		table.setShowStatusBar(true);
				
		tableModel = table.getModel();
		tableModel.setSelectionMode(TableModel.SELECTION_SINGLE);
		tableModel.addColumn(new TableColumn("M", 30, "match"));
		tableModel.addColumn(new TableColumn("Expression", 250, "exp"));
		tableModel.addColumn(new TableColumn("Path", 250, "path"));
		tableModel.addColumn(new TableColumn("RegExp", 60, "regExp"));
		tableModel.addColumn(new TableColumn("Ignore Case", 90, "ignoreCase"));
		tableModel.addColumn(new TableColumn("Skip", 60, "skip"));
		tableModel.addColumn(new TableColumn("From", 75, "validFrom"));
		tableModel.addColumn(new TableColumn("To", 75, "validTo"));
		tableModel.addElementSelectedListener(new ElementSelectedListener() {
			public void elementSelected(ElementSelectedEvent event) {
				onElementSelection((String)event.getElement());
			}
		});
		
		if (mappingList.size() > 150) {
			tableModel.setMaxLines(100);
		}
	
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
		inpValidFrom.setDate(currElement.getValidFrom());
		inpValidTo.setDate(currElement.getValidTo());
		
		StringBuilder sb = new StringBuilder();
		if (currElement.isRegExp()) sb.append("regExp;");
		if (currElement.isIgnoreCase()) sb.append("ignoreCase;");
		if (currElement.isSkipRecord()) sb.append("skipRecord;");
		chkOptions.setSelectedKey(sb.toString());
		
		try {
			IDimensionElement elm = dimension.parsePath(currElement.getElementPath());
			selElement.setDimensionElement(elm);
		} catch (Exception e) {
			log.error("Error loading dimension element", e);
		}
		
		btUpdate.setTitle("Update");
		btDelete.setEnabled(true);
		btMassInsert.setEnabled(false);
	
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
		btMoveTop.setEnabled(currElement != null && idx > 0);
		btMoveDown.setEnabled(currElement != null && idx + 1 < mappingList.size());
		btMoveBtm.setEnabled(currElement != null && idx + 1 < mappingList.size());
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

	/**
	 * @param text
	 */
	public void setTestString(String text) {
		
		labelProvider.setTestString(text);
		table.setRequireRedraw(true);
		
	}

	/**
	 * 
	 */
	public void deleteAll() {
		tableModel.clearSelection();
		mappingList.clear();
		table.setRequireRedraw(true);
		createNewElement();
	}
}
