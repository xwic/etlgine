/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import java.util.ArrayList;
import java.util.List;

import de.jwic.base.IControlContainer;
import de.jwic.controls.ActionBarControl;
import de.jwic.controls.ButtonControl;
import de.jwic.controls.InputBoxControl;
import de.jwic.controls.LabelControl;
import de.jwic.ecolib.controls.ErrorWarningControl;
import de.jwic.ecolib.tableviewer.TableColumn;
import de.jwic.ecolib.tableviewer.TableModel;
import de.jwic.ecolib.tableviewer.TableViewer;
import de.jwic.events.ElementSelectedEvent;
import de.jwic.events.ElementSelectedListener;
import de.jwic.events.SelectionEvent;
import de.jwic.events.SelectionListener;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.etlgine.server.ETLgineServer;
import de.xwic.etlgine.server.admin.BaseContentContainer;
import de.xwic.etlgine.server.admin.ImageLibrary;

/**
 * @author Developer
 *
 */
public class DimensionEditorControl extends BaseContentContainer {

	private final IDimension dimension;
	private TableViewer table;
	private TableModel tableModel;
	
	private LabelControl lblParent;
	private InputBoxControl inpKey, inpTitle, inpWeight;
	private ButtonControl btUpdate, btDelete;
	
	private IDimensionElement editedElement = null;
	private boolean insertMode = false;
	private boolean insertChild = false;

	private ErrorWarningControl errInfo;
	
	/**
	 * @param container
	 * @param name
	 */
	public DimensionEditorControl(IControlContainer container, String name, IDimension dimension) {
		super(container, name);
		this.dimension = dimension;
		
		setTitle("Dimension Editor (" + dimension.getKey() + ")");
		
		errInfo = new ErrorWarningControl(this, "errInfo");
				
		createActionBar();
		setupTable();
		setupEditor();
		
		setEditorEnabled(false);
		
	}

	/**
	 * @param b
	 */
	private void setEditorEnabled(boolean enabled) {
		inpKey.setEnabled(enabled);
		inpTitle.setEnabled(enabled);
		inpWeight.setEnabled(enabled);
		btUpdate.setEnabled(enabled);
		btDelete.setEnabled(enabled);
	}

	/**
	 * 
	 */
	private void setupEditor() {
		
		lblParent = new LabelControl(this, "lblParent");
				
		inpKey = new InputBoxControl(this, "inpKey");
		inpKey.setWidth(200);
		
		inpTitle = new InputBoxControl(this, "inpTitle");
		inpTitle.setWidth(200);
		
		inpWeight = new InputBoxControl(this, "inpWeight");
		inpWeight.setWidth(60);
		
		btUpdate = new ButtonControl(this, "btUpdate");
		btUpdate.setTitle("Update");
		btUpdate.addSelectionListener(new SelectionListener() {
			/* (non-Javadoc)
			 * @see de.jwic.events.SelectionListener#objectSelected(de.jwic.events.SelectionEvent)
			 */
			public void objectSelected(SelectionEvent event) {
				onUpdate();				
			}
		});
		
		btDelete = new ButtonControl(this, "btDelete");
		btDelete.setTitle("Delete");
		btDelete.setConfirmMsg("Do you really want to delete this dimension?");
		btDelete.addSelectionListener(new SelectionListener() {
			/* (non-Javadoc)
			 * @see de.jwic.events.SelectionListener#objectSelected(de.jwic.events.SelectionEvent)
			 */
			public void objectSelected(SelectionEvent event) {
				onDelete();
			}
		});
		
		
	}

	/**
	 * 
	 */
	protected void onDelete() {
		
		if (!insertMode) {
			editedElement.remove();
			tableModel.clearSelection();
			table.setRequireRedraw(true);
		}
		
	}

	/**
	 * 
	 */
	protected void onUpdate() {
		
		if (!insertMode) {
			String title = inpTitle.getText().trim();
			editedElement.setTitle(title.length() == 0 ? null : title);
			editedElement.setWeight(Double.parseDouble(inpWeight.getText()));
			updateEditor(null);
		} else {
			String key = inpKey.getText().trim();
			String title = inpTitle.getText().trim();
			try {
				IDimensionElement elm ;
				if (insertChild) {
					elm = editedElement.createDimensionElement(key);
				} else {
					elm = editedElement.getParent().createDimensionElement(key);
				}
				elm.setTitle(title.length() == 0 ? null : title);
				elm.setWeight(Double.parseDouble(inpWeight.getText()));
			} catch (Exception e) {
				errInfo.showError(e);
			}
		}
		tableModel.clearSelection();
		table.setRequireRedraw(true);
	}

	/**
	 * @param object
	 */
	private void updateEditor(IDimensionElement element) {
		
		insertMode = false;
		editedElement = element;
		
		if (element != null) {
			lblParent.setText(element.getParent().getPath());
			inpKey.setText(element.getKey());
			inpTitle.setText(element.getTitle() != null ? element.getTitle() : "");
			inpWeight.setText(Double.toString(element.getWeight()));
			setEditorEnabled(true);
			inpKey.setEnabled(false);
		} else {
			lblParent.setText("");
			inpKey.setText("");
			inpTitle.setText("");
			inpWeight.setText("");
			setEditorEnabled(false);
		}
		

	}

	/**
	 * 
	 */
	private void setupTable() {
		
		table = new TableViewer(this, "table");
		
		List<String> keyList = new ArrayList<String>();
		keyList.addAll(ETLgineServer.getInstance().getServerContext().getDataPoolManagerKeys());
		
		table.setContentProvider(new DimensionContentProvider(dimension));
		table.setTableLabelProvider(new DimensionEditorLabelProvider());
		table.setWidth(799);
		table.setHeight(300);
		table.setResizeableColumns(true);
		table.setScrollable(true);
		table.setShowStatusBar(false);
		table.setExpandableColumn(0);
		
		tableModel = table.getModel();
		tableModel.setSelectionMode(TableModel.SELECTION_SINGLE);
		tableModel.addColumn(new TableColumn("Key", 300, "key"));
		tableModel.addColumn(new TableColumn("Title", 240, "title"));
		tableModel.addColumn(new TableColumn("Weight", 150, "weight"));
		tableModel.addElementSelectedListener(new ElementSelectedListener() {
			public void elementSelected(ElementSelectedEvent event) {
				onSelection((String)event.getElement());
			}
		});

		
	}

	/**
	 * 
	 */
	protected void onSelection(String path) {
		log.info("Selected: " + path);
		
		IDimensionElement element = dimension.parsePath(path);
		updateEditor(element);
		btUpdate.setTitle("Update");
	}

	/**
	 * 
	 */
	private void createActionBar() {
		
		ActionBarControl abar = new ActionBarControl(this, "actionBar");
		
		ButtonControl btReturn = new ButtonControl(abar, "return");
		btReturn.setIconEnabled(ImageLibrary.IMAGE_RETURN);
		btReturn.setTitle("Return");
		btReturn.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				close();
			}
		});
		
		ButtonControl btAdd = new ButtonControl(abar, "add");
		btAdd.setIconEnabled(ImageLibrary.IMAGE_ADD);
		btAdd.setTitle("Add Element");
		btAdd.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onAddElement();
			}
		});

		ButtonControl btAddChild = new ButtonControl(abar, "addChild");
		btAddChild.setIconEnabled(ImageLibrary.IMAGE_ADD);	
		btAddChild.setTitle("Add Child Element");
		btAddChild.addSelectionListener(new SelectionListener() {
			public void objectSelected(SelectionEvent event) {
				onAddChildElement();
			}
		});

	}

	/**
	 * 
	 */
	protected void onAddChildElement() {

		if (editedElement == null) {
			editedElement = dimension;
		}
		
		insertMode = true;
		insertChild = true;
		
		lblParent.setText(editedElement.getPath());
		inpKey.setText("");
		inpKey.forceFocus();
		inpTitle.setText("");
		inpWeight.setText("1.0");
		setEditorEnabled(true);
		btDelete.setEnabled(false);
		btUpdate.setTitle("Insert");
		
	}

	/**
	 * 
	 */
	protected void onAddElement() {
		
		if (editedElement == null) {
			onAddChildElement();
			return;
		}
		
		insertMode = true;
		insertChild = false;
		
		lblParent.setText(editedElement.getParent().getPath());
		inpKey.setText("");
		inpKey.forceFocus();
		inpTitle.setText("");
		inpWeight.setText("1.0");
		
		setEditorEnabled(true);
		btDelete.setEnabled(false);
		btUpdate.setTitle("Insert");

		
	}

	/**
	 * 
	 */
	protected void close() {
		destroy();
		
	}

	/**
	 * @return the editedElement
	 */
	public IDimensionElement getEditedElement() {
		return editedElement;
	}
}
