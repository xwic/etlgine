/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import java.util.Iterator;

import de.jwic.data.IContentProvider;
import de.jwic.data.Range;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;

/**
 * @author Developer
 *
 */
public class DimensionContentProvider implements IContentProvider {
	
	private IDimension dimension;

	/**
	 * @param dimension
	 */
	public DimensionContentProvider(IDimension dimension) {
		super();
		this.dimension = dimension;
	}

	/* (non-Javadoc)
	 * @see de.jwic.ecolib.tableviewer.IContentProvider#getChildren(java.lang.Object)
	 */
	public Iterator<?> getChildren(Object object) {
		IDimensionElement elm = (IDimensionElement)object;
		return elm.getDimensionElements().iterator();
	}

	/* (non-Javadoc)
	 * @see de.jwic.ecolib.tableviewer.IContentProvider#getContentIterator(de.jwic.ecolib.tableviewer.Range)
	 */
	public Iterator<?> getContentIterator(Range range) {
		return dimension.getDimensionElements().iterator();
	}

	/* (non-Javadoc)
	 * @see de.jwic.ecolib.tableviewer.IContentProvider#getTotalSize()
	 */
	public int getTotalSize() {
		return dimension.getDimensionElements().size();
	}

	/* (non-Javadoc)
	 * @see de.jwic.ecolib.tableviewer.IContentProvider#getUniqueKey(java.lang.Object)
	 */
	public String getUniqueKey(Object object) {
		IDimensionElement elm = (IDimensionElement)object;
		return elm.getPath();
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
		IDimensionElement elm = (IDimensionElement)object;
		return !elm.isLeaf();
	}

}
