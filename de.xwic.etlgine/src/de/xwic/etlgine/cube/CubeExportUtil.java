/**
 * 
 */
package de.xwic.etlgine.cube;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import au.com.bytecode.opencsv.CSVWriter;
import de.xwic.cube.ICell;
import de.xwic.cube.ICellListener;
import de.xwic.cube.ICube;
import de.xwic.cube.IDimension;
import de.xwic.cube.IDimensionElement;
import de.xwic.cube.IMeasure;
import de.xwic.cube.Key;

/**
 * Export a cube into a text file. Only the leaf values are exported.
 * 
 * @author lippisch
 */
public class CubeExportUtil {

	private String[] data = null;
	private IMeasure[] measures = null;
	private CSVWriter out;
	private boolean leafsOnly;
	
	private class Exporter implements ICellListener {
		public boolean onCell(Key key, ICell cell) {
			if (!leafsOnly || key.containsLeafsOnly()) {
				int i = 0;
				for (IDimensionElement dim : key.getDimensionElements()) {
					data[i++] = dim.getPath();
				}
				for (int a = 0; a < measures.length; a++) {
					Double d = cell.getValue(a);
					data[i++] = d == null ? "" : d.toString(); 
				}
				out.writeNext(data);
			}
			return true;
		}
	}
	
	public void export(ICube cube, OutputStream output, boolean leafsOnly) throws IOException {
		
		this.leafsOnly = leafsOnly;
		out = new CSVWriter(new PrintWriter(output));
		
		data = new String[cube.getDimensions().size() + cube.getMeasures().size()];
		int i = 0;
		for (IDimension dim : cube.getDimensions()) {
			data[i++] = dim.getKey();
		}
		measures = new IMeasure[cube.getMeasures().size()];
		int a = 0;
		for (IMeasure me : cube.getMeasures()) {
			measures[a++] = me;
			data[i++] = me.getKey();
		}
		
		out.writeNext(data);
		
		cube.forEachCell(new Exporter());
		
		out.flush();
		
	}

	
}
