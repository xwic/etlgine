/**
 * 
 */
package de.xwic.etlgine.transformer;

import java.util.HashSet;
import java.util.Set;

import de.xwic.etlgine.AbstractTransformer;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;

/**
 * @author JBORNEMA
 *
 */
public class BooleanTransformer extends AbstractTransformer {

	protected Set<String> trueValues = new HashSet<String>();
	{
		trueValues.add("on");
		trueValues.add("true");
		trueValues.add("1");
		trueValues.add("-1");
		trueValues.add("y");
		trueValues.add("yes");
	}
	protected String[] columns = null;
	
	public BooleanTransformer() {
		super();
	}

	public BooleanTransformer(String... columns) {
		super();
		setColumns(columns);
	}

	@Override
	public void initialize(IProcessContext processContext) throws ETLException {
		super.initialize(processContext);
		
		if (columns == null || columns.length == 0) {
			throw new ETLException("No columns configured");
		}
	}

	@Override
	public void processRecord(IProcessContext processContext, IRecord record) throws ETLException {
		super.processRecord(processContext, record);
		
		for (String column : columns) {
			Object value = record.getData(column);
			if (value instanceof Boolean) {
				// nothing to transform
				continue;
			}
			Boolean b = null; 
			if (value instanceof Number) {
				Number n = (Number)value;
				b = new Boolean(!(n.intValue() == 0));
			} else if (value != null) {
				// parse string
				String s = value.toString().toLowerCase();
				b = trueValues.contains(s) ? b = Boolean.TRUE : Boolean.FALSE; 
			}
			
			record.setData(column, b);
		}
	}
	
	/**
	 * @return the columns
	 */
	public String[] getColumns() {
		return columns;
	}

	/**
	 * @param columns the columns to set
	 */
	public void setColumns(String... columns) {
		this.columns = columns;
	}

	/**
	 * @return the trueValues
	 */
	public Set<String> getTrueValues() {
		return trueValues;
	}

	/**
	 * @param trueValues the trueValues to set
	 */
	public void setTrueValues(Set<String> trueValues) {
		this.trueValues = trueValues;
	}
	
}
