/**
 * 
 */
package de.xwic.etlgine.transformer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
public class NumberTransformer extends AbstractTransformer {

	private Set<String> logMessages = new HashSet<String>();
	protected Class<? extends Number> numberClass = Integer.class;
	protected String[] columns = null;
	
	private boolean ignoreError = true;
	private boolean logError = true;
	
	@Override
	public void initialize(IProcessContext processContext) throws ETLException {
		super.initialize(processContext);
		
		if (columns == null || columns.length == 0) {
			throw new ETLException("No columns configured");
		}
	}
	
	@Override
	public void onProcessFinished(IProcessContext processContext) throws ETLException {
		super.onProcessFinished(processContext);
		
		logMessages.clear();
	}

	@Override
	public void processRecord(IProcessContext processContext, IRecord record) throws ETLException {
		super.processRecord(processContext, record);
		
		for (String column : columns) {
			Object value = record.getData(column);
			if (value instanceof Number) {
				// nothing to transform
				continue;
			}
			Number n = null; 
			if (value != null) {
				try {
					Constructor<? extends Number> c = numberClass.getConstructor(String.class);
					n = c.newInstance(value.toString());
				} catch (InvocationTargetException ite) {
					if (ignoreError) {
						// warn
						if (logError) {
							String msg = "Execption parsing value '" + value + "' to " + numberClass.getSimpleName() + ", column '" + column + "'";
							if (logMessages.add(msg)) {
								processContext.getMonitor().logWarn(msg);
							}
						}
					} else {
						throw new ETLException(ite.getTargetException());
					}
				} catch (Exception e) {
					throw new ETLException(e);
				}
			} else if (value != null) {
				// parse string
			}
			
			record.setData(column, n);
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
	 * @return the numberClass
	 */
	public Class<? extends Number> getNumberClass() {
		return numberClass;
	}

	/**
	 * @param numberClass the numberClass to set
	 */
	public void setNumberClass(Class<? extends Number> numberClass) {
		this.numberClass = numberClass;
	}

	/**
	 * @return the ignoreError
	 */
	public boolean isIgnoreError() {
		return ignoreError;
	}

	/**
	 * @param ignoreError the ignoreError to set
	 */
	public void setIgnoreError(boolean ignoreError) {
		this.ignoreError = ignoreError;
	}

	/**
	 * @return the logError
	 */
	public boolean isLogError() {
		return logError;
	}

	/**
	 * @param logError the logError to set
	 */
	public void setLogError(boolean logError) {
		this.logError = logError;
	}
	
}
