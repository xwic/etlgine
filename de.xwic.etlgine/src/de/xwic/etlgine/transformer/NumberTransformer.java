/**
 * 
 */
package de.xwic.etlgine.transformer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.xwic.etlgine.AbstractTransformer;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
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
	protected List<NumberFormat> numberFormats = new ArrayList<NumberFormat>();
	
	private boolean ignoreError = true;
	private boolean logError = true;
    private boolean avoidDouble = false;
    private boolean autoColumn = false;
	
	@Override
	public void initialize(IProcessContext processContext) throws ETLException {
		super.initialize(processContext);
		
		if ((columns == null || columns.length == 0) && !autoColumn) {
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
		if (autoColumn && columns == null) {
			ArrayList<String> numberColumns = new ArrayList<String>();
			for (IColumn column : processContext.getDataSet().getColumns()) {
				if (avoidDouble) {
					switch (column.getTypeHint()) {
						case UNKNOWN:
						case INT:
						case LONG:
						case DOUBLE: {
							numberColumns.add(column.getName());
						}
					}
				} else {
					numberColumns.add(column.getName());
				}
			}
			columns = (String[]) numberColumns.toArray(new String[numberColumns.size()]);
		}

		for (String column : columns) {
			Object value = record.getData(column);
			if (value instanceof Number) {
				if (avoidDouble) {
					Number nr = (Number) value;
					if (nr.longValue() != nr.doubleValue()) {
						// nothing to transform, keep it as double
					} else if (nr.longValue() <= Integer.MAX_VALUE && nr.longValue() >= Integer.MIN_VALUE) {
						record.setData(column, new Integer(nr.intValue()));
					} else {
						record.setData(column, new Long(nr.longValue()));
					}
				}
				continue;
			}
			if (autoColumn && avoidDouble) {
				// As we say, unfortunately most of the types will be UNKNOWN,
				// so we have to leave here and
				// handle only Numeric values detected by the instanceof, keep
				// the rest as it is.
				continue;
			}
			Number n = null;
			if (value != null) {
				if (numberFormats != null && numberFormats.size() > 0) {
					// first try one of the number formats
					try {
						for (NumberFormat nf : numberFormats) {
							try {
								n = nf.parse(value.toString());
								break;
							} catch (Throwable t) {
								// try next
							}
						}
					} catch (Throwable t) {
						if (!ignoreError) {
							throw new ETLException(t);
						}
					}
				}
				if (n == null) {
					try {
						Constructor<? extends Number> c = numberClass.getConstructor(String.class);
						n = c.newInstance(value.toString());
					} catch (InvocationTargetException ite) {
						if (ignoreError) {
							// warn
							if (logError) {
								String msg = "Execption parsing value '" + value + "' to " + numberClass.getSimpleName()
										+ ", column '" + column + "'";
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
				}
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

	/**
	 * @return the numberFormats
	 */
	public List<NumberFormat> getNumberFormats() {
		return numberFormats;
	}

	/**
	 * @param numberFormats the numberFormats to set
	 */
	public void setNumberFormats(List<NumberFormat> numberFormats) {
		this.numberFormats = numberFormats;
	}

	/**
	 * @return the avoidDouble
	 */
	public boolean isAvoidDouble() {
		return avoidDouble;
	}

	/**
	 * @param avoidDouble the avoidDouble to set
	 */
	public void setAvoidDouble(boolean avoidDouble) {
		this.avoidDouble = avoidDouble;
	}

	/**
	 * @return the autoColumn
	 */
	public boolean isAutoColumn() {
		return autoColumn;
	}

	/**
	 * @param autoColumn the autoColumn to set
	 */
	public void setAutoColumn(boolean autoColumn) {
		this.autoColumn = autoColumn;
	}

}
