/**
 * $Id: TrimTransformer.java,v 1.1 2009/01/26 07:30:43 jbornemann Exp $
 *
 * Copyright (c) 2008 Network Appliance.
 * All rights reserved.

 * com.netapp.msaa.etl.transformer.TrimTransformer.java
 * Created on Jan 15, 2009
 * 
 * @author jbornema
 */
package de.xwic.etlgine.transformer;

import de.xwic.etlgine.AbstractTransformer;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;

/**
 * Created on Jan 15, 2009
 * @author jbornema
 */

public class TrimTransformer extends AbstractTransformer {

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.AbstractTransformer#processRecord(de.xwic.etlgine.IProcessContext, de.xwic.etlgine.IRecord)
	 */
	@Override
	public void processRecord(IProcessContext processContext, IRecord record) throws ETLException {
		super.processRecord(processContext, record);

		IDataSet ds = processContext.getDataSet();

		// trim spaces
		for (IColumn column : ds.getColumns()) {
			Object value = record.getData(column);
			if (!(value instanceof String)) {
				continue;
			}
			value = ((String)value).trim();
			record.setData(column, value);
		}
	}
}
