/*
 * Copyright (c) NetApp Inc. - All Rights Reserved
 * 
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 * 
 * de.xwic.etlgine.monitor.jdbc.JDBCMonitorEnhanced 
 */
package de.xwic.etlgine.monitor.jdbc;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IColumn;
import de.xwic.etlgine.IColumn.DataType;
import de.xwic.etlgine.IContext;
import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.impl.ProcessContext;

/**
 * This class contains the enhancement to log also the queue where the job runs at job finish event.
 * The column 'QueueName' has to be added to the monitor table.
 * 
 * @author ionut
 *
 */
public class JDBCMonitorEnhanced extends JDBCMonitor {

	protected IColumn colQueueName;

	/*
	 * (non-Javadoc)
	 * @see de.xwic.etlgine.monitor.jdbc.JDBCMonitor#initialize(de.xwic.etlgine.IContext)
	 */
	@Override
	public void initialize(IContext context) throws ETLException {
		super.initialize(context);
		IDataSet dataSet = processContext.getDataSet();
		colQueueName = dataSet.addColumn("QueueName");
		colQueueName.setTypeHint(DataType.STRING);
		colQueueName.setLengthHint(48);

	}
	
	/*
	 * (non-Javadoc)
	 * @see de.xwic.etlgine.monitor.jdbc.JDBCMonitor#recordSetupExtensionPoint(de.xwic.etlgine.IRecord)
	 */
	@Override
	protected void recordSetupExtensionPoint(IRecord record) {
		if (eventType != null && eventType == EventType.JOB_EXECUTION_END) {
			record.setData(colQueueName, currentJob != null ? currentJob.getQueueName(): "");
		}
	}
}
