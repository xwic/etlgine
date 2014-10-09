package de.xwic.etlgine.loader.database.operation;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IRecord;

public interface IDatabaseOperation {

	void execute(IProcessContext processContext, IRecord record) throws ETLException;

}
