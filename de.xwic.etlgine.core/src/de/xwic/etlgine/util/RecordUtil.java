package de.xwic.etlgine.util;

import java.util.List;
import java.util.ListIterator;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IRecord;

public class RecordUtil {

	private RecordUtil() {
		//Utility class
	}

	/**
	 * <p>
	 * Builds a string composed of the PK columns and their values, useful in logging.
	 * 
	 * <p>
	 * Example:
	 * 
	 * <pre>
	 * [CASEID = 2005125005, ITEM_NO = 10, TRANSACTION_ID = Technical Case]
	 * </pre>
	 * 
	 * @param record
	 * @param pkColumns
	 * @throws ETLException
	 *             if the record does not contain one of the PK columns
	 * @return
	 */
	public static String buildPKString(final IRecord record, final List<String> pkColumns) throws ETLException {
		StringBuilder sb = new StringBuilder();

		sb.append("[");
		for (ListIterator<String> it = pkColumns.listIterator(); it.hasNext();) {
			String columnName = it.next();
			sb.append(columnName).append(" = ").append(record.getData(columnName));

			// Append a ', ' only if there are more elements
			if (it.hasNext()) {
				sb.append(", ");
			}
		}
		sb.append("]");

		return sb.toString();
	}

}
