package de.xwic.etlgine.loader.database;

import java.util.List;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.loader.database.DatabaseLoader.Mode;
import de.xwic.etlgine.util.Validate;

/**
 * Utility class to perform various validations.
 * 
 * @author mbogdan
 *
 */
public class DatabaseLoaderValidators {

	/**
	 * Throws an ETLException if the loader parameters are not according to the operating mode.
	 */
	public static void validateMode(final Mode mode, final List<String> pkColumns) throws ETLException {
		if (mode == Mode.INSERT_OR_UPDATE) {
			Validate.notNull(
					pkColumns,
					"When running in INSERT_OR_UPDATE mode, at least one column name inside 'pkColumns' is needed to determine if the operation will be INSERT or UPDATE.");
			Validate.notEmpty(
					pkColumns,
					"When running in INSERT_OR_UPDATE mode, at least one column name inside 'pkColumns' is needed to determine if the operation will be INSERT or UPDATE.");
		}
		//TODO Bogdan - the 2 other modes...
	}

	/**
	 * <p>
	 * Throws an ETLException if the identityManager has not been set for INSERT_OR_UPDATE or UPDATE modes.
	 * 
	 * <p>
	 * The identityManager is needed to determine whether an INSERT or an UPDATE operation should be performed, or to determine which record
	 * should be updated.
	 * 
	 * @param mode
	 * @param identityManager
	 */
	public static void validateIdentityManager(final Mode mode, final IdentityManager identityManager) throws ETLException {
		if (mode == Mode.INSERT_OR_UPDATE) {
			Validate.notNull(identityManager,
					"When running in INSERT_OR_UPDATE mode, the 'identityManager' is needed to determine if the operation will be INSERT or UPDATE.");
		}
		//TODO Bogdan - the UPDATE mode...
	}
}
