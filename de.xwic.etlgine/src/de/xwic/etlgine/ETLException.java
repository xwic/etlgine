/*
 * de.xwic.etlgine.ETLException 
 */
package de.xwic.etlgine;

/**
 * 
 * @author lippisch
 */
public class ETLException extends Exception {

	/**
	 * 
	 */
	public ETLException() {
		super();
	}

	/**
	 * @param message
	 * @param cause
	 */
	public ETLException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 */
	public ETLException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public ETLException(Throwable cause) {
		super(cause);
	}

}
