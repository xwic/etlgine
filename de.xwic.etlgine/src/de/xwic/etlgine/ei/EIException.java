/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.ei;

/**
 * Invoked if a generic exception occurred during an EI process.
 * @author lippisch
 */
public class EIException extends Exception {

	/**
	 * 
	 */
	public EIException() {
		super();
	}

	/**
	 * @param message
	 * @param cause
	 */
	public EIException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 */
	public EIException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public EIException(Throwable cause) {
		super(cause);
	}

	
}
