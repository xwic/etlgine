/*
 * de.xwic.etlgine.ETLException 
 */
package de.xwic.etlgine;

/**
 * 
 * @author lippisch
 */
public class ETLException extends Exception {

	protected IProcess process = null;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3204004770821027644L;

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

	/**
	 * @return the process
	 */
	public IProcess getProcess() {
		return process;
	}

	/**
	 * @param process the process to set
	 */
	public void setProcess(IProcess process) {
		this.process = process;
	}

}
