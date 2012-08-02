/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.ei;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp.DelegatingConnection;

/**
 * @author lippisch
 */
public class TracedConnection extends DelegatingConnection {

	private ConnectionHandler handler;
	private Throwable stackTrace;
	private long borrowTime;

	private final long connNr;

	/**
	 * @param connection
	 */
	public TracedConnection(long connNr, ConnectionHandler handler, Connection connection) {
		super(connection);
		this.connNr = connNr;
		this.handler = handler;
		this.borrowTime = System.currentTimeMillis();
		this.stackTrace = new RuntimeException("Connection Issued");
		stackTrace.fillInStackTrace();
	}
	
	/**
	 * Returns the time the connection was borrowed.
	 * @return
	 */
	public long getBorrowTime() {
		return borrowTime;
	}
	
	/**
	 * Returns the time in ms since the connection was borrowed.
	 * @return
	 */
	public long getAge() {
		return System.currentTimeMillis() - borrowTime;
	}

	/**
	 * Returns the stack trace that was taken when the connection
	 * was borrowed.
	 * @return
	 */
	public StackTraceElement[] getStackTrace() {
		return stackTrace.getStackTrace();
	}
	
	/**
	 * @throws SQLException
	 * @see java.sql.Connection#close()
	 */
	public void close() throws SQLException {
		super.close();
		handler.connectionClosed(this);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (connNr ^ (connNr >>> 32));
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TracedConnection other = (TracedConnection) obj;
		if (connNr != other.connNr)
			return false;
		return true;
	}

	/**
	 * @return the connCount
	 */
	public long getConnectionNumber() {
		return connNr;
	}

}
