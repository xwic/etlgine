/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.ei;

import java.util.Iterator;

/**
 * Iterator that can (must) be closed after usage.
 * @author lippisch
 */
public interface ResourceIterator<E> extends Iterator<E> {

	public void close();
	
}
