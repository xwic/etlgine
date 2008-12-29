/*
 * Copyright 2005 jWic group (http://www.jwic.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * de.jwic.ecolib.controls.StackedContainer
 * Created on 10.01.2006
 * $Id: StackedContainer.java,v 1.6 2006/09/28 06:47:00 lordsam Exp $
 */
package de.xwic.etlgine.server.admin;

import java.util.Stack;

import de.jwic.base.Control;
import de.jwic.base.ControlContainer;
import de.jwic.base.IControlContainer;
import de.jwic.base.JWicException;

/**
 * A ControlContainer that displays only one control at a time.
 * 
 * @author Florian Lippisch
 * @version $Revision: 1.6 $
 */
public class StackedContentContainer extends ControlContainer {

	private static final long serialVersionUID = 2177569108464586902L;
	private String currentControlName = null;
	private Stack<String> stack = new Stack<String>();
	private String widthHint = "";
	private String heightHint = "";

	/**
	 * @param container
	 * @param name
	 */
	public StackedContentContainer(IControlContainer container, String name) {
		super(container, name);
	}

	/**
	 * @param container
	 */
	public StackedContentContainer(IControlContainer container) {
		super(container);
	}
	
	/* (non-Javadoc)
	 * @see de.jwic.base.ControlContainer#registerControl(de.jwic.base.Control, java.lang.String)
	 */
	public void registerControl(Control control, String name) throws JWicException {
		super.registerControl(control, name);
		stack.push(control.getName());
		if (currentControlName == null) {
			currentControlName = control.getName();
			requireRedraw();
		}
	}

	/* (non-Javadoc)
	 * @see de.jwic.base.ControlContainer#unregisterControl(de.jwic.base.Control)
	 */
	public void unregisterControl(Control control) {
		stack.remove(control.getName());
		if (currentControlName.equals(control.getName())) {
			if (!stack.isEmpty()) {
				currentControlName = (String)stack.peek();
			} else {
				currentControlName = null;
			}
		}
		super.unregisterControl(control);
	}
	
	/**
	 * @return Returns the currentControlName.
	 */
	public String getCurrentControlName() {
		return currentControlName;
	}

	/**
	 * @param currentControlName The currentControlName to set.
	 */
	public void setCurrentControlName(String currentControlName) {
		this.currentControlName = currentControlName;
		requireRedraw();
	}
	
	/**
	 * @return Returns the heightHint.
	 */
	public String getHeightHint() {
		return heightHint;
	}

	/**
	 * @param heightHint The heightHint to set.
	 */
	public void setHeightHint(String heightHint) {
		this.heightHint = heightHint;
	}

	/**
	 * @return Returns the widthHint.
	 */
	public String getWidthHint() {
		return widthHint;
	}

	/**
	 * @param widthHint The widthHint to set.
	 */
	public void setWidthHint(String widthHint) {
		this.widthHint = widthHint;
	}
	
	/* (non-Javadoc)
	 * @see de.jwic.base.ControlContainer#isRenderingRelevant(de.jwic.base.Control)
	 */
	public boolean isRenderingRelevant(Control childControl) {
		return childControl.getName().equals(currentControlName);
	}

	/**
	 * Returns the stack.
	 * @return
	 */
	public Stack<String> getStack() {
		return stack;
	}
	
}
