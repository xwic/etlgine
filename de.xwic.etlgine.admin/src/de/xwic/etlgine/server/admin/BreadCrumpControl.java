/**
 * 
 */
package de.xwic.etlgine.server.admin;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Stack;

import de.jwic.base.Control;
import de.jwic.base.IControlContainer;

/**
 * @author lippisch
 *
 */
public class BreadCrumpControl extends Control {

	private final StackedContentContainer stack;

	/**
	 * @param container
	 * @param name
	 */
	public BreadCrumpControl(IControlContainer container, String name, StackedContentContainer stack) {
		super(container, name);
		this.stack = stack;
		stack.addPropertyChangeListener(new PropertyChangeListener() {
			public void propertyChange(PropertyChangeEvent evt) {
				requireRedraw();
			}
		});
	}

	/**
	 * Goto the specified module.
	 * @param name
	 */
	public void actionGoto(String name) {
		
		Stack<String> list = stack.getStack();
		while (list.size() > 1) {
			String topMost = list.peek();
			if (!topMost.equals(name)) {
				stack.getControl(topMost).destroy();
			} else {
				break;
			}
		}
		
	}
	
	/**
	 * Returns the name list.
	 * @return
	 */
	public Stack<String> getNameList() {
		return stack.getStack();
	}

	/**
	 * Returns the title for the element.
	 * @param name
	 * @return
	 */
	public String getTitle(String name) {
		Control ctrl = stack.getControl(name);
		if (ctrl == null) {
			return "Not Found";
		} else if (ctrl instanceof BaseContentContainer) {
			return ((BaseContentContainer)ctrl).getTitle();
		} else {
			return name;
		}
	}
	
}
