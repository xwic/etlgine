/*
 * Copyright 2007 jWic group (http://www.jwic.de)
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
 */
 /****************************** NOTE ***********************************
  * This library is required by the TreeViewer control. It depends on the
  * functions specified in the tblviewer library. This means that if you 
  * want to use the TreeViewer, you must also include the TableViewer
  * js library and css files.
  ***********************************************************************/
  
  /**
   * Expand a node in the selected element.
   */
  function trV_Expand(e) {
  	if (!e) e = window.event;
  	var element = tblViewer_getTarget(e);
  	var key = trV_getRowKey(element);
  	var ctrlId = trV_getControlID(element);

	JWic.fireAction(ctrlId, "expand", key);

	// prevent the click function from runing as we do not want a selection.  	
	// tblv_ignoreClick = window.event.X + "." + window.event.Y;
	e.cancelBubble = true;
	if (e.stopPropagation) e.stopPropagation();
	
  	return false;
  }
  
  /**
   * Collapse the node in the selected element.
   */
  function trV_Collapse(e) {
  	if (!e) e = window.event;
  	var element = tblViewer_getTarget(e);
  	var key = trV_getRowKey(element);
  	var ctrlId = trV_getControlID(element);

  	JWic.fireAction(ctrlId, "collapse", key);

	// prevent the click function from runing as we do not want a selection.  	
	e.cancelBubble = true;
	if (e.stopPropagation) e.stopPropagation();

  	return false;
  }
  
  function trV_getRowKey(element) {
  	var node = element;
  	while (node.attributes.getNamedItem("tbvRowKey") == null || node == null) {
  		node = node.parentNode;
  	}
  	if (node != null) {
  		return node.attributes.getNamedItem("tbvRowKey").value;
  	}
  	return "";
  }
  
  function trV_getControlID(element) {
  	var node = element;
  	while (node.attributes.getNamedItem("tbvctrlid") == null || node == null) {
  		node = node.parentNode;
  	}
  	if (node != null) {
  		return node.attributes.getNamedItem("tbvctrlid").value;
  	}
  	return "";
  }