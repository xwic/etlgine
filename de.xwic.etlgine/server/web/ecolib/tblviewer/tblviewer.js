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

	/* globals */
	var tblv_currResizer;
	var tblv_colIdx;
	var tblv_resXStart;
	var tblv_minX;
	var tblv_oldSize;
	var tblv_CtrlId;
	var tblv_pushedCol = "";
	var tblv_scrolledX = 0;

	/**
	 * Scroll the header and store scroll info for re-rendering.
	 */
	function tblViewer_handleScroll(e, viewerCtrlId) {
		if (!e) e = window.event;
		var sourceLayer = tblViewer_getTarget(e);
		
		var divHeader = document.getElementById("tblViewHead_" + viewerCtrlId);
		if (divHeader) {
			divHeader.scrollLeft = sourceLayer.scrollLeft;
		}
		var fixData = document.getElementById("tblViewLeftDataLayer_" + viewerCtrlId);
		if (fixData) {
			fixData.scrollTop = sourceLayer.scrollTop;
			
			var ie6fix = document.getElementById("ie6fixscroll_" + viewerCtrlId);
			ie6fix.style.display = "inline";
			ie6fix.style.display = "none";
		}
		document.forms['jwicform'].elements['fld_' + viewerCtrlId + '.top'].value = sourceLayer.scrollTop;
		document.forms['jwicform'].elements['fld_' + viewerCtrlId + '.left'].value = sourceLayer.scrollLeft;
	}
	
	/**
	 * Modifies the column css_class to simulate a pushed button.
	 */
	function tblViewer_pushColumn(colIdx, viewerCtrlId, fixed) {
		var tableObject = document.getElementById("tblViewData_" + viewerCtrlId);
		if (fixed) {
			tableObject = document.getElementById("tblViewLeftData_" + viewerCtrlId);
		}
		var colNodes = tblViewer_getColumnNodes(tableObject);
		for (var i = 0; i <colNodes.length; i++) {
			var objTH = colNodes[i];
			if (objTH.nodeName == "TH" && objTH.attributes.getNamedItem("colIdx").value == colIdx) {
				objTH.className = "pushed";
				tblv_pushedCol = objTH;
				break;
			}
		}
	}
	
	/**
	 * Modifies the column css_class so that it looks like before if
	 * the button was released.
	 */
	function tblViewer_releaseColumn() {
		if (tblv_pushedCol != "") {
			tblv_pushedCol.className = "";
			tblv_pushedCol = "";
			return;
		}
	}
	
	/**
	 * starts column resizing.
	 */
	function tblViewer_resizeColumn(e, viewerCtrlId, fixed) {
		if (!e) e = window.event;
		if (e.preventDefault) {
			e.preventDefault();
		}
		var imgSeperator = tblViewer_getTarget(e);

		// show a DIV layer
		var resizer = document.getElementById("tblViewResizer_" + viewerCtrlId);
		var tableObject = document.getElementById("tblViewData_" + viewerCtrlId);
		var tableContent = document.getElementById("tblContent_" + viewerCtrlId);
		
		var tblWidth = tableObject.parentNode.style.pixelWidth;
		var tblHeight = tableObject.parentNode.style.pixelHeight;
		if (tableContent) tblHeight = tableContent.clientHeight;
		var colIdx = imgSeperator.attributes.getNamedItem("colIdx").value;
		if (fixed) {
			tableObject = document.getElementById("tblViewLeftData_" + viewerCtrlId);
		}
		var offset = tblViewer_getOffset(tableObject);
		var colWidth = new Array();

		var minX = offset.left;
		if (document.body.leftMargin) {
			minX += parseInt(document.body.leftMargin);
		}
		log("offset: " + minX + "; " + offset.left + "; " + document.body.leftMargin);	
		// build list of the width of all columns
		var colNodes = tblViewer_getColumnNodes(tableObject);
		for (var i = 0; i <colNodes.length; i++) {
			var objTH = colNodes[i];
			if (objTH.nodeName == "TH" && objTH.attributes.getNamedItem("colIdx")) {
				var idx = objTH.attributes.getNamedItem("colIdx").value;
				colWidth[idx] = objTH.width;
				if (idx == colIdx) {
					break;
				}
				minX += parseInt(objTH.width);
				minX += parseInt(tblViewer_getStyle(objTH, "borderRightWidth", "border-right-width")) +
						+ parseInt(tblViewer_getStyle(objTH, "borderLeftWidth", "border-left-width"))
						+ parseInt(tblViewer_getStyle(objTH, "paddingLeft", "padding-left"))	
						+ parseInt(tblViewer_getStyle(objTH, "paddingRight", "padding-right"));
			}
		}
		
		var divHeader = document.getElementById("tblViewHead_" + viewerCtrlId);
		if (divHeader && !fixed) {
			tblv_scrolledX = divHeader.scrollLeft;
		} else {
			tblv_scrolledX = 0;
		}
		tblv_currResizer = resizer;
		tblv_colIdx = colIdx;
		tblv_resXStart = e.x ? e.x : e.clientX;
		tblv_minX = minX;
		tblv_oldSize = colWidth[colIdx];
		tblv_CtrlId = viewerCtrlId;

		var newWidth = tblViewer_getNewWidth(e);
		resizer.style.display = "inline";
		if (tblHeight > 30) {
			resizer.style.height = tblHeight + "px";
		}
		var newLeft = (minX + newWidth - tblv_scrolledX) + "px";
		resizer.style.left = newLeft;
		
		if (resizer.setCapture) { // IE mode
			resizer.onmousemove = tblViewer_resizeColumMove;
			resizer.onmouseup = tblViewer_resizeColumnDone;
			resizer.setCapture();
		} else { // Mozilla
			window.onmousemove = tblViewer_resizeColumMove;
			window.onmouseup = tblViewer_resizeColumnDone;
		}
		
		
	}
	
	/**
	 * returns the list of TH elements that specify the columns.
	 */
	function tblViewer_getColumnNodes(tableObject) {
		var objTR = tblViewer_findElement(tableObject, "TR");
		if (objTR == null) {
			log("ERROR tblViewer_getColumnNodes: cant find TH tags");
		}
		return objTR.childNodes;
	}
	/**
	 * Invoked when the mouse is moved.
	 */
	function tblViewer_resizeColumMove(e) {
		if (!e) e = window.event;
		var newWidth = tblViewer_getNewWidth(e);
		tblv_currResizer.style.left = (tblv_minX + newWidth - tblv_scrolledX) + "px";
	}
	/**
	 * Invoked when the user released the resizer.
	 */
	function tblViewer_resizeColumnDone(e) {
		if (!e) e = window.event;
		tblv_currResizer.style.display = "none";
		
		log("resizeColumnDone");
		
		if (tblv_currResizer.setCapture) { // IE mode
			tblv_currResizer.releaseCapture();
			tblv_currResizer.onmouseup = null;
			tblv_currResizer.onmousemove = null;
		} else { // Mozilla mode
			window.onmousemove = null;
			window.onmouseup = null;
		}
		
		var newWidth = tblViewer_getNewWidth(e);
		
		if (newWidth != tblv_oldSize) {
			jWic().fireAction(tblv_CtrlId, 'resizeColumn', tblv_colIdx + ";" + newWidth);
		}
	}
	
	function tblViewer_getNewWidth(e) {
		var x = e.x ? e.x : e.clientX;
		var diff = x - tblv_resXStart;
		var newWidth = parseInt(diff) + parseInt(tblv_oldSize);
		if (newWidth < 6) {
			newWidth = 6;
		}
		return newWidth;
	}
	

	/**
	 * Activated by onClick event on a row.
	 */
	function tblViewer_ClickRow(tblRow, e) {
	
		if (!e) e = window.event;

		var rowKey = tblRow.attributes.getNamedItem("tbvRowKey").value;
		// TR->TBODY->TABLE
		var tableNode = tblRow.parentNode.parentNode;
		var attributes = tableNode.attributes;
		var tbvCtrlId = attributes.getNamedItem("tbvctrlid").value;
		var tbvSelKey = attributes.getNamedItem("tbvSelKey").value;		
		var tbvSelMode = attributes.getNamedItem("tbvSelMode").value;
		
		var tableNode2;
		if (tableNode.id.indexOf("tblViewDataTbl_") == 0) {
			tableNode2 = document.getElementById("tblViewLeftDataTbl_" + tbvCtrlId);
		} else if (tableNode.id.indexOf("tblViewLeftDataTbl_") == 0) {
			tableNode2 = document.getElementById("tblViewDataTbl_" + tbvCtrlId);
		}
	
		log("tblViewer_ClickRow: " + tbvSelMode + "; " + tbvSelKey);
		if (tbvSelMode == "multi") {
			tblViewer_flipRowSelection(tblRow);
			if (tableNode2) tblViewer_flipRowSelection(tableNode2.rows[tblRow.rowIndex]);
		} else if (tbvSelMode == "single") {
			if (tbvSelKey != rowKey) {
				// deselect old row
				if (tbvSelKey != "") {
					// find selected row and deselect it.
					for (var rowNr = 0; rowNr < tableNode.rows.length; rowNr++)  {
						var trElm = tableNode.rows[rowNr];
						var trKeyItem = trElm.attributes.getNamedItem("tbvRowKey");
						if (trKeyItem != null && trKeyItem.value == tbvSelKey) {
							tblViewer_flipRowSelection(trElm);
							if (tableNode2) tblViewer_flipRowSelection(tableNode2.rows[trElm.rowIndex]);
							break;
						}
					}
				}
				// select
				tblViewer_flipRowSelection(tblRow); // select row
				tableNode.attributes.getNamedItem("tbvSelKey").value = rowKey;
				if (tableNode2) {
					tblViewer_flipRowSelection(tableNode2.rows[tblRow.rowIndex]);
					tableNode2.attributes.getNamedItem("tbvSelKey").value = rowKey;
				}
			}
		}
		// change selection
	
		// notify control
		jWic().fireAction(tbvCtrlId, 'selection', rowKey);
	
	}
	
	function tblViewer_flipRowSelection(tblRow) {
		if (tblRow.className == "selected") {
			tblRow.className = "";
		} else if (tblRow.className == "" || tblRow.className == "undefined") {
			tblRow.className = "selected";
		} else if (tblRow.className == "lastRow") {
			tblRow.className = "lastRowselected";
		} else if (tblRow.className == "lastRowselected") {
			tblRow.className = "lastRow";
		}
		//tblRow.innerHtml = tblRow.innerHtml;
	}
	
	function tblViewer_getOffset(obj) {
		var offset = function() { 
			var left;
			var top; 
		}
		var oLeft = 0;
		var oTop = 0;
		while ( obj.tagName != 'BODY' ) { 
			oLeft += obj.offsetLeft;
			oTop  += obj.offsetTop;
			obj = obj.offsetParent;
		}
		
		offset.left = oLeft;
		offset.top = oTop;
		
		return offset;
	}
	
	/**
	 * Returns the event target.
	 */
	function tblViewer_getTarget(e) {
		if (e.target) {
			return e.target;
		} else {
			return e.srcElement;
		}
	}
	
	/**
	 * Find a specified child element.
	 */
	function tblViewer_findElement(elm, sName) {

		if (elm.nodeName == sName) {
			return elm;
		}
		for (var i = 0; i < elm.childNodes.length; i++) {
			var child = elm.childNodes[i];
			if (child.nodeName == sName) {
				return child;
			}
			if (child.childNodes && child.childNodes.length > 0) {
				child = tblViewer_findElement(child, sName);
				if (child != null) {
					return child;
				}
			}
		}
		return null;

	}
	
	function tblViewer_getStyle(elm, ieName, mozName) {
		var v = "";
		if (elm.currentStyle)
			v = elm.currentStyle[ieName];
		else if (window.getComputedStyle) {
			try {
				v = document.defaultView.getComputedStyle(elm,null).getPropertyValue(mozName);
			} catch (e) {
				alert("error reading styles:" + e + "\n name: " + mozName + "\n elm" + elm);
			}
		}
		return v;
	}