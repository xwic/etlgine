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
 */

	var debugMode = false;
	var jwicObj = "";
    var req;
    var _ajaxProcessing = false;
    var _ajaxRenderControls = 0;
    var _ajaxControlListener = new Array();
    var _blocked = false;
    var _waitScreenDelay = 500;
    var _waitAjaxUpdate = 100;
    var _starttime = 0;
    var _logCount = 0;

	function log(message) {
		if (debugMode) {
			var elem = document.forms['jwicform'].elements['_debugLog'];
			elem.value = _logCount + ":" + message + "\n" + elem.value;
			_logCount++;
		}
	}

	/**
	 * Returns an array of SCRIPT blocks found within a text. This function is used
	 * to execute the JavaScript code during an AJAX based update. You can mark the
	 * script with 'onajax="false"' to not execute it during an AJAX update. But be
	 * aware that firefox runs the script on his own! This 'workaround' is only made
	 * for the IE, since this browser does not execute the script.
	 */
	function findScript(text) {
	
		var scripts = new Array();
		var startTag = /<script(?! onajax="false")[^>]*>/i
		var endTag = /<\/script>/i
		
		// find tags
		var resS = startTag.exec(text);
		while (resS && resS.length > 0) {
			text = text.substring(resS.index + resS[0].length);
			var resE = endTag.exec(text);
			if (resE && resE.length > 0) {
				var script = text.substring(0, resE.index);
				scripts.push(script);
				text = text.substring(resE.index + resE[0].length);
			} else {
				break; // No End Tag -> Exit
			}
			resS = startTag.exec(text);
		}
		
		return scripts;
	
	}

	/**
	 * AjaxControlListener object holds the listener function registered
	 * for an eventName under specified controlId.
	 * Used by jWic ajax implementation to signal the control that a
	 * specific event occured, e.g. "update" for Ajax updates/already updated the control.
	 * The specified function of the current control and all its containing
	 * controls functions are called with the event object.
	 */
	function AjaxControlListener(eventName, controlId, listener) {
		this.children = new Array;
		var controlName;
		this.func = listener;
		this.setControlName = function(newControlName) { controlName = newControlName; };
		this.getControlName = function() { return controlName; };
		this.setListener = function(listener) { this.func = listener; };
		this.getListener = function() { return this.func; };
		this.getChildren = function() { return this.children; };
		this.getControlListener = function(ctrlId) {
			var names = ctrlId.split(".");
			var container = _ajaxControlListener[eventName];
			for (var i = 0; container && i < names.length; i++) {
				var name = names[i];
				container = container.getChildren()[name];
			}
			return container;
		};
		// Call all listener registered to this control and containing controls
		this.callAll = function() {
			var a = arguments;
			if (this.func) {
				log("Call ajax control listener with arguments: " + a);
				this.func(a[0], a[1], a[2], a[3], a[4]);
			}
			for (var name in this.children) {
				if (typeof(this.children[name]) != 'undefined' && typeof(this.children[name].callAll) != 'undefined') {
					log("executing update listener for " + name);
					this.children[name].callAll(a[0], a[1], a[2], a[3], a[4]);
				}
			}
		};

		// constructor code
		if (controlId) {
			// create parent AjaxControlListener objects for this one
			var parentContainer = _ajaxControlListener[eventName];
			// create new root parent if non exists
			if (!parentContainer) {
				parentContainer = new AjaxControlListener(eventName);
				_ajaxControlListener[eventName] = parentContainer;
			}
			// create parents for this control
			var container = this;
			var controlNames = controlId.split(".");
			for (var i = 0; i < controlNames.length; i++) {
				var name = controlNames[i];
				container = parentContainer.getChildren()[name];
				if (!container) {
					if (i < controlNames.length - 1) {
						// create new container, since it's a parent
						container = new AjaxControlListener(eventName, name);
					} else {
						// last control reached, this instance should be added
						container = this;
					}
					parentContainer.getChildren()[name] = container;
				}
				parentContainer = container;
			}
			// set data of this container
			container.setControlName(name);
			container.setListener(listener);
		}
	}		

	/**
	 * AjaxUpdateEvent object is passed to the function registered
	 * with jWic().ajaxAddUpdateListener(controlId, function(event) {})
	 * to access its new html and finish the update process by calling
	 * either updateHTML() or cancelUpdate().
	 */
	function AjaxUpdateEvent(control, html) {
		var updated = false;
		this.getControl = function() { return control; };
		this.getHTML = function() { return html; };
		this.updateHTML = function() { 
			if (!updated) {
				updated = true; 
				jWic_ajaxUpdateHTML(control, html); 
			}
		};
		this.cancelUpdate = function() { 
			if (!updated) {
				updated = true; 
				_ajaxRenderControls--;
			}
		};
	}

	/**
	 * Return a AjaxControlListener that had been added under that controlId.
	 */
	function jWic_ajaxGetUpdateListener(controlId) {
		var update = _ajaxControlListener["update"];
		if (update) {
			return update.getControlListener(controlId);
		}
		return null;
	}

	/**
	 * Register a function under controlId that is called with an
	 * AjaxUpdateEvent object to finish the ajax update control process.
	 */ 
	function jWic_ajaxAddUpdateListener(controlId, func) {
		log("adding update listener for " + controlId + " with function: " + func);
		return new AjaxControlListener("update", "ctrl_" + controlId, func);
		
	}

	/**
	 * Shows or hides the block clicker, so the user cannot click
	 * other link during the processing of the current click.
	 */
	function blockClicks(dstDoc, removeBlock) {
		log("blockClicks (" + (removeBlock ? "remove" : "initiate") + ")");
		var elem = dstDoc.getElementById("click_blocker");
		_blocked = !removeBlock;
		if (elem) {
			if (!removeBlock) {
				var sysinfoXY = WindowSize();
				var msg = dstDoc.getElementById("click_blocker_message");
				var bodyHeight = (msg ? dstDoc.body.scrollHeight : sysinfoXY[1]) - 5;
				var bodyWidth = (msg ? dstDoc.body.scrollWidth : sysinfoXY[0] ) - 5;
				elem.style.top=0;
				elem.style.height = bodyHeight + 'px';
				elem.style.left = 0;
				elem.style.width = bodyWidth + 'px';
				elem.style.backgroundImage='url(none)';
				if (msg) {
					msg.style.position = "absolute";
					if (msg.parentNode.align == "right") {
						// buttom right place
						msg.style.top = (sysinfoXY[1] + sysinfoXY[3] - parseInt(msg.style.height) - 20) + 'px';
						msg.style.left = (sysinfoXY[0] + sysinfoXY[2] - parseInt(msg.style.width) - 20) + 'px';
					} else {
						// center message
						msg.style.top = ((sysinfoXY[1] - parseInt(msg.style.height)) / 2 + dstDoc.body.scrollTop - 5) + 'px';
						msg.style.left = ((sysinfoXY[0] - parseInt(msg.style.width)) / 2 + dstDoc.body.scrollLeft - 5) + 'px';
					}
				}
			}
			elem.style.visibility = removeBlock ? "hidden" : "visible";
			
			
			// disable 'select' elements. This is required since these elements
			// are on top of all other elements, including the 'click-blocker' (in most browsers).
			
			var elemID = "SELECT";
			var selects = dstDoc.getElementsByTagName(elemID);
	    	for( i = 0; i < selects.length; i++ ) {
	        	obj = selects[i];
    			if( obj ) {
    				if (removeBlock) {
    					if (typeof obj._bcOldState != 'undefined') {
    						obj.disabled = obj._bcOldState;
    					}
    				} else {
    					obj._bcOldState = obj.disabled;
	    				obj.disabled = true;
	    			}
    			}
			}
		}
	}
	
	function ajaxWaitScreen() {
		if (_ajaxProcessing) {
			blockClicks(document, false);
		}
	}
	
	function WindowSize() {
		var myWidth = 0, myHeight = 0, scrollTop, scrollLeft;
		var type;
		if (typeof(window.innerWidth) == 'number') {
			//Non-IE
			myWidth = window.innerWidth;
			myHeight = window.innerHeight;
			scrollLeft = window.pageXOffset;
			scrollTop = window.pageYOffset;
		} else if (document.documentElement && (document.documentElement.clientWidth || document.documentElement.clientHeight)) {
			//IE 6+ in 'standards compliant mode'
			myWidth = document.documentElement.clientWidth;
			myHeight = document.documentElement.clientHeight;
			scrollLeft = document.documentElement.scrollLeft;
			scrollTop = document.documentElement.scrollTop;
		} else if (document.body && (document.body.clientWidth || document.body.clientHeight)) {
			//IE 4 compatible
			myWidth = document.body.clientWidth;
			myHeight = document.body.clientHeight;
			scrollLeft = document.body.scrollLeft;
			scrollTop = document.body.scrollTop;
		}
		return [myWidth, myHeight, scrollLeft, scrollTop];
	}	

	/**
	 * Notifies a control that the user triggered a specific action.
	 *
	 * @param srcCtrl - the source control id
	 * @param sAction - the action that happened
	 * @param sParam - the parameters of the action
	 * @param trueSubmit - if the value is 'true', the page is POSTed to the server, 
	 *                     no matter if ajax updates are enabled
	 */
	function jWic_FireAction(srcCtrl, sAction, sParam, trueSubmit) {
		
		if (debugMode) {
			log("fireAction('" + srcCtrl + "', '" + sAction + "', '" + sParam + "', " + trueSubmit + ")");
		}

		if (_ajaxProcessing) {
		    // ignore any commands executed while an ajax request is beeing processed.
		    return;
		}
		for (id in this._submitListeners) {
			log("Executing SubmitListener id '" + id + "'");
			var func = this._submitListeners[id];
			if (typeof func == "function" && func.length == 0) {
				try {
					func();
				} catch (error) {
					log("Error executing submitListener ID '" + id + "': " + error.message);
				}
			} else {
				//log("warning: a registered listener is not a function: " + id + " type: " + (typeof id) + " value= " + func);
			}
		}
		
		_starttime = (new Date()).getTime();
		
		var dstDoc = this.dstDoc;
		var sysinfoXY = WindowSize();
		var sysinfo = sysinfoXY[0] + ";" 
			+ sysinfoXY[1] + ";"
			+ sysinfoXY[2] + ";"
			+ sysinfoXY[3];
  		dstDoc.forms['jwicform'].elements['__action'].value = sAction;
  		dstDoc.forms['jwicform'].elements['__acpara'].value = sParam;
  		dstDoc.forms['jwicform'].elements['__ctrlid'].value = srcCtrl;
		if (dstDoc.forms['jwicform'].elements['__sysinfo'])
			dstDoc.forms['jwicform'].elements['__sysinfo'].value = sysinfo;
		
		if (_ajaxMode && !trueSubmit) {
			window.setTimeout("ajaxWaitScreen()", _waitScreenDelay);
    		this.ajaxSubmit();
    	} else {
    		dstDoc.forms['jwicform'].submit();
	    	blockClicks(dstDoc, false);
	    }
		
	}
	
	/**
	 * Creates a string that contains all field data on the form which can be used to
	 * send a POST request to the server using XMLHttpRequest mechanism.
	 */
	function jWic_createContent(dstDoc) {
	
	    var content = "";
	    var logElem = false;
	    if (debugMode) {
		    logElem = document.forms['jwicform'].elements['_sendContent'];
		    logElem.value = "";
	    }
	    
	    
	    // build content out of all fields on the form.
	    for (var i = 0; i < dstDoc.forms['jwicform'].elements.length; i++) {
	        var element = dstDoc.forms['jwicform'].elements[i];
	        if (element.type == "file" && element.value != "") {
	        	// if a file-upload control is on the page that has a file assigned,
	        	// a real submit is required to transfer the file to the server.
	        	// make sure that the encoding type is multipart, before the data is submitted.
	        	dstDoc.forms['jwicform'].encoding = 'multipart/form-data';
	    		dstDoc.forms['jwicform'].submit();
		    	blockClicks(dstDoc, false);
		    	return;	// exit!
	        	
	        } else if (element.type == "select-multiple") {
	        	for (var x = 0; x < element.length; x++) {
	        		if (element.options[x].selected) {
		        		content += element.name + "=" + encodeURIComponent(element.options[x].value) + "&";
		    	        if (logElem) {
		    	        	logElem.value = logElem.value + element.name + "=" + element.options[x].value + "\n";
		    	        }
	        		}
	        	}
	        } else if (element.name != "" && (element.type != "radio" || element.checked)) {
    	        content += element.name + "=" + encodeURIComponent(element.value) + "&";
    	        if (logElem) {
    	        	if (element.name != "_debugLog" && 
    	        		element.name != "_sendContent" &&
    	        		element.name != "_ajaxMessage") {
    	        		logElem.value = logElem.value + element.name + "=" + element.value + "\n";
    	        	}
    	        }
   	        }
	    }
		return content;
	}
	
	/**
	 * Submits the page with a dedicated XmlHTTPRequest object. The action is handled
	 * by the server. The server responds with a list of controls that must be redrawn
	 * including the new HTML code.
	 */
	function jWic_ajaxSubmit() {
	
	    _ajaxProcessing = true;
	    window.status = "Sending Request to Server...";
		var dstDoc = this.dstDoc;
	    var content = jWic_createContent(dstDoc);
	    
        if (window.XMLHttpRequest) {
            req = new XMLHttpRequest();
        } else if (window.ActiveXObject) {
        	try {
	            req = new ActiveXObject("Microsoft.XMLHTTP");
	        } catch (e) {
	        	// ajax not supported
	        	log("Can't create XMLHttpRequest object. Using default processing");
	        	_ajaxProcessing = false;
	    		dstDoc.forms['jwicform'].submit();
		    	blockClicks(dstDoc, false);
	        	return;
	        }
        }
	    var url = document.location.href;
	    var idx = url.indexOf("#");
	    if (idx != -1) {
	    	// remove existing # and the rest of it
	    	url = url.substring(0, idx);
	    }
        content += "&_ajaxreq=1";
	    
	    log("ajaxSubmit()");

	    req.open("POST", url, true);
	    req.onreadystatechange = this.ajaxProcessAnswer;
		req.setRequestHeader("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
	    req.send(content);
	    window.status = "Waiting for Server...";
	
	}
	
	/**
	 * Sends a POST request to the specified control. The control must implement the
	 * IResourceControl interface to handle the request.
	 */
	function jWic_sendResourceRequest(controlId, callBack) {

		var monitorStateReq;
		if (window.XMLHttpRequest) {
            monitorStateReq = new XMLHttpRequest();
        } else if (window.ActiveXObject) {
        	try {
	            monitorStateReq = new ActiveXObject("Microsoft.XMLHTTP");
	        } catch (e) {
	        	return false;
	        }
        }
        
	    var content = jWic_createContent(document);
	    var url = document.location.href;
	    var idx = url.indexOf("#");
	    if (idx != -1) {
	    	// remove existing # and the rest of it
	    	url = url.substring(0, idx);
	    }
        content += "&_resreq=1&controlId=" + controlId;

	    log("Sending ResourceRequest (controlId=" + controlId + ")");
	    
	    monitorStateReq.open("POST", url, true);
	    monitorStateReq.onreadystatechange = callBack;
	    
        monitorStateReq.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
	    monitorStateReq.send(content);
		
		return monitorStateReq;
	}
	
	
	/**
	 * Invoked by the XMLHttpRequest when the state of the request changes. This is used
	 * to process the answer of the server.
	 */
	function jWic_ajaxProcessAnswer() {
	
        if (req.readyState == 4) {

    		var dstDoc = document;    // the instance invoked by req has no reference to this.dstDoc anymore...
	
            if (req.status == 200) {
            
            	var objCount = 0;
        	    var ticketField = dstDoc.forms['jwicform'].elements["__ticket"];
        	    var rootElem = req.responseXML.getElementsByTagName("response")[0];
        	    //dstDoc.forms['jwicform'].elements['debug'].value=req.responseText;
        	    log("Recieved ajax answer.");
        	    if (debugMode) {
        	    	var elem = dstDoc.forms['jwicform'].elements['_ajaxMessage'];
        	    	if (elem) {
        	    		var txt = req.responseText;
        	    		while (txt.search(/\t/) != -1) {
	        	    		txt = txt.replace(/\t/, "  ");
	        	    	}
        	    		elem.value = txt;
        	    	}
        	    }
        	    if (rootElem) {
            	    for (var i = 0; i < rootElem.childNodes.length; i++) {
            	        var node = rootElem.childNodes[i];
            	        if(node.nodeName == "ticket") {
            	            if (ticketField) {
            	                ticketField.value = node.childNodes[0].nodeValue;
            	            }
            	        } else           	        
            	        if (node.nodeName == "exception") {
            	            alert("An error occured during processing:\n\n" + node.childNodes[0].nodeValue + "\n\nPage will be reloaded.");
            	            _ajaxMode = false;
            	            _ajaxProcessing = false;
            	            jWic().fireAction("", "refresh", "", true);
            	        } else 
            	        if (node.nodeName == "focus") {
           	            	window.setTimeout("jWic().forceFocus('" + node.childNodes[0].nodeValue + "')", 50);
            	        } else           	        
            	        if (node.nodeName == "redraw") {
            	            // do a traditional submit to render the whole page
            	            _ajaxMode = false;
            	            _ajaxProcessing = false;
            	            // read sessionId from redraw node to fix double session initiation bug
            	            var msidNode = node.getElementsByTagName('sessionId')[0];
            	            if (msidNode && msidNode.childNodes[0]) {
            	            	var msid = msidNode.childNodes[0].nodeValue
	            	            if (msid) {
	            	            	dstDoc.forms['jwicform'].elements['_msid'].value = msid;
	            	            }
            	            }
            	            jWic().fireAction("", "redraw", "", true);
            	        } else           	        
            	        if (node.nodeName == "controlcontent") {
	            	        objCount++;
            	            var nodeid = "ctrl_" + node.getAttribute("id");
            	            var ctrlObject = dstDoc.getElementById(nodeid);
            	            if (ctrlObject) {
            	            	log("Updating control '" + nodeid + "'.");
            	            	try {
	            	                jWic_ajaxUpdate(ctrlObject, node.childNodes[0].nodeValue);
	            	            } catch (e) {
	            	            	alert("JavaScript errors in the code: " + e.message 
	            	            		+ "\nID:" + nodeid);
	            	            }
            	            } else {
            	            	log("WARNING: Cant find control '" + nodeid + "' to update.");
            	            }
            	        }
            	    }
            	} else {
            	    // Can occure if non valid utf-8 data has been rendererd or if a renderer
            	    // did write directly into the stream, during an ajax update.
            	    if (debugMode) {
            	    	alert("XML document seems invalid. Page will be refreshed.");
            	    }
					_ajaxMode = false;
					_ajaxProcessing = false;
                    jWic().fireAction("", "refresh", "", true);
                    return;
            	}
        	} else {
        	    alert("There was a problem retrieving the XML data (" + req.statusText + ")");
        	}
        	var duration = (new Date()).getTime() - _starttime;
    	    window.status = "Page Updated (" + objCount + " object(s), time: " + duration + " ms)";
    	    if (_blocked) {
   				ajaxRemoveBlockClicks();
   			}
        	_ajaxProcessing = false;
    	}
	}
	
	/**
	 * Invoked by ajaxProcessAnswer when a content of a control is inserted.
	 * Callback javascript is executed for updated control.
	 */
	function jWic_ajaxUpdate(control, html) {
		_ajaxRenderControls++;
		var listener = jWic_ajaxGetUpdateListener(control.id);
		if (listener) {
			// call all listeners
			var event = new AjaxUpdateEvent(control, html);
			try {
				listener.callAll(event);
			} catch (e) {
				alert("Error executing updateListener: " + e.method + ", listener: " + listener);
			}
		} else {
			log("direct update");
			jWic_ajaxUpdateHTML(control, html);
		}
	}
	/**
	 * Executes the real Ajax update to the control.
	 * Exported as jWic().ajaxUpdate(...) that a listener
	 * must call to finish the update.
	 */
	function jWic_ajaxUpdateHTML(control, html) {
		log("updateHTML for " + control.id);
		// cut out the surrounding empty span tags as they must
		// be ignored during an ajax update.
		if (control.id == "ctrl_root/control") {
			// root control to be updated, add root/control span
			html = "<span id=\"ctrl_root/control\">" + html + "</span>";
		} else {
			html = html.substring(6, html.length - 7);
		}
		control.innerHTML = ""; // fixes strange document size behaviour in IE6 with framesets
		control.outerHTML = html;
		
		// execute the script if its an IE browser
		if (ie) {
			var scripts = findScript(html);
			if (scripts && scripts.length > 0) {
				log("Scripts found in update: " + scripts.length);
				for (var i = 0; i < scripts.length; i++) {
					try {
						var dummy = eval(scripts[i]);
					} catch(e) {
						log("Error executing script in update: " + e);
					}
				}
			}
		}
		_ajaxRenderControls--;
	}
	/**
	 * Timeout to check if all controls finished its ajaxUpdate processing
	 * and then hide the blockClicks element.
	 */ 
	function ajaxRemoveBlockClicks() {
		if (_ajaxRenderControls == 0) {
			blockClicks(document, true);
		} else {
			window.setTimeout("ajaxRemoveBlockClicks()", _waitAjaxUpdate);
		}
	}
	
	/**
	 * MouseEventClass for document event handling
	 */
	function MouseEventClass(e) {
		if (!e) var e = window.event;
		if (e.button == 2 && e.type == "click") {
			// ignore click event for contextmenu event (FF2 fix)
			return false;
		}
		var dstDoc = jWic().dstDoc;
		var type = e.type;

		var field = dstDoc.forms['jwicform'].elements['__mouseevent'];
		if (field) {
			field.value = type + ";" + e.clientX + ";" + e.clientY;
		}
		if (jWic().dstDocEvent[type]) {
			return jWic().dstDocEvent[type](e);
		}
		return true;
	};
	/**
	 * KeydownEventClass for document event handling
	 */
	function KeyboardEventClass(e) {
		if (!e) var e = window.event;
		var type = e.type;
		var dstDoc = jWic().dstDoc;
		var field = dstDoc.forms['jwicform'].elements['__mouseevent'];
		if (field) {
			field.value = "";
		}
		if (jWic().dstDocEvent[type]) {
			return jWic().dstDocEvent[type](e);
		}
		return true;
	};

	/**
	 * Init the jWic object.
	 */
	function jWic_Init(theDocument) {
		if (typeof(_contextPath) == 'undefined') {
			_contextPath = "..";
		}
		this.dstDoc = theDocument;

		this.ddatalist = "";
		try {
			this.ddatalist = new jWicDDataList();
			this.ddatalist.init(this.dstDoc);
		} catch (e) {}
		
		this.calendar = "";
		this._submitListeners = new Array();
		try {	
			this.calendar = new jWicCalendar();
			this.calendar.setImageDir(_contextPath + "/jwic/calendar/");
			this.calendar.init(this.dstDoc)
		} catch (e) {}

		// init mouse events
		this.dstDocEvent = new Object();
		this.dstDocEvent["click"] = theDocument.onclick;
		this.dstDocEvent["contextmenu"] = theDocument.oncontextmenu;
		theDocument.onclick = MouseEventClass;
		theDocument.oncontextmenu = MouseEventClass;
		
		// key down event to clear mouse event data
		this.dstDocEvent["keydown"] = theDocument.onkeydown;
		theDocument.onkeydown = KeyboardEventClass;
	}
	
	/**
	 * Fix the scrolling of the ScrollableContainer.
	 */
	function jWic_fixScrolling(ctrlId, paneId) {
		if (typeof paneId == "undefined") {
			paneId = "div_" + ctrlId;
		}
		var pane = this.dstDoc.getElementById(paneId);
		if (pane) {
			var top = this.dstDoc.forms['jwicform'].elements['fld_' + ctrlId + '.top'].value;
			var left = this.dstDoc.forms['jwicform'].elements['fld_' + ctrlId + '.left'].value;            	    		
			pane.scrollTop = top;
			pane.scrollLeft = left;
			log("Set scrolling for '" + paneId + "' to " + top + ", " + left);
		} else {
			log("WARNING: Can't fix scrolling for " + paneId);
		}
	}

	function jWic_forceFocus(elementId) {
		var element = this.dstDoc.forms['jwicform'].elements[elementId];
		if (element) {
			try {
				log("force focus for element " + elementId);
				element.focus();
			} catch(e) {
				log("Error setting focus for element " + elementId + ": " + e);
			}
		} else {
			log("Can not send focus: Element " + elementId + " not found.");
		}
	}
	
	/**
	 * Add a listener that is invoked before the page is submited. The
	 * listener must be a function that is invoked without arguments.
	 */
	function jWic_addSubmitListener (id, listener) {
		if (typeof(this._submitListeners[id]) == 'undefined') {
			log("Adding submitListener with ID " + id);
			this._submitListeners[id] = listener;
		} else {
			log("SubmitListener with id '" + id + "' is already registered -> skiped");
		}
	}
	
	/**
	 * jWic constrictor
	 */
	function jWicBase() {
		this.forceFocus = jWic_forceFocus;
		this.sendResourceRequest = jWic_sendResourceRequest;
		this.fireAction = jWic_FireAction;
		this.ajaxSubmit = jWic_ajaxSubmit;
		this.ajaxProcessAnswer = jWic_ajaxProcessAnswer;
		this.fixScrolling = jWic_fixScrolling;
		this.ajaxAddUpdateListener = jWic_ajaxAddUpdateListener;
		this.init = jWic_Init;
		// array of submit listeners
		this.addSubmitListener = jWic_addSubmitListener;
	}

	/**
	 * Access to the jWic JavaScript objects.
	 */
	function jWic() {
		return jwicObj;
	}

	/**
	 * This function must be called from within the page file inside the BODY tag!
	 */
	function jWicInit() {
		try {
			jwicObj = new jWicBase();
			jwicObj.init(document);
		} catch (e) {
			// reload parent, for solving strange IE6 SP2 reload problem
			// infinity loop if jWicBase() contains errors ...
			parent.location.replace(parent.location.href);
		}
	}
