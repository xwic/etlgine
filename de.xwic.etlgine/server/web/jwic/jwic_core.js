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
 * -------------------------------------------------------------------------
 * This file contains the core javascript functions for the client server
 * communication. It is a replacement for the previous jwic.js file.
 */

/**
 * JWic defines the public API for JWic server/client communication.
 */
var JWic = {
	version :'4.0.2',
	debugMode : false,
	_logCount : 0,
	
	/**
	 * Indicates if the client is currently sending or waiting for an action
	 * request.
	 */
	isProcessing :false,
	
	commandQueue : new Array(),

	/**
	 * The time in milliseconds before the please wait message appears.
	 */
	pleaseWaitDelayTime :500,
	
	/**
	 * Send an action request to the server, including the form values. The
	 * result contains the modified controls, which will then be updated on the
	 * page.
	 */
	fireAction : function(senderControl, actionName, actionParameter) {
		
		JWic.commandQueue.push({
			senderControl : senderControl,
			actionName : actionName,
			actionParameter : actionParameter
		});
		if (!JWic.isProcessing) {
			JWic._processNextAction()
		}
	},
	
	_processNextAction : function() {
		
		if (JWic.commandQueue.length == 0 || JWic.isProcessing) {
			return;
		}
		
		var cmd = JWic.commandQueue[0]; // take first in
		JWic.commandQueue.splice(0, 1);
		
		JWic.isProcessing = true;
		window.setTimeout("JWicInternal.showClickBlocker(true)",
				JWic.pleaseWaitDelayTime);

		var jwicform = $('jwicform');
		jwicform.elements['__ctrlid'].value = cmd.senderControl;
		jwicform.elements['__action'].value = cmd.actionName;
		jwicform.elements['__acpara'].value = cmd.actionParameter;

		// collect system informations.
		var sysinfoXY = JWicInternal.getWindowSize();
		var sysinfo = sysinfoXY[0] + ";" + sysinfoXY[1] + ";" + sysinfoXY[2]
				+ ";" + sysinfoXY[3];
		if (jwicform.elements['__sysinfo']) {
			jwicform.elements['__sysinfo'].value = sysinfo;
		}

		JWicInternal.beforeRequestCallbacks.each(function (item) {
			item.value(item.key);
		});
		
		// check for file attachments
	    for (var i = 0; i < jwicform.elements.length; i++) {
	        var element = jwicform.elements[i];
	        if (element.type == "file" && element.value != "") {
	        	// if a file-upload control is on the page that has a file assigned,
	        	// a real submit is required to transfer the file to the server.
	        	// make sure that the encoding type is multipart, before the data is submitted.
	        	jwicform.fire("beforeSubmit");
	        	jwicform.encoding = 'multipart/form-data';
	        	jwicform.submit();
	        	return;
	        }
	    }

	    jwicform.fire("beforeSerialization");
		var paramData = jwicform.serialize(true);
		jwicform.fire("afterSerialization");

		paramData['_ajaxreq'] = '1';
		paramData['_format'] = 'JSON';

		var url = document.location.href;
		var idx = url.indexOf('#');
		if (idx != -1) {
			url = url.substring(0, idx);
		}
		new Ajax.Request(url, {
			method :'post',
			parameters :paramData,
			onSuccess : function(response) {
				JWicInternal.handleResponse(response);
			},
			onException : function(response, error) {
				alert(
						"An exception has occured processing the server response: "
								+ error, "Error Notification.");
				JWicInternal.endRequest();
			}
		});
	},
	
	/**
	 * Opens a request to a specific control. The control must implement the
	 * IResourceControl interface to reply to the request.
	 */
	resourceRequest: function(controlId, callBack, parameter) {
		
		var paramData = new Hash();
		if (Object.isHash(parameter)) {
			paramData.update(parameter);
		} else {
			paramData.set('parameter', parameter);
		}
		paramData.set('controlId', controlId);
		paramData.set('_resreq', '1');
		paramData.set('_msid', document.forms['jwicform'].elements['_msid'].value);

		var url = document.location.href;
		var idx = url.indexOf('#');
		if (idx != -1) {
			url = url.substring(0, idx);
		}
		new Ajax.Request(url, {
			method :'post',
			parameters :paramData,
			onSuccess : callBack,
			onFailure : function(response) { alert('resource request failed:' + response.status + " " + response.statusText) },
			onException : function(response) { alert('resource request failed (exception):' + response.status + " " + response.statusText) }
		});
		
	},

	/**
	 * Restores the scrolling position of a scrollable container. This is
	 * required if the container was updated and the old position must be
	 * restored.
	 */
	restoreScrolling : function(ctrlId, paneId) {
		if (typeof paneId == "undefined") {
			paneId = "div_" + ctrlId;
		}
		var pane = $(paneId);
		if (pane) {
			var form = $('jwicform');
			var top = form.elements['fld_' + ctrlId + '.top'].value;
			var left = form.elements['fld_' + ctrlId + '.left'].value;
			pane.scrollTop = top;
			pane.scrollLeft = left;
			// log("Set scrolling for '" + paneId + "' to " + top + ", " +
			// left);
		} else {
			// log("WARNING: Can't fix scrolling for " + paneId);
		}

	},
	/**
	 * Show a dialog with a message. Encapsulates the PWC library functions.
	 */
	alert : function(message, title) {
		Dialog.alert(message, {
			className :"alphacube",
			options :"",
			title :title ? title : ""
		});
	},
	
	/**
	 * Add a new callback.
	 */
	addBeforeRequestCallback : function(controlId, callback) {
		JWicInternal.beforeRequestCallbacks.set(controlId, callback);
	},
	
	log : function(message) {
		if (JWic.debugMode) {
			var elem = document.forms['jwicform'].elements['_debugLog'];
			elem.value = JWic._logCount + ":" + message + "\n" + elem.value;
			JWic._logCount++;
		}
	}


};

JWic.util = {
		clearSelection : function() {
			if(document.selection && document.selection.empty) {
				document.selection.empty();
			} else if (window.getSelection) {
				var sel = window.getSelection();
				if(sel && sel.removeAllRanges) {
					sel.removeAllRanges() ;
				}
			}
		},
		
		removeElement: function(val, removeMe, seperator) {
			if (!seperator) seperator = ";";
			var x = val.split(seperator);
			var n = "";
			x.each(function(elm) {
				if (elm != removeMe) {
					if (n.length != 0) {
						n += seperator;
					}
					n += elm;
				}
			})
			return n;
		}
}

/**
 * Defines the internal (private) API.
 */
var JWicInternal = {

	lastResizeTime :0,

	/**
	 * List which contains all destroy functions for the existing controls.
	 */
	destroyList :new Array(),

	/**
	 * List which contains an optional callback function per control that is
	 * invoked before a request is send to the server.
	 */
	beforeRequestCallbacks : new Hash(),

	/**
	 * Handle the response from an fireAction request.
	 */
	handleResponse : function(ajaxResponse) {
		var jwicform = $('jwicform');

		if (ajaxResponse.status == 0 && ajaxResponse.responseText == "") {
			alert("The server did not respond to the request. Please check your network connectivity and try again.");
			jwicform.elements['__ctrlid'].value = '';
			jwicform.elements['__action'].value = 'refresh';
			jwicform.submit();
			return;
		}
		var response = ajaxResponse.responseText.evalJSON(true);
		if (response.exception) {
			alert("A server side exception occured: " + response.exception + "\n"
					+ "Hit ok to refresh.");
			jwicform.elements['__ctrlid'].value = '';
			jwicform.elements['__action'].value = 'refresh';
			jwicform.submit();
			return;
		}

		if (typeof (response.ticket) != 'undefined') {
			// update ticket number
			jwicform.elements['__ticket'].value = response.ticket;

			if (response.requireRedraw) {
				// normaly submit the whole page, but clear action before
				jwicform.elements['__ctrlid'].value = '';
				jwicform.elements['__action'].value = 'refresh';
				jwicform.submit();
				return;
			}

			if (response.updateables) {
				response.updateables.each( function(elm) {
					var control = $("ctrl_" + elm.key);
					var scripts = new Array();
					if (elm.scripts) {
						for ( var i = 0; i < elm.scripts.length; i++) {
							scripts.push({
								key: elm.scripts[i].controlId, 
								script: elm.scripts[i].script.evalJSON(false)
							});
						}
					}

					if (control) {
						// call beforeUpdate in scripts
						for ( var i = 0; i < scripts.length; i++) {
							if (scripts[i].script.beforeUpdate) {
								scripts[i].script.beforeUpdate(control);
							}
						}
						var doReplace = true;
						for ( var i = 0; i < scripts.length; i++) {
							if (scripts[i].script.doUpdate) {
								if (scripts[i].script.doUpdate(control)) {
									doReplace = false;
									break;
								}
							}
						}
						if (doReplace) { // replace the DOM element with the
											// rendered snippit received

							// call destroy handler and remove them
							var deLst = JWicInternal.destroyList;
							for ( var i = deLst.length - 1; i >= 0; i--) {
								if (deLst[i] && (deLst[i].key == elm.key || deLst[i].key.startsWith(elm.key + "."))) {
									JWic.log("Destroy: " + deLst[i].key + " because of " + elm.key);
									deLst[i].destroy(control);
									deLst.splice(i, 1);
								}
							}
							// remove any beforeUpdateCallbacks
							
							var allKeys = JWicInternal.beforeRequestCallbacks.keys().clone();
							allKeys.each(function(key) {
								if (key.startsWith(elm.key)) {
									JWicInternal.beforeRequestCallbacks.unset(key);
								}
							});
							
							// register destroy handler
							for ( var i = 0; i < scripts.length; i++) {
								if (scripts[i].script.destroy) {
									deLst.push( {
										key :scripts[i].key,
										destroy :scripts[i].script.destroy
									});
								}
							}

							control.replace(elm.html);

							// call afterUpdate in scripts
							for ( var i = 0; i < scripts.length; i++) {
								if (scripts[i].script.afterUpdate) {
									scripts[i].script.afterUpdate(control);
								}
							}
						}
					} else {
						// alert("A control with the ID '" + elm.key + "' does
						// not exist on the page and can not be updated.");
					}
				});
			}
			this.endRequest();
		}
	},

	/**
	 * Clears all inProcess flags and hides the please wait screen, if it is
	 * visible.
	 */
	endRequest : function() {
		JWic.isProcessing = false;
		if (JWic.commandQueue.length > 0) {
			JWic._processNextAction()
		}

		JWicInternal.showClickBlocker(false);
	},
	/**
	 * Shows or hides the block clicker, so the user cannot click other link
	 * during the processing of the current click.
	 */
	showClickBlocker : function(showBlocker) {

		if (showBlocker && !JWic.isProcessing) {
			return;
		}

		var removeBlock = !showBlocker;
		// log("blockClicks (" + (removeBlock ? "remove" : "initiate") + ")");
		var elem = $("click_blocker");
		_blocked = !removeBlock;
		if (elem) {
			if (!removeBlock) {
				var sysinfoXY = this.getWindowSize();
				var msg = $("click_blocker_message");
				var bodyHeight = (msg ? document.body.scrollHeight
						: sysinfoXY[1]) - 5;
				var bodyWidth = (msg ? document.body.scrollWidth : sysinfoXY[0]) - 5;
				elem.style.top = 0;
				elem.style.height = bodyHeight + 'px';
				elem.style.left = 0;
				elem.style.width = bodyWidth + 'px';
				elem.style.backgroundImage = 'url(none)';
				if (msg) {
					msg.style.position = "absolute";
					if (msg.parentNode.align == "right") {
						// buttom right place
						msg.style.top = (sysinfoXY[1] + sysinfoXY[3]
								- parseInt(msg.style.height) - 20) + 'px';
						msg.style.left = (sysinfoXY[0] + sysinfoXY[2]
								- parseInt(msg.style.width) - 20) + 'px';
					} else {
						// center message
						msg.style.top = ((sysinfoXY[1] - parseInt(msg.style.height))
								/ 2 + document.body.scrollTop - 5) + 'px';
						msg.style.left = ((sysinfoXY[0] - parseInt(msg.style.width))
								/ 2 + document.body.scrollLeft - 5) + 'px';
					}
				}
			}
			elem.style.visibility = removeBlock ? "hidden" : "visible";

			// disable 'select' elements. This is required since these elements
			// are on top of all other elements, including the 'click-blocker'
			// (in most browsers).

			var elemID = "SELECT";
			var selects = document.getElementsByTagName(elemID);
			for (i = 0; i < selects.length; i++) {
				obj = selects[i];
				if (obj) {
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
	},

	/**
	 * Returns informations about the window size and scrolling position.
	 */
	getWindowSize : function() {
		var myWidth = 0, myHeight = 0, scrollTop, scrollLeft;
		var type;
		if (typeof (window.innerWidth) == 'number') {
			// Non-IE
		myWidth = window.innerWidth;
		myHeight = window.innerHeight;
		scrollLeft = window.pageXOffset;
		scrollTop = window.pageYOffset;
	} else if (document.documentElement
			&& (document.documentElement.clientWidth || document.documentElement.clientHeight)) {
		// IE 6+ in 'standards compliant mode'
		myWidth = document.documentElement.clientWidth;
		myHeight = document.documentElement.clientHeight;
		scrollLeft = document.documentElement.scrollLeft;
		scrollTop = document.documentElement.scrollTop;
	} else if (document.body
			&& (document.body.clientWidth || document.body.clientHeight)) {
		// IE 4 compatible
		myWidth = document.body.clientWidth;
		myHeight = document.body.clientHeight;
		scrollLeft = document.body.scrollLeft;
		scrollTop = document.body.scrollTop;
	}
	return [ myWidth, myHeight, scrollLeft, scrollTop ];
	},
	
	/**
	 * Trigger a refresh action if the window size was changed.
	 */
	winResizeHandler : function(evt) {
		if (!evt)
			evt = window.event;
		JWicInternal.lastResizeTime = new Date().getTime();
		window.setTimeout("JWicInternal.winResizeDoPost()", 200); // make sure to wait 200 ms before sending the update (IE specific)
	},
	/**
	 * Fires the refresh event.
	 */
	winResizeDoPost : function() {
		if ((new Date().getTime() - JWicInternal.lastResizeTime) >= 200) {
			JWicInternal.lastResizeTime = new Date().getTime();
			JWic.fireAction('', 'refresh', '');
		}
	}
};

window.onresize = JWicInternal.winResizeHandler;