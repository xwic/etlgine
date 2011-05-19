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
 * ------------------------------------------------------------------------
 * This file contains some functions to enhance compatibility with previous
 * jWic versions.
 */

	var debugMode = false;
	var jwicObj = "";
    var _logCount = 0;
    var _ajaxProcessing = false;

	function log(message) {
		if (debugMode) {
			var elem = document.forms['jwicform'].elements['_debugLog'];
			elem.value = _logCount + ":" + message + "\n" + elem.value;
			_logCount++;
		}
	}

	
	function WindowSize() {
		return JWicInternal.getWindowSize();
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
		
		if (trueSubmit) {
			$('jwicform').submit();
		} else { 
			JWic.fireAction(srcCtrl, sAction, sParam);
		}
	}

	
	/**
	 * Fix the scrolling of the ScrollableContainer.
	 */
	function jWic_fixScrolling(ctrlId, paneId) {
		JWic.restoreScrolling(ctrlId, paneId);
	}
	
	/**
	 * jWic constrictor
	 */
	function jWicBase() {
		this.fireAction = jWic_FireAction;
		this.fixScrolling = jWic_fixScrolling;
		// array of submit listeners
		// this.addSubmitListener = jWic_addSubmitListener;
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
			
			//jwicObj.init(document);
		} catch (e) {
			// reload parent, for solving strange IE6 SP2 reload problem
			// infinity loop if jWicBase() contains errors ...
			parent.location.replace(parent.location.href);
		}
	}

	jwicObj = new jWicBase();
	if (jWicCalendar) {
		jwicObj.calendar = new jWicCalendar();
	}
	
	
	// old functions
	
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
	 * Creates a string that contains all field data on the form which can be used to
	 * send a POST request to the server using XMLHttpRequest mechanism.
	 */
	function jWic_createContent(dstDoc) {
	
	    var content = "";
	    var logElem = false;
	    
	    
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
	 * MouseEventClass for document event handling
	 */
	function MouseEventClass(e) {
		var type = "move";
		if (document.forms['jwicform']) {
			var field = document.forms['jwicform'].elements['__mouseevent'];
			if (field) {
				field.value = type + ";" + Event.pointerX(e) + ";" + Event.pointerY(e);
			}
		}
		return true;
	}
	
	Event.observe(document, "mousemove", MouseEventClass);
	

	