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
 * Created on 2005-05-24
 */

/**
 * This library provides functions to work with the different browsers 
 * available and use them in a common way.
 */

    var ie = /MSIE/.test(navigator.userAgent);
    var moz = !ie && navigator.product == "Gecko";

    
    function emulateHTMLModel() {
    
    	HTMLElement.prototype.__defineSetter__("outerHTML", function (html) {
    	   var range = this.ownerDocument.createRange();
    	   range.setStartBefore(this);
    	   var fragment = range.createContextualFragment(html);
    	   this.parentNode.replaceChild(fragment, this);
    	   return html;
    	});
    }
    
    function emulateAttachEvent() {
    	window.attachEvent = function(type, func) {
    		if (type.substring(0, 2) == "on") {
    			type = type.substring(2);
    		}
    		window.addEventListener(type, func, false);
    	}
    }

    if (moz) {
	    emulateHTMLModel();
	    emulateAttachEvent();
	}
    