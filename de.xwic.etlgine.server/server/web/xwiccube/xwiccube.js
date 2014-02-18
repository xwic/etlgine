var Cube = (function(util){
	var defineProp, defineObservable,tmpl
		map = util.map,
		reduce = util.reduce;
	tmpl = function tmpl(control, templateId, data){
		var prop,
			template = control.find(templateId).clone();
		template.removeAttr("style").removeAttr('id');
		if(data){
			for(prop in data){
				if(data.hasOwnProperty(prop)){
					template.find('#'+prop).text(data[prop]);
				}
			}
		}
		return template;
	};
	defineProp = function(object, propName){
		var val;
		object[propName] = function(value){
			var oldVal;
			if(value == null){//its a getter if null
				return val; 
			}//else its a setter
			oldVal = val;
			val = value;
			if(value !== oldVal && 'function' === typeof object['firePropertyChanged']){
				object.firePropertyChanged(propName,val,oldVal);//fire event listeners if applicable
			}
			return object;
		};	
		return object[propName]; 
	};
	defineObservable = function(object,observableName){
		var listeners = {},
			eventName = observableName.charAt(0).toUpperCase() + observableName.substr(1); //capitalize
		
		object['fire'+eventName] = function(){
			var args = arguments,
				l =listeners[observableName] || []; 
			map(l,function(o){
				o.apply(object,args);
			});
			return object;
		};
		object.on = object.on || function(what,listener){
			listeners[what] = listeners[what] || [];
			listeners[what].push(listener);
			return object;
		}
		object.unbind = object.unbind || function(what,listener){
			if(listener == null){
				listeners[what] = null;
				return object;
			}
			var index = listeners[what].indexOf(listener);
			listeners[what].splice(index,1);
			return object;
		};
	};
	return {
		defineObservable : defineObservable,
		defineProp : defineProp,
		tmpl : tmpl
	};
}(JWic.util));

	var currentOpenCtrlId = null;
	var xcube_TreeReq = null;
	var insideClick = false;
	/**
	 * Open the tree for selection.
	 * @param ctrlId
	 * @return
	 */
	
	/**
	 * Process server response.
	 * @return
	 */
	function xcube_processResponse(response) {
		
		if (currentOpenCtrlId != null) {
			console.warn('process response')
				var resultString = response.responseText;
				try {
					var result = JSON.parse(resultString);
					var elm = document.getElementById("xcubeLeafselBox_" + currentOpenCtrlId);
					elm.xwc_data = result;
					
					var key ="/" + result.selection;
					var idx = key.lastIndexOf('/');
					while (idx != -1) {
						elm.xwc_expanded[key] = "+";
						key = key.substring(0, idx);
						var idx = key.lastIndexOf('/');
					}
					elm.xwc_expanded["/" + result.dimension] = "+"; // initialy expand root
					xcube_renderTree(elm);
				} catch (e) {
//					alert("Error parsing data:" + e);
				}
			
		}
	}
	
	function xcube_renderTree(elm) {
		
		
		var data = elm.xwc_data;
		var expandedKeys = elm.xwc_expanded;
		var code = "<table cellspacing=0 cellpadding=0 class=xcube-treetbl>";
		var selMode = elm.attributes["xwcSelectionMode"].value;
			
		
		var lastTrack = new Array();
		lastTrack[0] = true;
		// root element (all)
		code += "<tr><td><table cellspacing=0 cellpadding=0><tr><td>";
		code += "<td";
		if (data.selection == "") {
			code += " class=\"selected\"";
		}
		code += "><a href=\"#\" onClick=\"";
		if (selMode != "leaf") {
			code += "xcube_selectElement('/')";
		}
		code += ";return false;\">"
		code += "- All -";
		code += "</a></td></tr></table></td></tr>";

		
		code += xcube_renderTreeChilds(0, expandedKeys, lastTrack, "/" + data.selection, "", data.elements, selMode);
		
		code += "</table>";
		
		
		elm.innerHTML = code;
				
	}
	
	function xcube_renderTreeChilds(lvl, expandedKeys, lastTrack, selection, path, elements, selMode) {
		
		var imgPath = _contextPath + "/xwiccube/images/";
		
		var code = "";
		for (var i = 0; i < elements.length; i++) {
			var de = elements[i];
			var isLeaf = de.elements.length == 0;
			var isLast = (i + 1) >= elements.length;
			var myPath = path + "/" + de.key;
			var isExpanded = expandedKeys[myPath] == "+";
			
			var action = "";
			code += "<tr><td><table cellspacing=0 cellpadding=0><tr>";
			// indention
			for (var idl = 0; idl < lvl; idl++) {
				var imgName = lastTrack[idl] ? "blank.png" : "I.png";
				code += "<td width=19><img src=\"" + imgPath + imgName + "\" width=19 height=16></td>";
			}
			var imgName = (isLast ? "L" : "T");
			if (!isLeaf) {
				action = (isExpanded ? "-" : "+");
				imgName += (isExpanded ? "minus" : "plus");
			}
			code += "<td width=19";
			if (action != "") {
				code +=" class=\"xcube_tree_actionnode\" onclick=\"xcube_treeNodeToggle('" + myPath + "', '" + action + "')\")";
			}
			code += "><img src=\"" + imgPath + "/" + imgName + ".png\" width=19 height=16>";
			code += "</td>";
			
			code += "<td";
			if (myPath == selection) {
				code += " class=\"selected\"";
			}
			code += "><a href=\"#\" onClick=\"";
			if (selMode == "leaf" && !isLeaf) {
				code += "xcube_treeNodeToggle('" + myPath + "', '" + action + "')\")";
			} else {
				code += "xcube_selectElement('" + myPath + "')";
			}
			code += ";return false;\">"
			if (de.title && de.title != "") {
				code += de.title;
			} else {
				code += de.key;
			}
			code += "</a></td></tr></table></td></tr>";
			lastTrack[lvl] = isLast;
			if (!isLeaf && isExpanded) {
				code += xcube_renderTreeChilds(lvl + 1, expandedKeys, lastTrack, selection, myPath, de.elements, selMode);
			}
		}
		
		return code;
	}
	
	/**
	 * Fire selection event.
	 * @param elmId
	 * @return
	 */
	function xcube_selectElement(elmId) {
		JWic.fireAction(currentOpenCtrlId, 'selection', elmId.substring(1));
		xcube_closeTree();
	}
	
	/**
	 * Toggle tree node.
	 * @param key
	 * @param state
	 * @return
	 */
	function xcube_treeNodeToggle(key, state) {
		
		if (currentOpenCtrlId != null) {
			var elm = document.getElementById("xcubeLeafselBox_" + currentOpenCtrlId);
			if (elm) {
				elm.xwc_expanded[key] = state;
				xcube_renderTree(elm); // rerender
			}
		}
	}
	
	/**
	 * close the currently open tree.
	 * @return
	 */
	function xcube_closeTree() {
		if (currentOpenCtrlId != null) {
			var elm = document.getElementById("xcubeLeafselBox_" + currentOpenCtrlId);
			if (elm) {
				elm.style.display = "none";
			}
			currentOpenCtrlId = null;
		}
	}
	
	function xcube_alignElement(elmSrc, elmAlignTo) {
		
		var fixedX = -1;
		var fixedY = -1;
		var	leftpos=0
		var	toppos=0
		var fixedWidth = 200;
		var fixedHeight = 300;
		var aTag = elmAlignTo
		do {
			aTag = aTag.offsetParent;
			leftpos	+= aTag.offsetLeft;
			toppos += aTag.offsetTop;
		} while(aTag.tagName!="BODY");

		var x = fixedX==-1 ? elmAlignTo.offsetLeft	+ leftpos :	fixedX==-2 ? elmAlignTo.offsetLeft + leftpos - fixedWidth : fixedX
		if (x < 1) { x = 1 }
		elmSrc.style.left = x + "px";
		var y = fixedY==-1 ? elmAlignTo.offsetTop + toppos + elmAlignTo.offsetHeight + 2 : fixedY==-2 ? elmAlignTo.offsetTop + toppos + 2 - fixedHeight : fixedY
		if (y < 1) { y = 1 }
		elmSrc.style.top = y + "px";

	}