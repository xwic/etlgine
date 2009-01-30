/*
 * Functions to adapt the Prototype Window Class library with jWic (ecolib) controls.
 * This library must be included in the main page file AFTER the original window.js file.
 *
 */
 
var PWCAdapter = {	
	
	openPrototypeWindow : function(serverRefId, winName, winTitle, height, width, top, left, isModal, cssClass) {
		//alert("open:" + serverRefId + ", " + winName+ ", " + winTitle+ ", " + height+ ", " + width+ ", " + top+ ", " + left+ ", " + isModal+ ", " + cssClass);
		if (cssClass == undefined)
			cssClass = "mac_os_x";
			
		if (height == undefined || height == 0) 
			height = 100;

		if (width == undefined || width == 0) 
			width = 200;

		if (left == undefined) 
			left = -1;

		if (top == undefined) 
			top = -1;
		
		try {
			if ( Windows.getWindow(winName) != undefined ) {
				var oldWin = Windows.getWindow(winName);
				Windows.unregister(oldWin);
			}
			var container = document.getElementById(winName + "_container");
			var contentDiv = $(winName + "_div");
			var cHtml = contentDiv.innerHTML;
			contentDiv.innerHTML = '&nbsp;';

			var wWin = new Window(winName, {className: cssClass, zIndex:100, width: width, height: height, 
											left: left, top: top, title: winTitle, parent: container});

			wWin.keepMultiModalWindow= true;
			wWin.getContent().innerHTML = cHtml; 
			wWin.setDestroyOnClose();
			if (left == -1 || top == -1) {
				wWin.showCenter(isModal);
			} else {
				wWin.show(isModal);
			}
			wWin.serverid = serverRefId;
	
		} catch (exception) {
			alert(exception.message);
		}
	}

};

PWCAdapter.WindowObserver = Class.create();


PWCAdapter.WindowObserver.prototype = {
	initialize : function() {
	},
	onMove : function(eventName, win) {
		// build fieldname from serverid
		var idPrefix = "fld_" + win.serverid + ".";
		var location = win.getLocation();
		$(idPrefix + "top").value = parseInt(location.top);
		$(idPrefix + "left").value = parseInt(location.left);
	},
	onResize : function(eventName, win) {
		// build fieldname from serverid
		var idPrefix = "fld_" + win.serverid + ".";
		var size = win.getSize();
		$(idPrefix + "height").value = size.height;
		$(idPrefix + "width").value = size.width;
	},
	onShow : function(eventName, win) {
		// show SELECT tags inside the window that have been hidden.
		// this must be done after the window has been displayed to work correctly.
		if (Prototype.Browser.IE) {
			var tags = win.getContent().getElementsByTagName("select");
			for (var i = 0; i < tags.length; i++) {
				var element = tags[i];
		        if (WindowUtilities.isDefined(element.oldVisibility)) {
		          // Why?? Ask IE
		          try {
		            element.style.visibility = element.oldVisibility;
		          } catch(e) {
		            element.style.visibility = "visible";
		          }
		          element.oldVisibility = null;
		        }
		        else {
		        	if (element.style.visibility) {
		              element.style.visibility = "visible";
		            }
		        }
			}
		}
	}, 
	onClose : function(eventName, win) {
		jWic().fireAction(win.serverid, "close", '');
	}
};


if (Windows == undefined) {
	alert("The Windows object is unknown. Please check if the windows.js library is included in the page file BEFORE the window_adapter.js file.");
} else {
	Windows.addObserver(new PWCAdapter.WindowObserver());
}
