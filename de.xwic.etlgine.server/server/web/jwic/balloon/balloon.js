
var BalloonInstance = new BalloonClass();

function balloon() {
	return BalloonInstance;
}

function BalloonClass() {
	
	function BalloonEventClass(e) {
		var src = getSource(e);
		if (!src) {
			return;
		}
		e = src["event"];
		// don't close yourself!
		var type = src["event"].type;
		var balloonID = findBalloonID(src["source"]);
		if (balloonID) {
			if (docEvent[type]) docEvent[type](e);
			return true;
		}
		
		if (src["eventID"]) {
			showBalloon(src);
			return false;
		}
		closeBalloon(src);
		if (docEvent[type]) docEvent[type](e);
	};
	
	this.registerOn = registerOn;
	this.show = show;
	this.hide = hide;

	var registered = new Object();
	var visible = new Object();
	var docEvent = new Object();
	docEvent["click"] = document.onclick;
	docEvent["contextmenu"] = document.oncontextmenu;
	
	function registerOn(balloonID, controlID, eventID) {
		var onEvent = registered[eventID];
		if (!onEvent) {
			onEvent = new Object();
			registered[eventID] = onEvent;
		}
		var onControl = onEvent[controlID];
		if (!onControl) {
			onControl = new Object();
			onEvent[controlID] = onControl;
		}
		onControl["balloonID"] = balloonID;
	}

	function position(balloon, balloonID, x, y, cssClass, ws) {
		var type, top, left;
		var height = balloon.clientHeight;
		var width = balloon.clientWidth;
		var top = y - height + ws[3];
		//alert("top=" + top + ", scroll=" + ws[3] + ", y=" + y + ", h=" + balloon.clientHeight);
		left = x;
		if (top >= ws[3]) {
			type = "b";
		} else {
			top = y + ws[3]
			type = "t";
		}
		if (left + width <= ws[0] + ws[2]) {
			type += "l";
		} else {
			left = x - width;
			type += "r"; 
		}
		balloon.style.top = top + "px";
		balloon.style.left = left + "px";
		visible[balloonID] = balloonID;
		balloon.className = cssClass + type;
		
		// check top position when pointer is at bottom (dynamic height changes effect clientWidth on display: IE6.0, FF2)
		if (height != balloon.clientHeight || width != balloon.clientWidth) {
			position(balloon, balloonID, x, y, cssClass, ws);
		}
	}

	function show(balloonID, x, y, cssClass) {
		var balloon = document.getElementById("balloon_" + balloonID);
		if (balloon) {
			var ws = JWic.getWindowSize();
			position(balloon, balloonID, x, y, cssClass, ws);
		}
	}
	
	function hide(balloonID) {
		//alert(visible[balloonID]);
		var balloon = document.getElementById("balloon_" + balloonID);
		if (balloon && visible[balloonID]) {
			visible[balloonID] = null;
		}		
	}
	
	function findControlID(element, onEvent) {
		if (!element.parentNode) {
			return false;
		}
		if (element.id && element.id.indexOf("ctrl_") == 0) {
			var controlID = element.id.substring(5);
			if (!onEvent || onEvent[controlID]) {
				return controlID;
			}
		}
		return findControlID(element.parentNode, onEvent);
	}
	
	function findBalloonID(element) {
		if (!element.parentNode) {
			return false;
		}
		if (element.id && element.id.indexOf("balloon_") == 0) {
			var controlID = element.id.substring(5);
			return controlID;
		}
		return findBalloonID(element.parentNode);
	}
	
	function getSource(e) {
		var src = new Object();
		if (!e) var e = window.event;
		if (e.button == 2 && e.type == "click") {
			// ignore click event for contextmenu event (FF2 fix)
			return false;
		}
		var onEventID = e.type;
			
		var source = (window.event) ? e.srcElement : e.target;
		src["event"] = e;
		src["source"] = source;

		var onEvent = registered[onEventID];
		if (!onEvent) return src;
		var controlID = findControlID(source, onEvent);
		var onControl = onEvent[controlID];
		if (!onControl) return src; 
		var balloonID = onControl["balloonID"];

		src["controlID"] = controlID;
		src["balloonID"] = balloonID;
		src["eventID"] = onEventID; 

		return src;
	}
	
	function showBalloon(src) {
		var balloonID = src["balloonID"];
		var e = src["event"];
		var param = src["eventID"] + ";" + e.clientX + ";" + e.clientY;
		JWic.fireAction(balloonID, "show", param);
	}
	
	function closeBalloon(src) {
		for (balloonID in visible) {
			JWic.fireAction(balloonID, "hide", "");
		}
		visible = new Object();
	}

	document.onclick = BalloonEventClass;
	document.oncontextmenu = BalloonEventClass;
}