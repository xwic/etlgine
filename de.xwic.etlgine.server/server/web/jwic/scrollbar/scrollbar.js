// jWic JavaScript - ScrollBarControl
// 
// http://www.jwic.de

function setThisFrameBase(frame) {
  this.thisFrame = frame;
}
function ScrollbarBase(){
 // used methods
 this.init = initBase;
 this.moveY = moveYBase;
 this.moveX = moveXBase;
 this.addControl = addControlBase;
 this.setThisFrame = setThisFrameBase;
 this.setupScrollbars = setupScrollbarsBase

 // used variables
 this.thisFrame = "";
 this.aControlIds = new Array();
}

function addControlBase(CONTROL_ID, iLeftPos, iTopPos, iActualPosPercent, iType){
 this.aControlIds.push(CONTROL_ID);
 this.aControlIds.push(iLeftPos);
 this.aControlIds.push(iTopPos);
 this.aControlIds.push(iActualPosPercent);
 this.aControlIds.push(iType);
}

//
//Init all
//
function initBase(){
 for (var i=0; i<this.aControlIds.length; i++){
  wert1 = this.aControlIds[i];
  i++;
  wert2 = this.aControlIds[i];
  i++;
  wert3 = this.aControlIds[i];
  i++;
  wert4 = this.aControlIds[i];
  i++;
  wert5 = this.aControlIds[i];
  this.setupScrollbars(wert1, wert2, wert3, wert4, wert5);
 }
 this.aControlIds = new Array();
}

function setupScrollbarsBase(CONTROL_ID, iLeftPos, iTopPos, iActualPosPercent, iType){
	iLeftPos = "-1";
	iTopPos = "-1";
	var frame = this.thisFrame;
	if(frame.document.getElementById(CONTROL_ID + "_SCROLLER_START") != null) {
		//Find StartPoint
		theScrollerStartPoint = frame.document.getElementById(CONTROL_ID + "_SCROLLER_START");
		leftPosScroller = 0;
		topPosScroller = 0;
		theScrollerStartPointHeight = theScrollerStartPoint.offsetParent.offsetHeight;
		theScrollerStartPointWidth = theScrollerStartPoint.offsetParent.offsetWidth;
		if(Number(iLeftPos) < 0 ||Number(iTopPos) < 0) {
			do {
				theScrollerStartPoint = theScrollerStartPoint.offsetParent;
				leftPosScroller += theScrollerStartPoint.offsetLeft;
				topPosScroller += theScrollerStartPoint.offsetTop;
			} while(theScrollerStartPoint.tagName!="BODY");
			newTopPosScroller= topPosScroller;
			newLeftPosScroller= leftPosScroller + 1;
		} else {
			newTopPosScroller = iTopPos;
			newLeftPosScroller= iLeftPos;
		}
	
		//Set new Top Position
		if(frame.document.getElementById(CONTROL_ID + "_SCROLLER")  != null) {
			startL = newLeftPosScroller;
			startT = newTopPosScroller;
			if (Number(iActualPosPercent) > 0 && iType == "1") {// Horizontal Control
				startL = Number(newLeftPosScroller) + (Number(theScrollerStartPointWidth)*(Number(iActualPosPercent)/100));
				if(startL >= (newLeftPosScroller + theScrollerStartPointWidth - 26)+ 1)//Start plus width minus Image plus 1 px
					startL = (newLeftPosScroller + theScrollerStartPointWidth - 26);
			}
			if (Number(iActualPosPercent) > 0 && iType == "2") {// Vertical Control
				startT = Number(newTopPosScroller) + (Number(theScrollerStartPointHeight)*(Number(iActualPosPercent)/100));
				if(startT >= (newTopPosScroller + theScrollerStartPointHeight - 26)+ 1)//Start plus Height minus Image plus 1 px
					startT = (newTopPosScroller + theScrollerStartPointHeight - 26);
			}
			myScroller = frame.document.getElementById(CONTROL_ID + "_SCROLLER");
			myScroller.style.left = startL + "px";
			myScroller.style.top = startT + "px";
			myScroller.setAttribute("posX", newLeftPosScroller, false);
			myScroller.setAttribute("posY", newTopPosScroller, false);
			if(iType == "1")
				myScroller.setAttribute("scrollSize", theScrollerStartPointWidth, false);
			if(iType == "2")
				myScroller.setAttribute("scrollSize", theScrollerStartPointHeight, false);
	
		}
	}
}

//MOVE Y only IE
function moveYBase(theObject, myEvent, CONTROLID) {
	var frame = this.thisFrame;
	var deltaX = myEvent.clientX - parseInt(theObject.style.left);
	var deltaY = myEvent.clientY - parseInt(theObject.style.top);
	
	frame.document.attachEvent("onmousemove", MouseMoveHandlerY);   //Register handler
	frame.document.attachEvent("onmouseup", MouseUpHandlerY);       //Register handler
	myEvent.cancelBubble = true;//do not Bubble
	myEvent.returnValue = false;//do no Action
	theNewPosY = 0;
	
	//Handle Mouse move
	function MouseMoveHandlerY() {
		e = frame.window.event;
		myHeightY = theObject.getAttribute("scrollSize");
		myStartY = theObject.getAttribute("posY");
		theNewPosY = e.clientY - deltaY;
		if(theNewPosY < myStartY)
			theNewPosY = myStartY;
		if(theNewPosY >= (myStartY + myHeightY - 26)+ 1)//Start plus Height minus Image plus 1 px
			theNewPosY = (myStartY + myHeightY - 26);
		//MOVE
		theObject.style.top  = theNewPosY + "px";
		e.cancelBubble = true;		//Prevent bubbling
		//Edit Text
		theHTML = '<p>';
		theObject.innerHTML = theHTML;
	
	}
	//Handle Mouse up
	function MouseUpHandlerY() {
		e = frame.window.event;
		myHeightY = theObject.getAttribute("scrollSize");
		myStartY = theObject.getAttribute("posY");    
		theNewPosY = e.clientY - deltaY;
		frame.document.detachEvent("onmouseup", MouseUpHandlerY);     //Unregister
		frame.document.detachEvent("onmousemove", MouseMoveHandlerY); //Unregister
		e.cancelBubble = true;		//Prevent bubbling
		wert = String(theNewPosY >= (myStartY + myHeightY - 26) ? "100.00":(theNewPosY - myStartY)/(myHeightY/100));
		var sReturn = wert + ";" + theObject.getAttribute("posX") + ";" + myStartY + ";" + theNewPosY + ";";
		jWic().fireAction(CONTROLID,'goto', sReturn);
	}
}

//MOVE X only IE
function moveXBase(mover, myEvent, CONTROLID) {
	var deltaX = myEvent.clientX - parseInt(mover.style.left);
	var deltaY = myEvent.clientY - parseInt(mover.style.top);
	var frame = this.thisFrame;
	
	frame.document.attachEvent("onmousemove", MouseMoveHandlerX);   //Register handler
	frame.document.attachEvent("onmouseup", MouseUpHandlerX);       //Register handler
	myEvent.cancelBubble = true;//do not Bubble
	myEvent.returnValue = false;//do no Action
	theNewPosX = 0;
	
	//Handle Mouse move
	function MouseMoveHandlerX() {
		e = frame.window.event;	// IE event model
		myWidthX = mover.getAttribute("scrollSize");
		myStartX = mover.getAttribute("posX");
		theNewPosX = e.clientX - deltaX;
		//alert('theNewPosX: '+theNewPosX+' myStartX: '+myStartX+' myWidthX: '+myWidthX+' deltaX: '+deltaX+' myEvent.clientX: '+myEvent.clientX);
		if(theNewPosX < myStartX)
			theNewPosX = myStartX;
		if(theNewPosX >= (myStartX + myWidthX - 26)+ 1)//Start plus width minus Image plus 1 px
			theNewPosX = (myStartX + myWidthX - 26);
		//MOVE
		mover.style.left = theNewPosX + "px";
		e.cancelBubble = true;		//Prevent bubbling
		//alert('theNewPosX: '+theNewPosX+' myStartX: '+myStartX+' myWidthX: '+myWidthX+' deltaX: '+deltaX+' myEvent.clientX: '+myEvent.clientX);
		//Edit Text
		theHTML = '<p>';
		mover.innerHTML = theHTML;
	}
	
	//Handle Mouse up
	function MouseUpHandlerX() {
		e = frame.window.event;	// IE event model
		myWidthX = mover.getAttribute("scrollSize");
		myStartX = mover.getAttribute("posX");    
		theNewPosX = e.clientX - deltaX;		
		frame.document.detachEvent("onmouseup", MouseUpHandlerX);     //Unregister
		frame.document.detachEvent("onmousemove", MouseMoveHandlerX); //Unregister
		e.cancelBubble = true;		//Prevent bubbling
		wert = String(theNewPosX >= (myStartX + myWidthX - 26) ? "100.00":(theNewPosX - myStartX)/(myWidthX/100));
		//alert('theNewPosX: '+theNewPosX+' myStartX: '+myStartX+' myWidthX: '+myWidthX+' deltaX: '+deltaX+' myEvent.clientX: '+myEvent.clientX);
		var sReturn = wert + ";" + myStartX + ";" + mover.getAttribute("posY") + ";" + theNewPosX + ";";
		jWic().fireAction(CONTROLID,'goto', sReturn);
	}
}

function scrollbar() {
	if (!this.scrollbarObject) {
		this.scrollbarObject = new ScrollbarBase();
	}
	return this.scrollbarObject;
}

scrollbar().setThisFrame(this);