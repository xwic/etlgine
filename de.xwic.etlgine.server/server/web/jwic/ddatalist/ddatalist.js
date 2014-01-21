// jWic JavaScript Client Library - DHTML Data-List
// uses an IFRAME control to display a "drop-down" like control that dynamicaly
// loads data from the server.
// 
// 2005-05-21: Due to problems with the mozilla firefox browser not displaying a 
// 			   cursor in input fields, the initial (hidden) width is now 1x1 pixel.
//
// author: Florian Lippisch
// http://www.jwic.de

	var eWicDD_dstDocument = document;
	document.isDataList = true;

	// initialize the DataList
	function eWicDD_Init(newDocument) {
		this.isVis = false;
		this.defWidth = 200;
		this.defHeight = 120;
		this.dstDocument = newDocument;
		eWicDD_dstDocument = newDocument;
		// write the IFRAME
		this.dstDocument.write("<IFRAME id='ddataframe' style='position: absolute; border: 1px solid #000000; visibility: hidden; width:1; height:1;' FRAMEBORDER='0'></IFRAME>");
	}
	
	// show-frame
	function eWicDD_showFrame(srcControl, srcDataControl, listURL) {
	
		var fixedX = -1;
		var fixedY = -1;
		var	leftpos=0;
		var	toppos=0;
		
		this.srcCtrl = srcControl;
		this.srcDataCtrl = srcDataControl;
			
		var dframe = this.dstDocument.getElementById("ddataframe");
		dframe.style.width = this.defWidth;
		dframe.style.height = this.defHeight;
		dframe.style.visibility = "visible";
		
		this.dstDocument.ddselClick = (new Date()).getTime();
		this.dstDocument.onclick = function hideddl2 () { 		
			if ( (new Date()).getTime() - eWicDD_dstDocument.ddselClick > 1000) {
				eWicDD_hideFrame();
			}
		}		
		this.isVis = true;
		
		// move the frame under the posControl
		aTag = srcControl
		do {
			aTag = aTag.offsetParent;
			leftpos	+= aTag.offsetLeft;
			toppos += aTag.offsetTop;
		} while(aTag.tagName!="BODY");

		var x = fixedX==-1 ? srcControl.offsetLeft	+ leftpos :	fixedX==-2 ? srcControl.offsetLeft + leftpos - fixedWidth : fixedX
		if (x < 1) { x = 1 }
		dframe.style.left = x
		var y = fixedY==-1 ? srcControl.offsetTop + toppos + srcControl.offsetHeight + 2 : fixedY==-2 ? srcControl.offsetTop + toppos + 2 - fixedHeight : fixedY
		if (y < 1) { y = 1 }
		dframe.style.top = y
		dframe.src = listURL; // now load
		
	}
	// show-frame as info
	function eWicDD_showInfo(x, y, listURL) {
	
		var fixedX = -1;
		var fixedY = -1;
		var	leftpos=0;
		var	toppos=0;
		
		var dframe = this.dstDocument.getElementById("ddataframe");
		dframe.style.width = this.defWidth;
		dframe.style.height = this.defHeight;
		dframe.style.visibility = "visible";
		
		this.dstDocument.ddselClick = (new Date()).getTime();
		this.dstDocument.onclick = function hideddl2 () { 		
			if ( (new Date()).getTime() - eWicDD_dstDocument.ddselClick > 1000) {
				eWicDD_hideFrame();
			}
		}		
		this.isVis = true;
		
		dframe.style.left = x-this.defWidth;
		dframe.style.top = y-this.defHeight;
		dframe.src = listURL; // now load
	}
	// hide-frame
	function eWicDD_hideFrame() {
		var dframe = eWicDD_dstDocument.getElementById("ddataframe");
		dframe.style.visibility = "hidden";
		dframe.style.width = 1;
		dframe.style.height = 1;
		dframe.style.left = 0;
		dframe.style.top = 0;		
		dframe.src = "about:blank";
		this.isVis =false;
	}
	// set width of the frame
	function eWicDD_setWidth(newWidth) {
		var dframe = eWicDD_dstDocument.getElementById("ddataframe");
		dframe.style.width = newWidth;
	}
	
	// set selection
	function eWicDD_applySelection(sObjID, sObjTitle) {
		this.srcDataCtrl.value = sObjID;
		this.srcCtrl.value = sObjTitle;
		if (this.doSubmit) {
			this.doSubmit = false;
			if (this.submitField != "") {
				this.srcDataCtrl.form.elements[this.submitField].value = this.submitValue;
			}
			this.srcDataCtrl.form.submit();
		}
		this.hide();
	}
	
	// set Submit Flag
	function eWicDD_setSubmit(bolDoSubmit) {
		this.doSubmit = bolDoSubmit;
	}

	// set the field where the specified value is set on submit 
	function eWicDD_setSubmitField(sField, sValue) {
		this.submitField = sField;
		this.submitValue = sValue;
	}
		
	// dataList Contstructor
	function jWicDDataList() {
		this.version = "1.1";
		this.init = eWicDD_Init;
		this.show = eWicDD_showFrame;
		this.showInfo = eWicDD_showInfo;
		this.hide = eWicDD_hideFrame;
		this.setWidth = eWicDD_setWidth;
		this.applySelection = eWicDD_applySelection;
		this.setSubmit = eWicDD_setSubmit;
		this.doSubmit = false;
		this.setSubmitField = eWicDD_setSubmitField;
		this.submitField = "";
		this.submitValue = "";
	}