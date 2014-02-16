// Calendar constructor
function jWicCalendar() {

//	written	by Tan Ling	Wee	on 2 Dec 2001
//	last updated 23 June 2002
//	email :	fuushikaden@yahoo.com
//
//	modyfied by Jens Bornemann on 3 Jul 2002: changed imgDir to images, added fixedX, fixedY = -2, and these two parameters are now optional for popUpCalendar
// 	modified by Florian Lippisch on 25 Sep 2002: changed so it runs on foreign frames

	var	fixedX = -1			// x position (-1 if to appear right control, -2 to appear left control)
	var	fixedY = -1			// y position (-1 if to appear below control, -2 to appear above control)
	var	fixedWidth = 256
	var	fixedHeight = 188
	var startAt = 1			// 0 - sunday ; 1 - monday
	var showWeekNumber = 1	// 0 - don't show; 1 - show
	var showToday = 1		// 0 - don't show; 1 - show
	var imgDir = "/gfx/datepicker/"		// directory for images ... e.g. var imgDir="/img/"

	var gotoString = "Go To Current Month"
	var todayString = "Today is"
	var weekString = "Wk"
	var scrollLeftMessage = "Click to scroll to previous month. Hold mouse button to scroll automatically."
	var scrollRightMessage = "Click to scroll to next month. Hold mouse button to scroll automatically."
	var selectMonthMessage = "Click to select a month."
	var selectYearMessage = "Click to select a year."
	var selectDateMessage = "Select [date] as date." // do not replace [date], it will be replaced by date.

	var	crossobj, crossMonthObj, crossYearObj, monthSelected, yearSelected, dateSelected, omonthSelected, oyearSelected, odateSelected, monthConstructed, yearConstructed, intervalID1, intervalID2, timeoutID1, timeoutID2, ctlToPlaceValue, ctlNow, dateFormat, nStartingYear

	var	bPageLoaded=false
	var	ie=document.all
	var	dom=document.getElementById

	var	ns4=document.layers
	var	today =	new	Date()
	var	dateNow	 = today.getDate()
	var	monthNow = today.getMonth()
	var	yearNow	 = today.getYear()
	var	imgsrc = new Array("drop1.gif","drop2.gif","left1.gif","left2.gif","right1.gif","right2.gif")
	var	img	= new Array()
	
	var frmDocument = document;

	var styleAnchor = "text-decoration:none;color:black;"
	var styleLightBorder = "border-style:solid;border-width:1px;border-color:#a0a0a0;"
	var	monthName =	new	Array("January","February","March","April","May","June","July","August","September","October","November","December")

	var bShow = false;

    /* hides <select> and <applet> objects (for IE only) */
    function hideElement( elmID, overDiv )
    {
      if( ie )
      {
        for( i = 0; i < frmDocument.all.tags( elmID ).length; i++ )
        {
          obj = frmDocument.all.tags( elmID )[i];
          if( !obj || !obj.offsetParent )
          {
            continue;
          }
      
          // Find the element's offsetTop and offsetLeft relative to the BODY tag.
          objLeft   = obj.offsetLeft;
          objTop    = obj.offsetTop;
          objParent = obj.offsetParent;
          
          while( objParent && objParent.tagName.toUpperCase() != "BODY" )
          {
            objLeft  += objParent.offsetLeft;
            objTop   += objParent.offsetTop;
            objParent = objParent.offsetParent;
          }
      
          objHeight = obj.offsetHeight;
          objWidth = obj.offsetWidth;
      
          if(( overDiv.offsetLeft + overDiv.offsetWidth ) <= objLeft );
          else if(( overDiv.offsetTop + overDiv.offsetHeight ) <= objTop );
          else if( overDiv.offsetTop >= ( objTop + objHeight ));
          else if( overDiv.offsetLeft >= ( objLeft + objWidth ));
          else
          {
            obj.style.visibility = "hidden";
          }
        }
      }
    }
     
    /*
    * unhides <select> and <applet> objects (for IE only)
    */
    function showElement( elmID )
    {
      if( ie )
      {
        for( i = 0; i < frmDocument.all.tags( elmID ).length; i++ )
        {
          obj = frmDocument.all.tags( elmID )[i];
          
          if( !obj || !obj.offsetParent )
          {
            continue;
          }
        
          obj.style.visibility = "";
        }
      }
    }

	function HolidayRec (d, m, y, desc)
	{
		this.d = d
		this.m = m
		this.y = y
		this.desc = desc
	}

	var HolidaysCounter = 0
	var Holidays = new Array()

	function addHoliday (d, m, y, desc)
	{
		Holidays[HolidaysCounter++] = new HolidayRec ( d, m, y, desc )
	}
	function calSwapImage(srcImg, destImg){
		if (ie)	{ frmDocument.getElementById(srcImg).setAttribute("src",imgDir + destImg) }
	}

	function initCal()	{
		if (!ns4)
		{
			if (!ie) { yearNow += 1900	}

			crossobj=(dom)?frmDocument.getElementById("calendar").style : ie? frmDocument.all.calendar : frmDocument.calendar

			crossMonthObj=(dom)?frmDocument.getElementById("selectMonth").style : ie? frmDocument.all.selectMonth	: frmDocument.selectMonth

			crossYearObj=(dom)?frmDocument.getElementById("selectYear").style : ie? frmDocument.all.selectYear : frmDocument.selectYear
			
			hideCalendar()
			monthConstructed=false;
			yearConstructed=false;

			if (showToday==1)
			{
				frmDocument.getElementById("lblToday").innerHTML =	todayString + " <a onmousemove='window.status=\""+gotoString+"\"' onmouseout='window.status=\"\"' title='"+gotoString+"' style='"+styleAnchor+"' href='javascript:jWic().calendar.setMonthSelected(monthNow);yearSelected=yearNow;jWic().calendar.constructCalendar();'>"+dayName[(today.getDay()-startAt==-1)?6:(today.getDay()-startAt)]+", " + dateNow + " " + monthName[monthNow].substring(0,3)	+ "	" +	yearNow	+ "</a>"
			}

			sHTML1="<span id='spanLeft'	style='border-style:solid;border-width:1;border-color:#3366FF;cursor:pointer' onmouseover='jWic().calendar.swapImage(\"changeLeft\",\"left2.gif\");this.style.borderColor=\"#88AAFF\";window.status=\""+scrollLeftMessage+"\"' onclick='javascript:jWic().calendar.decMonth()' onmouseout='clearInterval(jWic().calendar.getIntervalID1());jWic().calendar.swapImage(\"changeLeft\",\"left1.gif\");this.style.borderColor=\"#3366FF\";window.status=\"\"' onmousedown='clearTimeout(jWic().calendar.getTimeoutID1());jWic().calendar.setTimeoutID1(setTimeout(\"jWic().calendar.StartDecMonth()\",500))'	onmouseup='clearTimeout(jWic().calendar.getTimeoutID1());clearInterval(jWic().calendar.getIntervalID1())'>&nbsp<IMG id='changeLeft' SRC='"+imgDir+"left1.gif' width=10 height=11 BORDER=0>&nbsp</span>&nbsp;"
			sHTML1+="<span id='spanRight' style='border-style:solid;border-width:1;border-color:#3366FF;cursor:pointer'	onmouseover='jWic().calendar.swapImage(\"changeRight\",\"right2.gif\");this.style.borderColor=\"#88AAFF\";window.status=\""+scrollRightMessage+"\"' onmouseout='clearInterval(jWic().calendar.getIntervalID1());jWic().calendar.swapImage(\"changeRight\",\"right1.gif\");this.style.borderColor=\"#3366FF\";window.status=\"\"' onclick='jWic().calendar.incMonth()' onmousedown='clearTimeout(jWic().calendar.getTimeoutID1());jWic().calendar.setTimeoutID1(setTimeout(\"jWic().calendar.StartIncMonth()\",500))'	onmouseup='clearTimeout(jWic().calendar.getTimeoutID1());clearInterval(jWic().calendar.getIntervalID1())'>&nbsp<IMG id='changeRight' SRC='"+imgDir+"right1.gif'	width=10 height=11 BORDER=0>&nbsp</span>&nbsp"
			sHTML1+="<span id='spanMonth' style='border-style:solid;border-width:1;border-color:#3366FF;cursor:pointer'	onmouseover='jWic().calendar.swapImage(\"changeMonth\",\"drop2.gif\");this.style.borderColor=\"#88AAFF\";window.status=\""+selectMonthMessage+"\"' onmouseout='jWic().calendar.swapImage(\"changeMonth\",\"drop1.gif\");this.style.borderColor=\"#3366FF\";window.status=\"\"' onclick='jWic().calendar.popUpMonth()'></span>&nbsp;"
			sHTML1+="<span id='spanYear' style='border-style:solid;border-width:1;border-color:#3366FF;cursor:pointer' onmouseover='jWic().calendar.swapImage(\"changeYear\",\"drop2.gif\");this.style.borderColor=\"#88AAFF\";window.status=\""+selectYearMessage+"\"'	onmouseout='jWic().calendar.swapImage(\"changeYear\",\"drop1.gif\");this.style.borderColor=\"#3366FF\";window.status=\"\"'	onclick='jWic().calendar.popUpYear()'></span>&nbsp;"
			
			frmDocument.getElementById("caption").innerHTML  =	sHTML1

			bPageLoaded=true
		}
	}

	function hideCalendar()	{
		crossobj.visibility="hidden"
		if (crossMonthObj != null){crossMonthObj.visibility="hidden"}
		if (crossYearObj !=	null){crossYearObj.visibility="hidden"}

		showElement( 'SELECT' );
		showElement( 'APPLET' );
	}

	function padZero(num) {
		return (num	< 10)? '0' + num : num ;
	}

	function constructDate(d,m,y)
	{
		sTmp = dateFormat
		sTmp = sTmp.replace	("dd","<e>")
		sTmp = sTmp.replace	("d","<d>")
		sTmp = sTmp.replace	("<e>",padZero(d))
		sTmp = sTmp.replace	("<d>",d)
		sTmp = sTmp.replace	("mmm","<o>")
		sTmp = sTmp.replace	("mm","<n>")
		sTmp = sTmp.replace	("m","<m>")
		sTmp = sTmp.replace	("<m>",m+1)
		sTmp = sTmp.replace	("<n>",padZero(m+1))
		sTmp = sTmp.replace	("<o>",monthName[m])
		return sTmp.replace ("yyyy",y)
	}

	function closeCalendar() {
		var	sTmp;
		hideCalendar();
		ctlToPlaceValue.value =	constructDate(dateSelected,monthSelected,yearSelected);
		ctlToPlaceValue.fireEvent("onChange");
	}

	/*** Month Pulldown	***/

	function StartDecMonth()
	{
		intervalID1=setInterval("decMonth()",80)
	}

	function StartIncMonth()
	{
		intervalID1=setInterval("incMonth()",80)
	}

	function incMonth () {
		monthSelected++
		if (monthSelected>11) {
			monthSelected=0
			yearSelected++
		}
		constructCalendar()
	}

	function decMonth () {
		monthSelected--
		if (monthSelected<0) {
			monthSelected=11
			yearSelected--
		}
		constructCalendar()
	}

	function constructMonth() {
		popDownYear()
		if (!monthConstructed) {
			sHTML =	""
			for	(i=0; i<12;	i++) {
				sName =	monthName[i];
				if (i==monthSelected){
					sName =	"<B>" +	sName +	"</B>"
				}
				sHTML += "<tr><td id='m" + i + "' onmouseover='this.style.backgroundColor=\"#FFCC99\"' onmouseout='this.style.backgroundColor=\"\"' style='cursor:pointer' onclick='monthConstructed=false;jWic().calendar.setMonthSelected(" + i + ");jWic().calendar.constructCalendar();jWic().calendar.popDownMonth();event.cancelBubble=true'>&nbsp;" + sName + "&nbsp;</td></tr>"
			}

			frmDocument.getElementById("selectMonth").innerHTML = "<table width=70	style='font-family:arial; font-size:11px; border-width:1; border-style:solid; border-color:#a0a0a0;' bgcolor='#FFFFDD' cellspacing=0 onmouseover='clearTimeout(jWic().calendar.getTimeoutID1())'	onmouseout='clearTimeout(jWic().calendar.getTimeoutID1());jWic().calendar.setTimeoutID1(setTimeout(\"jWic().calendar.popDownMonth()\",100));event.cancelBubble=true'>" +	sHTML +	"</table>"

			monthConstructed=true
		}
	}

	function popUpMonth() {
		constructMonth()
		crossMonthObj.visibility = (dom||ie)? "visible"	: "show"
		crossMonthObj.left = parseInt(crossobj.left) + 50
		crossMonthObj.top =	parseInt(crossobj.top) + 26

		hideElement( 'SELECT', frmDocument.getElementById("selectMonth") );
		hideElement( 'APPLET', frmDocument.getElementById("selectMonth") );			
	}

	function popDownMonth()	{
		crossMonthObj.visibility= "hidden"
	}

	/*** Year Pulldown ***/

	function incYear() {
		for	(i=0; i<7; i++){
			newYear	= (i+nStartingYear)+1
			if (newYear==yearSelected)
			{ txtYear =	"&nbsp;<B>"	+ newYear +	"</B>&nbsp;" }
			else
			{ txtYear =	"&nbsp;" + newYear + "&nbsp;" }
			frmDocument.getElementById("y"+i).innerHTML = txtYear
		}
		nStartingYear ++;
		bShow=true
	}

	function decYear() {
		for	(i=0; i<7; i++){
			newYear	= (i+nStartingYear)-1
			if (newYear==yearSelected)
			{ txtYear =	"&nbsp;<B>"	+ newYear +	"</B>&nbsp;" }
			else
			{ txtYear =	"&nbsp;" + newYear + "&nbsp;" }
			frmDocument.getElementById("y"+i).innerHTML = txtYear
		}
		nStartingYear --;
		bShow=true
	}

	function selectYear(nYear) {
		yearSelected=parseInt(nYear+nStartingYear);
		yearConstructed=false;
		constructCalendar();
		popDownYear();
	}

	function constructYear() {
		popDownMonth()
		sHTML =	""
		if (!yearConstructed) {

			sHTML =	"<tr><td align='center'	onmouseover='this.style.backgroundColor=\"#FFCC99\"' onmouseout='clearInterval(jWic().calendar.getIntervalID1());this.style.backgroundColor=\"\"' style='cursor:pointer'	onmousedown='clearInterval(jWic().calendar.getIntervalID1());jWic().calendar.setIntervalID1(setInterval(\"jWic().calendar.decYear()\",30))' onmouseup='clearInterval(jWic().calendar.getIntervalID1())'>-</td></tr>"

			j =	0
			nStartingYear =	yearSelected-3
			for	(i=(yearSelected-3); i<=(yearSelected+3); i++) {
				sName =	i;
				if (i==yearSelected){
					sName =	"<B>" +	sName +	"</B>"
				}

				sHTML += "<tr><td id='y" + j + "' onmouseover='this.style.backgroundColor=\"#FFCC99\"' onmouseout='this.style.backgroundColor=\"\"' style='cursor:pointer' onclick='jWic().calendar.selectYear("+j+");event.cancelBubble=true'>&nbsp;" + sName + "&nbsp;</td></tr>"
				j ++;
			}

			sHTML += "<tr><td align='center' onmouseover='this.style.backgroundColor=\"#FFCC99\"' onmouseout='clearInterval(jWic().calendar.getIntervalID2());this.style.backgroundColor=\"\"' style='cursor:pointer' onmousedown='clearInterval(jWic().calendar.getIntervalID2());jWic().calendar.setIntervalID2(setInterval(\"jWic().calendar.incYear()\",30))'	onmouseup='clearInterval(jWic().calendar.getIntervalID2())'>+</td></tr>"

			frmDocument.getElementById("selectYear").innerHTML	= "<table width=44 style='font-family:arial; font-size:11px; border-width:1; border-style:solid; border-color:#a0a0a0;'	bgcolor='#FFFFDD' onmouseover='clearTimeout(jWic().calendar.getTimeoutID2())' onmouseout='clearTimeout(jWic().calendar.getTimeoutID2());jWic().calendar.setTimeoutID2(setTimeout(\"jWic().calendar.popDownYear()\",100))' cellspacing=0>"	+ sHTML	+ "</table>"

			yearConstructed	= true
		}
	}

	function popDownYear() {
		clearInterval(intervalID1)
		clearTimeout(timeoutID1)
		clearInterval(intervalID2)
		clearTimeout(timeoutID2)
		crossYearObj.visibility= "hidden"
	}

	function popUpYear() {
		var	leftOffset

		constructYear()
		crossYearObj.visibility	= (dom||ie)? "visible" : "show"
		leftOffset = parseInt(crossobj.left) + frmDocument.getElementById("spanYear").offsetLeft
		if (ie)
		{
			leftOffset += 6
		}
		crossYearObj.left =	leftOffset
		crossYearObj.top = parseInt(crossobj.top) +	26
	}

	/*** calendar ***/
   function WeekNbr(n) {
      // Algorithm used:
      // From Klaus Tondering's Calendar frmDocument (The Authority/Guru)
      // hhtp://www.tondering.dk/claus/calendar.html
      // a = (14-month) / 12
      // y = year + 4800 - a
      // m = month + 12a - 3
      // J = day + (153m + 2) / 5 + 365y + y / 4 - y / 100 + y / 400 - 32045
      // d4 = (J + 31741 - (J mod 7)) mod 146097 mod 36524 mod 1461
      // L = d4 / 1460
      // d1 = ((d4 - L) mod 365) + L
      // WeekNumber = d1 / 7 + 1
 
      year = n.getFullYear();
      month = n.getMonth() + 1;
      if (startAt == 0) {
         day = n.getDate() + 1;
      }
      else {
         day = n.getDate();
      }
 
      a = Math.floor((14-month) / 12);
      y = year + 4800 - a;
      m = month + 12 * a - 3;
      b = Math.floor(y/4) - Math.floor(y/100) + Math.floor(y/400);
      J = day + Math.floor((153 * m + 2) / 5) + 365 * y + b - 32045;
      d4 = (((J + 31741 - (J % 7)) % 146097) % 36524) % 1461;
      L = Math.floor(d4 / 1460);
      d1 = ((d4 - L) % 365) + L;
      week = Math.floor(d1/7) + 1;
 
      return week;
   }

	function constructCalendar () {
		var aNumDays = Array (31,0,31,30,31,30,31,31,30,31,30,31)

		var dateMessage
		var	startDate =	new	Date (yearSelected,monthSelected,1)
		var endDate

		if (monthSelected==1)
		{
			endDate	= new Date (yearSelected,monthSelected+1,1);
			endDate	= new Date (endDate	- (24*60*60*1000));
			numDaysInMonth = endDate.getDate()
		}
		else
		{
			numDaysInMonth = aNumDays[monthSelected];
		}

		datePointer	= 0
		dayPointer = startDate.getDay() - startAt
		
		if (dayPointer<0)
		{
			dayPointer = 6
		}

		sHTML =	"<table	 border=0 style='font-family:verdana;font-size:10px;'><tr>"

		if (showWeekNumber==1)
		{
			sHTML += "<td width=27><b>" + weekString + "</b></td><td width=1 rowspan=7 bgcolor='#d0d0d0' style='padding:0px'><img src='"+imgDir+"divider.gif' width=1></td>"
		}

		for	(i=0; i<7; i++)	{
			sHTML += "<td width='27' align='right'><B>"+ dayName[i]+"</B></td>"
		}
		sHTML +="</tr><tr>"
		
		if (showWeekNumber==1)
		{
			sHTML += "<td align=right>" + WeekNbr(startDate) + "&nbsp;</td>"
		}

		for	( var i=1; i<=dayPointer;i++ )
		{
			sHTML += "<td>&nbsp;</td>"
		}
	
		for	( datePointer=1; datePointer<=numDaysInMonth; datePointer++ )
		{
			dayPointer++;
			sHTML += "<td align=right>"
			sStyle=styleAnchor
			if ((datePointer==odateSelected) &&	(monthSelected==omonthSelected)	&& (yearSelected==oyearSelected))
			{ sStyle+=styleLightBorder }

			sHint = ""
			for (k=0;k<HolidaysCounter;k++)
			{
				if ((parseInt(Holidays[k].d)==datePointer)&&(parseInt(Holidays[k].m)==(monthSelected+1)))
				{
					if ((parseInt(Holidays[k].y)==0)||((parseInt(Holidays[k].y)==yearSelected)&&(parseInt(Holidays[k].y)!=0)))
					{
						sStyle+="background-color:#FFDDDD;"
						sHint+=sHint==""?Holidays[k].desc:"\n"+Holidays[k].desc
					}
				}
			}

			var regexp= /\"/g
			sHint=sHint.replace(regexp,"&quot;")

			dateMessage = "onmousemove='window.status=\""+selectDateMessage.replace("[date]",constructDate(datePointer,monthSelected,yearSelected))+"\"' onmouseout='window.status=\"\"' "

			if ((datePointer==dateNow)&&(monthSelected==monthNow)&&(yearSelected==yearNow))
			{ sHTML += "<b><a "+dateMessage+" title=\"" + sHint + "\" style='"+sStyle+"' href='javascript:jWic().calendar.select("+datePointer+");jWic().calendar.close();'><font color=#ff0000>&nbsp;" + datePointer + "</font>&nbsp;</a></b>"}
			else if	(dayPointer % 7 == (startAt * -1)+1)
			{ sHTML += "<a "+dateMessage+" title=\"" + sHint + "\" style='"+sStyle+"' href='javascript:jWic().calendar.select("+datePointer + ");jWic().calendar.close();'>&nbsp;<font color=#909090>" + datePointer + "</font>&nbsp;</a>" }
			else
			{ sHTML += "<a "+dateMessage+" title=\"" + sHint + "\" style='"+sStyle+"' href='javascript:jWic().calendar.select("+datePointer + ");jWic().calendar.close();'>&nbsp;" + datePointer + "&nbsp;</a>" }

			sHTML += ""
			if ((dayPointer+startAt) % 7 == startAt) { 
				sHTML += "</tr><tr>" 
				if ((showWeekNumber==1)&&(datePointer<numDaysInMonth))
				{
					sHTML += "<td align=right>" + (WeekNbr(new Date(yearSelected,monthSelected,datePointer+1))) + "&nbsp;</td>"
				}
			}
		}
		//ctlToPlaceValue.value = sHTML;
		frmDocument.getElementById("content").innerHTML   = sHTML
		frmDocument.getElementById("spanMonth").innerHTML = "&nbsp;" +	monthName[monthSelected] + "&nbsp;<IMG id='changeMonth' SRC='"+imgDir+"drop1.gif' WIDTH='12' HEIGHT='10' BORDER=0>"
		frmDocument.getElementById("spanYear").innerHTML =	"&nbsp;" + yearSelected	+ "&nbsp;<IMG id='changeYear' SRC='"+imgDir+"drop1.gif' WIDTH='12' HEIGHT='10' BORDER=0>"
	}

	function popUpCalendar(ctl,	ctl2, format) {
		if (popUpCalendar.arguments.length > 3) fixedX = popUpCalendar.arguments[3]
		if (popUpCalendar.arguments.length > 4) fixedY = popUpCalendar.arguments[4]
		var	leftpos=0
		var	toppos=0

		if (bPageLoaded)
		{
			if ( crossobj.visibility ==	"hidden" ) {
				ctlToPlaceValue	= ctl2
				dateFormat=format;

				formatChar = " "
				aFormat	= dateFormat.split(formatChar)
				if (aFormat.length<3)
				{
					formatChar = "/"
					aFormat	= dateFormat.split(formatChar)
					if (aFormat.length<3)
					{
						formatChar = "."
						aFormat	= dateFormat.split(formatChar)
						if (aFormat.length<3)
						{
							formatChar = "-"
							aFormat	= dateFormat.split(formatChar)
							if (aFormat.length<3)
							{
								// invalid date	format
								formatChar=""
							}
						}
					}
				}

				tokensChanged =	0
				if ( formatChar	!= "" )
				{
					// use user's date
					aData =	ctl2.value.split(formatChar)

					for	(i=0;i<3;i++)
					{
						if ((aFormat[i]=="d") || (aFormat[i]=="dd"))
						{
							dateSelected = parseInt(aData[i], 10)
							tokensChanged ++
						}
						else if	((aFormat[i]=="m") || (aFormat[i]=="mm"))
						{
							monthSelected =	parseInt(aData[i], 10) - 1
							tokensChanged ++
						}
						else if	(aFormat[i]=="yyyy")
						{
							yearSelected = parseInt(aData[i], 10)
							tokensChanged ++
						}
						else if	(aFormat[i]=="mmm")
						{
							for	(j=0; j<12;	j++)
							{
								if (aData[i]==monthName[j])
								{
									monthSelected=j
									tokensChanged ++
								}
							}
						}
					}
				}

				if ((tokensChanged!=3)||isNaN(dateSelected)||isNaN(monthSelected)||isNaN(yearSelected))
				{
					dateSelected = dateNow
					monthSelected =	monthNow
					yearSelected = yearNow
				}

				odateSelected=dateSelected
				omonthSelected=monthSelected
				oyearSelected=yearSelected

				aTag = ctl
				do {
					aTag = aTag.offsetParent;
					leftpos	+= aTag.offsetLeft;
					toppos += aTag.offsetTop;
				} while(aTag.tagName!="BODY");

				var wnSize = WindowSize(); // jWic.js function.
				var x = fixedX==-1 ? ctl.offsetLeft	+ leftpos :	fixedX==-2 ? ctl.offsetLeft + leftpos - fixedWidth : fixedX
				if (x < 1) { x = 1 }
				crossobj.left = x + "px";
				var y = fixedY==-1 ? ctl.offsetTop + toppos + ctl.offsetHeight + 2 : fixedY==-2 ? ctl.offsetTop + toppos + 2 - fixedHeight : fixedY
				var maxY = wnSize[1] + wnSize[3] - 200;
				if (y > maxY) y = maxY;
				if (y < 1) { y = 1 }

				crossobj.top = y + "px";
				constructCalendar (1, monthSelected, yearSelected);
				crossobj.visibility=(dom||ie)? "visible" : "show"

				hideElement( 'SELECT', frmDocument.getElementById("calendar") );
				hideElement( 'APPLET', frmDocument.getElementById("calendar") );			

				bShow = true;
			}
			else
			{
				hideCalendar()
				if (ctlNow!=ctl) {popUpCalendar(ctl, ctl2, format)}
			}
			ctlNow = ctl
		}
	}

	
	function initLib(newDocument) {
		frmDocument = newDocument;

		if (dom)
		{
			for	(i=0;i<imgsrc.length;i++)
			{
				img[i] = new Image
				img[i].src = imgDir + imgsrc[i]
			}
			frmDocument.write ("<div onclick='bShow=true' id='calendar'	style='z-index:+999;position:absolute;visibility:hidden;'><table width="+((showWeekNumber==1)?250:220)+" style='font-family:arial;font-size:11px;border-width:1px;border-style:solid;border-color:#a0a0a0;font-family:arial; font-size:11px}' bgcolor='#ffffff'><tr bgcolor='#0000aa'><td><table width='"+((showWeekNumber==1)?248:218)+"'><tr><td style='padding:2px;font-family:arial; font-size:11px;'><font color='#ffffff'><B><span id='caption'></span></B></font></td><td align=right><a href='javascript:jWic().calendar.hide()'><IMG SRC='"+imgDir+"close.gif' WIDTH='15' HEIGHT='13' BORDER='0' ALT='Close the Calendar'></a></td></tr></table></td></tr><tr><td style='padding:5px' bgcolor=#ffffff><span id='content'></span></td></tr>")
			
			if (showToday==1)
			{
				frmDocument.write ("<tr bgcolor=#f0f0f0><td style='padding:5px' align=center><span id='lblToday'></span></td></tr>")
			}
			
			frmDocument.write ("</table></div><div id='selectMonth' style='z-index:+999;position:absolute;visibility:hidden;'></div><div id='selectYear' style='z-index:+999;position:absolute;visibility:hidden;'></div>");
		}

		if (startAt==0)
		{
			dayName = new Array	("Sun","Mon","Tue","Wed","Thu","Fri","Sat")
		}
		else
		{
			dayName = new Array	("Mon","Tue","Wed","Thu","Fri","Sat","Sun")
		}
		styleAnchor="text-decoration:none;color:black;"
		styleLightBorder="border-style:solid;border-width:1px;border-color:#a0a0a0;"

		/*frmDocument.onkeypress = function hidecal1 () { 
			if (event.keyCode==27) 
			{
				hideCalendar()
			}
		}
		frmDocument.onclick = function hidecal2 () { 		
			if (!bShow)
			{
				hideCalendar()
			}
			bShow = false
		}*/

		initCal();
		HolidaysCounter = 0
		Holidays = new Array()
		
		
		

		addHoliday(25,12,0, "Christmas Day")
		addHoliday(1,1,0, "New Year's Day");
	    
	}	
  function jWicCalSetSelection(sDateSelection) {
     dateSelected=sDateSelection;
  }
  function jWicCalSetImageDir(newDir) {
  	imgDir = newDir;
  }
  
  function getIntervalID1() {
  	return intervalID1;
  }
  function setIntervalID1(newInterval) {
  	intervalID1 = newInterval;
  }	
  function getIntervalID2() {
  	return intervalID2;
  }
  function setIntervalID2(newInterval) {
  	intervalID2 = newInterval;
  }	
  function getTimeoutID1() {
  	return timeoutID1;
  }
  function setTimeoutID1(newTimeoutID1) {
  	timeoutID1 = newTimeoutID1;
  }
  function getTimeoutID2() {
  	return timeoutID2;
  }
  function setTimeoutID2(newTimeoutID2) {
  	timeoutID2 = newTimeoutID2;
  }
  function setMonthSelected(newMonth) {
  	monthSelected = newMonth;
  }
	this.init = initLib;
	this.show = popUpCalendar;
	this.hide = hideCalendar;
	this.close = closeCalendar;
	this.select = jWicCalSetSelection;
	this.setImageDir = jWicCalSetImageDir;
	this.swapImage = calSwapImage;
	this.getIntervalID1 = getIntervalID1;
	this.setIntervalID1 = setIntervalID1;
	this.getIntervalID2 = getIntervalID2;
	this.setIntervalID2 = setIntervalID2;
	this.getTimeoutID1 = getTimeoutID1;
	this.setTimeoutID1 = setTimeoutID1;
	this.getTimeoutID2 = getTimeoutID2;
	this.setTimeoutID2 = setTimeoutID2;
	this.decMonth = decMonth;
	this.incMonth = incMonth;
	this.decYear = decYear;
	this.incYear = incYear;
	this.popUpMonth = popUpMonth;
	this.popDownMonth = popDownMonth;
	this.popUpYear = popUpYear;
	this.popDownYear = popDownYear;
	this.constructCalendar = constructCalendar;
	this.setMonthSelected = setMonthSelected;
	this.selectYear = selectYear;
	this.StartDecMonth = StartDecMonth;
	this.StartIncMonth = StartIncMonth;
	this.addHoliday = addHoliday;
  }