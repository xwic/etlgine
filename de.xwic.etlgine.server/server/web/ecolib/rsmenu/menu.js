/**
 * ReallySimpleMenu v.02
 *
 * Lightweight JavaScript cascading menu API.
 * Highlight: The menu can be html rendered with no JavaScript call.
 * JavaScript is only used to display the tables.
 *
 * author Jens Bornemann, 16-Jan-2006, jbornema@netapp.com
 *
 * Changes:
 * 2006-02-09 JBO
 *		- Added index start id name for velocity: $velocityCount, by default it's 1
 *		- Changed css class names
 **/
var reallySimpleMenu = new ReallySimpleMenuClass();
function rsMenu() {
	return reallySimpleMenu;
}
function ReallySimpleMenuClass() {
	/* public methods */
	this.update = update;
	this.closeCheck = closeCheck;
	this.setIndexStart = setIndexStart;
	
	/* Milliseconds to check menu closing */
	var msec = 100;
	/* public api method to ReallySimpleMenu */
	var api = "rsMenu()";
	/* index start var, by default 1, only used to fix table-display in firefox */
	var indexStart = 1;
	/* private fields */
	var menuOpen = new Object(); // keys of root menu with array of open table objects
	var menuFocus = new Object(); // keys of root menu with current focused table
	var menuSelected = new Object(); // keys of root menu with current selected item (td)
	
	function update(obj, show) {		
		var isTD = obj.tagName == "TD";
		if (isTD) {						
			handleTD(obj, show);
		} else {			
			handleTABLE(obj, show);
		}
	}
	function handleTABLE(table, show) {
		// don't change any css, update the objects
		var table_id = table.id;
		// get root name for the menu
		var i = table_id.indexOf("/");
		if (i != -1) {
			var root_name = table_id.substring(0, i);
		} else {
			return;
		}		
		
		if (show) {
			// update current focused menu
			menuFocus[root_name] = table_id;
		} else {
			// check focus
			if (menuFocus[root_name] == table_id)
				menuFocus[root_name] = null;
			// timeout for closing menu
			window.setTimeout(api + ".closeCheck('" + root_name + "')", msec);
		}	
	}
	function handleTD(td, show) {
		// set td to highlighted
		td.className = show ? "rsmenu_nav_menu_select" : "rsmenu_nav_menu";
		// get table id from td of the menu
		var table_id = td.id.replace("_td", "_table");
		// get root name for the menu
		var i = table_id.indexOf("/");
		if (i != -1) {
			var root_name = table_id.substring(0, i);
		} else {
			return;
		}
		
		// set current menu (for the focus)
		i = table_id.lastIndexOf("/");
		if (i != -1) {
			var focus_table_id = table_id.substring(0, i);
		} else {
			return;
		}
		if (show) {
			// update current focused menu
			menuFocus[root_name] = focus_table_id;
			// set select td
			menuSelected[root_name] = table_id;
		} else {
			// check selected td
			if (menuSelected[root_name] == table_id)
				menuSelected[root_name] = null;
			// check focus
			if (menuFocus[root_name] == focus_table_id)
				menuFocus[root_name] = null;
			// timeout for closing menu
			window.setTimeout(api + ".closeCheck('" + root_name + "')", msec);
			return;			
		}
		var table = document.getElementById(table_id);		
		if (table) {
			if (show) {
				// add menu to array of menuOpen (to close it later)
				var menus = menuOpen[root_name];
				if (!menus) {
					menus = new Array;
					menuOpen[root_name] = menus;
				}
				menus[table.id] = table;
				// show menu by changing its class
				table.className = td.className;
				var dim = 'px';
				// position menu
				var x = 0;
				var y = 0;
				for (var obj = td; obj != null && obj.id.indexOf("rsmenu-") != -1; obj = obj.offsetParent) {
					y += obj.offsetTop;
					x += obj.offsetLeft;
				}
				if ((td.id.indexOf("/") == td.id.lastIndexOf("/")) && table.orientation == '0' )
					// drop down
					y += td.offsetHeight;
				else
					// drop to the right
					x += td.offsetWidth;
				table.style.top = y + dim;
				table.style.left = x + dim;
				// fix table width for strange firefox behaviour
				td = document.getElementById(td.id + "/" + indexStart);
				if (td) {
					table.style.width = (td.offsetWidth + 8) + dim;
				}
			}
		}
	}
	function closeCheck(root_name) {
		// check for all menus to close now
		var menus = menuOpen[root_name];
		var menu_focus = menuFocus[root_name];
		var menu_selected = menuSelected[root_name];
		if (menu_selected)
			// td is selected, change its id to a table id
			menu_selected = menu_selected.replace("_td", "_table");
		// only one menu has the focus, so close the rest that doesn't belong to it
		for (var table_id in menus) {
			if (!menu_focus || (menu_selected && menu_selected.indexOf(table_id) != 0)
				|| (!menu_selected && menu_focus.indexOf(table_id) != 0)) {
				// close menu
				var table = menus[table_id];
				if (table) {
					table.className = "rsmenu_nav_menu";
					menus[table_id] = null;
				}
			}				
		}
	}
	function setIndexStart(idxStart) {
		indexStart = idxStart;
	}
}