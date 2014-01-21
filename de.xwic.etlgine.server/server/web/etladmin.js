
	var xetlCtrlId;
	var xetlContainer;
	var xetlRequest;
	function xetl_StartRefresh(ctrlId) {
		xetlCtrlId = ctrlId;
		xetlContainer = document.getElementById("srvStatus_" + ctrlId);
		
		xetlRequest = jWic_sendResourceRequest(xetlCtrlId, xetl_processResponse);
		
	}
	
	function xetl_processResponse() {
		
		if (xetlCtrlId != null && xetlRequest != null) {
			if (xetlRequest.readyState == 4 && xetlRequest.status == 200) {
				var resultString = xetlRequest.responseText;
				
				xetlContainer.innerHTML = resultString;
				
				window.setTimeout("xetl_statusRefresh()", 3000);
				xetlRequest = null;
			}
		}
		
	}
	
	function xetl_statusRefresh() {
		xetlRequest = jWic_sendResourceRequest(xetlCtrlId, xetl_processResponse);
	}