
JWic.ecolib = {};
JWic.ecolib.controls = {

		ProcessInfo : {
			
			/**
			 * Request a status update
			 */
			updateContent : function(controlId) {
		
					var ctrl = jQuery("#pi_" + JWic.util.JQryEscape(controlId));
					if (ctrl && !ctrl.requestPending) {
						ctrl.requestPending = true;
						JWic.resourceRequest(controlId, function(ajaxResponse) {
							try {
								//JWic.log("HandleResponde: " + controlId);
								JWic.ecolib.controls.ProcessInfo.handleResponse(controlId, ajaxResponse);
							} catch (x) {
								// the control was probably removed. Force a regular refresh
								JWic.fireAction('', 'refresh', '');
							}
						});
					}
			},
			
			/**
			 * Handle the response from the server and render the status.
			 */
			handleResponse : function(controlId, resp) {
				var data = jQuery.parseJSON(resp.responseText);
				var container = document.getElementById("pi_" + controlId);
				if (container) { // view container might have been removed in the meantime
					if (data.monitor) {
						var m = data.monitor;
						var piLabel = jQuery("#pi_label_" + JWic.util.JQryEscape(controlId));
						var piProg = jQuery("#pi_progress_" + JWic.util.JQryEscape(controlId));
						var piProgBar = jQuery("#pi_progressbar_" + JWic.util.JQryEscape(controlId));
						var piVal = jQuery("#pi_values_" + JWic.util.JQryEscape(controlId));
						if (piLabel) {
							piLabel.html(m.infoText);
						}
						if (piVal) {
							if (m.max != 0) {
								piVal.html(m.value + " / " + m.max);
							} else {
								piVal.html(m.value);
							}
						}
						if (piProg && m.max != 0) {
							var w = piProg.width() - 6;
							var total = m.max - m.min;
							var pos = m.value - m.min;
							var pr = pos / total * w;
							if (!piProgBar.hasClass("progressBarActive")) {
								piProgBar.addClass("progressBarActive");
							}
							piProgBar.width( Math.abs(pr));
						}
						
					}
	
					container.requestPending = false;
					if (data.active) {
						
						window.setTimeout(function(){
							JWic.ecolib.controls.ProcessInfo.updateContent(controlId)
						}, 500);
					}
					if (data.globalRefresh) {
						JWic.fireAction('', 'refresh', '');
					}
				}
				
			}
		}

}