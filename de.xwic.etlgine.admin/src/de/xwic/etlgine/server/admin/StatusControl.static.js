ETLAdmin.StatusControl = (function($,JWic,util){
	var resourceRequest = JWic.resourceRequest,
		escape = util.JQryEscape,
		ids = {};
	function StatusUpdate(data){
		this.bind = function(templateId){
			return $(templateId).tmpl(data);
		};
	}
	function doRequest(options){
		var id = options.controlID,
			control = $('#srvStatus_'+escape(id));
		resourceRequest(id,function(resp){
			var statusUpdate = $.parseJSON(resp.responseText),
				boundContent = new StatusUpdate(statusUpdate).bind('#'+escape(id)+"_template");
			control.html('').append(boundContent);
			
			ids[id] = window.setTimeout(function(){
				doRequest(options);
			},options.refreshInterval);
		});
	}
	
	return {
		initialize : function(options){
			doRequest(options);
		},
		destroy : function(options){
			window.clearTimeout(options.controlID);
		}
	};
}(jQuery,JWic,JWic.util));
