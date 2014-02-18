{//XlsDownloadControl.js
	afterUpdate: function(){
		var options = $control.buildJsonOptions(),
			id = '$control.controlID',
			escape = JWic.util.JQryEscape,
			logArea = jQuery('#log_'+escape(id)),
			iframe = jQuery('#iframe_'+escape(id));
		jQuery('#btn_'+escape(id)).on('click',function(){
			logArea.toggle();
		});
		if(options.showDownload){
			iframe[0].src = options.downloadURL;
		}
		if(options.logInfo){
			logArea.text(options.logInfo).show();
		}
	},
	destroy : function(){
		
	}

}