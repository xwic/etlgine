{//status control
	afterUpdate: function(){
		var options = $control.buildJsonOptions();
		options.controlID = '$control.controlID'
		ETLAdmin.StatusControl.initialize(options)
	},
	destroy: function(){
		var options = $control.buildJsonOptions();
		options.controlID = '$control.controlID'
		ETLAdmin.StatusControl.destroy(options);
	}
}
