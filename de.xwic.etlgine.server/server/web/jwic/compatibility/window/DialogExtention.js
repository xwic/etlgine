function addMinimizeToDialog(dialog){
	if(dialog !== undefined){
		
		
		dialog.bind('dialogresize',function(){
			jQuery.data(dialog,'width',dialog.parent().width());
			dialog.parent().css('height', 'auto');
			if(!dialog.is(':visible')){
				jQuery.data(dialog,'isMinimized',false);
				jQuery.data(dialog,'isMaximized',false);
				jQuery.data(dialog,'originalPosition',dialog.parent().offset());
				dialog.parent().css('width',jQuery.data(dialog,'width'));	
				dialog.show();	
				dialog.css('width','auto');
			}
		});
		
		var titlebar = dialog.parents('.ui-dialog').find('.ui-dialog-titlebar');
		//minimize
		jQuery('<a href="#" id="${control.controlID}_minimize" role="button" class="ui-corner-all ui-dialog ui-dialog-titlebar-min"><span class="ui-icon ui-icon-minusthick">minimize</span></button>')
			.appendTo(titlebar)
			.mouseover(function(){
				jQuery(this).addClass('ui-state-hover');
			})
			.mouseout(function(){
				jQuery(this).removeClass('ui-state-hover');
			})
			.click(function() {
				(function(){
					var dialogParent = dialog.parent();					
					dialogParent.css('overflow','hidden');
					if(jQuery.data(dialog,'isMaximized')){
						jQuery.data(dialog,'isMaximized',false);
						dialogParent.offset(jQuery.data(dialog,'originalPosition'));
						dialogParent.css('width',jQuery.data(dialog,'width'));	
						dialogParent.css('height', 'auto');
					}
					
					if(!jQuery.data(dialog,'isMinimized')){
						jQuery.data(dialog,'isMinimized',true);
						jQuery.data(dialog,'width',dialogParent.width());					
						dialog.hide();
						
					}else{
						jQuery.data(dialog,'isMinimized',false);
						dialogParent.css('width',jQuery.data(dialog,'width'));	
						dialog.show();				
					}
				})();
				dialog.trigger({type:'minimize',source:dialog});
			});
		
	}
}

function addMaximizeToDialog(dialog){
	if(dialog!==undefined ){
		
		dialog.bind('dialogresize',function(){
			jQuery.data(dialog,'width',dialog.parent().width());
			dialog.parent().css('height', 'auto');
			if(!dialog.is(':visible')){
				jQuery.data(dialog,'isMinimized',false);
				jQuery.data(dialog,'isMaximized',false);
				jQuery.data(dialog,'originalPosition',dialog.parent().offset());
				dialog.parent().css('width',jQuery.data(dialog,'width'));	
				dialog.show();	
				dialog.css('width','auto');
			}
		});
		
		
		var titlebar = dialog.parents('.ui-dialog').find('.ui-dialog-titlebar');			
		jQuery('<a href="#" id="${control.controlID}_maximize" role="button" class="ui-corner-all ui-dialog ui-dialog-titlebar-max"><span class="ui-icon ui-icon-newwin">maximize</span></button>')
		.appendTo(titlebar)
		.mouseover(function(){
			jQuery(this).addClass('ui-state-hover');
		})
		.mouseout(function(){
			jQuery(this).removeClass('ui-state-hover');
		})
		.click(function() {
			(function(){
				var dialogParent = dialog.parent();
				dialogParent.css('overflow','hidden');
				if(jQuery.data(dialog,'isMinimized')){
					jQuery.data(dialog,'isMinimized',false);	
					dialogParent.css('width',jQuery.data(dialog,'width'));	
					dialog.show();
					dialog.css('width','auto');
				}
				
				if(!jQuery.data(dialog,'isMaximized')){				
					jQuery.data(dialog,'isMaximized',true);
					jQuery.data(dialog,'originalPosition',dialogParent.offset());
					jQuery.data(dialog,'width',dialogParent.width());
					
					dialogParent.css('width',jQuery(window).width());
					dialogParent.css('height',jQuery(window).height());
					dialogParent.offset({top:0,left:0});
					dialog.css('width','auto');
				}else{
					jQuery.data(dialog,'isMaximized',false);
					dialogParent.offset(jQuery.data(dialog,'originalPosition'));					
					dialogParent.css('width',jQuery.data(dialog,'width'));
					dialogParent.css('height', 'auto');
				}
				
			})();
			
			dialog.trigger({type:'maximize',source:dialog})
		});
		
		
	}
}