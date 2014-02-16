/*
 * Copyright 2005 jWic group (http://www.jwic.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ------------------------------------------------------------------------
 * This file contains some functions to enhance compatibility with previous
 * jWic versions.
 */

/**
 * This file contains functions used by controls in the compatibility pack.
 */

(function($) {

	$.extend(JWic.controls,
		{
		
			/**
			 * Button control.
			 */
			ButtonLegacy : {
				
				initialize : function(tblElm, btnElm) {
					if(tblElm.attr && "true" == tblElm.attr("_ctrlEnabled")) {
						tblElm.bind('mouseover', function(){
							tblElm.addClass('j-hover');
						});
						tblElm.bind('mouseout', function(){
							tblElm.removeClass('j-hover');
							
						});
						tblElm.bind('click', JWic.controls.ButtonLegacy.clickHandler);
						btnElm.bind('click', JWic.controls.ButtonLegacy.clickHandler);
					}
				},
				
				destroy : function(tblElm, btnElm) {
					if(tblElm.attr && "true" == tblElm.attr("_ctrlEnabled")) {
						tblElm.unbind("mouseover");
						tblElm.unbind("mouseout");
						tblElm.unbind("click", JWic.controls.ButtonLegacy.clickHandler);
						btnElm.unbind("click", JWic.controls.ButtonLegacy.clickHandler);
					}
				},
				/**
				 * Invoked when the button is clicked.
				 */
				clickHandler : function(e) {
					e.stopPropagation();
					var elm = jQuery(e.target);
					while (!elm.attr('id') || elm.attr('id').indexOf('tbl_') != 0) {
						elm = jQuery(elm).parent();
					}
					var ctrlId = elm.attr('id').substring(4);
					var msg = elm.attr("_confirmMsg");
					if (msg && msg != "") {
						if (!confirm(msg)) {
							return false;
						}
					}
					JWic.fireAction(ctrlId, 'click', '');
				}
	
			} /** /Button */
		
		
		
		}
	);
})(jQuery);