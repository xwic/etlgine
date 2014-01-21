import de.xwic.etlgine.config.*;
/*
 *	This script defines the import and mapping details.
 */

def listener = etlgine.addPathListener("test", "Extract*.csv");
listener.onNewSource 
 
config.source.type = Source.TYPE_CSV
config.source.path = "e:\\"