/*
 *	Define a process chain that does a test job..
 */

import de.xwic.etlgine.trigger.*;

//job.trigger = new ScheduledTrigger(22, 29); // run at 22:00
job.trigger = new FileLookupTrigger("c:\\temp\\emea_reporting_2009-01-28.zip");
job.chainScriptName = "Testjob/DummyProcessChain.groovy";

