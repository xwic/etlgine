/*
 *	Define a process chain that does a test job..
 */

import de.xwic.etlgine.trigger.*;

job.jobId = "TestJob";
job.trigger = new ScheduledTrigger(02, 01); // run at 22:01
//job.trigger = new FileLookupTrigger("c:\\temp\\emea_reporting_2009-01-28.zip");
job.chainScriptName = "Testjob/DummyProcessChain.groovy";

