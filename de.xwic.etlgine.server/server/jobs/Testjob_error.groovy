/*
 *	Define a process chain that does a test job..
 */

import de.xwic.etlgine.trigger.*;
 
job.trigger = new JobExecutedTrigger(context, "TestJob");
//job.trigger = new ScheduledTrigger(60);
job.chainScriptName = "xxTestjob/DummyProcessChain.groovy";

