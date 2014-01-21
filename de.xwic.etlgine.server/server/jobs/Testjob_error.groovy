/*
 *	Define a process chain that does a test job..
 */

import de.xwic.etlgine.trigger.*;
 
job.trigger = new JobExecutedTrigger(context, "TestJob");
job.chainScriptName = "xxTestjob/DummyProcessChain.groovy";

