import de.xwic.etlgine.processes.CleanUpLogMonitorProcess

import de.xwic.etlgine.IETLProcess

processChain.createProcessFromScript("Testjob", "Testjob/DummyProcess.groovy");

//CleanUpLogMonitorProcess cleanUpLogMonitorProcess = new CleanUpLogMonitorProcess(processChain.getGlobalContext(), "Log CleanUp ", "etlpooldb", "DEV", 30, 30, true)
//CleanUpLogMonitorProcess cleanUpLogMonitorProcess = new CleanUpLogMonitorProcess(processChain.getGlobalContext(), "Log CleanUp ", "etlpooldb")
//IETLProcess process = processChain.addCustomProcess(cleanUpLogMonitorProcess);
