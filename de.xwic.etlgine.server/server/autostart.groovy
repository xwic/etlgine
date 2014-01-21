import de.xwic.etlgine.*;
import de.xwic.etlgine.server.*

class TestListener extends ServerContextAdapter implements IServerContextListener {
	
	public void jobExecutionEnd(ServerContextEvent event) {
		
		println "job execution finished: " + event.getJob().getName();
		
	}
	
}

context.addServerContextListener(new TestListener());