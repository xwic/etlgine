package de.xwic.etlgine.mail;

/**
 * You can make some custome mailagent with project
 * dependend business logic. this mail agent can
 * then be registered on the mailmanager. it
 * will handle the start and stop of the agent.
 * 
 * @author Ronny Pfretzschner
 *
 */
public interface IMailAgent extends Runnable {

	/**
	 * Exits the mail agent.
	 */
	public abstract void exitAgent();

	
	public abstract String getAgentId();
	
	public boolean equals(Object obj);
}