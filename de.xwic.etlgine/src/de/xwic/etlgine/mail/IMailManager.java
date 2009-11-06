/**
 * 
 */
package de.xwic.etlgine.mail;


/**
 * Simple Mail Manager.
 * 
 * @author Ronny Pfretzschner
 *
 */
public interface IMailManager {
	
	/**
	 * Send an email.
	 * 
	 * @param content, mail content
	 * @param subject
	 * @param toAddresses, recipients
	 * @param toAddressesCC, recipients on CC
	 * @param fromAddress, the one who sends the mail (mail address of actual user)
	 */
	public void sendEmail(String content, String subject, String[] toAddresses, String[] toAddressesCC,
			String fromAddress);
	
	/**
	 * Sends the email.
	 * 
	 * @param email
	 */
	public void sendEmail(IMail email) throws Exception;
	
	/**
	 * Starts serverside registered mail agents (thread) for automatic email sending.
	 * 
	 */
	public void startEmailAgents();
	
	/**
	 * Stops the mail agents.
	 */
	public void stopEmailAgents();
	
	/**
	 * Registers a custom made mail agent to get started running
	 * on the server.
	 * 
	 * @param mailAgent
	 */
	public void registerMailAgent(IMailAgent mailAgent);
}
