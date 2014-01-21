/**
 * 
 */
package de.xwic.etlgine.mail;

/**
 * @author Ronny Pfretzschner
 *
 */
public abstract class AbstractMailAgent implements IMailAgent {

	protected ITemplateEngine templateEngine = null;
	protected IMailManager mailEngine;
	
	protected String agentId = null;
	
	/**
	 * Creates the agent for the kit request reminder
	 * 
	 * @param manager
	 * @param daosupport
	 */
	public AbstractMailAgent(IMailManager manager, ITemplateEngine templateEngine) {
		if (manager == null) {
			throw new IllegalArgumentException("MailManager must be set for mail agent!");
		}
		if (templateEngine == null) {
			throw new IllegalArgumentException("TemplateEngine must be set for mail agent!");
		}
		
		this.mailEngine = manager;
		this.templateEngine = templateEngine;
		agentId = getAgentId();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((agentId == null) ? 0 : agentId.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final AbstractMailAgent other = (AbstractMailAgent) obj;
		if (agentId == null) {
			if (other.agentId != null)
				return false;
		} else if (!agentId.equals(other.agentId))
			return false;
		return true;
	}


	
	
}
