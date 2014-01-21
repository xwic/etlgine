/**
 * 
 */
package de.xwic.etlgine.mail.impl;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.InternetHeaders;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.internet.MimeUtility;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.xwic.etlgine.mail.IAttachment;
import de.xwic.etlgine.mail.IMail;
import de.xwic.etlgine.mail.IMailAgent;
import de.xwic.etlgine.mail.IMailManager;


/**
 * Implementation of mail sending via javax.mail.
 * 
 * @author Ronny Pfretzschner
 *
 */
public class JavaMailManager implements IMailManager {

	private Properties props = null;
	
	private Log log = LogFactory.getLog(JavaMailManager.class);
	
	private Map<IMailAgent, Thread> agents = new HashMap<IMailAgent, Thread>();
	
	/**
	 * Creates the manager with the given config path.
	 * 
	 * @param configPropertiesPath
	 */
	public JavaMailManager(Properties properties) {
		if (properties == null || properties.isEmpty()) {
			throw new IllegalArgumentException("Mailproperties are not set!");
		}
		props = properties;
	}
	
	
	/* (non-Javadoc)
	 * @see com.netapp.mail.IMailManager#sendEmail(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	public void sendEmail(String content, String subject, String[] toAddresses, String[] toAddressesCC,
			String fromAddress) {

		try {
			Session session = Session.getDefaultInstance(props);
			MimeMessage msg = new MimeMessage(session);
			
			msg.setSubject(subject);
			msg.setContent(content, "text/html");
			
			Address from = new InternetAddress(fromAddress, fromAddress);
			msg.setFrom(from);
			
			Address[] addressesMain = new Address[toAddresses.length];
			
			for (int i = 0; i < toAddresses.length; i++) {
				addressesMain[i] = new InternetAddress(toAddresses[i]);
			}
			
			Address[] addressescc = new Address[toAddressesCC.length];
			
			for (int i = 0; i < toAddressesCC.length; i++) {
				addressescc[i] = new InternetAddress(toAddressesCC[i]);
			}
			
			
			msg.setRecipients(Message.RecipientType.TO, addressesMain);
			msg.setRecipients(Message.RecipientType.CC, addressescc);
			
			Transport.send(msg);
			
		} catch (Exception e) {
			//RPF: TODO a better error handling is necessary
			//maybe with validationresult and error messages
			log.error("Error sending mail", e);
		}
	}

	
	/*
	 * (non-Javadoc)
	 * @see com.netapp.mail.IMailManager#startEmailAgents()
	 */
	public void startEmailAgents() {
		
		for (Iterator<IMailAgent> iterator = agents.keySet().iterator(); iterator.hasNext();) {
			IMailAgent agentRunnable = iterator.next();
			Thread agent = agents.get(agentRunnable);
			
			//is already running, stop old agent
			if (agentRunnable != null && agent != null) {
				log.info("Agent already running, shut down agent and restarting it now...");
				log.info("Stopping Email agent now...");
				agentRunnable.exitAgent();
				agent.interrupt();
			}
			
			agent.setDaemon(true);
			agent.setName(agentRunnable.getAgentId());
			agent.start();
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.netapp.mail.IMailManager#stopEmailAgents()
	 */
	public void stopEmailAgents() {
		for (Iterator<IMailAgent> iterator = agents.keySet().iterator(); iterator.hasNext();) {
			IMailAgent agentRunnable = iterator.next();
			Thread agent = agents.get(agentRunnable);

			if (agentRunnable != null && agent != null) {
				log.info("Stopping Email agent now...");
				agentRunnable.exitAgent();
				agent.interrupt();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.netapp.mail.IMailManager#registerMailAgent(com.netapp.mail.IMailAgent)
	 */
	public void registerMailAgent(IMailAgent mailAgent) {
		agents.put(mailAgent, new Thread(mailAgent));
	}

	/* (non-Javadoc)
	 * @see de.pol.netapp.spc.model.util.mail.IMailManager#sendEmail(de.pol.netapp.spc.model.util.mail.IMail)
	 */
	public void sendEmail(IMail mail) throws Exception {
		Session session = Session.getDefaultInstance(props);
		MimeMessage msg = new MimeMessage(session);
		
		msg.setSubject(mail.getSubject());
		
		Address from = new InternetAddress(mail.getSenderAddress(), mail.getSenderAddress());
		msg.setFrom(from);
		
		msg.setRecipients(Message.RecipientType.TO, convertToAddresses(mail.getToAddresses()));
		if(mail.getCcAddresses() != null) {
			msg.setRecipients(Message.RecipientType.CC, convertToAddresses(mail.getCcAddresses()));
		}

		List<IAttachment> attachments = mail.getAttachments();
		if(attachments == null || attachments.size() == 0) {
			msg.setContent(mail.getContent(), "text/html");
		}else {
			MimeMultipart multipart = new MimeMultipart();
			MimeBodyPart part = new MimeBodyPart(new InternetHeaders(), mail.getContent().getBytes());
			part.addHeader(IMail.CONTENT_TYPE, "text/html");
			multipart.addBodyPart(part);
			
			for(IAttachment att: attachments) {
				part = new MimeBodyPart(new InternetHeaders(), encodeBase64(att.getData()));
				part.addHeader(IMail.MIME_VERSION, "1.0");
				part.addHeader(IMail.CONTENT_TYPE, att.getContentType() + "; name=\"" + att.getFileName() +"\"");
				part.addHeader(IMail.CONTENT_ENCODING, "base64");
				multipart.addBodyPart(part);
			}
			msg.setContent(multipart);
		}
		
		Transport.send(msg);
	}
	
	private byte[] encodeBase64(byte[] data) throws Exception {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		OutputStream out = MimeUtility.encode(bout, "base64");
		
		out.write(data);
		return bout.toByteArray();
	}
	
	/**
	 * @param addressList
	 * @return an address array
	 * @throws AddressException 
	 */
	private Address[] convertToAddresses(List<String> addressList) throws AddressException{
		Address[] ar = new Address[addressList.size()];
		
		int i = 0;
		for(String addr: addressList) {
			ar[i++] = new InternetAddress(addr);
		}
		return ar;
	}


}
