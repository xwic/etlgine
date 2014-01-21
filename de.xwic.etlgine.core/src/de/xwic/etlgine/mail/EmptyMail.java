/**
 * 
 */
package de.xwic.etlgine.mail;

import java.util.List;

/**
 * @author Oleksiy Samokhvalov
 * 
 */
public class EmptyMail implements IMail {

	private List<IAttachment> attachments;
	private List<String> ccAddresses;
	private String content;
	private String senderAddress;
	private String subject;
	private List<String> toAddresses;

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.pol.netapp.spc.model.util.mail.IMail#getAttachments()
	 */
	public List<IAttachment> getAttachments() {
		return attachments;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.pol.netapp.spc.model.util.mail.IMail#getCcAddresses()
	 */
	public List<String> getCcAddresses() {
		return ccAddresses;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.pol.netapp.spc.model.util.mail.IMail#getContent()
	 */
	public String getContent() {
		return content;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.pol.netapp.spc.model.util.mail.IMail#getSenderAddress()
	 */
	public String getSenderAddress() {
		return senderAddress;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.pol.netapp.spc.model.util.mail.IMail#getSubject()
	 */
	public String getSubject() {
		return subject;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.pol.netapp.spc.model.util.mail.IMail#getToAddresses()
	 */
	public List<String> getToAddresses() {
		return toAddresses;
	}

	/**
	 * @param attachments
	 *            the attachments to set
	 */
	public void setAttachments(List<IAttachment> attachments) {
		this.attachments = attachments;
	}

	/**
	 * @param ccAddresses
	 *            the ccAddresses to set
	 */
	public void setCcAddresses(List<String> ccAddresses) {
		this.ccAddresses = ccAddresses;
	}

	/**
	 * @param content
	 *            the content to set
	 */
	public void setContent(String content) {
		this.content = content;
	}

	/**
	 * @param senderAddress
	 *            the senderAddress to set
	 */
	public void setSenderAddress(String senderAddress) {
		this.senderAddress = senderAddress;
	}

	/**
	 * @param subject
	 *            the subject to set
	 */
	public void setSubject(String subject) {
		this.subject = subject;
	}

	/**
	 * @param toAddresses
	 *            the toAddresses to set
	 */
	public void setToAddresses(List<String> toAddresses) {
		this.toAddresses = toAddresses;
	}

}