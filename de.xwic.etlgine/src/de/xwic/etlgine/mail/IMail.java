/**
 * 
 */
package de.xwic.etlgine.mail;

import java.util.List;

/**
 * @author Oleksiy Samokhvalov
 *
 */
public interface IMail {
	
	public static final String MIME_VERSION = "MIME-Version";
	public static final String CONTENT_TYPE = "Content-Type";
	public static final String CONTENT_ENCODING = "Content-Transfer-Encoding";
	
	/**
	 * @return the sender email.
	 */
	String getSenderAddress();
	
	/**
	 * @return the subject
	 */
	String getSubject();
	
	/**
	 * @return a list of the TO addresses.
	 */
	List<String> getToAddresses();
	
	/**
	 * @return a lis tof CC addresses.
	 */
	List<String> getCcAddresses();
	
	/**
	 * @return the email content.
	 */
	String getContent();
	
	/**
	 * @return a list of attachments.
	 */
	List<IAttachment> getAttachments();
}
