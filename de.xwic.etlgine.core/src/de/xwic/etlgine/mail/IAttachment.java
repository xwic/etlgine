/**
 * 
 */
package de.xwic.etlgine.mail;

/**
 * @author Oleksiy Samokhvalov
 *
 */
public interface IAttachment {
	/**
	 * @return the attachment content.
	 */
	byte[] getData();
	
	/**
	 * @return the content type of the attached data
	 */
	String getContentType();
	
	/**
	 * @return the file name of the attached data.
	 */
	String getFileName();
}
