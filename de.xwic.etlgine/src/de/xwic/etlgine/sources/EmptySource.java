/**
 * 
 */
package de.xwic.etlgine.sources;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author JBORNEMA
 *
 */
public class EmptySource extends FileSource {
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.sources.FileSource#isOptional()
	 */
	public boolean isOptional() {
		return false;
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.sources.FileSource#isAvailable()
	 */
	public boolean isAvailable() {
		return true;
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.sources.FileSource#getName()
	 */
	public String getName() {
		return "Empty Source";
	}
	
	/* (non-Javadoc)
	 * @see de.xwic.etlgine.sources.FileSource#getInputStream()
	 */
	public InputStream getInputStream() throws IOException {
		return new ByteArrayInputStream(new byte[0]);
	};

}
