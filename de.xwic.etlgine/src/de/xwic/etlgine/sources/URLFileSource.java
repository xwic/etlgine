/**
 * 
 */
package de.xwic.etlgine.sources;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * @author JBORNEMA
 *
 */
public class URLFileSource extends FileSource {

	protected URL url = null;
	
	/**
	 * 
	 */
	public URLFileSource() {

	}

	/**
	 * @param filename
	 */
	public URLFileSource(String filename) {
		super(filename);
	}

	/**
	 * @param file
	 */
	public URLFileSource(File file) {
		super(file);
	}
	
	public URLFileSource(URL url) {
		setUrl(url);
	}

	/**
	 * @return the url
	 */
	public URL getUrl() {
		return url;
	}

	/**
	 * @param url the url to set
	 */
	public void setUrl(URL url) {
		this.url = url;
	}

	@Override
	public InputStream getInputStream() throws IOException {
		return url == null ? super.getInputStream() : url.openStream();
	}
	
}
