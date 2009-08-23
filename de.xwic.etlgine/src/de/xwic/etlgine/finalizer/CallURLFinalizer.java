/**
 * 
 */
package de.xwic.etlgine.finalizer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IProcessContext;
import de.xwic.etlgine.IProcessFinalizer;
import de.xwic.etlgine.Result;

/**
 * Invokes a URL if the result is successfull.
 * @author lippisch
 */
public class CallURLFinalizer implements IProcessFinalizer {

	private String url = null;
	
	/**
	 * 
	 */
	public CallURLFinalizer() {
		super();
	}

	/**
	 * @param url
	 */
	public CallURLFinalizer(String url) {
		super();
		this.url = url;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IProcessFinalizer#onFinish(de.xwic.etlgine.IProcessContext)
	 */
	public void onFinish(IProcessContext context) throws ETLException {

			
			try {
				if (context.getResult() == Result.SUCCESSFULL) {
					if (url == null) {
						context.getMonitor().logError("can not call URL as none is specified.");
					} else {
						context.getMonitor().logInfo("Invoking URL " + url);
						 
						URL _url = new URL(url);
						InputStream in = _url.openStream();
						in.read();	// read just one byte and then close it.
						in.close();
					}
				}
			} catch (IOException ioe) {
				context.getMonitor().logError("Refresh failed. URL: " + url, ioe);
			}

		
	}

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}

}
