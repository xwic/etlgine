/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.jwic.base.Control;
import de.jwic.base.IControlContainer;
import de.jwic.base.IResourceControl;
import de.xwic.cube.ICube;
import de.xwic.etlgine.cube.CubeExportUtil;

/**
 * @author lippisch
 *
 */
public class CubeDownloadControl extends Control implements IResourceControl {

	private boolean showDownload = false;
	private ICube cube = null;
	private boolean leafsOnly;

	/**
	 * @param container
	 * @param name
	 */
	public CubeDownloadControl(IControlContainer container, String name) {
		super(container, name);
	}

	/**
	 * Initiate the download
	 * @param inTpl
	 * @param dataPool
	 * @param filters 
	 */
	public void startDownload(ICube cube, boolean leafsOnly) {
		this.cube = cube;
		this.leafsOnly = leafsOnly;
		setShowDownload(true);
		requireRedraw();
	}
	
	/* (non-Javadoc)
	 * @see de.jwic.base.IResourceControl#attachResource(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	public void attachResource(HttpServletRequest req, HttpServletResponse res)	throws IOException {

		CubeExportUtil exportUtil = new CubeExportUtil();
		try {
			String filename = cube.getKey() + ".csv";
			res.setContentType("application/x-msdownload");
			// added double quotes to fix chrome error: Error 349 (net::ERR_RESPONSE_HEADERS_MULTIPLE_CONTENT_DISPOSITION): Multiple Content-Disposition headers received. This is disallowed to protect against HTTP response splitting attacks. 
			res.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
			exportUtil.export(cube, res.getOutputStream(), leafsOnly);
			res.getOutputStream().close();
		} catch (Exception e) {
			log.error("Error generating CSV File", e);
		}

		
	}

	/**
	 * Returns the URL that calls the attachResource method.
	 * 
	 * @return
	 */
	public String getDownloadURL() {
		return getSessionContext().getCallBackURL() + "&"
			+ URL_RESOURCE_PARAM + "=1&"
			+ URL_CONTROLID_PARAM + "=" + getControlID();
	}

	
	/**
	 * @return the showDownload
	 */
	public boolean isShowDownload() {
		return showDownload;
	}

	/**
	 * @param showDownload the showDownload to set
	 */
	public void setShowDownload(boolean showDownload) {
		this.showDownload = showDownload;
	}
	
}
