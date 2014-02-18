/**
 * 
 */
package de.xwic.etlgine.server.admin.datapool;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import de.jwic.base.Control;
import de.jwic.base.IControlContainer;
import de.jwic.base.IResourceControl;
import de.jwic.base.IncludeJsOption;
import de.jwic.base.JavaScriptSupport;
import de.xwic.cube.IDataPool;
import de.xwic.cube.IDimensionElement;
import de.xwic.cube.xlsbridge.CubeToExcel;

/**
 * @author lippisch
 *
 */
@JavaScriptSupport
public class XlsDownloadControl extends Control implements IResourceControl {

	private boolean showDownload = false;
	private IDataPool dataPool = null;
	private InputStream inTpl = null;
	private String logInfo = "";
	private List<IDimensionElement> filters;

	/**
	 * @param container
	 * @param name
	 */
	public XlsDownloadControl(IControlContainer container, String name) {
		super(container, name);
	}

	/**
	 * Initiate the download
	 * @param inTpl
	 * @param dataPool
	 * @param filters 
	 */
	public void startDownload(InputStream inTpl, IDataPool dataPool, List<IDimensionElement> filters) {
		this.inTpl = inTpl;
		this.dataPool = dataPool;
		this.filters = filters;
		
		setShowDownload(true);
		logInfo = "";
		requireRedraw();
	}
	
	public void actionRefresh() {
		requireRedraw();
	}
	
	/* (non-Javadoc)
	 * @see de.jwic.base.IResourceControl#attachResource(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	public void attachResource(HttpServletRequest req, HttpServletResponse res)	throws IOException {

		CubeToExcel cte = new CubeToExcel(dataPool);
		cte.setFilters(filters);
		try {
			String filename = "xcube_export.xls";
			HSSFWorkbook wb = cte.createWorkbook(inTpl);
	
			res.setContentType("application/x-msdownload");
			// added double quotes to fix chrome error: Error 349 (net::ERR_RESPONSE_HEADERS_MULTIPLE_CONTENT_DISPOSITION): Multiple Content-Disposition headers received. This is disallowed to protect against HTTP response splitting attacks. 
			res.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
			wb.write(res.getOutputStream());
			res.getOutputStream().close();
			logInfo = cte.getLog();
		} catch (Exception e) {
			log.error("Error generating workbook", e);
			logInfo = e.toString() + "\n\n" + cte.getLog();
		}

		
	}

	/**
	 * Returns the URL that calls the attachResource method.
	 * 
	 * @return
	 */
	@IncludeJsOption
	public String getDownloadURL() {
		return getSessionContext().getCallBackURL() + "&"
			+ URL_RESOURCE_PARAM + "=1&"
			+ URL_CONTROLID_PARAM + "=" + getControlID();
	}

	
	/**
	 * @return the showDownload
	 */
	@IncludeJsOption
	public boolean isShowDownload() {
		return showDownload;
	}

	/**
	 * @param showDownload the showDownload to set
	 */
	public void setShowDownload(boolean showDownload) {
		this.showDownload = showDownload;
	}

	/**
	 * @return the logInfo
	 */
	@IncludeJsOption
	public String getLogInfo() {
		return logInfo;
	}

	
}
