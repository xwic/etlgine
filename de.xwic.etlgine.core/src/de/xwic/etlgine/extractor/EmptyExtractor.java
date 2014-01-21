/**
 * 
 */
package de.xwic.etlgine.extractor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import de.xwic.etlgine.AbstractExtractor;
import de.xwic.etlgine.ETLException;
import de.xwic.etlgine.IDataSet;
import de.xwic.etlgine.IETLProcess;
import de.xwic.etlgine.IRecord;
import de.xwic.etlgine.ISource;
import de.xwic.etlgine.sources.FileSource;

/**
 * @author JBORNEMA
 *
 */
public class EmptyExtractor extends AbstractExtractor {

	protected static EmptyExtractor defaultExtractor = new EmptyExtractor();
	protected static ISource defaultSource = new FileSource() {
		
		public boolean isOptional() {
			return false;
		}
		
		public boolean isAvailable() {
			return true;
		}
		
		public String getName() {
			return "Empty Source";
		}
		
		public InputStream getInputStream() throws IOException {
			return new ByteArrayInputStream(new byte[0]);
		};
	};
	
	/**
	 * Register default empty extractor and empty source for process.
	 */
	public static void register(IETLProcess process) {
		process.setExtractor(defaultExtractor);
		if (process.getSources().size() == 0) {
			process.addSource(defaultSource);
		}
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IExtractor#close()
	 */
	public void close() throws ETLException {
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IExtractor#getNextRecord()
	 */
	public IRecord getNextRecord() throws ETLException {
		return null;
	}

	/* (non-Javadoc)
	 * @see de.xwic.etlgine.IExtractor#openSource(de.xwic.etlgine.ISource, de.xwic.etlgine.IDataSet)
	 */
	public void openSource(ISource source, IDataSet dataSet) throws ETLException {
	}

}
