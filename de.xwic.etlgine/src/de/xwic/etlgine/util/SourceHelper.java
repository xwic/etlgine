/**
 * 
 */
package de.xwic.etlgine.util;

import java.io.File;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import de.xwic.etlgine.impl.ETLProcess;
import de.xwic.etlgine.sources.FileSource;

/**
 * @author JBORNEMA
 *
 */
public class SourceHelper {

	/**
	 * FileOrder enum for adding the file sources ordered by modified date asc or desc.
	 * @author JBORNEMA
	 *
	 */
	public enum FileOrder {
		/**
		 * Underlying file system
		 */
		NONE,
		/**
		 * File last modified ascending
		 */
		MODIFIED_ASC,
		/**
		 * File last modified descending
		 */
		MODIFIED_DESC
	}
	
	/**
	 * Helper key for having the files in right order.
	 * @author JBORNEMA
	 *
	 */
	private class FileKey implements Comparable<Object> {
		Integer idx;
		File file;
		FileOrder order;
		public FileKey(File file, FileOrder order, int idx) {
			this.file = file;
			this.order = order;
			this.idx = idx;
		}

		@Override
		public int compareTo(Object o) {
			FileKey fk = (FileKey)o;
			int c = 0;
			switch (order) {
				case MODIFIED_ASC : {
					c = new Date(file.lastModified()).compareTo(new Date(fk.file.lastModified()));
					break;
				}
				case MODIFIED_DESC : {
					c = new Date(fk.file.lastModified()).compareTo(new Date(file.lastModified()));
					break;
				}
			}
			if (c == 0) {
				c = idx.compareTo(fk.idx);
			}
			return c;
		}
	}

	/**
	 * Add all files in specified path as FileSource to the process. 
	 * File order is determined by underlying file system.
	 * 
	 * @param process
	 * @param path
	 * @return
	 */
	public Collection<FileSource> addFileSources(ETLProcess process, String path) {
		return addFileSources(process, path, null);
	}

	/**
	 * Add all files matching the regular expression in specified path as FileSource to the process.
	 * File order is determined by underlying file system.
	 * 
	 * @param process
	 * @param path
	 * @param regexFilename
	 * @return
	 */
	public Collection<FileSource> addFileSources(ETLProcess process, String path, String regexFilename) {
		return addFileSources(process, path, regexFilename, null);
	}
	
	/**
	 * Add all files matching the regular expression in specified path as FileSource to the process. 
	 * File order as specified.
	 * 
	 * @param process
	 * @param path
	 * @param regexFilename
	 * @param order
	 * @return
	 */
	public Collection<FileSource> addFileSources(ETLProcess process, String path, String regexFilename, FileOrder order) {
		Map<FileKey, FileSource> sources = new TreeMap<FileKey, FileSource>();
		
		File filepath = new File(path);
		if (filepath.isDirectory()) {

			if (order == null) {
				order = FileOrder.MODIFIED_ASC;
			}
			Pattern filepattern = regexFilename != null ? Pattern.compile(regexFilename) : null; 
			
			int idx = 0;
			File[] files = filepath.listFiles();
			for (File file : files) {
				
				if (filepattern == null || filepattern.matcher(file.getName()).matches()) {
					FileSource fs = new FileSource(file);
					sources.put(new FileKey(file, order, idx++), fs);
				}
			}
		}
		
		for (FileSource fs : sources.values()) {
			process.addSource(fs);
		}
		
		return sources.values();
	}
}
