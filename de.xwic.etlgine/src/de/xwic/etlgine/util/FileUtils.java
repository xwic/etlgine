/**
 * 
 */
package de.xwic.etlgine.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * @author lippisch
 */
public class FileUtils {

	/**
	 * Copy the source file to the specified target folder or file. If the target is
	 * a folder, the source file name is preserved.
	 * 
	 * @param source
	 * @param target
	 * @throws IOException
	 */
	public static void copyFile(File source, File target) throws IOException {
		
		if (target.isDirectory()) {
			target = new File(target, source.getName());
		}
		
		FileChannel inChannel = new FileInputStream(source).getChannel();
		FileChannel outChannel = new FileOutputStream(target, false).getChannel();
        try {
        	//int maxCount = (64 * 1024 * 1024) - (32 * 1024); // copy in blocks of 64 MB because of windows limitations
        	int maxCount = (1024 * 1024); // copy in blocks of 1 MB.
            long size = inChannel.size();
            long position = 0;
            while (position < size) {
               position += 
                 inChannel.transferTo(position, maxCount, outChannel);
            }	
        } finally {
            if (inChannel != null) inChannel.close();
            if (outChannel != null) outChannel.close();
        }
		
	}

	/**
	 * Recursively delete the folder.
	 * @param file
	 */
	public static void deleteEntireFolder(File folder) {

		if (folder.isFile()) {
			folder.delete();
		} else {
			File[] files = folder.listFiles();
			for (File f : files) {
				if (f.isDirectory()) {
					deleteEntireFolder(f);
				} else {
					f.delete();
				}
			}
			folder.delete();
		}
		
	}

}
