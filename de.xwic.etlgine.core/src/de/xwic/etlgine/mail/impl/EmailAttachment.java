package de.xwic.etlgine.mail.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import javax.activation.MimetypesFileTypeMap;

import org.apache.poi.util.IOUtils;

import de.xwic.etlgine.mail.IAttachment;

public class EmailAttachment implements IAttachment {

	private File file = null;

	public EmailAttachment(File file) {
		this.file = file;
	}

	@Override
	public byte[] getData() {
		InputStream fis = null;
		try {
			fis=new FileInputStream(file.getPath());
			return IOUtils.toByteArray(fis);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if (null != fis){
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	@Override
	public String getContentType() {
		return new MimetypesFileTypeMap().getContentType(file);
	}

	@Override
	public String getFileName() {
		return file.getName();
	}
}
