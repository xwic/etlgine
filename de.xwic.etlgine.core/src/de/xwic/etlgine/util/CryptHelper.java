/*
 * Copyright (c) 2009 Network Appliance, Inc.
 * All rights reserved.
 */

package de.xwic.etlgine.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

/**
 * This little tool is used to encrypt/decrypt username and password informations
 * stored in config files. It only provides a little security as it can be hacked
 * easily by analyzing the code, but it is better then storing it unencrypted
 * and readable by everyone via Notepad...
 * 
 * @author lippisch
 */
public class CryptHelper {

	private static String algorithm = "DESede";
	
	private final static byte[] MYKEY = new byte[] {
		-84, -19, 0, 5, 115, 114, 0, 20, 106, 97, 118, 97, 46, 115, 101, 99, 117, 114, 105, 116, 
		121, 46, 75, 101, 121, 82, 101, 112, -67, -7, 79, -77, -120, -102, -91, 67, 2, 0, 4, 76, 
		0, 9, 97, 108, 103, 111, 114, 105, 116, 104, 109, 116, 0, 18, 76, 106, 97, 118, 97, 47, 
		108, 97, 110, 103, 47, 83, 116, 114, 105, 110, 103, 59, 91, 0, 7, 101, 110, 99, 111, 100, 
		101, 100, 116, 0, 2, 91, 66, 76, 0, 6, 102, 111, 114, 109, 97, 116, 113, 0, 126, 0, 
		1, 76, 0, 4, 116, 121, 112, 101, 116, 0, 27, 76, 106, 97, 118, 97, 47, 115, 101, 99, 
		117, 114, 105, 116, 121, 47, 75, 101, 121, 82, 101, 112, 36, 84, 121, 112, 101, 59, 120, 112, 
		116, 0, 6, 68, 69, 83, 101, 100, 101, 117, 114, 0, 2, 91, 66, -84, -13, 23, -8, 6, 
		8, 84, -32, 2, 0, 0, 120, 112, 0, 0, 0, 24, -119, 69, 117, -68, 87, -104, -70, 37, 
		4, -53, 13, 109, -56, -29, 21, 2, 91, -94, 122, 98, 107, -85, 37, 87, 116, 0, 3, 82, 
		65, 87, 126, 114, 0, 25, 106, 97, 118, 97, 46, 115, 101, 99, 117, 114, 105, 116, 121, 46, 
		75, 101, 121, 82, 101, 112, 36, 84, 121, 112, 101, 0, 0, 0, 0, 0, 0, 0, 0, 18, 
		0, 0, 120, 114, 0, 14, 106, 97, 118, 97, 46, 108, 97, 110, 103, 46, 69, 110, 117, 109, 
		0, 0, 0, 0, 0, 0, 0, 0, 18, 0, 0, 120, 112, 116, 0, 6, 83, 69, 67, 82, 
		69, 84
	};
	
	/**
	 * Create a new key string.
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 */
	public static String createNewKey() throws NoSuchAlgorithmException, IOException {
		
		SecretKey newKey = KeyGenerator.getInstance(algorithm).generateKey();
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(newKey);
		
		// now create string
		int size = bos.size();
		byte[] data = bos.toByteArray();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < size; i++) {
			Byte b = new Byte(data[i]);
			if (i > 0) {
				sb.append(", ");
				if (i % 20 == 0) {
					sb.append("\n");
				}
			}
			sb.append(b.byteValue());
		}
		
		return sb.toString();
				
	}

	/**
	 * Read my internal key.
	 * @return
	 */
	private static SecretKey getMyKey() {
		
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(MYKEY);
			ObjectInputStream ois = new ObjectInputStream(bis);
			return (SecretKey)ois.readObject();
		} catch (Exception e) {
			throw new RuntimeException("Error reading my key.", e);
		}
		
	}

	/**
	 * Encrypt a string into a hex encoded string.
	 * @param string
	 * @return
	 */
	public static String encryptToString(String string) {
		
		byte[] data = encrypt(string);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < data.length; i++) {
			int val = data[i];
			if (val < 0) {
				val = -val + 128;
			}
			String hex = "0" + Integer.toHexString(val);
			if (hex.length() > 2) {
				hex = hex.substring(1);
			}
			sb.append(hex);
			
		}
		return sb.toString();
		
	}
	
	/**
	 * Decrypt a string from an encoded string.
	 * @param encodedString
	 * @return
	 */
	public static String decryptFromString(String encodedString) {
		
		byte[] data = new byte[encodedString.length() / 2];
		for (int i = 0; i < data.length; i++) {
			String piece = encodedString.substring(i * 2, (i * 2) + 2);
			int value = Integer.parseInt(piece, 16);
			if (value > 128) {
				value = -(value - 128);
			}
			data[i] = new Integer(value).byteValue();
			
		}
		return decrypt(data);
		
	}
	
	/**
	 * Encrypt a string into bytes.
	 * @param string
	 * @return
	 */
	public static byte[] encrypt(String string)  {
		
		try {
			Cipher cipher = Cipher.getInstance(algorithm);
			cipher.init(Cipher.ENCRYPT_MODE, getMyKey());
			return cipher.doFinal(string.getBytes());
		} catch (Exception e) {
			throw new RuntimeException("Error encrypting string!", e);
		}
		
	}

	/**
	 * Decrypt an array into a string.
	 * @param string
	 * @return
	 */
	public static String decrypt(byte[] data)  {
		
		try {
			Cipher cipher = Cipher.getInstance(algorithm);
			cipher.init(Cipher.DECRYPT_MODE, getMyKey());
			return new String(cipher.doFinal(data));
		} catch (Exception e) {
			throw new RuntimeException("Error decrypting string!", e);
		}
		
	}

	
}
