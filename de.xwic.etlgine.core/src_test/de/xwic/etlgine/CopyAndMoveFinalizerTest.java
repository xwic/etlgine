package de.xwic.etlgine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import de.xwic.etlgine.extractor.jdbc.JDBCSource;
import de.xwic.etlgine.finalizer.CopyAndMoveFinalizer;
import de.xwic.etlgine.impl.ETLProcess;
import de.xwic.etlgine.sources.FileSource;
import de.xwic.etlgine.util.FileUtils;



@RunWith(MockitoJUnitRunner.class)
public class CopyAndMoveFinalizerTest {
	
	private String destinationToCopy = null;
	

	/**
	 * A mock/stub (fake) object that will be used with the transformer
	 */
	@Mock
	private IProcessContext processContext;
	
	private IETLProcess createProcess() {
		IETLProcess testProc = new ETLProcess("test");
		Mockito.when(processContext.getMonitor()).thenReturn(IMonitor.Empty);
		Mockito.when(processContext.getProcess()).thenReturn(testProc);
		return testProc;
	}
	
	@Before
	public void setup(){
		destinationToCopy = null;
	}
	
	@After
	public void cleanup(){
		if (null != destinationToCopy){
			FileUtils.deleteEntireFolder(new File(destinationToCopy));
		}
	}
	
	@Test(expected=ETLException.class)
	public void invalidNullServerKey() throws ETLException{
		new CopyAndMoveFinalizer(null,true);
	}
	
	@Test
	public void invalidEmptyServerKey() {
		try {
			new CopyAndMoveFinalizer("",true);
			fail("ETLException should have been thrown");
		} catch (ETLException e) {
			assertEquals("ServerKey should not be empty or null",e.getMessage());
		}
	}
	
	@Test
	public void invalidEmptyDestToCopy() {
		try {
			new CopyAndMoveFinalizer("","s",true);
			fail("ETLException should have been thrown");
		} catch (ETLException e) {
			assertEquals("The copy destination should not be empty or null",e.getMessage());
		}
	}
	
	@Test
	public void invalidEmptyDestToMove() throws ETLException, IOException {
		
		IETLProcess testProc = createProcess();
		File source = File.createTempFile("test", ".tmp");
		testProc.addSource(new FileSource(source));
		
		CopyAndMoveFinalizer transf = new CopyAndMoveFinalizer("s","",false);
		try {
			transf.onFinish(processContext);
			fail("ETLException should have been thrown");
		} catch (ETLException e) {
			assertEquals("The move destination should not be empty or null",e.getMessage());
		}
	}
	
	@Test
	public void invalidEmptyDestToCopy1() throws IOException {
		
		File source = File.createTempFile("test", ".tmp");
		String sourcePath = source.getAbsolutePath();
		
		try {
			new CopyAndMoveFinalizer(sourcePath,"","s",true);
			fail("ETLException should have been thrown");
		} catch (ETLException e) {
			assertEquals("The copy destination should not be empty or null",e.getMessage());
		}
	}
	
	@Test
	public void invalidSourcePath() throws IOException {
		
		try {
			new CopyAndMoveFinalizer("a","b","c",true);
			fail("ETLException should have been thrown");
		} catch (ETLException e) {
			assertEquals("The sourcePath does not exist" + "a",e.getMessage());
		}
	}
	
	@Test
	public void invalidSourcePathServerKey() throws IOException, ETLException {
		
		IETLProcess testProc = createProcess();
		String serverKey = "tpm_copy2_files";
		String source = " ";
		
		Mockito.when(processContext.getProperty(serverKey + ".source.path")).thenReturn(source);
		Mockito.when(processContext.getProperty(serverKey + ".dest.path1")).thenReturn(source + "FolderToCopy\\");
		Mockito.when(processContext.getProperty(serverKey + ".dest.path2")).thenReturn(source + "FolderToMove\\");
		
		CopyAndMoveFinalizer transf = new CopyAndMoveFinalizer(serverKey,true);
		try {
			transf.onFinish(processContext);
			fail("ETLException should have been thrown");
		} catch (ETLException e) {
			assertEquals("The sourcePath does not exist" + " ",e.getMessage());
		}
	}
	
	@Test
	public void invalidSourcePathNoSource() throws IOException, ETLException {
		
		IETLProcess testProc = createProcess();
		String source = "s:/x";
		testProc.addSource(new FileSource(source));
		
		String destToCopy = "FolderToCopy"+File.separator;
		String destToMove = "FolderToMove"+File.separator;
		
		CopyAndMoveFinalizer transf = new CopyAndMoveFinalizer(destToCopy,destToMove,true);
		
		try {
			transf.onFinish(processContext);
			fail("ETLException should have been thrown");
		}
		catch(ETLException e){
			assertEquals("The sourcePath does not exist" + "null",e.getMessage());
		}
	}
	
	@Test
	public void invalidEmptyDestToMove1() throws IOException, ETLException {
		
		IETLProcess testProc = createProcess();
		File source = File.createTempFile("test", ".tmp");
		String sourcePath = source.getAbsolutePath();
		String destToCopy = "V:\\abbsdd:xa\\";
		
		CopyAndMoveFinalizer transf = new CopyAndMoveFinalizer(sourcePath,destToCopy,"",true); 
		try {
			transf.onFinish(processContext);
			fail("ETLException should have been thrown");
		} catch (ETLException e) {
			assertEquals("The file could not be copied: " + destToCopy + source.getName(),e.getMessage());
		}
	}
	
	@Test
	public void CopyAndMoveTransformerVersionOneNoSource() throws IOException, ETLException {
		
		IETLProcess testProc = createProcess();
		File source = File.createTempFile("test", ".tmp");
		testProc.addSource(new FileSource(source));
		
		String sourcePath = source.getAbsolutePath();
		String sourceName = source.getName();
		String destToCopy = sourcePath + "FolderToCopy"+File.separator;
		String destToMove = sourcePath + "FolderToMove"+File.separator;
		long length = source.length();
		
		CopyAndMoveFinalizer transf = new CopyAndMoveFinalizer(destToCopy, destToMove, true);
		transf.onFinish(processContext);
		assertTrue("The file is not copied in the destination path",new File(destToCopy + sourceName).exists());
		assertEquals("The source file and the copied file are not the same size;",length,new File(destToCopy + sourceName).length());
		assertTrue("The file is not moved in the destination path "+destToMove + sourceName,new File(destToMove + sourceName).exists());
		assertFalse("The file is not moved from the source path",new File(sourcePath + sourceName).exists());
		
		destinationToCopy = destToCopy + sourceName;
		FileUtils.deleteEntireFolder(new File(destToMove + sourceName));
	}

	@Test
	public void CopyAndMoveTransformerVersionOneNoSourceFlagFalse() throws IOException, ETLException {
		
		IETLProcess testProc = createProcess();
		File source = File.createTempFile("test", ".tmp");
		testProc.addSource(new FileSource(source));
		
		String sourcePath = source.getAbsolutePath();
		String sourceName = source.getName();
		String destToCopy = sourcePath + "FolderToCopy"+File.separator;
		long length = source.length();
		
		CopyAndMoveFinalizer transf = new CopyAndMoveFinalizer(destToCopy,"", true);
		transf.onFinish(processContext);
		assertTrue("The file is not copied in the destination path",new File(destToCopy + sourceName).exists());
		assertEquals("The source file and the copied file are not the same size;",length,new File(destToCopy + sourceName).length());
		
		destinationToCopy = destToCopy + sourceName; 
	}
	
	@Test
	public void CopyAndMoveTransformerVersionOneCopyOnErrorTrue() throws IOException, ETLException {
		
		IETLProcess testProc = createProcess();
		File source = File.createTempFile("test", ".tmp");
		testProc.addSource(new FileSource(source));
		
		String sourcePath = source.getAbsolutePath();
		String sourceName = source.getName();
		String destToCopy = sourcePath + "FolderToCopy"+File.separator;
		String destToMove = sourcePath + "FolderToMove"+File.separator;
		long length = source.length();
		
		CopyAndMoveFinalizer transf = new CopyAndMoveFinalizer(destToCopy,destToMove, true);
		transf.setCopyOnError(true);
		transf.onFinish(processContext);
		assertTrue("The file is not copied in the destination path",new File(destToCopy + sourceName).exists());
		assertEquals("The source file and the copied file are not the same size;",length,new File(destToCopy + sourceName).length());
		assertTrue("The file is not moved in the destination path "+destToMove + sourceName,new File(destToMove + sourceName).exists());
		assertFalse("The file is not moved from the source path",new File(sourcePath + sourceName).exists());
		
		destinationToCopy = destToCopy + sourceName;
		FileUtils.deleteEntireFolder(new File(destToMove + sourceName)); 
	}
	
	@Test
	public void CopyAndMoveTransformerVersionOneCopyOnError() throws IOException, ETLException {
		
		IETLProcess testProc = createProcess();
		File source = File.createTempFile("test", ".tmp");
		testProc.addSource(new FileSource(source));
		
		String sourcePath = source.getAbsolutePath();
		String sourceName = source.getName();
		String destToCopy = sourcePath + "FolderToCopy"+File.separator;
		String destToMove = sourcePath + "FolderToMove"+File.separator;
		long length = source.length();
		
		Mockito.when(processContext.getResult()).thenReturn(Result.FAILED);
		
		CopyAndMoveFinalizer transf = new CopyAndMoveFinalizer(destToCopy,destToMove, true);
		transf.onFinish(processContext);
	}
	
	@Test
	public void CopyAndMoveTransformerVersionOneNullSource() throws ETLException{
		
		IETLProcess testProc = createProcess();
		JDBCSource source = new JDBCSource();
		testProc.addSource(source);
		
		CopyAndMoveFinalizer transf = new CopyAndMoveFinalizer("s","s",true);
		try {
			transf.onFinish(processContext); 
		}
		catch(ETLException e){
			assertEquals("There is no FileSource" + source.getName(),e.getMessage());
		}

	}
	
	@Test
	public void CopyAndMoveTransformerVersionOneInvalidDestToCopy() throws ETLException,IOException {
		
		File source = File.createTempFile("test",".tmp");
		String sourcePath = source.getAbsolutePath();
		IETLProcess testProc = createProcess();
		 
		CopyAndMoveFinalizer transf = new CopyAndMoveFinalizer(sourcePath,"V:\\abbsdd:xa\\","s",true);
		try {
			transf.onFinish(processContext);
		}
		catch(ETLException e) {
			assertEquals("The file could not be copied: " + "V:\\abbsdd:xa\\" + source.getName(),e.getMessage());
		}
	}
	
	@Test
	public void CopyAndMoveTransformerVersionOneServerKey() throws ETLException, IOException{
		IETLProcess testProc = createProcess();
		File source = File.createTempFile("test",".tmp");
		String serverKey = "tpm_copy2_files";
		
		Mockito.when(processContext.getProperty(serverKey + ".source.path")).thenReturn(source.getAbsolutePath());
		Mockito.when(processContext.getProperty(serverKey + ".dest.path1")).thenReturn(source.getAbsolutePath() + "FolderToCopy"+File.separator);
		Mockito.when(processContext.getProperty(serverKey + ".dest.path2")).thenReturn(source.getAbsolutePath() + "FolderToMove"+File.separator);
		
		String sourcePath = processContext.getProperty(serverKey + ".source.path");
		String destToCopy = processContext.getProperty(serverKey + ".dest.path1");
		String destToMove = processContext.getProperty(serverKey + ".dest.path2");
		String sourceName = source.getName();
		long length = source.length();
		
		CopyAndMoveFinalizer transf = new CopyAndMoveFinalizer(serverKey,true);
		transf.onFinish(processContext);
		assertTrue("The file is not copied in the destination path",new File(destToCopy + sourceName).exists());
		assertEquals("The source file and the copied file are not the same size;",length,new File(destToCopy + sourceName).length());
		assertTrue("The file is not moved in the destination path "+destToMove + sourceName,new File(destToMove + sourceName).exists());
		assertFalse("The file is not moved from the source path",new File(sourcePath + sourceName).exists());
		
		destinationToCopy = destToCopy + sourceName;
		FileUtils.deleteEntireFolder(new File(destToMove + sourceName));
	}
	
	@Test
	public void invalidEmptySourcePath() {
		try {
			new CopyAndMoveFinalizer("","D:\\","D:\\",true);
			fail("ETLException should have been thrown");
		} catch (ETLException e) {
			assertEquals("SourcePath should not be empty or null",e.getMessage());
		}
	}
	
	@Test
	public void copyAndMoveSuccessfully() throws ETLException, IOException{
		
		IETLProcess testProc = createProcess();
		File source = File.createTempFile("test",".tmp");
		String sourcePath = source.getAbsolutePath();
		String sourceName = source.getName();
		String destToCopy = sourcePath + "FolderToCopy"+File.separator;
		String destToMove = sourcePath + "FolderToMove"+File.separator;
		long length = source.length();
		
		CopyAndMoveFinalizer transf = new CopyAndMoveFinalizer(sourcePath,destToCopy, destToMove, true);
		transf.onFinish(processContext);
		assertTrue("The file is not copied in the destination path",new File(destToCopy + sourceName).exists());
		assertEquals("The source file and the copied file are not the same size;",length,new File(destToCopy + sourceName).length());
		assertTrue("The file is not moved in the destination path "+destToMove + sourceName,new File(destToMove + sourceName).exists());
		assertFalse("The file is not moved from the source path",new File(sourcePath + sourceName).exists());
		
		destinationToCopy = destToCopy + sourceName;
		FileUtils.deleteEntireFolder(new File(destToMove + sourceName));
		
	}
	
	@Test
	public void copyAndMoveSuccessfullyFlagFalse() throws ETLException, IOException{
		
		IETLProcess testProc = createProcess();
		File source = File.createTempFile("test",".tmp");
		String sourcePath = source.getAbsolutePath();
		String sourceName = source.getName();
		String destToCopy = sourcePath + "FolderToCopy"+File.separator;
		String destToMove = sourcePath + "FolderToMove"+File.separator;
		CopyAndMoveFinalizer transf = new CopyAndMoveFinalizer(sourcePath,destToCopy, destToMove, false);
		transf.onFinish(processContext);
		
		assertFalse("The file is not copied in the copy destination path",new File(destToCopy + sourceName).exists());
		assertTrue("The file is not moved in the destination path",new File(destToMove + sourceName).exists());
		assertFalse("The file is not moved from the source path",new File(sourcePath + sourceName).exists());
		
		FileUtils.deleteEntireFolder(new File(destToMove + sourceName));
	
	}
	
}
