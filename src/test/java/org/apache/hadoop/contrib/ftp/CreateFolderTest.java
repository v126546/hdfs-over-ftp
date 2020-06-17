package org.apache.hadoop.contrib.ftp;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.commons.net.ftp.FTPClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateFolderTest {
	
	
	static FTPClient ftp;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		AllTests.setUp();
		ftp = AllTests.getFtpClient();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		AllTests.tearDown();
	}
	@Test
	public void createRelativeDir() throws IOException {
		
		ftp.changeWorkingDirectory(FTPConfig.getTestHdfsDir());
		assertEquals(250, ftp.getReplyCode());
		
		String testDirRelative = "hdfs_over_ftp_unittest_"+new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Timestamp(System.currentTimeMillis())); 
		ftp.makeDirectory(testDirRelative);
		
		int statResult = ftp.stat(testDirRelative);
		// check if folder exists
		assertEquals(212, statResult);
		
		ftp.removeDirectory(testDirRelative);
		
		
		statResult = ftp.stat(testDirRelative);
		// check if folder is deleted
		assertEquals(450, statResult);
		
	}
	
	@Test
	public void createAbsoluteDir() throws IOException {
		String testDirAbsolute = FTPConfig.getTestHdfsDir()+"/hdfs_over_ftp_unittest_"+new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Timestamp(System.currentTimeMillis())); 
		ftp.makeDirectory(testDirAbsolute);
		
		int statResult = ftp.stat(testDirAbsolute);
		// check if folder exists
		assertEquals(212, statResult);
		
		ftp.removeDirectory(testDirAbsolute);
		
		
		statResult = ftp.stat(testDirAbsolute);
		// check if folder is deleted
		assertEquals(450, statResult);
		
	}

}
