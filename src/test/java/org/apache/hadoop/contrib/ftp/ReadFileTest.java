package org.apache.hadoop.contrib.ftp;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.contrib.ftp.utils.RandomInputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReadFileTest {
	
	
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
	public void readRelativeFile() throws IOException {
		
		String testFile = "DAT1000rel.random";
		
		ftp.changeWorkingDirectory(FTPConfig.getTestHdfsDir());
		assertEquals(250, ftp.getReplyCode());
		
		String testDirRelative = "hdfs_over_ftp_unittest_"+new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Timestamp(System.currentTimeMillis())); 
		ftp.makeDirectory(testDirRelative);
		
		ftp.changeWorkingDirectory(testDirRelative);
		
		ftp.storeFile(testFile, new RandomInputStream(1000) );
		
		int statResult = ftp.stat(testFile);
		
		// check if file exists
		assertEquals(213, statResult);
		
		ByteArrayOutputStream out= new ByteArrayOutputStream();
		
		boolean ReadResult = ftp.retrieveFile(testFile, out);
		
		assertEquals(true, ReadResult);
		
		assertEquals(1000, out.size());
		
		
		ftp.deleteFile(testFile);
		
		statResult = ftp.stat(testFile);
		
		// check if file is deleted
		assertEquals(450, statResult);
		
		ftp.changeWorkingDirectory("..");
		
		ftp.removeDirectory(testDirRelative);
		
		statResult = ftp.stat(testDirRelative);
		// check if folder is deleted
		assertEquals(450, statResult);
		
	}
	
	
	@Test
	public void readNoPermFile() throws IOException {
		// File with 000 permission may be readable wen ranger is used
		if (FTPConfig.getTestRangerUsed() ) return;
		
		String testFile = "DAT1000rel.random";
		
		ftp.changeWorkingDirectory(FTPConfig.getTestHdfsDir());
		assertEquals(250, ftp.getReplyCode());
		
		String testDirRelative = "hdfs_over_ftp_unittest_"+new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Timestamp(System.currentTimeMillis())); 
		ftp.makeDirectory(testDirRelative);
		
		ftp.changeWorkingDirectory(testDirRelative);
		
		ftp.storeFile(testFile, new RandomInputStream(1000) );
		
		int statResult = ftp.stat(testFile);
		
		// check if file exists
		assertEquals(213, statResult);
		
		ftp.sendSiteCommand("CHMOD 000 "+testFile);
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		
		boolean ReadResult = ftp.retrieveFile(testFile, out);
		
		assertEquals(false, ReadResult);
		
		assertEquals(0, out.size());
		
		//boolean chmodResult = ftp.sendSiteCommand("CHMOD 000 "+testFile);
		
		
		ftp.deleteFile(testFile);
		
		statResult = ftp.stat(testFile);
		
		// check if file is deleted
		assertEquals(450, statResult);
		
		ftp.changeWorkingDirectory("..");
		
		ftp.removeDirectory(testDirRelative);
		
		statResult = ftp.stat(testDirRelative);
		// check if folder is deleted
		assertEquals(450, statResult);
		
	}
	
	

}
