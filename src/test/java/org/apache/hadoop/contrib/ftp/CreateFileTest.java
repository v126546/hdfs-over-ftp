package org.apache.hadoop.contrib.ftp;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.contrib.ftp.utils.RandomInputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateFileTest {
	
	
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
	public void createRelativeFile() throws IOException {
		
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
	public void createAbsoluteFile() throws IOException {
		
		
		String testDirRelative = FTPConfig.getTestHdfsDir()+"/hdfs_over_ftp_unittest_"+new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Timestamp(System.currentTimeMillis()));
		String testFile = testDirRelative+"/DAT1000abs.random";
		
		ftp.makeDirectory(testDirRelative);
		
		ftp.storeFile(testFile, new RandomInputStream(1000) );
		
		int statResult = ftp.stat(testFile);
		
		// check if file exists
		assertEquals(213, statResult);
		
		ftp.deleteFile(testFile);
		
		statResult = ftp.stat(testFile);
		
		// check if file is deleted
		assertEquals(450, statResult);
		
		
		ftp.removeDirectory(testDirRelative);
		
		statResult = ftp.stat(testDirRelative);
		// check if folder is deleted
		assertEquals(450, statResult);
		
	}
	
	@Test
	public void createMixedFile() throws IOException {
		
		String testFile = "DAT1000rel.random";
		
		ftp.changeWorkingDirectory(FTPConfig.getTestHdfsDir());
		assertEquals(250, ftp.getReplyCode());
		String testDirRelative = "hdfs_over_ftp_unittest_"+new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Timestamp(System.currentTimeMillis()));
		String absTestDirRelative = FTPConfig.getTestHdfsDir()+"/"+testDirRelative;
		String absTestFile = absTestDirRelative+"/"+testFile;
		ftp.makeDirectory(testDirRelative);
		
		ftp.changeWorkingDirectory(testDirRelative);
		
		ftp.storeFile(testFile, new RandomInputStream(1000) );
		
		int statResult = ftp.stat(absTestFile);
		
		// check if file exists
		assertEquals(213, statResult);
		
		ftp.deleteFile(testFile);
		
		statResult = ftp.stat(absTestFile);
		
		// check if file is deleted
		assertEquals(450, statResult);
		
		ftp.changeWorkingDirectory("..");
		
		ftp.removeDirectory(testDirRelative);
		
		statResult = ftp.stat(absTestDirRelative);
		// check if folder is deleted
		assertEquals(450, statResult);
		
	}

}
