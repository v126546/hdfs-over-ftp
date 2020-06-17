package org.apache.hadoop.contrib.ftp;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.contrib.ftp.utils.RandomInputStream;
import org.apache.hadoop.contrib.ftp.utils.TestFTPClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MLSxCmdTest {
	
	
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
	public void mlstCommand() throws IOException {
		
		String testDirAbsolute = FTPConfig.getTestHdfsDir()+"/hdfs_over_ftp_unittest_"+new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Timestamp(System.currentTimeMillis()));
		String testFile = testDirAbsolute+"/DAT1000mlst.random";
		
		ftp.makeDirectory(testDirAbsolute);
		
		ftp.storeFile(testFile, new RandomInputStream(1000) );
		
		int statResult = ftp.stat(testFile);
		
		// check if file exists
		assertEquals(213, statResult);
		
		
		FTPFile mlstResult = ftp.mlistFile(testFile);
		//assertEquals(200, mlstResult);
		assertEquals(250, ftp.getReplyCode());
		
		assertEquals(1000, mlstResult.getSize() );
		assertEquals(TestFTPClient.getFtpUser().toLowerCase(), mlstResult.getUser().toLowerCase());
		
		ftp.deleteFile(testFile);
		
		statResult = ftp.stat(testFile);
		
		// check if file is deleted
		assertEquals(450, statResult);
		
		
		ftp.removeDirectory(testDirAbsolute);
		
		statResult = ftp.stat(testDirAbsolute);
		// check if folder is deleted
		assertEquals(450, statResult);
		
	}
	
	@Test
	public void mlsdCommand() throws IOException {
		
		String testDirAbsolute = FTPConfig.getTestHdfsDir()+"/hdfs_over_ftp_unittest_"+new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Timestamp(System.currentTimeMillis()));
		String testFileA = testDirAbsolute+"/DAT1000mlsdA.random";
		String testFileB = testDirAbsolute+"/DAT2000mlsdB.random";
		
		ftp.makeDirectory(testDirAbsolute);
		
		ftp.storeFile(testFileA, new RandomInputStream(1000) );
		int statResult = ftp.stat(testFileA);
		// check if file exists
		assertEquals(213, statResult);
		
		ftp.storeFile(testFileB, new RandomInputStream(2000) );
		statResult = ftp.stat(testFileB);
		// check if file exists
		assertEquals(213, statResult);
		
		
		FTPFile[] mlstResult = ftp.mlistDir(testDirAbsolute);
		//assertEquals(200, mlstResult);
		assertEquals(226, ftp.getReplyCode());
		
		assertEquals("DAT1000mlsdA.random", mlstResult[0].getName());
		assertEquals(mlstResult[0].getSize(), 1000);
		assertEquals(TestFTPClient.getFtpUser().toLowerCase(), mlstResult[0].getUser().toLowerCase());
		assertEquals("hdfs", mlstResult[0].getGroup()); 
		
		assertEquals("DAT2000mlsdB.random", mlstResult[1].getName());
		assertEquals(2000, mlstResult[1].getSize() );
		assertEquals(TestFTPClient.getFtpUser().toLowerCase(), mlstResult[1].getUser().toLowerCase());
		assertEquals("hdfs", mlstResult[1].getGroup()); 
		
		ftp.deleteFile(testFileA);
		statResult = ftp.stat(testFileA);
		// check if file is deleted
		assertEquals(450, statResult);
		
		ftp.deleteFile(testFileB);
		statResult = ftp.stat(testFileB);
		// check if file is deleted
		assertEquals(450, statResult);
		
		
		ftp.removeDirectory(testDirAbsolute);
		
		statResult = ftp.stat(testDirAbsolute);
		// check if folder is deleted
		assertEquals(450, statResult);
		
	}
	
	//@Test
	public void mlstParseException() throws IOException {
		
		ftp.sendCommand("MLSD test*/abc");
		assertEquals("503 PORT or PASV must be issued first", ftp.getReplyString());
		
		ftp.sendCommand("MLSD test*/abc");
		assertEquals("503 PORT or PASV must be issued first\r\n", ftp.getReplyString());
		
	}

	
	

}
