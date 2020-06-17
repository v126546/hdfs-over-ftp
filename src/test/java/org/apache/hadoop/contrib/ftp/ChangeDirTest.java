package org.apache.hadoop.contrib.ftp;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.commons.net.ftp.FTPClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ChangeDirTest {
	
	
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
	public void changeDir() throws IOException {
		
		ftp.changeWorkingDirectory(FTPConfig.getTestHdfsDir());
		assertEquals(250, ftp.getReplyCode());
		
		String testDirRelative = "hdfs_over_ftp_unittest_"+new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Timestamp(System.currentTimeMillis())); 
		ftp.makeDirectory(testDirRelative);
		
		ftp.cwd(testDirRelative);
		
		String dir = ftp.printWorkingDirectory();
		
		assertEquals(FTPConfig.getTestHdfsDir()+"/"+testDirRelative, dir);
	}
	
	
}
