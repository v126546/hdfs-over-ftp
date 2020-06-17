package org.apache.hadoop.contrib.ftp;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.contrib.ftp.utils.RandomInputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AppendFileTest {
	
	
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
	public void appendRelativeFile() throws IOException {
		
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
		
		ftp.appendFile(testFile, new RandomInputStream(4000));
		
		FTPFile mlstResult = ftp.mlistFile(testFile);
		//assertEquals(200, mlstResult);
		assertEquals(250, ftp.getReplyCode());
		
		assertEquals(5000, mlstResult.getSize() );
		
	}
	
	
	
}
