package org.apache.hadoop.contrib.ftp;

//import javax.security.auth.login.Configuration;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.contrib.ftp.utils.TestFTPClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ LoginTest.class, SSLTest.class, CreateFolderTest.class, CreateFileTest.class, ReadFileTest.class, ChangeDirTest.class,
		AppendFileTest.class, ChmodFileTest.class, MLSxCmdTest.class })
public class AllTests {
	private static boolean initialized;
	private static FTPClient ftpClient;

	@BeforeClass
	public static void setUp() throws Exception {
		if (initialized)
			return;
		initialized = true;
		
		FTPConfig.init("hdfs-over-ftp.properties", "hdfs-over-ftp-test.properties");
		
		HdfsOverFtpServer.startServer();
		ftpClient = TestFTPClient.connectClient();
		
		ftpClient.makeDirectory(FTPConfig.getTestHdfsDir());
		
	}

	@AfterClass
	public static void tearDown() {
		if (!initialized)
			return;
		initialized = false;
		HdfsOverFtpServer.stopServer();
	}

	public static FTPClient getFtpClient() {
		return ftpClient;
	}
	

}
