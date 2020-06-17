package org.apache.hadoop.contrib.ftp;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPReply;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class LoginTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		AllTests.setUp();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		AllTests.tearDown();
	}

	@Test
	public void login() {
		FTPClient ftp = new FTPClient();
		FTPClientConfig config = new FTPClientConfig();

		ftp.configure(config);

		try {
			int reply;
			String server = "localhost";
			int port = FTPConfig.getPort();
			ftp.connect(server, port);
			
			assertEquals(220, ftp.getReplyCode());
			
			ftp.login(FTPConfig.getTestUser(), FTPConfig.getTestPassword());
			
			assertEquals(230, ftp.getReplyCode());

			// After connection attempt, you should check the reply code to verify
			// success.
			reply = ftp.getReplyCode();

			if(!FTPReply.isPositiveCompletion(reply)) {
				ftp.disconnect();
			}
			
			
			ftp.logout();
			
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			if(ftp.isConnected()) {
				try {
					ftp.disconnect();
				} catch(IOException ioe) {
					// do nothing
				}
			}
			
		}
	}

}
