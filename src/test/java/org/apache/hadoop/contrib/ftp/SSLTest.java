package org.apache.hadoop.contrib.ftp;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPSClient;
import org.apache.commons.net.util.TrustManagerUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SSLTest {

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
		FTPSClient ftps = new FTPSClient(true);
		
		ftps.setTrustManager(TrustManagerUtils.getAcceptAllTrustManager());

		
		FTPClientConfig config = new FTPClientConfig();

		ftps.configure(config);

		try {
			int reply;
			String server = "localhost";
			int port = FTPConfig.getSslPort();
			ftps.connect(server, port);
			
			assertEquals(220, ftps.getReplyCode());
			
			ftps.login(FTPConfig.getTestUser(), FTPConfig.getTestPassword());
			
			assertEquals(230, ftps.getReplyCode());

			// After connection attempt, you should check the reply code to verify
			// success.
			reply = ftps.getReplyCode();

			if(!FTPReply.isPositiveCompletion(reply)) {
				ftps.disconnect();
			}
			
			
			ftps.logout();
			
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			if(ftps.isConnected()) {
				try {
					ftps.disconnect();
				} catch(IOException ioe) {
					// do nothing
				}
			}
			
		}
	}

}
