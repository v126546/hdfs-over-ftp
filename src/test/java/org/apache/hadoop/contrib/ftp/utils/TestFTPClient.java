package org.apache.hadoop.contrib.ftp.utils;

import java.io.IOException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.contrib.ftp.FTPConfig;
import org.apache.hadoop.contrib.ftp.HdfsOverFtpServer;
import org.apache.log4j.Logger;

public class TestFTPClient {

	private static Logger log = Logger.getLogger(TestFTPClient.class);
	
	static FTPClient ftpClient;
	static String server = "localhost";
	
	public static FTPClient connectClient() throws Exception
	{
		ftpClient = new FTPClient();
		FTPClientConfig config = new FTPClientConfig();

		ftpClient.configure(config);

	
		int reply;
		
		int port = FTPConfig.getPort();
		log.info("Connection to "+server+":"+port);
		ftpClient.connect(server, port);
		log.info("Client connected");
		log.info(ftpClient.getReplyString());
		
		
		ftpClient.login(FTPConfig.getTestUser(), FTPConfig.getTestPassword());
		log.info(ftpClient.getReplyString());
		

		// After connection attempt, you should check the reply code to verify
		// success.
		reply = ftpClient.getReplyCode();

		if(!FTPReply.isPositiveCompletion(reply)) {
			ftpClient.disconnect();
			log.fatal("FTP server refused connection.");
		}
		
		return ftpClient;
	}
	
	
	public static void disconnectClient() throws IOException
	{
		ftpClient.logout();
		log.info("Client disconnected");
	}


	public static String getFtpUser() {
		return FTPConfig.getTestUser();
	}

}
