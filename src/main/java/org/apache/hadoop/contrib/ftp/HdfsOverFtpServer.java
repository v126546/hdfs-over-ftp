package org.apache.hadoop.contrib.ftp;

import java.io.File;

import org.apache.ftpserver.DataConnectionConfigurationFactory;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.command.CommandFactoryFactory;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.message.MessageResourceFactory;
import org.apache.ftpserver.ssl.SslConfigurationFactory;
import org.apache.hadoop.contrib.ftp.commands.MLSDextend;
import org.apache.hadoop.contrib.ftp.commands.MLSTextend;
import org.apache.hadoop.contrib.ftp.commands.OPTSextend;
import org.apache.hadoop.contrib.ftp.commands.SITEextend;
import org.apache.log4j.Logger;

/**
 * Start-up class of FTP server
 */
public class HdfsOverFtpServer {

	private static Logger log = Logger.getLogger(HdfsOverFtpServer.class);
	
	private static FtpServer server = null;

	public static void main(String[] args) throws Exception {
		
		FTPConfig.init("hdfs-over-ftp.properties");
		startServer();
	}
	
	
	/**
	 * Starts FTP server
	 *
	 * @throws Exception
	 */


	public static void startServer() throws Exception 
	{
		
		
		log.info("\n###############################################################################################\n"
		+"################# Starting Hdfs-Over-Ftp server. version "+ HdfsOverFtpServer.class.getPackage().getImplementationVersion() + " ##################################\n"
		+"################# port: " + FTPConfig.getPort() +" data-ports: " + FTPConfig.getPassivePorts() +" ############################################\n"
		+"###############################################################################################");

		System.out.println("Starting Hdfs-Over-Ftp server. Version: "+ HdfsOverFtpServer.class.getPackage().getImplementationVersion());
		
		HdfsOverFtpSystem.setCore_site_path(FTPConfig.getCore_site_path());
		HdfsOverFtpSystem.setHdfs_site_path(FTPConfig.getHdfs_site_path());
		
		FtpServerFactory serverFactory = new FtpServerFactory();
		
		String listener = "default";
		

		if (FTPConfig.getPort() != 0)
		{
			ListenerFactory factory = new ListenerFactory();
			
			DataConnectionConfigurationFactory dccFactory = new DataConnectionConfigurationFactory();
			dccFactory.setPassivePorts(FTPConfig.getPassivePorts());
			
			factory.setDataConnectionConfiguration(dccFactory.createDataConnectionConfiguration());
	
			// set the port of the listener
			factory.setPort(FTPConfig.getPort());
			// replace the default listener
			serverFactory.addListener(listener, factory.createListener());
			// if another listener is added it will be called ssl
			listener = "ssl";
		}
		
		
		
		if (FTPConfig.getSslPort() != 0)
		{
		
			ListenerFactory sslFactory = new ListenerFactory();
			
			DataConnectionConfigurationFactory dccSSlFactory = new DataConnectionConfigurationFactory();
			dccSSlFactory.setPassivePorts(FTPConfig.getSslPassivePorts());
			
			sslFactory.setDataConnectionConfiguration(dccSSlFactory.createDataConnectionConfiguration());
	
			// set the port of the listener
			sslFactory.setPort(FTPConfig.getSslPort());
			
			
			// define SSL configuration
			SslConfigurationFactory ssl = new SslConfigurationFactory();
			File sslKey = new File(FTPConfig.getKeystore());
			ssl.setKeystoreFile(sslKey);
			ssl.setKeystorePassword(FTPConfig.getKeystorePass());
	
			// set the SSL configuration for the listener
			sslFactory.setSslConfiguration(ssl.createSslConfiguration());
			sslFactory.setImplicitSsl(true);
			
			serverFactory.addListener(listener, sslFactory.createListener());
		}
		
		
		
		
		MessageResourceFactory msgResFactory = new MessageResourceFactory();
		msgResFactory.setCustomMessageDirectory(new File("./src/main/resources"));
		
		//System.setProperty("hadoop.home.dir", "\\");
		
		
		KerberosUserManager userManager = new KerberosUserManager("admin"); 
		
		 
		serverFactory.setUserManager(userManager);
		serverFactory.setMessageResource(msgResFactory.createMessageResource());
		
		serverFactory.setFileSystem(new HdfsFileSystemManager());
		
		
		CommandFactoryFactory cmFact = new CommandFactoryFactory();
		cmFact.setUseDefaultCommands(true);
		cmFact.addCommand("MLST", new MLSTextend());
		cmFact.addCommand("MLSD", new MLSDextend());
		cmFact.addCommand("OPTS", new OPTSextend());
		cmFact.addCommand("SITE", new SITEextend());
		serverFactory.setCommandFactory(cmFact.createCommandFactory());
		
		
		// start the server
		server = serverFactory.createServer();
		
		server.start();
	}
	
	public static void stopServer() {
		server.stop();
	}
	

}
