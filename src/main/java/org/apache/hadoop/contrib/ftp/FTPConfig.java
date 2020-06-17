package org.apache.hadoop.contrib.ftp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class FTPConfig {
	
	private static Logger log = Logger.getLogger(FTPConfig.class);
	
	private static int port = 0;
	private static String passivePorts = null;
	
	private static int sslPort = 0;
	private static String sslPassivePorts = null;
	
	private static String keystore = null;
	private static String keystorePass = null;
	
	private static String hdfs_site_path = null;
	private static String core_site_path = null;
	
	private static String kerberosRealm = null;
	private static String kerberosKDC = null;
	private static String krb5ConfLocation = null;
	private static Boolean krb5Debug = false;
	
	private static String testUser = null;
	private static String testPassword = null;
	private static String testHdfsDir = "/tmp";
	private static Boolean testRangerUsed = false;

	
	public static void init(String configFile) throws IOException
	{
		init(configFile, null);
	}
	
	public static void init(String configFile, String overwriteConfigFile) throws IOException {
		
		Properties props = new Properties();

		props.load(new FileInputStream(new File(configFile)));
		if (overwriteConfigFile != null && overwriteConfigFile.length() > 0)
		{
			props.load(new FileInputStream(new File(overwriteConfigFile)));
		}

		try {
			port = Integer.parseInt(props.getProperty("port"));
			log.info("port is set. ftp server will be started");
		} catch (Exception e) {
			log.info("port is not set.");
		}


		if (port != 0) {
			passivePorts = props.getProperty("data-ports");
			if (passivePorts == null) {
				log.fatal("data-ports is not set");
				System.exit(1);
			}
		}
		
		
		
		try {
			sslPort = Integer.parseInt(props.getProperty("ssl-port"));
			log.info("port is set. ftp server will be started");
		} catch (Exception e) {
			log.info("ssl port is not set.");
		}


		if (sslPort != 0) {
			
			sslPassivePorts = props.getProperty("ssl-data-ports");
			if (sslPassivePorts == null) {
				log.fatal("ssl data-ports is not set");
				System.exit(1);
			}
			
			keystore = props.getProperty("keystore");
			if (keystore == null) {
				log.fatal("ssl keystore is not set");
				System.exit(1);
			}
			
			keystorePass = props.getProperty("keystore-pass");
			if (keystorePass == null) {
				log.fatal("ssl keystore password is not set");
				System.exit(1);
			}
		}


		
		/* ####################
		 *   Kerberos Config 
		 * #################### */
		
		krb5ConfLocation = props.getProperty("krb5-location");
		kerberosRealm = props.getProperty("realm");
		kerberosKDC = props.getProperty("kdc");
		if (props.getProperty("krb5-debug") != null)
		{
			krb5Debug = props.getProperty("krb5-debug").equalsIgnoreCase("true")?true:false;
		}
		
		if (krb5ConfLocation != null &&
			(kerberosRealm != null || kerberosKDC != null) ) {
			log.warn("realm and kdc is ignored when krb5-location is set");
			kerberosRealm = null;
			kerberosKDC = null;
		}
		else if (krb5ConfLocation == null && 
			(kerberosRealm == null || kerberosKDC == null))
		{
			log.fatal("Either krb5-location or realm + kdc mus be set");
			System.exit(1);
		}
		
		
		/* ####################
		 *    Hadoop Config 
		 * #################### */
		
		core_site_path = props.getProperty("core-site-location");
		if (core_site_path == null) {
			log.fatal("core-site-location is not set or file not found");
			System.exit(1);
		}
		if (!(new File(core_site_path).exists())) {
			log.fatal("File not found: "+core_site_path);
			System.exit(1);
		}
		
		hdfs_site_path = props.getProperty("hdfs-site-location");
		if (hdfs_site_path == null ) {
			log.fatal("hdfs-site-location is not set or file not found");
			System.exit(1);
		}
		if (!(new File(hdfs_site_path).exists()) ){
			log.fatal("File not found: "+hdfs_site_path);
			System.exit(1);
		}
		
		
		/* ####################
		 *   Test User Config  
		 * #################### */
		
		testUser = props.getProperty("test-user");
		testPassword = props.getProperty("test-password");
		
		if (props.getProperty("access-control-system-used") != null)
		{
			testRangerUsed = props.getProperty("access-control-system-used").equalsIgnoreCase("true")?true:false;
		}
		
		if (props.getProperty("test-hdfs-dir") != null)
		{
			testHdfsDir = props.getProperty("test-hdfs-dir");
		}
		
	}
	
	public static String getKerberosServicePrincipal() {
		Configuration conf = new Configuration(); 
		conf.addResource(new Path(core_site_path));
		conf.addResource(new Path(hdfs_site_path));
		
		
		Iterator<Map.Entry<String,String>> kvItr = conf.iterator();
		while (kvItr.hasNext()) {
			Map.Entry<String,String> entry = kvItr.next();
			String key = entry.getKey();
			log.info(key + " = " +entry.getValue());
		}
		
		String principal = conf.get("dfs.namenode.kerberos.principal");
		return principal;
	}

	public static int getPort() {
		return port;
	}
	
	public static String getKrb5ConfLocation() {
		return krb5ConfLocation;
	}
	
	public static String getKerberosRealm() {
		return kerberosRealm;
	}

	public static String getKerberosKDC() {
		return kerberosKDC;
	}

	public static String getHdfs_site_path() {
		return hdfs_site_path;
	}

	public static String getCore_site_path() {
		return core_site_path;
	}

	public static String getPassivePorts() {
		return passivePorts;
	}

	public static Boolean getKrb5Debug() {
		return krb5Debug;
	}

	public static String getTestUser() {
		return testUser;
	}

	public static String getTestPassword() {
		return testPassword;
	}

	public static Boolean getTestRangerUsed() {
		return testRangerUsed;
	}

	public static String getTestHdfsDir() {
		return testHdfsDir;
	}

	public static String getKeystore() {
		return keystore;
	}

	public static String getKeystorePass() {
		return keystorePass;
	}

	public static int getSslPort() {
		return sslPort;
	}

	public static String getSslPassivePorts() {
		return sslPassivePorts;
	}



	
}
