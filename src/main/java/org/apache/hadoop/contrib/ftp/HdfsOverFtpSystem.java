package org.apache.hadoop.contrib.ftp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.ftpserver.ftplet.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to store DFS connection
 */
public class HdfsOverFtpSystem {

	private static FileSystem fs = null;

	
	private static String hdfs_site_path = null;
	private static String core_site_path = null;

	
	private final static Logger log = LoggerFactory.getLogger(HdfsOverFtpSystem.class);

	private static void hdfsInit(User user) throws IOException {
		
		
		Configuration conf = new Configuration(); 

		conf.set("hadoop.security.authentication", "kerberos");
		conf.set("dfs.namenode.kerberos.principal.pattern", "*");
		
		
		conf.addResource(new Path(hdfs_site_path) );
		conf.addResource(new Path(core_site_path) );

		UserGroupInformation.setConfiguration(conf);
		// Subject is taken from current user context
		
		UserGroupInformation.loginUserFromSubject(((HdfsUser)user).getKerberosSubject());

		fs = FileSystem.get(conf);
		
		try {
			String hdfs_uri = conf.get("fs.defaultFS");
			
			log.debug("hdfs_uri: "+hdfs_uri);
			
			fs.initialize(new URI(hdfs_uri), conf);
		} catch (URISyntaxException e) {
			log.error("DFS Initialization error", e);
		}
		((HdfsUser)user).setHdfsFilesystem(fs);
	}

	

	/**
	 * Get dfs
	 *
	 * @return dfs
	 * @throws IOException
	 */
	public static FileSystem getDfs(User user) throws IOException {
		if (((HdfsUser)user).getHdfsFilesystem() == null) {
			hdfsInit(user);
		}
		return ((HdfsUser)user).getHdfsFilesystem();
	}
	
	public static void setHdfs_site_path(String hdfs_site_path) {
		HdfsOverFtpSystem.hdfs_site_path = hdfs_site_path;
	}

	public static void setCore_site_path(String core_site_path) {
		HdfsOverFtpSystem.core_site_path = core_site_path;
	}

}
